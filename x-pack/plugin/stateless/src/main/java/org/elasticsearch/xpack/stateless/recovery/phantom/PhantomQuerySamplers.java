/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.recovery.phantom;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.shard.IndexEventListener;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.stateless.cache.SearchRecoveryTimeoutCalculationService;
import org.elasticsearch.xpack.stateless.recovery.phantom.PhantomQueryMetrics.SamplerEnd;
import org.elasticsearch.xpack.stateless.recovery.phantom.PhantomQueryMetrics.SamplingStartOutcome;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiPredicate;
import java.util.function.Consumer;

import static org.elasticsearch.core.Strings.format;

/**
 * The samplers that this node runs for copies of its shards that are recovering elsewhere, keyed by shard and the allocation id of the
 * recovering copy.
 *
 * <p>
 * Nobody collects the sample when the recovering copy fails or goes away, so a sampler ends when the routing table shows that its copy is
 * no longer initializing. The lease covers the cases where the routing table says nothing: no warming wait outlasts
 * {@code recovery_warming_total_timeout_cap}, so after that nobody can still want the sample.
 */
public class PhantomQuerySamplers implements IndexEventListener, ClusterStateListener {

    private static final Logger logger = LogManager.getLogger(PhantomQuerySamplers.class);

    private record Entry(PhantomQuerySampler sampler, Scheduler.Cancellable lease) {}

    private final ThreadPool threadPool;
    private final ClusterService clusterService;
    private final PhantomQueriesSettings settings;
    private final PhantomQueryMetrics metrics;
    private final PhantomQuerySampleBudget nodeBudget;

    // the inner maps are immutable and replaced on change, so that the observer iterates them without locking
    private final ConcurrentMap<ShardId, Map<String, Entry>> entriesByShard = ConcurrentCollections.newConcurrentMap();
    private volatile TimeValue leaseDuration;

    @SuppressWarnings("this-escape") // the gauges are registered last, after every field is assigned
    public PhantomQuerySamplers(
        ThreadPool threadPool,
        ClusterService clusterService,
        PhantomQueriesSettings settings,
        PhantomQueryMetrics metrics
    ) {
        this.threadPool = threadPool;
        this.clusterService = clusterService;
        this.settings = settings;
        this.metrics = metrics;
        this.nodeBudget = new PhantomQuerySampleBudget(settings::maxBytesPerNode);
        clusterService.getClusterSettings()
            .initializeAndWatch(
                SearchRecoveryTimeoutCalculationService.OFFLINE_WARMING_TOTAL_TIMEOUT_CAP_SETTING,
                value -> this.leaseDuration = value
            );
        metrics.registerSamplerGauges(this::samplerCount, nodeBudget::usedInBytes);
    }

    /**
     * Starts sampling for the copy, and says whether the node samples for it afterwards and, if not, why.
     */
    public SamplingStartOutcome startSampling(ShardId shardId, String targetAllocationId) {
        if (settings.enabled() == false) {
            return SamplingStartOutcome.DECLINED_DISABLED;
        }
        final SamplingStartOutcome notInitializing = whyNotInitializingReplica(clusterService.state(), shardId, targetAllocationId);
        if (notInitializing != null) {
            return notInitializing;
        }
        final var alreadyStarted = new AtomicBoolean();
        entriesByShard.compute(shardId, (id, current) -> {
            if (current != null && current.containsKey(targetAllocationId)) {
                // a retried recovery must not lose its sample
                alreadyStarted.set(true);
                return current;
            }
            final var sampler = new PhantomQuerySampler(settings.maxQueriesPerSample(), settings.maxBytesPerSample(), nodeBudget, metrics);
            final Scheduler.Cancellable lease = threadPool.schedule(() -> expire(shardId, sampler), leaseDuration, threadPool.generic());
            final Map<String, Entry> updated = current == null ? new HashMap<>() : new HashMap<>(current);
            updated.put(targetAllocationId, new Entry(sampler, lease));
            logger.debug("{} started sampling for the copy [{}]", shardId, targetAllocationId);
            return Map.copyOf(updated);
        });
        // the copy may have started after the check above and after the cluster state listener last ran, in which case only the lease
        // would end this sampler
        final SamplingStartOutcome gone = whyNotInitializingReplica(clusterService.state(), shardId, targetAllocationId);
        if (gone != null) {
            discard(removeIf(shardId, (allocationId, entry) -> allocationId.equals(targetAllocationId)), SamplerEnd.TARGET_GONE);
            return gone;
        }
        return alreadyStarted.get() ? SamplingStartOutcome.ALREADY_STARTED : SamplingStartOutcome.STARTED;
    }

    public List<EncodedQueryRequest> collect(ShardId shardId, String targetAllocationId) {
        final List<Entry> removed = removeIf(shardId, (allocationId, entry) -> allocationId.equals(targetAllocationId));
        if (removed.isEmpty()) {
            return List.of();
        }
        final Entry entry = removed.get(0);
        entry.lease().cancel();
        logger.debug("{} collected the sample for the copy [{}]", shardId, targetAllocationId);
        final List<EncodedQueryRequest> sample = entry.sampler().collect();
        metrics.recordSamplerEnded(SamplerEnd.COLLECTED);
        // an empty sample is recorded too, so that shards that receive no traffic show up
        metrics.recordSampleCollected(sample.size(), sample.stream().mapToLong(EncodedQueryRequest::sizeInBytes).sum());
        return sample;
    }

    // the observer calls this for every query-phase request of the node, so it has to stay a single map lookup
    boolean isSampling(ShardId shardId) {
        return entriesByShard.containsKey(shardId);
    }

    void forEachSampler(ShardId shardId, Consumer<PhantomQuerySampler> consumer) {
        final Map<String, Entry> entries = entriesByShard.get(shardId);
        if (entries != null) {
            for (Entry entry : entries.values()) {
                consumer.accept(entry.sampler());
            }
        }
    }

    public int samplerCount() {
        return entriesByShard.values().stream().mapToInt(Map::size).sum();
    }

    public long usedInBytes() {
        return nodeBudget.usedInBytes();
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (entriesByShard.isEmpty() || event.routingTableChanged() == false) {
            return;
        }
        final ClusterState state = event.state();
        for (ShardId shardId : entriesByShard.keySet()) {
            discard(
                removeIf(shardId, (allocationId, entry) -> isInitializingReplica(state, shardId, allocationId) == false),
                SamplerEnd.TARGET_GONE
            );
        }
    }

    @Override
    public void afterIndexShardClosed(ShardId shardId, @Nullable IndexShard indexShard, Settings indexSettings) {
        discard(removeIf(shardId, (allocationId, entry) -> true), SamplerEnd.SHARD_CLOSED);
    }

    private void expire(ShardId shardId, PhantomQuerySampler sampler) {
        final List<Entry> removed = removeIf(shardId, (allocationId, entry) -> entry.sampler() == sampler);
        if (removed.isEmpty() == false) {
            logger.debug(() -> format("%s the lease of a sampler expired before the sample was collected", shardId));
        }
        // the lease has fired already, so unlike discard(List, SamplerEnd) there is no lease to cancel
        for (Entry entry : removed) {
            entry.sampler().discard();
            metrics.recordSamplerEnded(SamplerEnd.LEASE_EXPIRED);
        }
    }

    // the caller has to discard or collect the entries that this removes
    private List<Entry> removeIf(ShardId shardId, BiPredicate<String, Entry> predicate) {
        final List<Entry> removed = new ArrayList<>();
        entriesByShard.computeIfPresent(shardId, (id, current) -> {
            final Map<String, Entry> remaining = new HashMap<>();
            current.forEach((allocationId, entry) -> {
                if (predicate.test(allocationId, entry)) {
                    removed.add(entry);
                } else {
                    remaining.put(allocationId, entry);
                }
            });
            return remaining.isEmpty() ? null : Map.copyOf(remaining);
        });
        return removed;
    }

    private void discard(List<Entry> entries, SamplerEnd end) {
        for (Entry entry : entries) {
            entry.lease().cancel();
            entry.sampler().discard();
            metrics.recordSamplerEnded(end);
        }
    }

    static boolean isInitializingReplica(ClusterState state, ShardId shardId, String allocationId) {
        final ShardRouting routing = findReplica(state, shardId, allocationId);
        return routing != null && routing.initializing();
    }

    /**
     * Why the copy is not an initializing replica of the shard, as the outcome that a request to sample for it gets, or {@code null} when
     * it is. A copy that the cluster state does not contain is told from one that it contains, but that cannot be sampled for: the first
     * is what a node that lags behind the requester looks like.
     */
    @Nullable
    static SamplingStartOutcome whyNotInitializingReplica(ClusterState state, ShardId shardId, String allocationId) {
        final ShardRouting routing = findShard(state, shardId, allocationId);
        if (routing == null) {
            return SamplingStartOutcome.DECLINED_TARGET_UNKNOWN;
        }
        return routing.primary() == false && routing.initializing() ? null : SamplingStartOutcome.DECLINED_TARGET_NOT_INITIALIZING;
    }

    // never the primary: in stateless it is on an indexing node, and only replicas are search copies
    @Nullable
    static ShardRouting findReplica(ClusterState state, ShardId shardId, String allocationId) {
        final ShardRouting routing = findShard(state, shardId, allocationId);
        return routing == null || routing.primary() ? null : routing;
    }

    // the copy of the shard with this allocation id, primary or not, or null when the cluster state has none
    @Nullable
    static ShardRouting findShard(ClusterState state, ShardId shardId, String allocationId) {
        final IndexShardRoutingTable shardRoutingTable = findShardRoutingTable(state, shardId);
        return shardRoutingTable == null ? null : shardRoutingTable.getByAllocationId(allocationId);
    }

    @Nullable
    static IndexShardRoutingTable findShardRoutingTable(ClusterState state, ShardId shardId) {
        final ProjectMetadata project = state.metadata().lookupProject(shardId.getIndex()).orElse(null);
        if (project == null) {
            return null;
        }
        final RoutingTable routingTable = state.globalRoutingTable().routingTables().get(project.id());
        if (routingTable == null) {
            return null;
        }
        final IndexRoutingTable indexRoutingTable = routingTable.index(shardId.getIndex());
        return indexRoutingTable == null ? null : indexRoutingTable.shard(shardId.id());
    }
}
