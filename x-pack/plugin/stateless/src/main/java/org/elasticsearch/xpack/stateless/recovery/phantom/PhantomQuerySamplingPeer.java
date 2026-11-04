/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.recovery.phantom;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ChannelActionListener;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.stateless.recovery.phantom.PhantomQueryMetrics.SamplingStartOutcome;
import org.elasticsearch.xpack.stateless.recovery.phantom.PhantomQuerySamplingActions.CollectSampleRequest;
import org.elasticsearch.xpack.stateless.recovery.phantom.PhantomQuerySamplingActions.CollectSampleResponse;
import org.elasticsearch.xpack.stateless.recovery.phantom.PhantomQuerySamplingActions.StartSamplingRequest;
import org.elasticsearch.xpack.stateless.recovery.phantom.PhantomQuerySamplingActions.StartSamplingResponse;

import java.util.List;
import java.util.concurrent.Executor;

/**
 * Declines rather than fails whenever it cannot sample, so that the recovering copy continues without a sample.
 *
 * <p>
 * It checks its current cluster state without waiting for it to catch up with the one that added the recovering copy, unlike
 * {@code start_handoff}, which needs the answer for correctness: here a decline only costs a missed warm-up.
 */
public class PhantomQuerySamplingPeer {

    private static final Logger logger = LogManager.getLogger(PhantomQuerySamplingPeer.class);

    private final ClusterService clusterService;
    private final IndicesService indicesService;
    private final PhantomQuerySamplers samplers;
    private final PhantomQueriesSettings settings;
    private final PhantomQueryMetrics metrics;
    private final NamedWriteableRegistry namedWriteableRegistry;

    public PhantomQuerySamplingPeer(
        ClusterService clusterService,
        IndicesService indicesService,
        PhantomQuerySamplers samplers,
        PhantomQueriesSettings settings,
        PhantomQueryMetrics metrics,
        NamedWriteableRegistry namedWriteableRegistry
    ) {
        this.clusterService = clusterService;
        this.indicesService = indicesService;
        this.samplers = samplers;
        this.settings = settings;
        this.metrics = metrics;
        this.namedWriteableRegistry = namedWriteableRegistry;
    }

    public void registerHandlers(TransportService transportService, Executor executor) {
        transportService.registerRequestHandler(
            PhantomQuerySamplingActions.START_SAMPLING_ACTION_NAME,
            executor,
            false,
            false,
            StartSamplingRequest::new,
            (request, channel, task) -> handleStartSampling(request, new ChannelActionListener<>(channel))
        );
        transportService.registerRequestHandler(
            PhantomQuerySamplingActions.COLLECT_SAMPLE_ACTION_NAME,
            executor,
            false,
            false,
            CollectSampleRequest::new,
            (request, channel, task) -> handleCollectSample(request, new ChannelActionListener<>(channel))
        );
    }

    public void handleStartSampling(StartSamplingRequest request, ActionListener<StartSamplingResponse> listener) {
        ActionListener.completeWith(listener, () -> new StartSamplingResponse(startSampling(request)));
    }

    private boolean startSampling(StartSamplingRequest request) {
        final ShardId shardId = request.shardId();
        final String targetAllocationId = request.targetAllocationId();
        SamplingStartOutcome outcome = declineOutcome(shardId, targetAllocationId);
        if (outcome == null) {
            // the samplers check again, because the copy may have started or the setting may have changed since
            outcome = samplers.startSampling(shardId, targetAllocationId);
        }
        if (outcome.sampling() == false) {
            logger.debug("{} declined to sample for the copy [{}]: {}", shardId, targetAllocationId, outcome);
        }
        // the only place that records it, whichever of the two checks decided
        metrics.recordStartSampling(outcome);
        return outcome.sampling();
    }

    /**
     * Why the peer declines before it asks the samplers, or {@code null} when it does not.
     */
    @Nullable
    private SamplingStartOutcome declineOutcome(ShardId shardId, String targetAllocationId) {
        if (settings.enabled() == false) {
            return SamplingStartOutcome.DECLINED_DISABLED;
        }
        if (hasActiveSearchReplica(shardId) == false) {
            return SamplingStartOutcome.DECLINED_NO_ACTIVE_REPLICA;
        }
        final ShardRouting target = PhantomQuerySamplers.findShard(clusterService.state(), shardId, targetAllocationId);
        if (target == null) {
            return SamplingStartOutcome.DECLINED_TARGET_UNKNOWN;
        }
        if (target.primary() || target.initializing() == false || isSearchReplica(target) == false) {
            return SamplingStartOutcome.DECLINED_TARGET_NOT_INITIALIZING;
        }
        return null;
    }

    private boolean hasActiveSearchReplica(ShardId shardId) {
        final IndexService indexService = indicesService.indexService(shardId.getIndex());
        final IndexShard indexShard = indexService == null ? null : indexService.getShardOrNull(shardId.id());
        if (indexShard == null) {
            return false;
        }
        final ShardRouting routing = indexShard.routingEntry();
        return routing.active() && isSearchReplica(routing) && indexShard.state() == IndexShardState.STARTED;
    }

    private static boolean isSearchReplica(ShardRouting routing) {
        return routing.primary() == false && routing.isSearchable() && routing.isPromotableToPrimary() == false;
    }

    public void handleCollectSample(CollectSampleRequest request, ActionListener<CollectSampleResponse> listener) {
        ActionListener.completeWith(listener, () -> {
            final List<EncodedQueryRequest> requests = samplers.collect(request.shardId(), request.targetAllocationId());
            return new CollectSampleResponse(
                requests.isEmpty() ? PhantomQuerySample.EMPTY : new PhantomQuerySample(requests, namedWriteableRegistry)
            );
        });
    }
}
