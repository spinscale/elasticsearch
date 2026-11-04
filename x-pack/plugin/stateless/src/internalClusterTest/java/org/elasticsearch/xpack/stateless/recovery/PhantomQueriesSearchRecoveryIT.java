/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.recovery;

import org.apache.logging.log4j.core.LogEvent;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.blobcache.BlobCacheMetrics;
import org.elasticsearch.blobcache.shared.SharedBlobCacheService;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.command.CancelAllocationCommand;
import org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.search.stats.SearchStats;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.MockScriptEngine;
import org.elasticsearch.script.MockScriptPlugin;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.TestTelemetryPlugin;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.test.NodeShutdownTestUtils;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.threadpool.ThreadPoolStats;
import org.elasticsearch.xpack.encryption.EncryptionPlugin;
import org.elasticsearch.xpack.esql.plugin.EsqlPlugin;
import org.elasticsearch.xpack.stateless.AbstractStatelessPluginIntegTestCase;
import org.elasticsearch.xpack.stateless.cache.SearchCommitPrefetcherDynamicSettings;
import org.elasticsearch.xpack.stateless.cache.SharedBlobCacheWarmingService;
import org.elasticsearch.xpack.stateless.recovery.phantom.PhantomQueriesSettings;
import org.elasticsearch.xpack.stateless.recovery.phantom.PhantomQueryMetrics;
import org.elasticsearch.xpack.stateless.recovery.phantom.PhantomQueryMetrics.RecoveryType;
import org.elasticsearch.xpack.stateless.recovery.phantom.PhantomQueryMetrics.ReplayOutcome;
import org.elasticsearch.xpack.stateless.recovery.phantom.PhantomQueryMetrics.ShardSizeBucket;
import org.elasticsearch.xpack.stateless.recovery.phantom.PhantomQueryReplayer;
import org.elasticsearch.xpack.stateless.recovery.phantom.PhantomQuerySamplers;
import org.elasticsearch.xpack.stateless.recovery.phantom.PhantomQuerySamplingActions;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.UnaryOperator;

import static org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteUtils.reroute;
import static org.elasticsearch.blobcache.shared.SharedBytes.PAGE_SIZE;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.in;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;

/**
 * Relocations are held open in the {@code start_handoff} request to the source, and a new copy in the collection of its sample, so that the
 * peer certainly serves queries while it samples.
 */
public class PhantomQueriesSearchRecoveryIT extends AbstractStatelessPluginIntegTestCase {

    private static final ByteSizeValue REGION_SIZE = ByteSizeValue.ofBytes(4L * PAGE_SIZE);
    private static final ByteSizeValue CACHE_SIZE = ByteSizeValue.ofMb(8);

    /** A filter script that does nothing, except on a phantom query, which it blocks until {@link #PHANTOM_QUERY_GATE} opens. */
    private static final String BLOCK_PHANTOM_QUERIES_SCRIPT = "block_phantom_queries";
    private static final AtomicReference<CountDownLatch> PHANTOM_QUERY_GATE = new AtomicReference<>();
    private static final AtomicInteger BLOCKED_PHANTOM_QUERIES = new AtomicInteger();

    /**
     * Blocks the phantom queries that carry a script query, which is the only way to keep them running, as they run on the {@code warmer}
     * pool of the recovering node and nothing else does. Searches of users, and the queries of the peer, are not affected.
     */
    public static class BlockPhantomQueriesPlugin extends MockScriptPlugin {
        @Override
        protected Map<String, Function<Map<String, Object>, Object>> pluginScripts() {
            return Map.of(BLOCK_PHANTOM_QUERIES_SCRIPT, vars -> {
                final CountDownLatch gate = PHANTOM_QUERY_GATE.get();
                if (gate != null && ThreadPool.Names.WARMER.equals(EsExecutors.executorName(Thread.currentThread()))) {
                    BLOCKED_PHANTOM_QUERIES.incrementAndGet();
                    try {
                        gate.await(60, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
                return true;
            });
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        final var plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(TestTelemetryPlugin.class);
        plugins.add(BlockPhantomQueriesPlugin.class);
        // for its thread pool, which is also not a place for phantom queries to run, and the encryption that it needs
        plugins.add(EncryptionPlugin.class);
        plugins.add(EsqlPlugin.class);
        return plugins;
    }

    @Before
    public void closePhantomQueryGate() {
        PHANTOM_QUERY_GATE.set(null);
        BLOCKED_PHANTOM_QUERIES.set(0);
    }

    @After
    public void openPhantomQueryGate() {
        final CountDownLatch gate = PHANTOM_QUERY_GATE.getAndSet(null);
        if (gate != null) {
            gate.countDown();
        }
    }

    @Override
    protected Settings.Builder nodeSettings() {
        // the sample is only collected within the warming window, which only a recovery that warms the internal files has
        return super.nodeSettings().put(SharedBlobCacheWarmingService.SEARCH_OFFLINE_WARMING_ENABLED_SETTING.getKey(), true)
            .put(SearchCommitPrefetcherDynamicSettings.STATELESS_SEARCH_USE_INTERNAL_FILES_REPLICATED_CONTENT.getKey(), true)
            .put(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(), CACHE_SIZE.getStringRep())
            .put(SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING.getKey(), REGION_SIZE.getStringRep())
            .put(SharedBlobCacheService.SHARED_CACHE_RANGE_SIZE_SETTING.getKey(), REGION_SIZE.getStringRep())
            // with another search node around, the allocator would rebalance the shard by itself
            .put(
                EnableAllocationDecider.CLUSTER_ROUTING_REBALANCE_ENABLE_SETTING.getKey(),
                EnableAllocationDecider.Rebalance.NONE.toString()
            )
            // held requests must not time out while the test inspects the cluster
            .put(
                TransportStatelessUnpromotableRelocationAction.START_HANDOFF_REQUEST_TIMEOUT_SETTING.getKey(),
                TimeValue.timeValueMinutes(1)
            )
            .put(disableIndexingDiskAndMemoryControllersNodeSettings());
    }

    @Override
    protected boolean addMockFsRepository() {
        return false;
    }

    private static Settings phantomQueries(boolean enabled) {
        return Settings.builder().put(PhantomQueriesSettings.ENABLED_SETTING.getKey(), enabled).build();
    }

    private static final class SearchLoad implements Releasable {

        private final AtomicBoolean running = new AtomicBoolean(true);
        private final AtomicLong completed = new AtomicLong();
        private final List<Thread> threads = new ArrayList<>();

        SearchLoad(Client client, String indexName, int numberOfThreads) {
            this(client, indexName, numberOfThreads, QueryBuilders.matchAllQuery());
        }

        SearchLoad(Client client, String indexName, int numberOfThreads, QueryBuilder query) {
            for (int i = 0; i < numberOfThreads; i++) {
                final Thread thread = new Thread(() -> {
                    while (running.get()) {
                        try {
                            final var response = client.prepareSearch(indexName).setQuery(query).setSize(5).get();
                            response.decRef();
                            completed.incrementAndGet();
                        } catch (Exception e) {
                            // searches may fail while shards move, which the load does not care about
                        }
                    }
                }, "phantom-queries-search-load-" + i);
                thread.start();
                threads.add(thread);
            }
        }

        long completed() {
            return completed.get();
        }

        @Override
        public void close() {
            running.set(false);
            for (Thread thread : threads) {
                try {
                    thread.join(30_000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError(e);
                }
                assertFalse("the search load stops", thread.isAlive());
            }
        }
    }

    private String createIndexWithDocs() {
        final String indexName = randomIdentifier();
        assertAcked(
            prepareCreate(
                indexName,
                Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
            )
        );
        indexDocs(indexName, randomIntBetween(100, 500));
        flushAndRefresh(indexName);
        ensureGreen(indexName);
        return indexName;
    }

    private void relocateAwayFrom(String node, String indexName) {
        updateIndexSettings(Settings.builder().put("index.routing.allocation.exclude._name", node), indexName);
    }

    private static PhantomQuerySamplers samplers(String node) {
        return internalCluster().getInstance(PhantomQuerySamplers.class, node);
    }

    private static long sampledQueries(String node) {
        final TestTelemetryPlugin telemetry = getTelemetryPlugin(node);
        telemetry.collect();
        return getTotalLongCounterValue(PhantomQueryMetrics.SAMPLED_COUNTER, telemetry);
    }

    private static void assertNothingIsLeftBehind(String node) throws Exception {
        assertBusy(() -> {
            assertThat("samplers of " + node, samplers(node).samplerCount(), equalTo(0));
            assertThat("bytes of the samples of " + node, samplers(node).usedInBytes(), equalTo(0L));
            assertThat("the gauge of the samplers of " + node, gauge(node, PhantomQueryMetrics.SAMPLERS_GAUGE), equalTo(0L));
            assertThat("the gauge of the bytes of " + node, gauge(node, PhantomQueryMetrics.SAMPLE_BYTES_GAUGE), equalTo(0L));
        });
    }

    // ---- metrics of sampling and replay

    private static Map<String, Long> countersBy(String node, String metric, String attribute) {
        final TestTelemetryPlugin telemetry = getTelemetryPlugin(node);
        telemetry.collect();
        final Map<String, Long> counts = new HashMap<>();
        for (Measurement measurement : telemetry.getLongCounterMeasurement(metric)) {
            counts.merge((String) measurement.attributes().get(attribute), measurement.getLong(), Long::sum);
        }
        return counts;
    }

    private static Map<String, Long> startOutcomes(String node) {
        return countersBy(node, PhantomQueryMetrics.START_SAMPLING_COUNTER, PhantomQueryMetrics.SAMPLING_START_OUTCOME_ATTRIBUTE);
    }

    private static Map<String, Long> collections(String node) {
        return countersBy(node, PhantomQueryMetrics.SAMPLE_COLLECTION_COUNTER, PhantomQueryMetrics.COLLECTION_OUTCOME_ATTRIBUTE);
    }

    private static Map<String, Long> endedSamplers(String node) {
        return countersBy(node, PhantomQueryMetrics.SAMPLERS_ENDED_COUNTER, PhantomQueryMetrics.SAMPLER_END_ATTRIBUTE);
    }

    private static Map<String, Long> droppedRequests(String node) {
        return countersBy(node, PhantomQueryMetrics.DROPPED_COUNTER, PhantomQueryMetrics.DROP_REASON_ATTRIBUTE);
    }

    private static Map<String, Long> notExecuted(String node) {
        return countersBy(node, PhantomQueryMetrics.NOT_EXECUTED_COUNTER, PhantomQueryMetrics.REPLAY_OUTCOME_ATTRIBUTE);
    }

    private static long totalOf(Map<String, Long> counts) {
        return counts.values().stream().mapToLong(Long::longValue).sum();
    }

    private static List<Long> longHistogram(String node, String metric) {
        final TestTelemetryPlugin telemetry = getTelemetryPlugin(node);
        telemetry.collect();
        return telemetry.getLongHistogramMeasurement(metric).stream().map(Measurement::getLong).toList();
    }

    /** What the node reports now, which is the last of the measurements that were collected. */
    private static long gauge(String node, String metric) {
        final TestTelemetryPlugin telemetry = getTelemetryPlugin(node);
        telemetry.collect();
        final List<Measurement> measurements = telemetry.getLongAsyncGaugeMeasurement(metric);
        assertThat("[" + metric + "] is reported by " + node, measurements, not(empty()));
        return measurements.get(measurements.size() - 1).getLong();
    }

    /** The only sample that the peer handed over, as the number of requests and the kilobytes of its size. */
    private static void assertTheSampleThatThePeerHandedOver(String peer, long expectedQueries) {
        assertThat(longHistogram(peer, PhantomQueryMetrics.SAMPLE_QUERIES_HISTOGRAM), equalTo(List.of(expectedQueries)));
        final List<Long> sizes = longHistogram(peer, PhantomQueryMetrics.SAMPLE_SIZE_HISTOGRAM);
        assertThat(sizes, hasSize(1));
        assertThat("a sample with requests has a size", sizes.get(0), expectedQueries > 0 ? greaterThan(0L) : equalTo(0L));
    }

    private static void assertThatSearchShardIsStartedOn(String indexName, String nodeName) {
        final var clusterState = client(internalCluster().getMasterName()).admin()
            .cluster()
            .prepareState(TEST_REQUEST_TIMEOUT)
            .get()
            .getState();
        final var nodeId = clusterState.nodes().resolveNode(nodeName).getId();
        final List<ShardRouting> searchShards = clusterState.routingTable(ProjectId.DEFAULT)
            .index(indexName)
            .shard(0)
            .unpromotableShards()
            .stream()
            .filter(ShardRouting::active)
            .toList();
        final String routing = "search copies "
            + clusterState.routingTable(ProjectId.DEFAULT).index(indexName).shard(0).unpromotableShards();
        assertThat(routing, searchShards, hasSize(1));
        assertThat(routing, searchShards.get(0).started(), is(true));
        assertThat(routing + ", expected on " + nodeName, searchShards.get(0).currentNodeId(), equalTo(nodeId));
    }

    private static final class HeldHandoff {
        final CountDownLatch received = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);

        HeldHandoff(String source) {
            MockTransportService.getInstance(source)
                .addRequestHandlingBehavior(
                    TransportStatelessUnpromotableRelocationAction.START_HANDOFF_ACTION_NAME,
                    (handler, request, channel, task) -> {
                        received.countDown();
                        safeAwait(release, TimeValue.timeValueSeconds(60));
                        handler.messageReceived(request, channel, task);
                    }
                );
        }

        void release() {
            release.countDown();
        }
    }

    private static AtomicInteger countRequests(String node, String action) {
        final var count = new AtomicInteger();
        MockTransportService.getInstance(node).addRequestHandlingBehavior(action, (handler, request, channel, task) -> {
            count.incrementAndGet();
            handler.messageReceived(request, channel, task);
        });
        return count;
    }

    public void testRelocationSamplesQueriesOfTheSourceAndReleasesTheSampleOnceTheShardStarted() throws Exception {
        final String masterAndIndexNode = startMasterAndIndexNode();
        final String source = startSearchNode(phantomQueries(true));
        final String indexName = createIndexWithDocs();
        final String target = startSearchNode(phantomQueries(true));
        ensureStableCluster(3);
        final var startSamplingRequests = countRequests(source, PhantomQuerySamplingActions.START_SAMPLING_ACTION_NAME);
        final var collectSampleRequests = countRequests(source, PhantomQuerySamplingActions.COLLECT_SAMPLE_ACTION_NAME);
        final var handoff = new HeldHandoff(source);
        assertThat(sampledQueries(source), equalTo(0L));

        try (var load = new SearchLoad(client(masterAndIndexNode), indexName, 2)) {
            relocateAwayFrom(source, indexName);
            safeAwait(handoff.received);

            assertBusy(() -> {
                assertThat("the source samples for the target", samplers(source).samplerCount(), equalTo(1));
                assertThat("the source retains requests", samplers(source).usedInBytes(), greaterThan(0L));
                assertThat(sampledQueries(source), greaterThan(0L));
                assertThat("the gauge of the samplers", gauge(source, PhantomQueryMetrics.SAMPLERS_GAUGE), equalTo(1L));
                assertThat("the gauge of the bytes", gauge(source, PhantomQueryMetrics.SAMPLE_BYTES_GAUGE), greaterThan(0L));
            });
            assertThat(startOutcomes(source), equalTo(Map.of("STARTED", 1L)));
            assertThat("nothing ended while the target recovers", endedSamplers(source), equalTo(Map.of()));
            assertThat("nothing was collected yet", longHistogram(source, PhantomQueryMetrics.SAMPLE_QUERIES_HISTOGRAM), empty());
            assertThat(startSamplingRequests.get(), equalTo(1));
            assertThat("the sample is collected only once the engine of the target is open", collectSampleRequests.get(), equalTo(0));
            assertThat("the load reaches the source meanwhile", load.completed(), greaterThan(0L));

            handoff.release();
            ensureGreen(indexName);
        } finally {
            handoff.release();
        }

        assertThatSearchShardIsStartedOn(indexName, target);
        assertThat("the target collected the sample", collectSampleRequests.get(), equalTo(1));
        assertNothingIsLeftBehind(source);
        assertNothingIsLeftBehind(target);
        assertThat(endedSamplers(source), equalTo(Map.of("COLLECTED", 1L)));
        assertThat(collections(target), equalTo(Map.of("COLLECTED", 1L)));
        final List<Long> sampleQueries = longHistogram(source, PhantomQueryMetrics.SAMPLE_QUERIES_HISTOGRAM);
        assertThat(sampleQueries, hasSize(1));
        assertTheSampleThatThePeerHandedOver(source, sampleQueries.get(0));
        assertThat("the sample has requests", sampleQueries.get(0), greaterThan(0L));
        assertThat("nothing was dropped within the default limits", droppedRequests(source), equalTo(Map.of()));
    }

    public void testCancelledRelocationLeavesNoSampleOnThePeer() throws Exception {
        final String masterAndIndexNode = startMasterAndIndexNode();
        final String source = startSearchNode(phantomQueries(true));
        final String indexName = createIndexWithDocs();
        final String target = startSearchNode(phantomQueries(true));
        ensureStableCluster(3);
        final var collectSampleRequests = countRequests(source, PhantomQuerySamplingActions.COLLECT_SAMPLE_ACTION_NAME);
        final var handoff = new HeldHandoff(source);

        try (var load = new SearchLoad(client(masterAndIndexNode), indexName, 2)) {
            relocateAwayFrom(source, indexName);
            safeAwait(handoff.received);
            assertBusy(() -> {
                assertThat(samplers(source).samplerCount(), equalTo(1));
                assertThat("the source retains requests", samplers(source).usedInBytes(), greaterThan(0L));
            });

            // lifted first, or the allocator would start another relocation to the target
            updateIndexSettings(Settings.builder().putNull("index.routing.allocation.exclude._name"), indexName);
            reroute(client(), new CancelAllocationCommand(indexName, 0, target, false));

            assertNothingIsLeftBehind(source);
        } finally {
            handoff.release();
        }

        ensureGreen(indexName);
        assertThatSearchShardIsStartedOn(indexName, source);
        assertThat("the sample was not collected", collectSampleRequests.get(), equalTo(0));
        assertNothingIsLeftBehind(source);
        assertNothingIsLeftBehind(target);
        assertThat(sampledQueries(source), greaterThan(0L));
        assertThat(startOutcomes(source), equalTo(Map.of("STARTED", 1L)));
        assertBusy(
            () -> assertThat(
                "the routing table ended it, and nobody collected it",
                endedSamplers(source),
                equalTo(Map.of("TARGET_GONE", 1L))
            )
        );
        assertThat(
            "a sample that nobody collected is not recorded",
            longHistogram(source, PhantomQueryMetrics.SAMPLE_QUERIES_HISTOGRAM),
            empty()
        );
        assertThat(longHistogram(source, PhantomQueryMetrics.SAMPLE_SIZE_HISTOGRAM), empty());
        // the cancelled recovery goes on once the handoff is released, finds that its warming window went with the shard, and skips the
        // collection
        assertThat("the target collected nothing", collections(target), anyOf(equalTo(Map.of()), equalTo(Map.of("SKIPPED", 1L))));
    }

    public void testNothingIsSampledWhenPhantomQueriesAreDisabledOnTheRecoveringNode() throws Exception {
        final String masterAndIndexNode = startMasterAndIndexNode();
        // enabled on the source, so that only the target decides
        final String source = startSearchNode(phantomQueries(true));
        final String indexName = createIndexWithDocs();
        final String target = startSearchNode(phantomQueries(false));
        ensureStableCluster(3);
        final var startSamplingRequests = countRequests(source, PhantomQuerySamplingActions.START_SAMPLING_ACTION_NAME);
        final var collectSampleRequests = countRequests(source, PhantomQuerySamplingActions.COLLECT_SAMPLE_ACTION_NAME);
        final var handoff = new HeldHandoff(source);

        try (var load = new SearchLoad(client(masterAndIndexNode), indexName, 2)) {
            relocateAwayFrom(source, indexName);
            safeAwait(handoff.received);

            final long servedBefore = load.completed();
            assertBusy(() -> assertThat(load.completed(), greaterThanOrEqualTo(servedBefore + 20)));
            assertThat(samplers(source).samplerCount(), equalTo(0));

            handoff.release();
            ensureGreen(indexName);
        } finally {
            handoff.release();
        }

        assertThatSearchShardIsStartedOn(indexName, target);
        assertThat(startSamplingRequests.get(), equalTo(0));
        assertThat(collectSampleRequests.get(), equalTo(0));
        assertThat(sampledQueries(source), equalTo(0L));
        assertNothingIsLeftBehind(source);
        assertThat("the peer was not asked", startOutcomes(source), equalTo(Map.of()));
        assertThat(endedSamplers(source), equalTo(Map.of()));
        assertThat("the replay records that phantom queries were disabled", collections(target), equalTo(Map.of()));
    }

    public void testNewCopySamplesTheStartedCopyAndReleasesTheSampleWhenItIsCollected() throws Exception {
        final String masterAndIndexNode = startMasterAndIndexNode();
        final String peer = startSearchNode(phantomQueries(true));
        final String indexName = createIndexWithDocs();
        final String newCopyNode = startSearchNode(phantomQueries(true));
        ensureStableCluster(3);
        final var startSamplingRequests = countRequests(peer, PhantomQuerySamplingActions.START_SAMPLING_ACTION_NAME);
        final var collectReceived = new CountDownLatch(1);
        final var releaseCollection = new CountDownLatch(1);
        MockTransportService.getInstance(peer)
            .addRequestHandlingBehavior(PhantomQuerySamplingActions.COLLECT_SAMPLE_ACTION_NAME, (handler, request, channel, task) -> {
                collectReceived.countDown();
                safeAwait(releaseCollection, TimeValue.timeValueSeconds(60));
                handler.messageReceived(request, channel, task);
            });

        try (var load = new SearchLoad(client(masterAndIndexNode), indexName, 2)) {
            updateIndexSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 2), indexName);
            safeAwait(collectReceived);

            assertThat("the started copy is the peer of the new copy", startSamplingRequests.get(), equalTo(1));
            // it keeps sampling while the collection is held
            assertBusy(() -> {
                assertThat(samplers(peer).samplerCount(), equalTo(1));
                assertThat(samplers(peer).usedInBytes(), greaterThan(0L));
                assertThat(sampledQueries(peer), greaterThan(0L));
            });

            releaseCollection.countDown();
            ensureGreen(indexName);
        } finally {
            releaseCollection.countDown();
        }

        assertNothingIsLeftBehind(peer);
        assertNothingIsLeftBehind(newCopyNode);
        assertThat(startOutcomes(peer), equalTo(Map.of("STARTED", 1L)));
        assertThat(endedSamplers(peer), equalTo(Map.of("COLLECTED", 1L)));
        assertThat(collections(newCopyNode), equalTo(Map.of("COLLECTED", 1L)));
        final List<Long> sampleQueries = longHistogram(peer, PhantomQueryMetrics.SAMPLE_QUERIES_HISTOGRAM);
        assertThat(sampleQueries, hasSize(1));
        assertTheSampleThatThePeerHandedOver(peer, sampleQueries.get(0));
    }

    // ---- replay

    private static final String INCLUDE_NODES = "index.routing.allocation.include._name";
    private static final int LARGE_DOCS = 600;
    private static final int LARGE_DOC_FIELD_LENGTH = 350;

    private static final class HeldCollection {
        final CountDownLatch received = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);

        HeldCollection(String peer) {
            MockTransportService.getInstance(peer)
                .addRequestHandlingBehavior(PhantomQuerySamplingActions.COLLECT_SAMPLE_ACTION_NAME, (handler, request, channel, task) -> {
                    received.countDown();
                    safeAwait(release, TimeValue.timeValueSeconds(60));
                    handler.messageReceived(request, channel, task);
                });
        }

        void release() {
            release.countDown();
        }
    }

    private static String nodeId(String nodeName) {
        return internalCluster().clusterService(nodeName).localNode().getId();
    }

    private static List<Measurement> replays(String node) {
        final TestTelemetryPlugin telemetry = getTelemetryPlugin(node);
        telemetry.collect();
        return telemetry.getDoubleHistogramMeasurement(PhantomQueryMetrics.REPLAY_DURATION_HISTOGRAM);
    }

    private static long executedQueries(String node) {
        final TestTelemetryPlugin telemetry = getTelemetryPlugin(node);
        telemetry.collect();
        return getTotalLongCounterValue(PhantomQueryMetrics.EXECUTED_COUNTER, telemetry);
    }

    private static long executedQueries(String node, PhantomQueryMetrics.QueryOutcome outcome) {
        final TestTelemetryPlugin telemetry = getTelemetryPlugin(node);
        telemetry.collect();
        return telemetry.getLongCounterMeasurement(PhantomQueryMetrics.EXECUTED_COUNTER)
            .stream()
            .filter(m -> outcome.name().equals(m.attributes().get(PhantomQueryMetrics.QUERY_OUTCOME_ATTRIBUTE)))
            .mapToLong(Measurement::getLong)
            .sum();
    }

    /** Misses that the searches of the shard itself had, and not the ones that warming, or the replay on the warmer pool, had. */
    private static long searchExecutorBlobCacheMisses(String node) {
        final TestTelemetryPlugin telemetry = getTelemetryPlugin(node);
        telemetry.collect();
        return telemetry.getLongCounterMeasurement("es.blob_cache.miss_that_triggered_read.total")
            .stream()
            .filter(m -> ThreadPool.Names.SEARCH.equals(m.attributes().get(BlobCacheMetrics.ES_EXECUTOR_ATTRIBUTE_KEY)))
            .mapToLong(Measurement::getLong)
            .sum();
    }

    /** The replay is recorded before the recovery is over, so there is nothing to wait for once the shard has started. */
    private static void assertSingleReplay(String node, ReplayOutcome outcome, RecoveryType recoveryType) {
        assertSingleReplay(replays(node), outcome, recoveryType);
    }

    private static void assertSingleReplay(List<Measurement> replays, ReplayOutcome outcome, RecoveryType recoveryType) {
        assertThat("replays: " + replays, replays, hasSize(1));
        final Map<String, Object> attributes = replays.get(0).attributes();
        assertThat(
            attributes.keySet(),
            equalTo(
                Set.of(
                    PhantomQueryMetrics.REPLAY_OUTCOME_ATTRIBUTE,
                    PhantomQueryMetrics.RECOVERY_TYPE_ATTRIBUTE,
                    PhantomQueryMetrics.SHARD_SIZE_BUCKET_ATTRIBUTE
                )
            )
        );
        assertThat(attributes.get(PhantomQueryMetrics.REPLAY_OUTCOME_ATTRIBUTE), equalTo(outcome.name()));
        assertThat(attributes.get(PhantomQueryMetrics.RECOVERY_TYPE_ATTRIBUTE), equalTo(recoveryType.name()));
        assertThat(
            "the shard size is one of the buckets",
            (String) attributes.get(PhantomQueryMetrics.SHARD_SIZE_BUCKET_ATTRIBUTE),
            is(in(Arrays.stream(ShardSizeBucket.values()).map(ShardSizeBucket::label).toList()))
        );
    }

    /** The shards of these tests are far below a gibibyte, and the size of their commit is known once their engine is open. */
    private static void assertSmallShard(List<Measurement> replays) {
        assertThat(replays.get(0).attributes().get(PhantomQueryMetrics.SHARD_SIZE_BUCKET_ATTRIBUTE), equalTo("<=1GiB"));
    }

    private static void assertThatSearchCopyIsStartedOn(String indexName, String nodeName) {
        final ClusterState clusterState = client(internalCluster().getMasterName()).admin()
            .cluster()
            .prepareState(TEST_REQUEST_TIMEOUT)
            .get()
            .getState();
        final String nodeId = clusterState.nodes().resolveNode(nodeName).getId();
        final var shardTable = clusterState.routingTable(ProjectId.DEFAULT).index(indexName).shard(0);
        final List<ShardRouting> copies = shardTable.unpromotableShards().stream().filter(c -> nodeId.equals(c.currentNodeId())).toList();
        assertThat("search copies " + shardTable.unpromotableShards() + " on " + nodeName, copies, hasSize(1));
        assertThat(copies.get(0).started(), is(true));
    }

    /** The index node is always allowed, because the filter applies to the primary as well. */
    private String createIndexWithLargeDocs(int replicas, String allowedNodes) {
        final String indexName = randomIdentifier();
        assertAcked(
            prepareCreate(
                indexName,
                Settings.builder()
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, replicas)
                    .put(INCLUDE_NODES, allowedNodes)
                    // a copy that is lost is replaced at once
                    .put(UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.getKey(), TimeValue.ZERO)
            )
        );
        // more than the regions that the offline warming of a recovery fetches, so that the queries of the shard have regions to miss
        indexDocs(
            indexName,
            LARGE_DOCS,
            UnaryOperator.identity(),
            null,
            () -> Map.of("field", randomAlphaOfLength(LARGE_DOC_FIELD_LENGTH))
        );
        flushAndRefresh(indexName);
        ensureGreen(indexName);
        return indexName;
    }

    private enum RecoveryKind {
        /** The only copy moves from the peer to another node. */
        RELOCATION,
        /** The number of replicas grows, and the new copy lands on another node. */
        NEW_COPY,
        /** A node with a copy is stopped while the peer keeps the other copy, and the lost copy is replaced on another node. */
        REPLACEMENT
    }

    private record Observation(String target, long executedQueries, List<Measurement> replays, long searchExecutorMisses) {}

    /**
     * Recovers a search copy on a new node under concurrent search load, and reports what the recovering node did before its copy started,
     * and how often the searches of the copy had to read from the object store once it had. The sample is only collected once the peer
     * retained some queries, so that the recovery has something to replay.
     */
    private Observation recoverUnderLoad(RecoveryKind kind, String master, String peer, boolean phantomQueriesOnTarget) throws Exception {
        final String target = startSearchNode(phantomQueries(phantomQueriesOnTarget));
        final String nodeWithTheLostCopy = kind == RecoveryKind.REPLACEMENT ? startSearchNode(phantomQueries(true)) : null;
        ensureStableCluster(internalCluster().size());
        final String indexName = kind == RecoveryKind.REPLACEMENT
            ? createIndexWithLargeDocs(2, master + "," + peer + "," + nodeWithTheLostCopy)
            : createIndexWithLargeDocs(1, master + "," + peer);
        final String targetNodeId = nodeId(target);
        getTelemetryPlugin(target).resetMeter();
        final HeldCollection collection = phantomQueriesOnTarget ? new HeldCollection(peer) : null;

        try (var load = new SearchLoad(client(master), indexName, 2)) {
            switch (kind) {
                case RELOCATION -> updateIndexSettings(Settings.builder().put(INCLUDE_NODES, master + "," + target), indexName);
                case NEW_COPY -> updateIndexSettings(
                    Settings.builder()
                        .put(INCLUDE_NODES, master + "," + peer + "," + target)
                        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 2),
                    indexName
                );
                case REPLACEMENT -> {
                    updateIndexSettings(
                        Settings.builder().put(INCLUDE_NODES, master + "," + peer + "," + nodeWithTheLostCopy + "," + target),
                        indexName
                    );
                    internalCluster().stopNode(nodeWithTheLostCopy);
                }
            }
            if (collection != null) {
                safeAwait(collection.received);
                assertBusy(() -> assertThat("the peer retained queries", samplers(peer).usedInBytes(), greaterThan(0L)));
                collection.release();
            }
            ensureGreen(indexName);
        } finally {
            if (collection != null) {
                collection.release();
            }
        }

        assertThatSearchCopyIsStartedOn(indexName, target);
        final long executed = executedQueries(target);
        final List<Measurement> replays = replays(target);
        for (int i = 0; i < 3; i++) {
            final var response = client(master).prepareSearch(indexName)
                .setPreference("_only_nodes:" + targetNodeId)
                .setQuery(QueryBuilders.matchAllQuery())
                .setSize(5)
                .get();
            response.decRef();
        }
        return new Observation(target, executed, replays, searchExecutorBlobCacheMisses(target));
    }

    private void assertTheRecoveryReplaysBeforeTheShardStartsAndWarmsTheCopy(RecoveryKind kind, RecoveryType recoveryType)
        throws Exception {
        final String master = startMasterAndIndexNode();
        final String peer = startSearchNode(phantomQueries(true));

        final Observation enabled = recoverUnderLoad(kind, master, peer, true);
        assertThat("the phantom queries ran", enabled.executedQueries(), greaterThan(0L));
        assertSingleReplay(enabled.replays(), ReplayOutcome.COMPLETED, recoveryType);
        assertSmallShard(enabled.replays());
        assertThat(executedQueries(enabled.target(), PhantomQueryMetrics.QueryOutcome.SUCCESS), equalTo(enabled.executedQueries()));

        // what the peer sampled is what the recovering copy collected, executed, and timed
        assertThat(startOutcomes(peer), equalTo(Map.of("STARTED", 1L)));
        assertThat(endedSamplers(peer), equalTo(Map.of("COLLECTED", 1L)));
        assertThat(droppedRequests(peer), equalTo(Map.of()));
        assertThat(collections(enabled.target()), equalTo(Map.of("COLLECTED", 1L)));
        assertTheSampleThatThePeerHandedOver(peer, enabled.executedQueries());
        assertThat("a replay that completed leaves nothing that never started", notExecuted(enabled.target()), equalTo(Map.of()));
        assertThat(
            "every query that started has a duration",
            (long) getTelemetryPlugin(enabled.target()).getDoubleHistogramMeasurement(PhantomQueryMetrics.EXECUTION_DURATION_HISTOGRAM)
                .size(),
            equalTo(enabled.executedQueries())
        );

        final Observation disabled = recoverUnderLoad(kind, master, peer, false);
        assertThat(disabled.executedQueries(), equalTo(0L));
        assertSingleReplay(disabled.replays(), ReplayOutcome.DISABLED, recoveryType);
        assertSmallShard(disabled.replays());
        assertThat("nothing was asked of the peer again", startOutcomes(peer), equalTo(Map.of("STARTED", 1L)));
        assertThat(collections(disabled.target()), equalTo(Map.of()));
        logger.info("--> {}: enabled {}, disabled {}", kind, enabled, disabled);

        assertThat(
            "a copy that replayed queries reads less from the object store once it serves searches: with "
                + enabled
                + ", without "
                + disabled,
            enabled.searchExecutorMisses(),
            lessThan(disabled.searchExecutorMisses())
        );
        assertNothingIsLeftBehind(peer);
        assertNothingIsLeftBehind(enabled.target());
        assertNothingIsLeftBehind(disabled.target());
    }

    public void testRelocationReplaysTheSampleBeforeTheShardStartsAndWarmsTheCopy() throws Exception {
        assertTheRecoveryReplaysBeforeTheShardStartsAndWarmsTheCopy(RecoveryKind.RELOCATION, RecoveryType.RELOCATION);
    }

    public void testAddedReplicaReplaysTheSampleBeforeTheShardStartsAndWarmsTheCopy() throws Exception {
        assertTheRecoveryReplaysBeforeTheShardStartsAndWarmsTheCopy(RecoveryKind.NEW_COPY, RecoveryType.NEW_COPY);
    }

    public void testReplacementOfALostCopyReplaysTheSampleBeforeTheShardStartsAndWarmsTheCopy() throws Exception {
        assertTheRecoveryReplaysBeforeTheShardStartsAndWarmsTheCopy(RecoveryKind.REPLACEMENT, RecoveryType.NEW_COPY);
    }

    // ---- the warming window

    private static List<TaskInfo> phantomQueryTasks(String node) {
        return client().admin()
            .cluster()
            .prepareListTasks(nodeId(node))
            .setActions(PhantomQueryReplayer.TASK_ACTION)
            .setDetailed(true)
            .get()
            .getTasks();
    }

    public void testReplayEndsAtTheWarmingDeadlineWithoutWaitingForQueriesThatStillRun() throws Exception {
        // long enough for the peer to retain some queries first, and short enough for the test
        final var window = TimeValue.timeValueSeconds(10);
        final String master = startMasterAndIndexNode();
        final String peer = startSearchNode(phantomQueries(true));
        final String indexName = createIndexWithDocs();
        final String target = startSearchNode(
            Settings.builder()
                .put(phantomQueries(true))
                .put(SharedBlobCacheWarmingService.SEARCH_RECOVERY_WARMING_TIMEOUT_RELOCATION_SETTING.getKey(), window)
                .build()
        );
        ensureStableCluster(3);
        final var gate = new CountDownLatch(1);
        PHANTOM_QUERY_GATE.set(gate);
        final var collection = new HeldCollection(peer);
        final var blockingQuery = QueryBuilders.scriptQuery(
            new Script(ScriptType.INLINE, MockScriptEngine.NAME, BLOCK_PHANTOM_QUERIES_SCRIPT, Map.of())
        );

        try (var load = new SearchLoad(client(master), indexName, 2, blockingQuery)) {
            relocateAwayFrom(peer, indexName);
            safeAwait(collection.received);
            // more than the two that run at once, so that some queries are still queued at the deadline
            assertBusy(() -> assertThat(sampledQueries(peer), greaterThan(5L)));
            collection.release();

            assertBusy(() -> assertThat("the phantom queries run, and are blocked", BLOCKED_PHANTOM_QUERIES.get(), greaterThan(0)));
            final List<TaskInfo> running = phantomQueryTasks(target);
            assertThat(running, not(empty()));
            for (TaskInfo task : running) {
                assertThat("a child task of the recovery", task.parentTaskId().isSet(), is(true));
                assertThat(task.cancellable(), is(true));
                assertThat("they have not reached the deadline", task.cancelled(), is(false));
            }

            // the deadline ends the replay, and the recovery goes on although the queries do not stop
            ensureGreen(TimeValue.timeValueSeconds(60), indexName);
        } finally {
            collection.release();
        }

        assertThatSearchCopyIsStartedOn(indexName, target);
        assertSingleReplay(target, ReplayOutcome.DEADLINE, RecoveryType.RELOCATION);
        assertThat("replay ended within the window", replays(target).get(0).getDouble(), lessThanOrEqualTo((double) window.seconds()));
        final List<TaskInfo> afterTheDeadline = phantomQueryTasks(target);
        assertThat("the queries are still blocked, and were told to stop", afterTheDeadline, not(empty()));
        for (TaskInfo task : afterTheDeadline) {
            assertThat(task.cancelled(), is(true));
        }

        gate.countDown();
        assertBusy(() -> assertThat("no phantom task remains", phantomQueryTasks(target), empty()));
        assertBusy(() -> assertThat(executedQueries(target, PhantomQueryMetrics.QueryOutcome.CANCELLED), greaterThan(0L)));
        assertNothingIsLeftBehind(peer);
        assertNothingIsLeftBehind(target);

        // the queries that were queued when the deadline ended the replay never start, and each of them is counted once it is found out
        final List<Long> sampleQueries = longHistogram(peer, PhantomQueryMetrics.SAMPLE_QUERIES_HISTOGRAM);
        assertThat(sampleQueries, hasSize(1));
        assertBusy(() -> {
            assertThat(notExecuted(target).keySet(), equalTo(Set.of("DEADLINE")));
            assertThat(notExecuted(target).get("DEADLINE"), greaterThan(0L));
            assertThat(
                "the queries that started and the ones that did not make up the sample",
                executedQueries(target) + totalOf(notExecuted(target)),
                equalTo(sampleQueries.get(0))
            );
        });
        assertThat(collections(target), equalTo(Map.of("COLLECTED", 1L)));
    }

    public void testNothingIsReplayedWhenTheWarmingTimedOut() throws Exception {
        final String master = startMasterAndIndexNode();
        final String peer = startSearchNode(phantomQueries(true));
        final String indexName = createIndexWithDocs();
        final String target = startSearchNode(
            Settings.builder()
                .put(phantomQueries(true))
                // the wait ends at once, and with it the time that replay may have
                .put(
                    SharedBlobCacheWarmingService.SEARCH_RECOVERY_WARMING_TIMEOUT_RELOCATION_SETTING.getKey(),
                    TimeValue.timeValueMillis(1)
                )
                .build()
        );
        ensureStableCluster(3);
        final var collectRequests = countRequests(peer, PhantomQuerySamplingActions.COLLECT_SAMPLE_ACTION_NAME);

        try (var load = new SearchLoad(client(master), indexName, 2)) {
            relocateAwayFrom(peer, indexName);
            ensureGreen(indexName);
        }

        assertThatSearchCopyIsStartedOn(indexName, target);
        assertSingleReplay(target, ReplayOutcome.NO_TIME_LEFT, RecoveryType.RELOCATION);
        assertSmallShard(replays(target));
        assertThat(executedQueries(target), equalTo(0L));
        assertThat("a sample that cannot be replayed is not collected", collectRequests.get(), equalTo(0));
        assertNothingIsLeftBehind(peer);
        assertThat("the collection was skipped, which is why there is no sample", collections(target), equalTo(Map.of("SKIPPED", 1L)));
        assertThat("nothing was collected from the peer", longHistogram(peer, PhantomQueryMetrics.SAMPLE_QUERIES_HISTOGRAM), empty());
    }

    private static void markForSigtermShutdown(String nodeName, long startedAtMillis, TimeValue gracePeriod) {
        final ClusterService masterClusterService = internalCluster().getCurrentMasterNodeInstance(ClusterService.class);
        safeAwait(
            (ActionListener<Void> listener) -> NodeShutdownTestUtils.putShutdownMetadata(
                masterClusterService,
                SingleNodeShutdownMetadata.builder()
                    .setType(SingleNodeShutdownMetadata.Type.SIGTERM)
                    .setStartedAtMillis(startedAtMillis)
                    .setReason("test")
                    .setGracePeriod(gracePeriod),
                nodeName,
                listener
            )
        );
    }

    public void testNoWindowWhenTheRelocationSourceIsShutDownAndItsGracePeriodElapsed() throws Exception {
        final String master = startMasterAndIndexNode();
        final String source = startSearchNode(phantomQueries(true));
        final String indexName = createIndexWithDocs();
        final String target = startSearchNode(phantomQueries(true));
        ensureStableCluster(3);

        try (var load = new SearchLoad(client(master), indexName, 2)) {
            assertBusy(() -> assertThat(load.completed(), greaterThan(0L)));
            // the grace period ended a minute ago, so the target does not wait for warming, and has no window to replay in
            final long now = internalCluster().getCurrentMasterNodeInstance(ClusterService.class).threadPool().absoluteTimeInMillis();
            markForSigtermShutdown(source, now - TimeValue.timeValueMinutes(1).millis(), TimeValue.timeValueSeconds(1));
            // the shutdown api would do this, but the cluster state is changed directly here
            reroute(client());
            // the only other search node is the target, so the copy of the source relocates there
            assertBusy(() -> assertThatSearchCopyIsStartedOn(indexName, target));
            ensureGreen(indexName);
        }

        assertThatSearchCopyIsStartedOn(indexName, target);
        assertSingleReplay(target, ReplayOutcome.NO_WINDOW, RecoveryType.RELOCATION);
        assertThat(executedQueries(target), equalTo(0L));
        assertNothingIsLeftBehind(source);
        assertThat(collections(target), equalTo(Map.of("SKIPPED", 1L)));
    }

    public void testNoWindowWhenANewCopyIsAddedWhileASearchNodeThatHoldsShardsIsMarkedForShutdown() throws Exception {
        final String master = startMasterAndIndexNode();
        final String leaving = startSearchNode(phantomQueries(true));
        final String peer = startSearchNode(phantomQueries(true));
        final String newCopyNode = startSearchNode(phantomQueries(true));
        ensureStableCluster(4);

        // the copy of this index has nowhere else to go, so the node that is shutting down holds a shard throughout
        final String pinned = createIndexWithDocsOn(1, master + "," + leaving);
        // the index that grows has a started copy, which makes a window for the new one, if no search node that holds shards leaves
        final String indexName = createIndexWithDocsOn(1, master + "," + peer);
        final long now = internalCluster().getCurrentMasterNodeInstance(ClusterService.class).threadPool().absoluteTimeInMillis();
        markForSigtermShutdown(leaving, now, TimeValue.timeValueMinutes(10));

        try (var load = new SearchLoad(client(master), indexName, 2)) {
            updateIndexSettings(
                Settings.builder()
                    .put(INCLUDE_NODES, master + "," + peer + "," + newCopyNode)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 2),
                indexName
            );
            ensureGreen(indexName);
        }

        assertThatSearchCopyIsStartedOn(indexName, newCopyNode);
        assertThatSearchCopyIsStartedOn(pinned, leaving);
        assertSingleReplay(newCopyNode, ReplayOutcome.NO_WINDOW, RecoveryType.NEW_COPY);
        assertThat(executedQueries(newCopyNode), equalTo(0L));
        assertNothingIsLeftBehind(peer);
        assertThat(collections(newCopyNode), equalTo(Map.of("SKIPPED", 1L)));
    }

    /** The index node has to be among the allowed nodes, because the filter applies to the primary as well. */
    private String createIndexWithDocsOn(int replicas, String allowedNodes) {
        final String indexName = randomIdentifier();
        assertAcked(
            prepareCreate(
                indexName,
                Settings.builder()
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, replicas)
                    .put(INCLUDE_NODES, allowedNodes)
            )
        );
        indexDocs(indexName, randomIntBetween(100, 500));
        flushAndRefresh(indexName);
        ensureGreen(indexName);
        return indexName;
    }

    // ---- side effects

    private static final class CountingExpectation implements MockLog.LoggingExpectation {
        final AtomicInteger events = new AtomicInteger();

        @Override
        public void match(LogEvent event) {
            events.incrementAndGet();
        }

        @Override
        public void assertMatched() {}
    }

    private static long completedTasks(String node, String threadPool) {
        final var stats = client().admin().cluster().prepareNodesStats(nodeId(node)).setThreadPool(true).get().getNodes().get(0);
        for (ThreadPoolStats.Stats poolStats : stats.getThreadPool()) {
            if (threadPool.equals(poolStats.name())) {
                return poolStats.completed();
            }
        }
        throw new AssertionError("no thread pool [" + threadPool + "] on " + node);
    }

    private static SearchStats searchStatsOfTheCopyOn(String indexName, String node) {
        final String nodeId = nodeId(node);
        final var stats = client().admin().indices().prepareStats(indexName).setSearch(true).get();
        return Arrays.stream(stats.getShards())
            .filter(shardStats -> nodeId.equals(shardStats.getShardRouting().currentNodeId()))
            .map(shardStats -> shardStats.getStats().getSearch())
            .findFirst()
            .orElseThrow(() -> new AssertionError("no stats of the copy on " + node));
    }

    public void testReplayLeavesNoTraceOnStatsSlowLogReaderContextsAndSearchThreadPools() throws Exception {
        final String master = startMasterAndIndexNode();
        final String source = startSearchNode(phantomQueries(true));
        final String indexName = createIndexWithDocs();
        final String target = startSearchNode(phantomQueries(true));
        ensureStableCluster(3);
        final var collection = new HeldCollection(source);
        final var slowLogs = new CountingExpectation();

        // stopped before the sample is collected, so that nothing but the replay runs queries on the recovering node
        try (var load = new SearchLoad(client(master), indexName, 2)) {
            relocateAwayFrom(source, indexName);
            safeAwait(collection.received);
            assertBusy(() -> assertThat(samplers(source).usedInBytes(), greaterThan(0L)));
        }
        updateIndexSettings(
            Settings.builder()
                .put("index.search.slowlog.threshold.query.warn", "0ms")
                .put("index.search.slowlog.threshold.fetch.warn", "0ms"),
            indexName
        );
        final long searchTasksBefore = completedTasks(target, ThreadPool.Names.SEARCH);
        final long esqlTasksBefore = completedTasks(target, EsqlPlugin.ESQL_WORKER_THREAD_POOL_NAME);

        try (var mockLog = MockLog.capture("index.search.slowlog.query", "index.search.slowlog.fetch")) {
            mockLog.addExpectation(slowLogs);
            collection.release();
            ensureGreen(indexName);

            assertThatSearchCopyIsStartedOn(indexName, target);
            assertThat("the phantom queries ran", executedQueries(target), greaterThan(0L));
            assertSingleReplay(target, ReplayOutcome.COMPLETED, RecoveryType.RELOCATION);

            final SearchStats stats = searchStatsOfTheCopyOn(indexName, target);
            assertThat("query executions", stats.getTotal().getQueryCount(), equalTo(0L));
            assertThat("fetch executions", stats.getTotal().getFetchCount(), equalTo(0L));
            assertThat("the load that autoscaling reads", stats.getTotal().getSearchLoadRate(), equalTo(0.0));
            assertThat("open reader contexts", stats.getOpenContexts(), equalTo(0L));
            assertThat("slow log entries", slowLogs.events.get(), equalTo(0));
            assertThat("search tasks", completedTasks(target, ThreadPool.Names.SEARCH), equalTo(searchTasksBefore));
            assertThat("esql tasks", completedTasks(target, EsqlPlugin.ESQL_WORKER_THREAD_POOL_NAME), equalTo(esqlTasksBefore));

            // a search of a user is counted, and logged, which makes the checks above able to fail
            final var response = client(master).prepareSearch(indexName)
                .setPreference("_only_nodes:" + nodeId(target))
                .setQuery(QueryBuilders.matchAllQuery())
                .setSize(5)
                .get();
            response.decRef();
            assertThat(searchStatsOfTheCopyOn(indexName, target).getTotal().getQueryCount(), equalTo(1L));
            assertThat(slowLogs.events.get(), greaterThan(0));
            mockLog.assertAllExpectationsMatched();
        }
        assertNothingIsLeftBehind(source);
    }

    // ---- the peer and mixed settings

    public void testNoSampleWhenPhantomQueriesAreDisabledOnThePeer() throws Exception {
        final String master = startMasterAndIndexNode();
        final String source = startSearchNode(phantomQueries(false));
        final String indexName = createIndexWithDocs();
        final String target = startSearchNode(phantomQueries(true));
        ensureStableCluster(3);
        final var startRequests = countRequests(source, PhantomQuerySamplingActions.START_SAMPLING_ACTION_NAME);
        final var collectRequests = countRequests(source, PhantomQuerySamplingActions.COLLECT_SAMPLE_ACTION_NAME);

        try (var load = new SearchLoad(client(master), indexName, 2)) {
            relocateAwayFrom(source, indexName);
            ensureGreen(indexName);
        }

        assertThatSearchCopyIsStartedOn(indexName, target);
        assertThat("the target asked", startRequests.get(), equalTo(1));
        assertThat("the target collects what the peer did not sample", collectRequests.get(), equalTo(1));
        assertSingleReplay(target, ReplayOutcome.NO_SAMPLE, RecoveryType.RELOCATION);
        assertThat(executedQueries(target), equalTo(0L));
        assertThat("the peer sampled nothing", sampledQueries(source), equalTo(0L));
        assertNothingIsLeftBehind(source);
        assertThat("the peer says why", startOutcomes(source), equalTo(Map.of("DECLINED_DISABLED", 1L)));
        assertThat("the target says what it got", collections(target), equalTo(Map.of("EMPTY", 1L)));
        assertThat(endedSamplers(source), equalTo(Map.of()));
        assertThat(
            "a peer without a sampler records no sample",
            longHistogram(source, PhantomQueryMetrics.SAMPLE_QUERIES_HISTOGRAM),
            empty()
        );
    }

    public void testSampleIsReplayedWhenPointInTimeRelocationIsDisabled() throws Exception {
        final var pitRelocationDisabled = Settings.builder().put(SearchService.PIT_RELOCATION_ENABLED.getKey(), false);
        final String master = startMasterAndIndexNode();
        final String source = startSearchNode(Settings.builder().put(phantomQueries(true)).put(pitRelocationDisabled.build()).build());
        final String indexName = createIndexWithDocs();
        final String target = startSearchNode(Settings.builder().put(phantomQueries(true)).put(pitRelocationDisabled.build()).build());
        ensureStableCluster(3);
        final var collection = new HeldCollection(source);

        try (var load = new SearchLoad(client(master), indexName, 2)) {
            relocateAwayFrom(source, indexName);
            safeAwait(collection.received);
            assertBusy(() -> assertThat(samplers(source).usedInBytes(), greaterThan(0L)));
            collection.release();
            ensureGreen(indexName);
        } finally {
            collection.release();
        }

        assertThatSearchCopyIsStartedOn(indexName, target);
        assertThat(executedQueries(target), greaterThan(0L));
        assertSingleReplay(target, ReplayOutcome.COMPLETED, RecoveryType.RELOCATION);
        assertNothingIsLeftBehind(source);
    }

    // ---- requests that the limits drop

    /**
     * Relocates the only search copy away from a source that has the given limits, holds the collection of the sample while the load keeps
     * the source busy, lets the test look at the source at that moment, and finishes the relocation. The sample is empty, because the
     * limits of the source leave no room for any request.
     */
    private void relocateFromASourceWithLimits(Settings limits, Consumer<String> whileTheSourceSamples) throws Exception {
        final String master = startMasterAndIndexNode();
        final String source = startSearchNode(Settings.builder().put(phantomQueries(true)).put(limits).build());
        final String indexName = createIndexWithDocs();
        final String target = startSearchNode(phantomQueries(true));
        ensureStableCluster(3);
        final var collection = new HeldCollection(source);

        try (var load = new SearchLoad(client(master), indexName, 2)) {
            relocateAwayFrom(source, indexName);
            safeAwait(collection.received);
            final long servedBefore = load.completed();
            assertBusy(() -> assertThat("the load keeps reaching the source", load.completed(), greaterThanOrEqualTo(servedBefore + 50)));
            whileTheSourceSamples.accept(source);
            collection.release();
            ensureGreen(indexName);
        } finally {
            collection.release();
        }

        assertThatSearchCopyIsStartedOn(indexName, target);
        assertThat("nothing fit", sampledQueries(source), equalTo(0L));
        assertThat(endedSamplers(source), equalTo(Map.of("COLLECTED", 1L)));
        assertTheSampleThatThePeerHandedOver(source, 0L);
        assertThat(collections(target), equalTo(Map.of("EMPTY", 1L)));
        assertSingleReplay(target, ReplayOutcome.NO_SAMPLE, RecoveryType.RELOCATION);
        assertThat(executedQueries(target), equalTo(0L));
        assertNothingIsLeftBehind(source);
        assertNothingIsLeftBehind(target);
    }

    public void testTheByteLimitOfASampleDropsOneRequestAndStopsSamplingForTheCopy() throws Exception {
        // a request is bigger than a byte
        final var limits = Settings.builder().put(PhantomQueriesSettings.MAX_BYTES_PER_SAMPLE_SETTING.getKey(), "1b").build();

        relocateFromASourceWithLimits(limits, source -> {
            // the load has served many requests since sampling started, and only the first, which did not fit, is counted
            assertThat(droppedRequests(source), equalTo(Map.of("SAMPLE_BYTES", 1L)));
            assertThat("the sampler is still there until the sample is collected", samplers(source).samplerCount(), equalTo(1));
        });
    }

    public void testAFullNodeBudgetDropsEveryRequestAndKeepsTheSamplerOpen() throws Exception {
        final var limits = Settings.builder().put(PhantomQueriesSettings.MAX_BYTES_PER_NODE_SETTING.getKey(), "1b").build();

        relocateFromASourceWithLimits(limits, source -> {
            assertThat("only the budget of the node dropped requests", droppedRequests(source).keySet(), equalTo(Set.of("NODE_BUDGET")));
            final long dropped = droppedRequests(source).get("NODE_BUDGET");
            assertThat("the load went on, and each request that it sent was counted", dropped, greaterThan(1L));
            assertThat("the sampler stays open, because other samples may free budget", samplers(source).samplerCount(), equalTo(1));
            assertThat(samplers(source).usedInBytes(), equalTo(0L));
        });
    }
}
