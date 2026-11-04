/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.recovery;

import org.elasticsearch.blobcache.shared.SharedBlobCacheService;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.command.CancelAllocationCommand;
import org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.telemetry.TestTelemetryPlugin;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.xpack.stateless.AbstractStatelessPluginIntegTestCase;
import org.elasticsearch.xpack.stateless.cache.SearchCommitPrefetcherDynamicSettings;
import org.elasticsearch.xpack.stateless.cache.SharedBlobCacheWarmingService;
import org.elasticsearch.xpack.stateless.recovery.phantom.PhantomQueriesSettings;
import org.elasticsearch.xpack.stateless.recovery.phantom.PhantomQueryMetrics;
import org.elasticsearch.xpack.stateless.recovery.phantom.PhantomQuerySamplers;
import org.elasticsearch.xpack.stateless.recovery.phantom.PhantomQuerySamplingActions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteUtils.reroute;
import static org.elasticsearch.blobcache.shared.SharedBytes.PAGE_SIZE;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

/**
 * Relocations are held open in the {@code start_handoff} request to the source, and a new copy in the collection of its sample, so that the
 * peer certainly serves queries while it samples.
 */
public class PhantomQueriesSearchRecoveryIT extends AbstractStatelessPluginIntegTestCase {

    private static final ByteSizeValue REGION_SIZE = ByteSizeValue.ofBytes(4L * PAGE_SIZE);
    private static final ByteSizeValue CACHE_SIZE = ByteSizeValue.ofMb(8);

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        final var plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(TestTelemetryPlugin.class);
        return plugins;
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
            for (int i = 0; i < numberOfThreads; i++) {
                final Thread thread = new Thread(() -> {
                    while (running.get()) {
                        try {
                            final var response = client.prepareSearch(indexName).setQuery(QueryBuilders.matchAllQuery()).setSize(5).get();
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
        });
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
            });
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
    }
}
