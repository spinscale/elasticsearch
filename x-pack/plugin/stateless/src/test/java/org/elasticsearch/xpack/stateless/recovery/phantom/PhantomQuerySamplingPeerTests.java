/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.recovery.phantom;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.replication.ClusterStateCreationUtils;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.telemetry.RecordingMeterRegistry;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.stateless.cache.SearchRecoveryTimeoutCalculationService;
import org.elasticsearch.xpack.stateless.recovery.phantom.PhantomQuerySamplingActions.CollectSampleRequest;
import org.elasticsearch.xpack.stateless.recovery.phantom.PhantomQuerySamplingActions.CollectSampleResponse;
import org.elasticsearch.xpack.stateless.recovery.phantom.PhantomQuerySamplingActions.StartSamplingRequest;
import org.elasticsearch.xpack.stateless.recovery.phantom.PhantomQuerySamplingActions.StartSamplingResponse;
import org.junit.After;
import org.junit.Before;

import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.Executor;

import static org.elasticsearch.cluster.metadata.Metadata.DEFAULT_PROJECT_ID;
import static org.elasticsearch.xpack.stateless.recovery.phantom.PhantomQueryTestUtils.INDEX_NAME;
import static org.elasticsearch.xpack.stateless.recovery.phantom.PhantomQueryTestUtils.clusterStateWithInitializingSearchReplica;
import static org.elasticsearch.xpack.stateless.recovery.phantom.PhantomQueryTestUtils.initializingAllocationId;
import static org.elasticsearch.xpack.stateless.recovery.phantom.PhantomQueryTestUtils.namedWriteableRegistry;
import static org.elasticsearch.xpack.stateless.recovery.phantom.PhantomQueryTestUtils.queryRequest;
import static org.elasticsearch.xpack.stateless.recovery.phantom.PhantomQueryTestUtils.shards;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class PhantomQuerySamplingPeerTests extends ESTestCase {

    private final ShardId shardId = PhantomQueryTestUtils.shardId();
    private TestThreadPool threadPool;
    private ClusterSettings clusterSettings;
    private ClusterService clusterService;
    private PhantomQuerySamplers samplers;
    private IndicesService indicesService;
    private IndexService localIndexService;
    private IndexShard localShard;
    private ClusterState initialState;

    @Before
    public void createPeer() {
        threadPool = new TestThreadPool(getTestName());
        initialState = clusterStateWithInitializingSearchReplica();
        clusterService = createClusterService(initialState);
        samplers = new PhantomQuerySamplers(
            threadPool,
            clusterService,
            new PhantomQueriesSettings(clusterSettings),
            new PhantomQueryMetrics(new RecordingMeterRegistry())
        );
        indicesService = mock(IndicesService.class);
        localIndexService = mock(IndexService.class);
        localShard = mock(IndexShard.class);
        when(indicesService.indexService(shardId.getIndex())).thenReturn(localIndexService);
        when(localIndexService.getShardOrNull(shardId.id())).thenReturn(localShard);
        when(localShard.routingEntry()).thenReturn(startedSearchCopy(initialState));
        when(localShard.state()).thenReturn(IndexShardState.STARTED);
    }

    @After
    public void tearDownPeer() {
        clusterService.close();
        terminate(threadPool);
    }

    private ClusterService createClusterService(ClusterState state) {
        final Set<Setting<?>> registered = new HashSet<>(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        registered.addAll(PhantomQueriesSettings.settings());
        registered.add(SearchRecoveryTimeoutCalculationService.OFFLINE_WARMING_TOTAL_TIMEOUT_CAP_SETTING);
        clusterSettings = new ClusterSettings(settings(true), registered);
        return ClusterServiceUtils.createClusterService(state, threadPool, clusterSettings);
    }

    private static Settings settings(boolean enabled) {
        return Settings.builder().put(PhantomQueriesSettings.ENABLED_SETTING.getKey(), enabled).build();
    }

    private PhantomQuerySamplingPeer peer() {
        return new PhantomQuerySamplingPeer(
            clusterService,
            indicesService,
            samplers,
            new PhantomQueriesSettings(clusterSettings),
            namedWriteableRegistry()
        );
    }

    private static ShardRouting startedSearchCopy(ClusterState state) {
        return shards(state).stream().filter(routing -> routing.started() && routing.isSearchable()).findFirst().orElseThrow();
    }

    private StartSamplingRequest startRequest(String allocationId) {
        return new StartSamplingRequest(shardId, allocationId);
    }

    private boolean startSampling(PhantomQuerySamplingPeer peer, StartSamplingRequest request) {
        final var response = new PlainActionFuture<StartSamplingResponse>();
        peer.handleStartSampling(request, response);
        return safeGet(response).accepted();
    }

    private void setEnabled(boolean enabled) {
        clusterSettings.applySettings(settings(enabled));
    }

    private void applyClusterState(ClusterState state) {
        // the utility applies the version of the state it gets plus one
        ClusterServiceUtils.setState(clusterService, ClusterState.builder(state).version(clusterService.state().version()).build());
    }

    private void assertNothingIsSampled() {
        assertThat(samplers.isSampling(shardId), is(false));
        assertThat(samplers.samplerCount(), equalTo(0));
        assertThat(samplers.usedInBytes(), equalTo(0L));
    }

    public void testAcceptsAnInitializingSearchCopyWhileTheLocalCopyIsActive() {
        final String target = initializingAllocationId(initialState);

        assertThat(startSampling(peer(), startRequest(target)), is(true));

        assertThat(samplers.isSampling(shardId), is(true));
        assertThat(samplers.samplerCount(), equalTo(1));
    }

    public void testAcceptsTheTargetOfARelocationOfTheLocalCopy() {
        final ClusterState relocating = PhantomQueryTestUtils.clusterStateWithRelocatingSearchReplica();
        applyClusterState(relocating);
        final ShardRouting source = shards(relocating).stream().filter(ShardRouting::relocating).findFirst().orElseThrow();
        when(localShard.routingEntry()).thenReturn(source);
        final String target = source.getTargetRelocatingShard().allocationId().getId();

        assertThat("the source of a relocation is active", source.active(), is(true));
        assertThat(startSampling(peer(), startRequest(target)), is(true));

        assertThat(samplers.samplerCount(), equalTo(1));
    }

    public void testDeclinesWhilePhantomQueriesAreDisabled() {
        setEnabled(false);

        assertThat(startSampling(peer(), startRequest(initializingAllocationId(initialState))), is(false));

        assertNothingIsSampled();
    }

    public void testDeclinesWithoutALocalCopyOfTheShard() {
        when(indicesService.indexService(shardId.getIndex())).thenReturn(null);
        assertThat(startSampling(peer(), startRequest(initializingAllocationId(initialState))), is(false));
        assertNothingIsSampled();

        when(indicesService.indexService(shardId.getIndex())).thenReturn(localIndexService);
        when(localIndexService.getShardOrNull(shardId.id())).thenReturn(null);
        assertThat(startSampling(peer(), startRequest(initializingAllocationId(initialState))), is(false));
        assertNothingIsSampled();
    }

    public void testDeclinesWhenTheLocalCopyIsNotActive() {
        final ShardRouting initializing = shards(initialState).stream().filter(ShardRouting::initializing).findFirst().orElseThrow();
        when(localShard.routingEntry()).thenReturn(initializing);

        assertThat(startSampling(peer(), startRequest(initializingAllocationId(initialState))), is(false));

        assertNothingIsSampled();
    }

    public void testDeclinesWhenTheLocalCopyIsNotAStartedShard() {
        when(localShard.state()).thenReturn(
            randomFrom(IndexShardState.CREATED, IndexShardState.RECOVERING, IndexShardState.POST_RECOVERY, IndexShardState.CLOSED)
        );

        assertThat(startSampling(peer(), startRequest(initializingAllocationId(initialState))), is(false));

        assertNothingIsSampled();
    }

    public void testDeclinesWhenTheLocalCopyCannotServeSearches() {
        final ShardRouting primary = shards(initialState).stream().filter(ShardRouting::primary).findFirst().orElseThrow();
        assertThat(primary.active(), is(true));
        when(localShard.routingEntry()).thenReturn(primary);

        assertThat(startSampling(peer(), startRequest(initializingAllocationId(initialState))), is(false));

        assertNothingIsSampled();
    }

    public void testDeclinesWhenTheRequesterIsNotInTheRoutingTable() {
        assertThat(startSampling(peer(), startRequest(randomUUID())), is(false));

        assertNothingIsSampled();
    }

    public void testDeclinesWhenTheRequesterIsAStartedCopy() {
        for (ShardRouting started : shards(initialState).stream().filter(ShardRouting::started).toList()) {
            assertThat(startSampling(peer(), startRequest(started.allocationId().getId())), is(false));
        }

        assertNothingIsSampled();
    }

    public void testDeclinesWhenTheRequesterIsAnInitializingCopyThatCannotServeSearches() {
        final ClusterState withInitializingIndexCopy = ClusterStateCreationUtils.state(
            DEFAULT_PROJECT_ID,
            INDEX_NAME,
            true,
            ShardRoutingState.STARTED,
            ShardRouting.Role.INDEX_ONLY,
            List.of(
                new Tuple<>(ShardRoutingState.STARTED, ShardRouting.Role.SEARCH_ONLY),
                new Tuple<>(ShardRoutingState.INITIALIZING, ShardRouting.Role.INDEX_ONLY)
            )
        );
        applyClusterState(withInitializingIndexCopy);
        final String indexCopy = initializingAllocationId(withInitializingIndexCopy);

        assertThat(startSampling(peer(), startRequest(indexCopy)), is(false));

        assertNothingIsSampled();
    }

    public void testDeclinesWhenTheRequesterIsAnInitializingPrimary() {
        final ClusterState withInitializingPrimary = PhantomQueryTestUtils.clusterStateWithInitializingPrimary(
            randomFrom(ShardRouting.Role.INDEX_ONLY, ShardRouting.Role.DEFAULT)
        );
        applyClusterState(withInitializingPrimary);

        assertThat(startSampling(peer(), startRequest(initializingAllocationId(withInitializingPrimary))), is(false));

        assertNothingIsSampled();
    }

    public void testDeclinesWhenTheLocalCopyIsAPrimaryThatServesSearches() {
        // searchable as well, like a primary in a stateful cluster, so that only it being a primary rules it out
        final ClusterState state = ClusterStateCreationUtils.state(
            DEFAULT_PROJECT_ID,
            INDEX_NAME,
            true,
            ShardRoutingState.STARTED,
            ShardRouting.Role.DEFAULT,
            List.of(
                new Tuple<>(ShardRoutingState.STARTED, ShardRouting.Role.SEARCH_ONLY),
                new Tuple<>(ShardRoutingState.INITIALIZING, ShardRouting.Role.SEARCH_ONLY)
            )
        );
        applyClusterState(state);
        final ShardRouting primary = shards(state).stream().filter(ShardRouting::primary).findFirst().orElseThrow();
        assertThat(primary.active() && primary.isSearchable(), is(true));
        when(localShard.routingEntry()).thenReturn(primary);

        assertThat(startSampling(peer(), startRequest(initializingAllocationId(state))), is(false));

        assertNothingIsSampled();
    }

    public void testDeclinesWhenTheShardIsNotInTheClusterState() {
        final String target = initializingAllocationId(initialState);
        applyClusterState(PhantomQueryTestUtils.withoutIndex());

        assertThat(startSampling(peer(), startRequest(target)), is(false));

        assertNothingIsSampled();
    }

    public void testDeclinesWhenTheRequesterStartedBeforeThisNodeCaughtUp() {
        final String target = initializingAllocationId(initialState);
        applyClusterState(PhantomQueryTestUtils.withCopyStarted(initialState, target));

        assertThat(startSampling(peer(), startRequest(target)), is(false));

        assertNothingIsSampled();
    }

    public void testDeclinesAtOnceWhenThisNodeDoesNotKnowTheRequesterYetAndAcceptsAfterItCaughtUp() {
        final String target = initializingAllocationId(initialState);
        final ClusterState stateBeforeTheCopyWasAdded = PhantomQueryTestUtils.withoutCopy(initialState, target);
        applyClusterState(stateBeforeTheCopyWasAdded);
        final var peer = peer();
        final var response = new PlainActionFuture<StartSamplingResponse>();

        peer.handleStartSampling(startRequest(target), response);

        assertThat("it answers at once", response.isDone(), is(true));
        assertThat(safeGet(response).accepted(), is(false));
        assertNothingIsSampled();

        applyClusterState(initialState);
        assertThat(startSampling(peer, startRequest(target)), is(true));
        assertThat(samplers.samplerCount(), equalTo(1));
    }

    public void testRepeatedRequestKeepsTheSample() {
        final String target = initializingAllocationId(initialState);
        final var peer = peer();
        assertThat(startSampling(peer, startRequest(target)), is(true));
        offerToAllSamplers("kept");

        assertThat(startSampling(peer, startRequest(target)), is(true));

        assertThat(samplers.samplerCount(), equalTo(1));
        assertThat(collect(peer, target).size(), equalTo(1));
    }

    private void offerToAllSamplers(String value) {
        final var observed = new ObservedQueryRequest(queryRequest(shardId, value));
        samplers.forEachSampler(shardId, sampler -> sampler.offer(observed));
    }

    private PhantomQuerySample collect(PhantomQuerySamplingPeer peer, String allocationId) {
        final var response = new PlainActionFuture<CollectSampleResponse>();
        peer.handleCollectSample(new CollectSampleRequest(shardId, allocationId), response);
        return safeGet(response).sample();
    }

    public void testCollectsTheSampleAndStopsSampling() throws Exception {
        final String target = initializingAllocationId(initialState);
        final var peer = peer();
        assertThat(startSampling(peer, startRequest(target)), is(true));
        for (int i = 0; i < 3; i++) {
            offerToAllSamplers(String.format(Locale.ROOT, "value-%d", i));
        }
        assertThat(samplers.usedInBytes(), greaterThan(0L));

        final PhantomQuerySample sample = collect(peer, target);

        assertThat(sample.size(), equalTo(3));
        for (int i = 0; i < 3; i++) {
            assertThat(
                "the order in which the requests arrived is kept",
                PhantomQueryTestUtils.decode(sample.requests().get(i)).source().query(),
                equalTo(queryRequest(shardId, String.format(Locale.ROOT, "value-%d", i)).source().query())
            );
        }
        assertNothingIsSampled();
        offerToAllSamplers("after");
        assertNothingIsSampled();
    }

    public void testCollectsNothingWithoutASampler() {
        final PhantomQuerySample sample = collect(peer(), initializingAllocationId(initialState));

        assertThat(sample.isEmpty(), is(true));
        assertNothingIsSampled();
    }

    public void testCollectsNothingAfterTheRequestWasDeclined() {
        setEnabled(false);
        final String target = initializingAllocationId(initialState);
        final var peer = peer();
        assertThat(startSampling(peer, startRequest(target)), is(false));

        assertThat(collect(peer, target).isEmpty(), is(true));
    }

    public void testCollectingForOneCopyKeepsTheSamplesOfOthers() {
        final ClusterState twoInitializingCopies = PhantomQueryTestUtils.clusterStateWithInitializingSearchReplicas(2);
        applyClusterState(twoInitializingCopies);
        final List<String> targets = PhantomQueryTestUtils.initializingAllocationIds(twoInitializingCopies);
        final var peer = peer();
        for (String target : targets) {
            assertThat(startSampling(peer, startRequest(target)), is(true));
        }
        offerToAllSamplers("shared");

        assertThat(collect(peer, targets.get(0)).size(), equalTo(1));

        assertThat(samplers.samplerCount(), equalTo(1));
        assertThat(collect(peer, targets.get(1)).size(), equalTo(1));
        assertNothingIsSampled();
    }

    public void testCollectedSampleIsWrittenForTheVersionOfTheChannel() throws Exception {
        final String target = initializingAllocationId(initialState);
        final var peer = peer();
        assertThat(startSampling(peer, startRequest(target)), is(true));
        offerToAllSamplers("value");
        final var response = new PlainActionFuture<CollectSampleResponse>();
        peer.handleCollectSample(new CollectSampleRequest(shardId, target), response);

        final NamedWriteableRegistry registry = namedWriteableRegistry();
        final var olderVersion = randomValueOtherThan(TransportVersion.current(), TransportVersionUtils::randomCompatibleVersion);
        final var read = copyWriteable(safeGet(response), registry, CollectSampleResponse::new, olderVersion);

        assertThat(read.sample().size(), equalTo(1));
        assertThat(read.sample().requests().get(0).version(), equalTo(olderVersion));
    }

    public void testRegistersTheHandlersOnTheGivenExecutor() {
        final TransportService transportService = mock(TransportService.class);
        final Executor executor = command -> {};

        peer().registerHandlers(transportService, executor);

        verify(transportService).registerRequestHandler(
            eq(PhantomQuerySamplingActions.START_SAMPLING_ACTION_NAME),
            same(executor),
            eq(false),
            eq(false),
            any(),
            any()
        );
        verify(transportService).registerRequestHandler(
            eq(PhantomQuerySamplingActions.COLLECT_SAMPLE_ACTION_NAME),
            same(executor),
            eq(false),
            eq(false),
            any(),
            any()
        );
    }
}
