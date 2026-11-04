/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.recovery;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.replication.ClusterStateCreationUtils;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.project.TestProjectResolvers;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.SplitShardCountSummary;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.recovery.PeerRecoveryTargetService;
import org.elasticsearch.indices.recovery.RecoveriesCollection;
import org.elasticsearch.indices.recovery.RecoveryCancelledException;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.indices.recovery.RecoveryTarget;
import org.elasticsearch.indices.recovery.StatelessUnpromotableRelocationAction;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.telemetry.InstrumentType;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.RecordingMeterRegistry;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.IndexSettingsModule;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.stateless.StatelessComponents;
import org.elasticsearch.xpack.stateless.cache.SearchRecoveryWarmingWindows;
import org.elasticsearch.xpack.stateless.engine.translog.TranslogReplicator;
import org.elasticsearch.xpack.stateless.objectstore.ObjectStoreService;
import org.elasticsearch.xpack.stateless.recovery.phantom.EncodedQueryRequest;
import org.elasticsearch.xpack.stateless.recovery.phantom.PhantomQueriesSettings;
import org.elasticsearch.xpack.stateless.recovery.phantom.PhantomQueryMetrics;
import org.elasticsearch.xpack.stateless.recovery.phantom.PhantomQuerySample;
import org.elasticsearch.xpack.stateless.recovery.phantom.PhantomQuerySamplers;
import org.elasticsearch.xpack.stateless.recovery.phantom.PhantomQuerySamplingActions;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.cluster.metadata.Metadata.DEFAULT_PROJECT_ID;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TransportStatelessUnpromotableRelocationActionTests extends ESTestCase {

    private static final long RECOVERY_ID = 42L;
    private static final long WINDOW_DEADLINE_MILLIS = 1_000L;

    private TestThreadPool threadPool;
    private SearchRecoveryWarmingWindows windows;
    private IndexShard indexShard;
    private ShardId shardId;
    private String allocationId;
    private TransportStatelessUnpromotableRelocationAction action;
    private final AtomicReference<ActionListener<Void>> preRecoveryListener = new AtomicReference<>();
    private final SearchService searchService = mock(SearchService.class);
    private final PhantomQueriesSettings phantomQueriesSettings = mock(PhantomQueriesSettings.class);
    private final RecordingMeterRegistry meterRegistry = new RecordingMeterRegistry();
    private TransportService transportService;
    private ClusterService clusterService;

    @Before
    public void setUpAction() {
        threadPool = new TestThreadPool(getTestName());
        windows = new SearchRecoveryWarmingWindows();
        shardId = new ShardId(randomIdentifier(), "_na_", 0);
        allocationId = randomIdentifier();

        transportService = mock(TransportService.class);
        // the phantom queries that are replayed are registered as child tasks of the recovery
        when(transportService.getTaskManager()).thenReturn(new TaskManager(Settings.EMPTY, threadPool, Set.of()));
        when(transportService.getThreadPool()).thenReturn(threadPool);
        when(phantomQueriesSettings.maxConcurrentPerNode()).thenReturn(2);

        clusterService = mock(ClusterService.class);
        when(clusterService.getSettings()).thenReturn(Settings.EMPTY);
        when(clusterService.getClusterSettings()).thenReturn(ClusterSettings.createBuiltInClusterSettings());
        when(clusterService.state()).thenReturn(ClusterState.EMPTY_STATE);

        final var newCopyRouting = TestShardRouting.newShardRouting(
            shardId,
            randomIdentifier(),
            false,
            ShardRoutingState.INITIALIZING,
            ShardRouting.Role.SEARCH_ONLY
        );
        assertThat(newCopyRouting.relocatingNodeId(), nullValue());

        indexShard = mock(IndexShard.class);
        when(indexShard.indexSettings()).thenReturn(IndexSettingsModule.newIndexSettings(shardId.getIndex(), Settings.EMPTY));
        when(indexShard.routingEntry()).thenReturn(newCopyRouting);
        doAnswer(invocation -> {
            assertTrue("one wait for warming", preRecoveryListener.compareAndSet(null, invocation.getArgument(0)));
            return null;
        }).when(indexShard).preRecovery(any());

        final var indexService = mock(IndexService.class);
        when(indexService.getShard(shardId.id())).thenReturn(indexShard);
        final var indicesService = mock(IndicesService.class);
        when(indicesService.indexServiceSafe(shardId.getIndex())).thenReturn(indexService);

        final var recoveryState = mock(RecoveryState.class);
        when(recoveryState.getIndex()).thenReturn(mock(RecoveryState.Index.class));
        final var recoveryTarget = mock(RecoveryTarget.class);
        when(recoveryTarget.state()).thenReturn(recoveryState);
        when(recoveryTarget.shardId()).thenReturn(shardId);
        final var peerRecoveryTargetService = mock(PeerRecoveryTargetService.class);
        when(peerRecoveryTargetService.getRecoveryRef(RECOVERY_ID, shardId)).thenReturn(new RecoveryRefWithoutRelease(recoveryTarget));

        action = new TransportStatelessUnpromotableRelocationAction(
            transportService,
            clusterService,
            new ActionFilters(Set.of()),
            indicesService,
            peerRecoveryTargetService,
            TestProjectResolvers.DEFAULT_PROJECT_ONLY,
            searchService,
            new PITRelocationService(),
            new StatelessComponents(mock(TranslogReplicator.class), mock(ObjectStoreService.class)),
            new PitRelocationMetrics(MeterRegistry.NOOP),
            windows,
            mock(PhantomQuerySamplers.class),
            phantomQueriesSettings,
            new PhantomQueryMetrics(meterRegistry),
            new NamedWriteableRegistry(new SearchModule(Settings.EMPTY, List.of()).getNamedWriteables())
        );
    }

    private static class RecoveryRefWithoutRelease extends RecoveriesCollection.RecoveryRef {
        RecoveryRefWithoutRelease(RecoveryTarget recoveryTarget) {
            super(recoveryTarget);
        }

        @Override
        public void close() {}
    }

    @After
    public void terminateThreadPool() {
        terminate(threadPool);
    }

    private void startRecovery(ActionListener<ActionResponse.Empty> listener) {
        action.doExecute(
            mock(Task.class),
            new StatelessUnpromotableRelocationAction.Request(RECOVERY_ID, shardId, allocationId, randomNonNegativeLong()),
            listener
        );
    }

    private PlainActionFuture<ActionResponse.Empty> startRecovery() {
        final var response = new PlainActionFuture<ActionResponse.Empty>();
        startRecovery(response);
        return response;
    }

    private OptionalLong windowDeadline() {
        return windows.relativeDeadlineMillis(shardId, allocationId);
    }

    private void startWarmingWindow() {
        windows.startWindow(shardId, allocationId, WINDOW_DEADLINE_MILLIS);
    }

    public void testWindowIsRemovedWhenTheRecoveryCompletes() {
        startWarmingWindow();

        final var response = startRecovery();
        assertThat("the recovery waits for warming", response.isDone(), is(false));
        assertThat(windowDeadline(), equalTo(OptionalLong.of(WINDOW_DEADLINE_MILLIS)));

        preRecoveryListener.get().onResponse(null);
        assertThat(safeGet(response), equalTo(ActionResponse.Empty.INSTANCE));
        assertThat(windowDeadline(), equalTo(OptionalLong.empty()));
    }

    public void testWindowIsRemovedBeforeTheListenerIsCalled() {
        startWarmingWindow();
        final var seenByListener = new AtomicReference<OptionalLong>();

        startRecovery(ActionListener.running(() -> seenByListener.set(windowDeadline())));
        preRecoveryListener.get().onResponse(null);

        assertThat(seenByListener.get(), equalTo(OptionalLong.empty()));
    }

    public void testWindowIsRemovedWhenTheRecoveryFails() {
        startWarmingWindow();
        final var failure = new ElasticsearchException("simulated");

        final var thrown = safeAwaitFailure(ActionResponse.Empty.class, listener -> {
            startRecovery(listener);
            assertThat("the recovery waits for warming", windowDeadline(), equalTo(OptionalLong.of(WINDOW_DEADLINE_MILLIS)));
            preRecoveryListener.get().onFailure(failure);
        });

        assertSame(failure, thrown);
        assertThat(windowDeadline(), equalTo(OptionalLong.empty()));
    }

    public void testWindowIsRemovedWhenTheRecoveryIsCancelledDuringTheWait() {
        doNothing().doThrow(new RecoveryCancelledException(shardId, null, DiscoveryNodeUtils.create("target")))
            .when(indexShard)
            .ensureRecoveryNotCancelled();
        startWarmingWindow();

        safeAwaitFailure(RecoveryCancelledException.class, ActionResponse.Empty.class, listener -> {
            startRecovery(listener);
            assertThat("the recovery waits for warming", windowDeadline(), equalTo(OptionalLong.of(WINDOW_DEADLINE_MILLIS)));
            preRecoveryListener.get().onResponse(null);
        });

        assertThat(windowDeadline(), equalTo(OptionalLong.empty()));
    }

    public void testOtherWindowsAreKept() {
        final var otherAllocationId = randomValueOtherThan(allocationId, ESTestCase::randomIdentifier);
        final var otherShardId = new ShardId(randomIdentifier(), randomUUID(), randomIntBetween(0, 5));
        startWarmingWindow();
        windows.startWindow(shardId, otherAllocationId, 2_000L);
        windows.startWindow(otherShardId, allocationId, 3_000L);

        final var response = startRecovery();
        preRecoveryListener.get().onResponse(null);
        safeGet(response);

        assertThat(windowDeadline(), equalTo(OptionalLong.empty()));
        assertThat(windows.relativeDeadlineMillis(shardId, otherAllocationId), equalTo(OptionalLong.of(2_000L)));
        assertThat(windows.relativeDeadlineMillis(otherShardId, allocationId), equalTo(OptionalLong.of(3_000L)));
    }

    private record SentToPeer(String action, TransportRequest request, TransportResponseHandler<?> handler) {}

    private final List<SentToPeer> sentToPeer = new ArrayList<>();

    private void enablePhantomQueriesWithAStartedPeer() {
        when(phantomQueriesSettings.enabled()).thenReturn(true);
        final ClusterState state = ClusterStateCreationUtils.state(
            DEFAULT_PROJECT_ID,
            shardId.getIndexName(),
            true,
            ShardRoutingState.STARTED,
            ShardRouting.Role.INDEX_ONLY,
            List.of(
                new Tuple<>(ShardRoutingState.STARTED, ShardRouting.Role.SEARCH_ONLY),
                new Tuple<>(ShardRoutingState.INITIALIZING, ShardRouting.Role.SEARCH_ONLY)
            )
        );
        final ShardRouting recovering = state.routingTable(DEFAULT_PROJECT_ID)
            .shardRoutingTable(shardId)
            .allShards()
            .filter(ShardRouting::initializing)
            .findFirst()
            .orElseThrow();
        allocationId = recovering.allocationId().getId();
        when(indexShard.routingEntry()).thenReturn(recovering);
        when(clusterService.state()).thenReturn(state);
        when(clusterService.localNode()).thenReturn(state.nodes().get(recovering.currentNodeId()));
        final var connection = mock(Transport.Connection.class);
        when(connection.getTransportVersion()).thenReturn(TransportVersion.current());
        when(transportService.getConnection(any(DiscoveryNode.class))).thenReturn(connection);
        doAnswer(invocation -> {
            sentToPeer.add(new SentToPeer(invocation.getArgument(1), invocation.getArgument(2), invocation.getArgument(5)));
            return null;
        }).when(transportService)
            .sendChildRequest(
                any(Transport.Connection.class),
                anyString(),
                any(TransportRequest.class),
                any(Task.class),
                any(TransportRequestOptions.class),
                any()
            );
    }

    private List<String> actionsSentToPeer() {
        return sentToPeer.stream().map(SentToPeer::action).toList();
    }

    private void startWarmingWindowWithTimeLeft() {
        windows.startWindow(shardId, allocationId, threadPool.relativeTimeInMillis() + 60_000L);
    }

    @SuppressWarnings("unchecked")
    private void answerLastRequestToPeer(PhantomQuerySample sample) {
        final var handler = (TransportResponseHandler<PhantomQuerySamplingActions.CollectSampleResponse>) sentToPeer.getLast().handler();
        handler.handleResponse(new PhantomQuerySamplingActions.CollectSampleResponse(sample));
    }

    public void testSamplingStartsBeforeTheWarmingWaitAndTheSampleIsCollectedOnceTheEngineIsOpen() throws Exception {
        enablePhantomQueriesWithAStartedPeer();
        startWarmingWindowWithTimeLeft();

        final var response = startRecovery();

        assertThat("sampling starts before the recovery waits for warming", actionsSentToPeer(), equalTo(List.of(START_SAMPLING)));
        assertThat(sentToPeer.get(0).request(), instanceOf(PhantomQuerySamplingActions.StartSamplingRequest.class));
        assertThat("the recovery waits for warming, and not for the peer", preRecoveryListener.get(), notNullValue());
        assertThat(response.isDone(), is(false));
        verify(indexShard, never()).openEngineAndSkipTranslogRecovery();

        preRecoveryListener.get().onResponse(null);

        verify(indexShard).openEngineAndSkipTranslogRecovery();
        assertThat(actionsSentToPeer(), equalTo(List.of(START_SAMPLING, COLLECT_SAMPLE)));
        assertThat("the recovery waits for the sample", response.isDone(), is(false));

        answerLastRequestToPeer(PhantomQuerySample.EMPTY);
        assertThat(safeGet(response), equalTo(ActionResponse.Empty.INSTANCE));
        assertThat(windowDeadline(), equalTo(OptionalLong.empty()));
    }

    public void testSampleIsCollectedWhetherOrNotPitRelocationIsEnabled() {
        when(searchService.isPitRelocationEnabled()).thenReturn(randomBoolean());
        enablePhantomQueriesWithAStartedPeer();
        startWarmingWindowWithTimeLeft();
        final var response = startRecovery();

        preRecoveryListener.get().onResponse(null);

        assertThat(actionsSentToPeer(), equalTo(List.of(START_SAMPLING, COLLECT_SAMPLE)));
        answerLastRequestToPeer(PhantomQuerySample.EMPTY);
        assertThat(safeGet(response), equalTo(ActionResponse.Empty.INSTANCE));
    }

    public void testNoSampleIsCollectedWhenTheWarmingWindowIsUsedUp() {
        enablePhantomQueriesWithAStartedPeer();
        windows.startWindow(shardId, allocationId, threadPool.relativeTimeInMillis() - randomLongBetween(0, 60_000L));
        final var response = startRecovery();

        preRecoveryListener.get().onResponse(null);

        assertThat(
            "the peer was asked to sample, but the sample is not worth collecting",
            actionsSentToPeer(),
            equalTo(List.of(START_SAMPLING))
        );
        assertThat(safeGet(response), equalTo(ActionResponse.Empty.INSTANCE));
    }

    public void testNoSampleIsCollectedWhenTheRecoveryHasNoWarmingWindow() {
        enablePhantomQueriesWithAStartedPeer();
        final var response = startRecovery();

        preRecoveryListener.get().onResponse(null);

        assertThat(actionsSentToPeer(), equalTo(List.of(START_SAMPLING)));
        assertThat(safeGet(response), equalTo(ActionResponse.Empty.INSTANCE));
    }

    public void testNothingIsSentToPeersWhilePhantomQueriesAreDisabled() {
        enablePhantomQueriesWithAStartedPeer();
        when(phantomQueriesSettings.enabled()).thenReturn(false);
        startWarmingWindowWithTimeLeft();
        final var response = startRecovery();

        preRecoveryListener.get().onResponse(null);

        assertThat(sentToPeer.isEmpty(), is(true));
        assertThat(safeGet(response), equalTo(ActionResponse.Empty.INSTANCE));
    }

    public void testNoSampleIsCollectedWhenTheRecoveryFailsBeforeTheEngineIsOpen() {
        enablePhantomQueriesWithAStartedPeer();
        startWarmingWindowWithTimeLeft();
        final var failure = new ElasticsearchException("simulated");

        final var thrown = safeAwaitFailure(ActionResponse.Empty.class, listener -> {
            startRecovery(listener);
            preRecoveryListener.get().onFailure(failure);
        });

        assertSame(failure, thrown);
        assertThat(actionsSentToPeer(), equalTo(List.of(START_SAMPLING)));
    }

    public void testFailureToCollectTheSampleDoesNotFailTheRecovery() {
        enablePhantomQueriesWithAStartedPeer();
        startWarmingWindowWithTimeLeft();
        final var response = startRecovery();
        preRecoveryListener.get().onResponse(null);
        assertThat(actionsSentToPeer(), equalTo(List.of(START_SAMPLING, COLLECT_SAMPLE)));

        sentToPeer.getLast().handler().handleException(new TransportException("simulated, for example a timeout"));

        assertThat(safeGet(response), equalTo(ActionResponse.Empty.INSTANCE));
    }

    public void testFailureToStartSamplingDoesNotFailTheRecovery() {
        enablePhantomQueriesWithAStartedPeer();
        doThrow(new IllegalStateException("simulated")).when(transportService).getConnection(any(DiscoveryNode.class));
        startWarmingWindowWithTimeLeft();
        final var response = startRecovery();

        preRecoveryListener.get().onResponse(null);

        assertThat(safeGet(response), equalTo(ActionResponse.Empty.INSTANCE));
        assertThat(sentToPeer.isEmpty(), is(true));
    }

    private static final String START_SAMPLING = PhantomQuerySamplingActions.START_SAMPLING_ACTION_NAME;
    private static final String COLLECT_SAMPLE = PhantomQuerySamplingActions.COLLECT_SAMPLE_ACTION_NAME;

    // ---- replay

    private List<Measurement> replays() {
        return meterRegistry.getRecorder().getMeasurements(InstrumentType.DOUBLE_HISTOGRAM, PhantomQueryMetrics.REPLAY_DURATION_HISTOGRAM);
    }

    private List<String> replayOutcomes() {
        return replays().stream().map(m -> (String) m.attributes().get(PhantomQueryMetrics.REPLAY_OUTCOME_ATTRIBUTE)).toList();
    }

    private PhantomQuerySample sampleOf(int size) throws IOException {
        final List<EncodedQueryRequest> requests = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            final var searchRequest = new SearchRequest(shardId.getIndexName()).source(
                new SearchSourceBuilder().query(QueryBuilders.termQuery("field", "value-" + i))
            );
            searchRequest.allowPartialSearchResults(true);
            final var request = new ShardSearchRequest(
                new OriginalIndices(searchRequest),
                searchRequest,
                shardId,
                0,
                1,
                AliasFilter.EMPTY,
                1.0f,
                System.currentTimeMillis(),
                null,
                null,
                null,
                SplitShardCountSummary.UNSET,
                true
            );
            try (var out = new BytesStreamOutput()) {
                out.setTransportVersion(TransportVersion.current());
                request.writeTo(out);
                requests.add(new EncodedQueryRequest(out.copyBytes(), TransportVersion.current()));
            }
        }
        return new PhantomQuerySample(requests, null);
    }

    /** Runs what the warming execution is given, and holds the queries until the test completes them. */
    private BlockingQueue<ActionListener<Void>> holdWarmingQueries() {
        final BlockingQueue<ActionListener<Void>> held = new LinkedBlockingQueue<>();
        doAnswer(invocation -> {
            assertSame("the copy that recovers", indexShard, invocation.getArgument(1));
            assertThat(((ShardSearchRequest) invocation.getArgument(0)).shardId(), equalTo(shardId));
            held.add(invocation.getArgument(5));
            return null;
        }).when(searchService).executeQueryPhaseForWarming(any(), any(), any(), any(), any(), any());
        return held;
    }

    private ActionListener<Void> awaitQuery(BlockingQueue<ActionListener<Void>> held) throws InterruptedException {
        return Objects.requireNonNull(held.poll(SAFE_AWAIT_TIMEOUT.seconds(), TimeUnit.SECONDS), "a phantom query is executed");
    }

    public void testTheSampleIsReplayedAfterItWasCollectedAndTheRecoveryWaitsForTheReplay() throws Exception {
        enablePhantomQueriesWithAStartedPeer();
        startWarmingWindowWithTimeLeft();
        final var held = holdWarmingQueries();
        final var response = startRecovery();
        preRecoveryListener.get().onResponse(null);
        assertThat(actionsSentToPeer(), equalTo(List.of(START_SAMPLING, COLLECT_SAMPLE)));
        assertThat("nothing is replayed before the sample is collected", held, hasSize(0));

        answerLastRequestToPeer(sampleOf(2));

        final var first = awaitQuery(held);
        final var second = awaitQuery(held);
        assertThat("the recovery waits for the replay", response.isDone(), is(false));
        assertThat("and keeps its warming window for the replay", windowDeadline(), not(equalTo(OptionalLong.empty())));
        first.onResponse(null);
        assertThat(response.isDone(), is(false));
        second.onResponse(null);

        assertThat(safeGet(response), equalTo(ActionResponse.Empty.INSTANCE));
        assertThat(replayOutcomes(), equalTo(List.of("COMPLETED")));
        assertThat(replays().get(0).attributes().get(PhantomQueryMetrics.RECOVERY_TYPE_ATTRIBUTE), equalTo("NEW_COPY"));
        assertThat(windowDeadline(), equalTo(OptionalLong.empty()));
    }

    public void testTheReplayDoesNotNeedTheSearchReadinessOfTheShard() throws Exception {
        enablePhantomQueriesWithAStartedPeer();
        startWarmingWindowWithTimeLeft();
        final var held = holdWarmingQueries();
        final var response = startRecovery();
        preRecoveryListener.get().onResponse(null);
        answerLastRequestToPeer(sampleOf(1));

        awaitQuery(held).onResponse(null);

        assertThat(safeGet(response), equalTo(ActionResponse.Empty.INSTANCE));
        verify(indexShard, never()).acquireSearcher(anyString());
        verify(indexShard, never()).acquireExternalSearcherSupplier(any());
    }

    public void testFailedQueriesDoNotFailTheRecovery() throws Exception {
        enablePhantomQueriesWithAStartedPeer();
        startWarmingWindowWithTimeLeft();
        final var held = holdWarmingQueries();
        final var response = startRecovery();
        preRecoveryListener.get().onResponse(null);
        answerLastRequestToPeer(sampleOf(2));

        awaitQuery(held).onFailure(new IllegalArgumentException("the query needs remote work"));
        awaitQuery(held).onFailure(new ElasticsearchException("simulated"));

        assertThat(safeGet(response), equalTo(ActionResponse.Empty.INSTANCE));
        assertThat(replayOutcomes(), equalTo(List.of("COMPLETED")));
    }

    public void testTheReplayOfARelocationTargetIsRecordedAsARelocation() throws Exception {
        final ClusterState state = ClusterStateCreationUtils.state(
            DEFAULT_PROJECT_ID,
            shardId.getIndexName(),
            true,
            ShardRoutingState.STARTED,
            ShardRouting.Role.INDEX_ONLY,
            List.of(new Tuple<>(ShardRoutingState.RELOCATING, ShardRouting.Role.SEARCH_ONLY))
        );
        final ShardRouting target = state.routingTable(DEFAULT_PROJECT_ID)
            .shardRoutingTable(shardId)
            .allShards()
            .filter(ShardRouting::relocating)
            .findFirst()
            .orElseThrow()
            .getTargetRelocatingShard();
        assertThat(target.relocatingNodeId(), notNullValue());
        allocationId = target.allocationId().getId();
        when(phantomQueriesSettings.enabled()).thenReturn(true);
        when(indexShard.routingEntry()).thenReturn(target);
        when(clusterService.state()).thenReturn(state);
        when(clusterService.localNode()).thenReturn(state.nodes().get(target.currentNodeId()));
        final var connection = mock(Transport.Connection.class);
        when(connection.getTransportVersion()).thenReturn(TransportVersion.current());
        when(transportService.getConnection(any(DiscoveryNode.class))).thenReturn(connection);
        doAnswer(invocation -> {
            sentToPeer.add(new SentToPeer(invocation.getArgument(1), invocation.getArgument(2), invocation.getArgument(5)));
            return null;
        }).when(transportService)
            .sendChildRequest(
                any(Transport.Connection.class),
                anyString(),
                any(TransportRequest.class),
                any(Task.class),
                any(TransportRequestOptions.class),
                any()
            );
        startWarmingWindowWithTimeLeft();
        final var held = holdWarmingQueries();
        final var response = startRecovery();
        // the handoff is not what is tested here: it is skipped when the source is gone from the cluster state by the time of the handoff
        final String sourceNodeId = target.relocatingNodeId();
        when(clusterService.state()).thenReturn(
            ClusterState.builder(state).nodes(DiscoveryNodes.builder(state.nodes()).remove(sourceNodeId)).build()
        );
        preRecoveryListener.get().onResponse(null);
        assertThat(
            "the source is the peer, and sampling starts before the engine opens",
            actionsSentToPeer(),
            equalTo(List.of(START_SAMPLING, COLLECT_SAMPLE))
        );

        answerLastRequestToPeer(sampleOf(1));
        awaitQuery(held).onResponse(null);

        assertThat(safeGet(response), equalTo(ActionResponse.Empty.INSTANCE));
        assertThat(replayOutcomes(), equalTo(List.of("COMPLETED")));
        assertThat(replays().get(0).attributes().get(PhantomQueryMetrics.RECOVERY_TYPE_ATTRIBUTE), equalTo("RELOCATION"));
    }

    public void testNothingIsReplayedWhenTheSettingWasDisabledAfterSamplingStarted() throws Exception {
        enablePhantomQueriesWithAStartedPeer();
        startWarmingWindowWithTimeLeft();
        final var held = holdWarmingQueries();
        final var response = startRecovery();
        preRecoveryListener.get().onResponse(null);
        assertThat(actionsSentToPeer(), equalTo(List.of(START_SAMPLING, COLLECT_SAMPLE)));

        when(phantomQueriesSettings.enabled()).thenReturn(false);
        answerLastRequestToPeer(sampleOf(2));

        assertThat(safeGet(response), equalTo(ActionResponse.Empty.INSTANCE));
        assertThat(held, hasSize(0));
        assertThat(replayOutcomes(), equalTo(List.of("DISABLED")));
    }

    public void testNoSampleIsRecordedWhenCollectingTheSampleFailed() {
        enablePhantomQueriesWithAStartedPeer();
        startWarmingWindowWithTimeLeft();
        final var response = startRecovery();
        preRecoveryListener.get().onResponse(null);

        sentToPeer.getLast().handler().handleException(new TransportException("simulated, for example a timeout"));

        assertThat(safeGet(response), equalTo(ActionResponse.Empty.INSTANCE));
        assertThat(replayOutcomes(), equalTo(List.of("NO_SAMPLE")));
    }

    public void testNoSampleIsRecordedWhenStartingToSampleFailed() {
        enablePhantomQueriesWithAStartedPeer();
        doThrow(new IllegalStateException("simulated")).when(transportService).getConnection(any(DiscoveryNode.class));
        startWarmingWindowWithTimeLeft();
        final var response = startRecovery();
        preRecoveryListener.get().onResponse(null);

        assertThat(safeGet(response), equalTo(ActionResponse.Empty.INSTANCE));
        assertThat(replayOutcomes(), equalTo(List.of("NO_SAMPLE")));
    }

    public void testNoSampleIsRecordedForAnEmptySample() {
        enablePhantomQueriesWithAStartedPeer();
        startWarmingWindowWithTimeLeft();
        final var response = startRecovery();
        preRecoveryListener.get().onResponse(null);

        answerLastRequestToPeer(PhantomQuerySample.EMPTY);

        assertThat(safeGet(response), equalTo(ActionResponse.Empty.INSTANCE));
        assertThat(replayOutcomes(), equalTo(List.of("NO_SAMPLE")));
    }

    public void testNoTimeLeftAndNoWindowAreRecorded() {
        enablePhantomQueriesWithAStartedPeer();
        windows.startWindow(shardId, allocationId, threadPool.relativeTimeInMillis() - randomLongBetween(0, 60_000L));
        final var noTimeLeft = startRecovery();
        preRecoveryListener.getAndSet(null).onResponse(null);
        safeGet(noTimeLeft);
        assertThat(replayOutcomes(), equalTo(List.of("NO_TIME_LEFT")));

        windows.removeWindow(shardId, allocationId);
        final var noWindow = startRecovery();
        preRecoveryListener.get().onResponse(null);
        safeGet(noWindow);
        assertThat(replayOutcomes(), equalTo(List.of("NO_TIME_LEFT", "NO_WINDOW")));
    }

    public void testDisabledIsRecordedWhileTheSettingIsOff() {
        startWarmingWindowWithTimeLeft();
        final var response = startRecovery();
        preRecoveryListener.get().onResponse(null);

        assertThat(safeGet(response), equalTo(ActionResponse.Empty.INSTANCE));
        assertThat(replayOutcomes(), equalTo(List.of("DISABLED")));
    }

    public void testNothingIsReplayedWhenTheRecoveryFailsBeforeTheEngineIsOpen() {
        enablePhantomQueriesWithAStartedPeer();
        startWarmingWindowWithTimeLeft();
        final var held = holdWarmingQueries();

        safeAwaitFailure(ActionResponse.Empty.class, listener -> {
            startRecovery(listener);
            preRecoveryListener.get().onFailure(new ElasticsearchException("simulated"));
        });

        assertThat(held, hasSize(0));
        assertThat(replays(), hasSize(0));
    }
}
