/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.recovery.phantom;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.telemetry.InstrumentType;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.RecordingMeterRegistry;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.transport.NodeNotConnectedException;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.stateless.cache.SearchRecoveryWarmingWindows;
import org.elasticsearch.xpack.stateless.recovery.phantom.PhantomQuerySamplingActions.CollectSampleRequest;
import org.elasticsearch.xpack.stateless.recovery.phantom.PhantomQuerySamplingActions.CollectSampleResponse;
import org.elasticsearch.xpack.stateless.recovery.phantom.PhantomQuerySamplingActions.StartSamplingRequest;
import org.elasticsearch.xpack.stateless.recovery.phantom.PhantomQuerySamplingActions.StartSamplingResponse;
import org.junit.Before;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import static org.elasticsearch.xpack.stateless.recovery.phantom.PhantomQueryTestUtils.clusterStateWithInitializingSearchReplica;
import static org.elasticsearch.xpack.stateless.recovery.phantom.PhantomQueryTestUtils.clusterStateWithRelocatingSearchReplica;
import static org.elasticsearch.xpack.stateless.recovery.phantom.PhantomQueryTestUtils.counterBy;
import static org.elasticsearch.xpack.stateless.recovery.phantom.PhantomQueryTestUtils.counterTotal;
import static org.elasticsearch.xpack.stateless.recovery.phantom.PhantomQueryTestUtils.encode;
import static org.elasticsearch.xpack.stateless.recovery.phantom.PhantomQueryTestUtils.namedWriteableRegistry;
import static org.elasticsearch.xpack.stateless.recovery.phantom.PhantomQueryTestUtils.queryRequest;
import static org.elasticsearch.xpack.stateless.recovery.phantom.PhantomQueryTestUtils.shards;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.in;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PhantomQuerySampleRequesterTests extends ESTestCase {

    private static final TimeValue REQUEST_TIMEOUT = TimeValue.timeValueSeconds(30);

    private record Sent(
        Transport.Connection connection,
        String action,
        TransportRequest request,
        Task parentTask,
        TransportRequestOptions options,
        TransportResponseHandler<?> handler
    ) {}

    private final ShardId shardId = PhantomQueryTestUtils.shardId();
    private final List<Sent> sent = new ArrayList<>();
    private final Map<String, Transport.Connection> connectionsByNodeId = new HashMap<>();
    private final AtomicLong now = new AtomicLong(10_000L);
    private TransportVersion connectionVersion;
    private TransportService transportService;
    private ClusterService clusterService;
    private PhantomQueriesSettings settings;
    private SearchRecoveryWarmingWindows windows;
    private RecordingMeterRegistry meterRegistry;
    private ShardRouting relocatingSource;
    private PhantomQuerySampleRequester requester;
    private Task task;

    @Before
    public void createRequester() {
        connectionVersion = TransportVersion.current();
        transportService = mock(TransportService.class);
        when(transportService.getConnection(any(DiscoveryNode.class))).thenAnswer(invocation -> {
            final DiscoveryNode node = invocation.getArgument(0);
            return connectionsByNodeId.computeIfAbsent(node.getId(), id -> {
                final var connection = mock(Transport.Connection.class);
                when(connection.getTransportVersion()).thenAnswer(i -> connectionVersion);
                return connection;
            });
        });
        doAnswer(invocation -> {
            sent.add(
                new Sent(
                    invocation.getArgument(0),
                    invocation.getArgument(1),
                    invocation.getArgument(2),
                    invocation.getArgument(3),
                    invocation.getArgument(4),
                    invocation.getArgument(5)
                )
            );
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
        clusterService = mock(ClusterService.class);
        settings = mock(PhantomQueriesSettings.class);
        when(settings.enabled()).thenReturn(true);
        windows = new SearchRecoveryWarmingWindows();
        meterRegistry = new RecordingMeterRegistry();
        task = mock(Task.class);
        requester = new PhantomQuerySampleRequester(
            transportService,
            clusterService,
            settings,
            windows,
            new PhantomQueryMetrics(meterRegistry),
            EsExecutors.DIRECT_EXECUTOR_SERVICE,
            REQUEST_TIMEOUT,
            now::get,
            ESTestCase::random
        );
    }

    private ShardRouting runAsRecoveringCopy(ClusterState state, ShardRouting recoveringCopy) {
        when(clusterService.state()).thenReturn(state);
        when(clusterService.localNode()).thenReturn(state.nodes().get(recoveringCopy.currentNodeId()));
        return recoveringCopy;
    }

    private ShardRouting recoveringNewCopy() {
        final ClusterState state = clusterStateWithInitializingSearchReplica();
        return runAsRecoveringCopy(state, shards(state).stream().filter(ShardRouting::initializing).findFirst().orElseThrow());
    }

    private ShardRouting recoveringRelocationTarget() {
        final ClusterState state = clusterStateWithRelocatingSearchReplica();
        relocatingSource = shards(state).stream().filter(ShardRouting::relocating).findFirst().orElseThrow();
        return runAsRecoveringCopy(state, relocatingSource.getTargetRelocatingShard());
    }

    private static String allocationId(ShardRouting routing) {
        return routing.allocationId().getId();
    }

    private Sent onlySent() {
        assertThat(sent, hasSize(1));
        return sent.get(0);
    }

    private void giveWindowWithTimeLeft(ShardRouting recovering) {
        windows.startWindow(shardId, allocationId(recovering), now.get() + randomLongBetween(1, 60_000));
    }

    @SuppressWarnings("unchecked")
    private static <T extends TransportResponse> TransportResponseHandler<T> handlerOf(Sent sent) {
        return (TransportResponseHandler<T>) sent.handler();
    }

    private PhantomQuerySample collect(PhantomQuerySampleRequester.Sampling sampling) {
        final var future = new PlainActionFuture<PhantomQuerySample>();
        sampling.collectSample(task, future);
        return future.isDone() ? safeGet(future) : null;
    }

    public void testRelocationTargetAsksItsSourceToSampleWithoutWaitingForTheAnswer() {
        final ShardRouting target = recoveringRelocationTarget();

        requester.startSampling(task, target);

        final Sent start = onlySent();
        assertThat(start.action(), equalTo(PhantomQuerySamplingActions.START_SAMPLING_ACTION_NAME));
        assertThat(start.connection(), sameInstance(connectionsByNodeId.get(relocatingSource.currentNodeId())));
        assertThat(start.parentTask(), sameInstance(task));
        assertThat(start.options().timeout(), equalTo(REQUEST_TIMEOUT));
        assertThat(start.request(), instanceOf(StartSamplingRequest.class));
        final var request = (StartSamplingRequest) start.request();
        assertThat(request.shardId(), equalTo(shardId));
        assertThat(request.targetAllocationId(), equalTo(allocationId(target)));
    }

    public void testNewCopyAsksOneOfTheStartedSearchCopiesToSample() {
        final ShardRouting recovering = recoveringNewCopy();
        final Set<String> startedNodeIds = new HashSet<>();
        shards(clusterService.state()).stream()
            .filter(routing -> routing.started() && routing.isSearchable())
            .forEach(routing -> startedNodeIds.add(routing.currentNodeId()));

        requester.startSampling(task, recovering);

        final Sent start = onlySent();
        assertThat(start.action(), equalTo(PhantomQuerySamplingActions.START_SAMPLING_ACTION_NAME));
        assertThat(connectionsByNodeId.keySet().iterator().next(), is(in(startedNodeIds)));
        assertThat(start.connection(), sameInstance(connectionsByNodeId.values().iterator().next()));
        assertThat(((StartSamplingRequest) start.request()).targetAllocationId(), equalTo(allocationId(recovering)));
    }

    public void testAsksNobodyWhilePhantomQueriesAreDisabled() {
        when(settings.enabled()).thenReturn(false);

        final var sampling = requester.startSampling(task, recoveringNewCopy());

        assertThat(sent, empty());
        assertThat(collect(sampling).isEmpty(), is(true));
        assertThat(sent, empty());
    }

    public void testAsksNobodyWhenTheShardHasNoStartedSearchCopy() {
        final ClusterState state = PhantomQueryTestUtils.clusterStateWithInitializingSearchReplica();
        final var withoutStartedCopy = PhantomQueryTestUtils.withShards(
            state,
            shards(state).stream().filter(routing -> routing.started() == false || routing.isSearchable() == false).toList()
        );
        final ShardRouting recovering = runAsRecoveringCopy(
            withoutStartedCopy,
            shards(withoutStartedCopy).stream().filter(ShardRouting::initializing).findFirst().orElseThrow()
        );

        requester.startSampling(task, recovering);

        assertThat(sent, empty());
    }

    public void testAsksNobodyWhenTheConnectionToThePeerDoesNotSupportSampling() {
        connectionVersion = TransportVersionUtils.randomVersionNotSupporting(PhantomQuerySamplingActions.TRANSPORT_VERSION);
        final ShardRouting recovering = randomBoolean() ? recoveringNewCopy() : recoveringRelocationTarget();
        giveWindowWithTimeLeft(recovering);

        final var sampling = requester.startSampling(task, recovering);

        assertThat(sent, empty());
        assertThat("there is nothing to collect", collect(sampling).isEmpty(), is(true));
        assertThat(sent, empty());
    }

    public void testAsksNobodyWhenThePeerIsNotConnected() {
        final ShardRouting recovering = recoveringRelocationTarget();
        doThrow(new NodeNotConnectedException(clusterService.state().nodes().get(relocatingSource.currentNodeId()), "not connected")).when(
            transportService
        ).getConnection(any(DiscoveryNode.class));

        final var sampling = requester.startSampling(task, recovering);

        assertThat(sent, empty());
        assertThat(collect(sampling).isEmpty(), is(true));
    }

    public void testFailureToSendTheRequestIsNotThrown() {
        final ShardRouting recovering = recoveringRelocationTarget();
        giveWindowWithTimeLeft(recovering);
        doThrow(new IllegalStateException("simulated")).when(transportService)
            .sendChildRequest(
                any(Transport.Connection.class),
                anyString(),
                any(TransportRequest.class),
                any(Task.class),
                any(TransportRequestOptions.class),
                any()
            );

        final var sampling = requester.startSampling(task, recovering);

        assertThat(collect(sampling).isEmpty(), is(true));
    }

    public void testUnexpectedFailureToChooseAPeerIsNotThrown() {
        final ShardRouting recovering = recoveringNewCopy();
        when(clusterService.state()).thenThrow(new IllegalStateException("simulated"));

        final var sampling = requester.startSampling(task, recovering);

        assertThat(sent, empty());
        assertThat(collect(sampling).isEmpty(), is(true));
    }

    public void testAnswerOfThePeerToStartSamplingChangesNothing() {
        final ShardRouting recovering = recoveringRelocationTarget();
        giveWindowWithTimeLeft(recovering);
        final var sampling = requester.startSampling(task, recovering);
        final TransportResponseHandler<StartSamplingResponse> handler = handlerOf(onlySent());

        handler.handleResponse(new StartSamplingResponse(randomBoolean()));
        handler.handleException(new TransportException("simulated"));

        assertThat(sent, hasSize(1));
        collect(sampling);
        assertThat("the sample is collected whatever the peer answered", sent, hasSize(2));
    }

    public void testCollectsTheSampleOfThePeerWhileTheWindowHasTimeLeft() {
        final ShardRouting recovering = recoveringRelocationTarget();
        giveWindowWithTimeLeft(recovering);
        final var sampling = requester.startSampling(task, recovering);
        sent.clear();
        final var future = new PlainActionFuture<PhantomQuerySample>();

        sampling.collectSample(task, future);

        final Sent collect = onlySent();
        assertThat(collect.action(), equalTo(PhantomQuerySamplingActions.COLLECT_SAMPLE_ACTION_NAME));
        assertThat(collect.connection(), sameInstance(connectionsByNodeId.get(relocatingSource.currentNodeId())));
        assertThat(collect.parentTask(), sameInstance(task));
        assertThat(collect.options().timeout(), equalTo(REQUEST_TIMEOUT));
        final var request = (CollectSampleRequest) collect.request();
        assertThat(request.shardId(), equalTo(shardId));
        assertThat(request.targetAllocationId(), equalTo(allocationId(recovering)));
        assertThat("the recovery waits for the peer", future.isDone(), is(false));

        final var sample = new PhantomQuerySample(List.of(encode(queryRequest(shardId, "value"))), namedWriteableRegistry());
        final TransportResponseHandler<CollectSampleResponse> handler = handlerOf(collect);
        handler.handleResponse(new CollectSampleResponse(sample));

        assertThat(safeGet(future), sameInstance(sample));
    }

    public void testNewCopyCollectsTheSampleOfItsPeer() {
        final ShardRouting recovering = recoveringNewCopy();
        giveWindowWithTimeLeft(recovering);
        final var sampling = requester.startSampling(task, recovering);
        final DiscoveryNode peer = clusterService.state().nodes().get(connectionsByNodeId.keySet().iterator().next());
        sent.clear();

        sampling.collectSample(task, new PlainActionFuture<>());

        final Sent collect = onlySent();
        assertThat(collect.action(), equalTo(PhantomQuerySamplingActions.COLLECT_SAMPLE_ACTION_NAME));
        assertThat(
            "the sample comes from the peer that was asked to sample",
            collect.connection(),
            sameInstance(connectionsByNodeId.get(peer.getId()))
        );
    }

    public void testCollectsNothingWhenTheRecoveryHasNoWarmingWindow() {
        final ShardRouting recovering = recoveringRelocationTarget();
        final var sampling = requester.startSampling(task, recovering);
        sent.clear();

        assertThat(collect(sampling).isEmpty(), is(true));

        assertThat(sent, empty());
    }

    public void testCollectsNothingWhenTheWarmingWindowIsUsedUp() {
        final ShardRouting recovering = recoveringRelocationTarget();
        final var sampling = requester.startSampling(task, recovering);
        sent.clear();

        // a deadline of exactly now has no time left either
        windows.startWindow(shardId, allocationId(recovering), now.get() - randomLongBetween(0, 60_000));
        assertThat(collect(sampling).isEmpty(), is(true));

        assertThat(sent, empty());
    }

    public void testCollectsWhenAtLeastAMillisecondIsLeft() {
        final ShardRouting recovering = recoveringRelocationTarget();
        final var sampling = requester.startSampling(task, recovering);
        sent.clear();
        windows.startWindow(shardId, allocationId(recovering), now.get() + 1);

        sampling.collectSample(task, new PlainActionFuture<>());

        assertThat(sent, hasSize(1));
    }

    public void testWindowOfAnotherCopyDoesNotCount() {
        final ShardRouting recovering = recoveringRelocationTarget();
        final var sampling = requester.startSampling(task, recovering);
        sent.clear();
        windows.startWindow(shardId, randomUUID(), now.get() + 60_000);

        assertThat(collect(sampling).isEmpty(), is(true));

        assertThat(sent, empty());
    }

    public void testCollectsNothingWhenPhantomQueriesWereDisabledSinceTheRequestToSample() {
        final ShardRouting recovering = recoveringRelocationTarget();
        giveWindowWithTimeLeft(recovering);
        final var sampling = requester.startSampling(task, recovering);
        sent.clear();
        when(settings.enabled()).thenReturn(false);

        assertThat(collect(sampling).isEmpty(), is(true));

        assertThat(sent, empty());
    }

    public void testFailureToCollectTheSampleGivesAnEmptySample() {
        final ShardRouting recovering = recoveringRelocationTarget();
        giveWindowWithTimeLeft(recovering);
        final var sampling = requester.startSampling(task, recovering);
        sent.clear();
        final var future = new PlainActionFuture<PhantomQuerySample>();
        sampling.collectSample(task, future);
        final TransportResponseHandler<CollectSampleResponse> handler = handlerOf(onlySent());

        handler.handleException(new TransportException("simulated, for example a timeout"));

        assertThat(safeGet(future), sameInstance(PhantomQuerySample.EMPTY));
    }

    public void testFailureToSendTheRequestToCollectGivesAnEmptySample() {
        final ShardRouting recovering = recoveringRelocationTarget();
        giveWindowWithTimeLeft(recovering);
        final var sampling = requester.startSampling(task, recovering);
        sent.clear();
        doThrow(new IllegalStateException("simulated")).when(transportService).getConnection(any(DiscoveryNode.class));

        assertThat(collect(sampling), sameInstance(PhantomQuerySample.EMPTY));

        assertThat(sent, empty());
    }

    public void testNothingToCollectWhenNobodyWasAsked() {
        assertThat(collect(PhantomQuerySampleRequester.Sampling.NONE), sameInstance(PhantomQuerySample.EMPTY));

        assertThat(sent, empty());
    }

    // ---- sample collection metrics

    private Map<String, Long> collections() {
        return counterBy(meterRegistry, PhantomQueryMetrics.SAMPLE_COLLECTION_COUNTER, PhantomQueryMetrics.COLLECTION_OUTCOME_ATTRIBUTE);
    }

    private Measurement onlyCollection() {
        final List<Measurement> measurements = meterRegistry.getRecorder()
            .getMeasurements(InstrumentType.LONG_COUNTER, PhantomQueryMetrics.SAMPLE_COLLECTION_COUNTER);
        assertThat(measurements, hasSize(1));
        assertThat(measurements.get(0).getLong(), equalTo(1L));
        return measurements.get(0);
    }

    private void assertOnlyCollection(String outcome, String recoveryType) {
        final Measurement collection = onlyCollection();
        assertThat(
            collection.attributes(),
            equalTo(
                Map.of(PhantomQueryMetrics.COLLECTION_OUTCOME_ATTRIBUTE, outcome, PhantomQueryMetrics.RECOVERY_TYPE_ATTRIBUTE, recoveryType)
            )
        );
    }

    private PhantomQuerySample collectFromPeer(ShardRouting recovering, PhantomQuerySample sampleOfThePeer) {
        giveWindowWithTimeLeft(recovering);
        final var sampling = requester.startSampling(task, recovering);
        sent.clear();
        final var future = new PlainActionFuture<PhantomQuerySample>();
        sampling.collectSample(task, future);
        final TransportResponseHandler<CollectSampleResponse> handler = handlerOf(onlySent());
        handler.handleResponse(new CollectSampleResponse(sampleOfThePeer));
        return safeGet(future);
    }

    public void testCountsASampleThatWasCollected() {
        final boolean relocation = randomBoolean();
        final ShardRouting recovering = relocation ? recoveringRelocationTarget() : recoveringNewCopy();
        final var sample = new PhantomQuerySample(List.of(encode(queryRequest(shardId, "value"))), namedWriteableRegistry());

        assertThat(collectFromPeer(recovering, sample), sameInstance(sample));

        assertOnlyCollection("COLLECTED", relocation ? "RELOCATION" : "NEW_COPY");
    }

    public void testCountsAnEmptySampleThatThePeerReturned() {
        final boolean relocation = randomBoolean();
        final ShardRouting recovering = relocation ? recoveringRelocationTarget() : recoveringNewCopy();

        assertThat(collectFromPeer(recovering, PhantomQuerySample.EMPTY).isEmpty(), is(true));

        assertOnlyCollection("EMPTY", relocation ? "RELOCATION" : "NEW_COPY");
    }

    public void testNothingIsCountedBeforeTheSampleIsCollected() {
        final ShardRouting recovering = recoveringRelocationTarget();
        giveWindowWithTimeLeft(recovering);

        requester.startSampling(task, recovering);

        assertThat(counterTotal(meterRegistry, PhantomQueryMetrics.SAMPLE_COLLECTION_COUNTER), equalTo(0L));
    }

    public void testCountsARecoveryThatHasNoPeerWhenTheShardHasNoStartedSearchCopy() {
        final ClusterState state = PhantomQueryTestUtils.clusterStateWithInitializingSearchReplica();
        final var withoutStartedCopy = PhantomQueryTestUtils.withShards(
            state,
            shards(state).stream().filter(routing -> routing.started() == false || routing.isSearchable() == false).toList()
        );
        final ShardRouting recovering = runAsRecoveringCopy(
            withoutStartedCopy,
            shards(withoutStartedCopy).stream().filter(ShardRouting::initializing).findFirst().orElseThrow()
        );

        final var sampling = requester.startSampling(task, recovering);

        assertOnlyCollection("NO_PEER", "NEW_COPY");
        collect(sampling);
        assertThat(
            "collecting nothing records nothing more",
            counterTotal(meterRegistry, PhantomQueryMetrics.SAMPLE_COLLECTION_COUNTER),
            equalTo(1L)
        );
    }

    public void testCountsARecoveryThatHasNoPeerWhenNoPeerSupportsSampling() {
        connectionVersion = TransportVersionUtils.randomVersionNotSupporting(PhantomQuerySamplingActions.TRANSPORT_VERSION);
        final boolean relocation = randomBoolean();
        final ShardRouting recovering = relocation ? recoveringRelocationTarget() : recoveringNewCopy();
        giveWindowWithTimeLeft(recovering);

        collect(requester.startSampling(task, recovering));

        assertOnlyCollection("NO_PEER", relocation ? "RELOCATION" : "NEW_COPY");
    }

    public void testCountsARecoveryThatHasNoPeerWhenThePeerIsNotConnected() {
        final ShardRouting recovering = recoveringRelocationTarget();
        doThrow(new NodeNotConnectedException(clusterService.state().nodes().get(relocatingSource.currentNodeId()), "not connected")).when(
            transportService
        ).getConnection(any(DiscoveryNode.class));

        collect(requester.startSampling(task, recovering));

        assertOnlyCollection("NO_PEER", "RELOCATION");
    }

    public void testCountsNothingWhilePhantomQueriesAreDisabled() {
        when(settings.enabled()).thenReturn(false);

        collect(requester.startSampling(task, recoveringNewCopy()));

        assertThat(
            "the replay records that they were disabled",
            counterTotal(meterRegistry, PhantomQueryMetrics.SAMPLE_COLLECTION_COUNTER),
            equalTo(0L)
        );
    }

    public void testCountsAnUnexpectedFailureToChooseAPeerAsFailed() {
        final ShardRouting recovering = recoveringNewCopy();
        when(clusterService.state()).thenThrow(new IllegalStateException("simulated"));

        collect(requester.startSampling(task, recovering));

        assertOnlyCollection("FAILED", "NEW_COPY");
    }

    public void testCountsAFailureToSendTheRequestToStartAsFailedOnce() {
        final ShardRouting recovering = recoveringRelocationTarget();
        giveWindowWithTimeLeft(recovering);
        doThrow(new IllegalStateException("simulated")).when(transportService)
            .sendChildRequest(
                any(Transport.Connection.class),
                anyString(),
                any(TransportRequest.class),
                any(Task.class),
                any(TransportRequestOptions.class),
                any()
            );

        collect(requester.startSampling(task, recovering));

        assertOnlyCollection("FAILED", "RELOCATION");
    }

    public void testCountsACollectionThatWasNotAttemptedAsSkipped() {
        // no window: warming that does not wait
        ShardRouting recovering = recoveringRelocationTarget();
        collect(requester.startSampling(task, recovering));
        assertThat(collections(), equalTo(Map.of("SKIPPED", 1L)));

        // a window that is used up
        recovering = recoveringRelocationTarget();
        final var usedUp = requester.startSampling(task, recovering);
        windows.startWindow(shardId, allocationId(recovering), now.get() - randomLongBetween(0, 60_000));
        collect(usedUp);
        assertThat(collections(), equalTo(Map.of("SKIPPED", 2L)));

        // the setting was disabled since the request to sample
        recovering = recoveringRelocationTarget();
        giveWindowWithTimeLeft(recovering);
        final var disabledSince = requester.startSampling(task, recovering);
        when(settings.enabled()).thenReturn(false);
        collect(disabledSince);
        assertThat(collections(), equalTo(Map.of("SKIPPED", 3L)));
        assertThat(
            sent.stream().map(Sent::action).filter(PhantomQuerySamplingActions.COLLECT_SAMPLE_ACTION_NAME::equals).count(),
            equalTo(0L)
        );
    }

    public void testCountsAFailedOrTimedOutCollectionAsFailed() {
        final ShardRouting recovering = recoveringRelocationTarget();
        giveWindowWithTimeLeft(recovering);
        final var sampling = requester.startSampling(task, recovering);
        sent.clear();
        sampling.collectSample(task, new PlainActionFuture<>());
        final TransportResponseHandler<CollectSampleResponse> handler = handlerOf(onlySent());

        handler.handleException(new TransportException("simulated, for example a timeout"));

        assertOnlyCollection("FAILED", "RELOCATION");
    }

    public void testCountsAFailureToSendTheRequestToCollectAsFailed() {
        final ShardRouting recovering = recoveringNewCopy();
        giveWindowWithTimeLeft(recovering);
        final var sampling = requester.startSampling(task, recovering);
        sent.clear();
        doThrow(new IllegalStateException("simulated")).when(transportService).getConnection(any(DiscoveryNode.class));

        collect(sampling);

        assertOnlyCollection("FAILED", "NEW_COPY");
    }

    public void testCountsNothingForTheCollectionOfNoSampling() {
        collect(PhantomQuerySampleRequester.Sampling.NONE);

        assertThat(counterTotal(meterRegistry, PhantomQueryMetrics.SAMPLE_COLLECTION_COUNTER), equalTo(0L));
    }

    public void testOneRecoveryIsCountedAtMostOnce() {
        final ShardRouting recovering = recoveringRelocationTarget();
        giveWindowWithTimeLeft(recovering);
        final var sampling = requester.startSampling(task, recovering);
        // whatever the peer answers to the request to start sampling
        final TransportResponseHandler<StartSamplingResponse> start = handlerOf(onlySent());
        start.handleResponse(new StartSamplingResponse(randomBoolean()));
        start.handleException(new TransportException("simulated"));
        assertThat(counterTotal(meterRegistry, PhantomQueryMetrics.SAMPLE_COLLECTION_COUNTER), equalTo(0L));
        sent.clear();

        sampling.collectSample(task, new PlainActionFuture<>());
        final TransportResponseHandler<CollectSampleResponse> collect = handlerOf(onlySent());
        collect.handleResponse(new CollectSampleResponse(PhantomQuerySample.EMPTY));

        assertThat(collections(), equalTo(Map.of("EMPTY", 1L)));
    }
}
