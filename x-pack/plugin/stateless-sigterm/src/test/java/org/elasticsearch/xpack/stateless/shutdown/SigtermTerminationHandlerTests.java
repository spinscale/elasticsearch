/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.shutdown;

import org.apache.logging.log4j.Level;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.ShutdownPersistentTasksStatus;
import org.elasticsearch.cluster.metadata.ShutdownPluginsStatus;
import org.elasticsearch.cluster.metadata.ShutdownShardMigrationStatus;
import org.elasticsearch.cluster.metadata.ShutdownShardSnapshotsStatus;
import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.readiness.ReadinessRequest;
import org.elasticsearch.readiness.ReadinessService;
import org.elasticsearch.readiness.TransportReadinessAction;
import org.elasticsearch.telemetry.InstrumentType;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.RecordingMeterRegistry;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.RemoteTransportClient;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.xpack.shutdown.GetShutdownStatusAction;
import org.elasticsearch.xpack.shutdown.PutShutdownNodeAction;
import org.elasticsearch.xpack.shutdown.SingleNodeShutdownStatus;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static org.elasticsearch.action.support.master.MasterNodeRequest.INFINITE_MASTER_NODE_TIMEOUT;
import static org.elasticsearch.cluster.node.DiscoveryNodeRole.INDEX_ROLE;
import static org.elasticsearch.cluster.node.DiscoveryNodeRole.ML_ROLE;
import static org.elasticsearch.cluster.node.DiscoveryNodeRole.SEARCH_ROLE;
import static org.elasticsearch.core.TimeValue.timeValueMillis;
import static org.elasticsearch.core.TimeValue.timeValueSeconds;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SigtermTerminationHandlerTests extends ESTestCase {
    private static final TimeValue SHORT_GRACE_PERIOD = TimeValue.timeValueMinutes(14);
    private static final TimeValue MARKER_CHECK_DELAY = TimeValue.timeValueMinutes(1);

    ThreadPool threadPool;

    @Before
    public void setup() {
        threadPool = new TestThreadPool(this.getTestName());
    }

    @After
    public void teardown() {
        terminate(threadPool);
    }

    @TestLogging(
        reason = "Testing logging at INFO level",
        value = "org.elasticsearch.xpack.stateless.shutdown.SigtermTerminationHandler:INFO"
    )
    public void testShutdownCompletesImmediate() {
        final TimeValue pollInterval = randomPositiveTimeValue();
        final TimeValue timeout = randomPositiveTimeValue();
        final String nodeId = randomAlphaOfLength(10);
        TestThreadPool threadPool = new TestThreadPool(this.getTestName());

        try (var mockLog = MockLog.capture(SigtermTerminationHandler.class)) {
            Client client = mock(Client.class);
            when(client.threadPool()).thenReturn(threadPool);
            doAnswer(invocation -> {
                PutShutdownNodeAction.Request putRequest = invocation.getArgument(1, PutShutdownNodeAction.Request.class);
                assertEquals(timeout, putRequest.ackTimeout());
                assertEquals(timeout, putRequest.masterNodeTimeout());
                assertThat(putRequest.getNodeId(), equalTo(nodeId));
                assertThat(putRequest.getType(), equalTo(SingleNodeShutdownMetadata.Type.SIGTERM));
                ActionListener<AcknowledgedResponse> listener = invocation.getArgument(2);
                listener.onResponse(AcknowledgedResponse.TRUE);
                return null; // real method is void
            }).when(client).execute(eq(PutShutdownNodeAction.INSTANCE), any(), any());

            doAnswer(invocation -> {
                GetShutdownStatusAction.Request getRequest = invocation.getArgument(1, GetShutdownStatusAction.Request.class);
                assertThat(getRequest.getNodeIds()[0], equalTo(nodeId));
                assertEquals(INFINITE_MASTER_NODE_TIMEOUT, getRequest.masterNodeTimeout());
                ActionListener<GetShutdownStatusAction.Response> listener = invocation.getArgument(2);
                listener.onResponse(
                    new GetShutdownStatusAction.Response(
                        Collections.singletonList(
                            new SingleNodeShutdownStatus(
                                SingleNodeShutdownMetadata.builder()
                                    .setNodeId(nodeId)
                                    .setNodeEphemeralId(nodeId)
                                    .setType(SingleNodeShutdownMetadata.Type.SIGTERM)
                                    .setReason(this.getTestName())
                                    .setStartedAtMillis(randomNonNegativeLong())
                                    .setGracePeriod(timeout)
                                    .build(),
                                new ShutdownShardMigrationStatus(SingleNodeShutdownMetadata.Status.COMPLETE, 0, 0, 0),
                                ShutdownPersistentTasksStatus.fromRemainingTasks(0, 0),
                                new ShutdownPluginsStatus(true),
                                ShutdownShardSnapshotsStatus.fromShardCounts(0, 0, 0)
                            )
                        )
                    )
                );
                return null; // real method is void
            }).when(client).execute(eq(GetShutdownStatusAction.INSTANCE), any(), any());

            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "handler started message",
                    SigtermTerminationHandler.class.getCanonicalName(),
                    Level.INFO,
                    "handling graceful shutdown request"
                )
            );
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "handler completed message",
                    SigtermTerminationHandler.class.getCanonicalName(),
                    Level.INFO,
                    "shutdown completed"
                )
            );

            RecordingMeterRegistry meterRegistry = new RecordingMeterRegistry();
            new SigtermTerminationHandler(
                client,
                threadPool,
                null,
                null,
                pollInterval,
                timeout,
                nodeId,
                new SigtermShutdownMetrics(meterRegistry),
                ShortShutdownMarker.disabled(),
                SHORT_GRACE_PERIOD,
                MARKER_CHECK_DELAY
            ).handleTermination();

            mockLog.assertAllExpectationsMatched();

            verify(client, times(1)).execute(eq(PutShutdownNodeAction.INSTANCE), any(), any());
            verify(client, times(1)).execute(eq(GetShutdownStatusAction.INSTANCE), any(), any());

            assertShutdownAndMigrationMetrics(meterRegistry, SigtermTerminationHandler.ShutdownStatus.COMPLETE, false, true);
        } finally {
            threadPool.shutdownNow();
        }
    }

    public void testMigrationMetricSkippedWhenPutShutdownFails() {
        final TimeValue pollInterval = randomPositiveTimeValue();
        final TimeValue timeout = randomPositiveTimeValue();
        final String nodeId = randomAlphaOfLength(10);

        final var client = mock(Client.class);
        when(client.threadPool()).thenReturn(threadPool);
        doAnswer(invocation -> {
            ActionListener<AcknowledgedResponse> listener = invocation.getArgument(2);
            listener.onFailure(new RuntimeException("put shutdown failed"));
            return null;
        }).when(client).execute(eq(PutShutdownNodeAction.INSTANCE), any(), any());

        final var meterRegistry = new RecordingMeterRegistry();
        new SigtermTerminationHandler(
            client,
            threadPool,
            null,
            null,
            pollInterval,
            timeout,
            nodeId,
            new SigtermShutdownMetrics(meterRegistry),
            ShortShutdownMarker.disabled(),
            SHORT_GRACE_PERIOD,
            MARKER_CHECK_DELAY
        ).handleTermination();

        final List<Measurement> shutdownMeasurements = meterRegistry.getRecorder()
            .getMeasurements(InstrumentType.DOUBLE_HISTOGRAM, SigtermShutdownMetrics.SHUTDOWN_DURATION_HISTOGRAM);
        assertThat(shutdownMeasurements, hasSize(1));
        assertThat(
            shutdownMeasurements.getFirst().attributes().get(SigtermShutdownMetrics.ATTRIBUTE_NAME_STATUS),
            equalTo(SigtermTerminationHandler.ShutdownStatus.FAILED.name().toLowerCase(Locale.ROOT))
        );
        assertThat(
            meterRegistry.getRecorder()
                .getMeasurements(InstrumentType.DOUBLE_HISTOGRAM, SigtermShutdownMetrics.SHARD_MIGRATION_DURATION_HISTOGRAM),
            hasSize(0)
        );
    }

    public void testMigrationMetricNotCompletedWhenGetShutdownFails() {
        final TimeValue pollInterval = randomPositiveTimeValue();
        final TimeValue timeout = randomPositiveTimeValue();
        final String nodeId = randomAlphaOfLength(10);

        final var client = mock(Client.class);
        when(client.threadPool()).thenReturn(threadPool);
        doAnswer(invocation -> {
            ActionListener<AcknowledgedResponse> listener = invocation.getArgument(2);
            listener.onResponse(AcknowledgedResponse.TRUE);
            return null;
        }).when(client).execute(eq(PutShutdownNodeAction.INSTANCE), any(), any());
        doAnswer(invocation -> {
            ActionListener<GetShutdownStatusAction.Response> listener = invocation.getArgument(2);
            listener.onFailure(new RuntimeException("get shutdown status failed"));
            return null;
        }).when(client).execute(eq(GetShutdownStatusAction.INSTANCE), any(), any());

        final var meterRegistry = new RecordingMeterRegistry();
        new SigtermTerminationHandler(
            client,
            threadPool,
            null,
            null,
            pollInterval,
            timeout,
            nodeId,
            new SigtermShutdownMetrics(meterRegistry),
            ShortShutdownMarker.disabled(),
            SHORT_GRACE_PERIOD,
            MARKER_CHECK_DELAY
        ).handleTermination();

        final List<Measurement> migrationMeasurements = meterRegistry.getRecorder()
            .getMeasurements(InstrumentType.DOUBLE_HISTOGRAM, SigtermShutdownMetrics.SHARD_MIGRATION_DURATION_HISTOGRAM);
        assertThat(migrationMeasurements, hasSize(1));
        assertThat(
            migrationMeasurements.getFirst().attributes().get(SigtermShutdownMetrics.ATTRIBUTE_NAME_MIGRATION_COMPLETED),
            equalTo(false)
        );

        final List<Measurement> shutdownMeasurements = meterRegistry.getRecorder()
            .getMeasurements(InstrumentType.DOUBLE_HISTOGRAM, SigtermShutdownMetrics.SHUTDOWN_DURATION_HISTOGRAM);
        assertThat(shutdownMeasurements, hasSize(1));
        assertThat(shutdownMeasurements.getFirst().attributes().get(SigtermShutdownMetrics.ATTRIBUTE_NAME_TIMED_OUT), equalTo(false));
    }

    public void testMigrationMetricUsesMigrationCompleteTimeBeforeOverallComplete() throws Exception {
        final var taskQueue = new DeterministicTaskQueue(new Random(random().nextLong()));
        taskQueue.setExecutionDelayVariabilityMillis(0);
        final var deterministicThreadPool = taskQueue.getThreadPool();
        final TimeValue pollInterval = TimeValue.timeValueMillis(5);
        final TimeValue timeout = timeValueSeconds(10);
        final String nodeId = randomAlphaOfLength(10);
        // Capture on the test thread, the GetShutdownStatus mock may run on the handler thread.
        final String reason = getTestName();

        // Three polls: migration COMPLETE on the second, plugins ready only on the last.
        // Migration lasts 1 * pollInterval; overall shutdown lasts 2 * pollInterval.
        final int totalPolls = 3;
        final var rounds = new AtomicInteger(totalPolls);

        final var client = mock(Client.class);
        when(client.threadPool()).thenReturn(deterministicThreadPool);
        doAnswer(invocation -> {
            ActionListener<AcknowledgedResponse> listener = invocation.getArgument(2);
            listener.onResponse(AcknowledgedResponse.TRUE);
            return null;
        }).when(client).execute(eq(PutShutdownNodeAction.INSTANCE), any(), any());

        doAnswer(invocation -> {
            int remaining = rounds.decrementAndGet();
            final boolean migrationComplete = remaining <= 1; // incomplete on first poll, complete from second onward
            final boolean pluginsReady = remaining == 0;
            ActionListener<GetShutdownStatusAction.Response> listener = invocation.getArgument(2);
            listener.onResponse(
                new GetShutdownStatusAction.Response(
                    Collections.singletonList(
                        new SingleNodeShutdownStatus(
                            SingleNodeShutdownMetadata.builder()
                                .setNodeId(nodeId)
                                .setNodeEphemeralId(nodeId)
                                .setType(SingleNodeShutdownMetadata.Type.SIGTERM)
                                .setReason(reason)
                                .setStartedAtMillis(randomNonNegativeLong())
                                .setGracePeriod(timeout)
                                .build(),
                            new ShutdownShardMigrationStatus(
                                migrationComplete
                                    ? SingleNodeShutdownMetadata.Status.COMPLETE
                                    : SingleNodeShutdownMetadata.Status.IN_PROGRESS,
                                0,
                                0,
                                0
                            ),
                            ShutdownPersistentTasksStatus.fromRemainingTasks(0, 0),
                            new ShutdownPluginsStatus(pluginsReady),
                            ShutdownShardSnapshotsStatus.fromShardCounts(0, 0, 0)
                        )
                    )
                )
            );
            return null;
        }).when(client).execute(eq(GetShutdownStatusAction.INSTANCE), any(), any());

        final var meterRegistry = new RecordingMeterRegistry();
        final var handler = new SigtermTerminationHandler(
            client,
            deterministicThreadPool,
            null,
            null,
            pollInterval,
            timeout,
            nodeId,
            new SigtermShutdownMetrics(meterRegistry),
            ShortShutdownMarker.disabled(),
            SHORT_GRACE_PERIOD,
            MARKER_CHECK_DELAY
        );

        // handleTermination blocks on a latch while deferred polls are scheduled. Drive the queue from this thread.
        // First poll runs synchronously on the handler thread and schedules (totalPolls - 1) deferred polls.
        final var handlerThread = new Thread(handler::handleTermination, "sigterm-handler");
        handlerThread.start();
        assertBusy(() -> assertTrue("first poll should have scheduled the next deferred poll", taskQueue.hasDeferredTasks()));
        for (int i = 0; i < totalPolls - 1; i++) {
            assertTrue("expected deferred poll " + (i + 1), taskQueue.hasDeferredTasks());
            taskQueue.advanceTime();
            taskQueue.runAllRunnableTasks();
        }
        assertFalse("no further deferred polls after overall COMPLETE", taskQueue.hasDeferredTasks());
        assertFalse(taskQueue.hasRunnableTasks());
        safeJoin(handlerThread);

        verify(client, times(totalPolls)).execute(eq(GetShutdownStatusAction.INSTANCE), any(), any());

        final List<Measurement> migrationMeasurements = meterRegistry.getRecorder()
            .getMeasurements(InstrumentType.DOUBLE_HISTOGRAM, SigtermShutdownMetrics.SHARD_MIGRATION_DURATION_HISTOGRAM);
        assertThat(migrationMeasurements, hasSize(1));
        assertThat(migrationMeasurements.getFirst().getDouble(), equalTo(pollInterval.millis() / 1000.0));
        assertThat(
            migrationMeasurements.getFirst().attributes().get(SigtermShutdownMetrics.ATTRIBUTE_NAME_MIGRATION_COMPLETED),
            equalTo(true)
        );

        final List<Measurement> shutdownMeasurements = meterRegistry.getRecorder()
            .getMeasurements(InstrumentType.DOUBLE_HISTOGRAM, SigtermShutdownMetrics.SHUTDOWN_DURATION_HISTOGRAM);
        assertThat(shutdownMeasurements, hasSize(1));
        assertThat(shutdownMeasurements.getFirst().getDouble(), equalTo((totalPolls - 1L) * pollInterval.millis() / 1000.0));
        assertThat(
            shutdownMeasurements.getFirst().attributes().get(SigtermShutdownMetrics.ATTRIBUTE_NAME_STATUS),
            equalTo(SigtermTerminationHandler.ShutdownStatus.COMPLETE.name().toLowerCase(Locale.ROOT))
        );
        assertThat(shutdownMeasurements.getFirst().attributes().get(SigtermShutdownMetrics.ATTRIBUTE_NAME_TIMED_OUT), equalTo(false));
    }

    public void testShutdownTimeoutRecordsTimedOutMetrics() {
        final TimeValue pollInterval = TimeValue.timeValueMillis(5);
        // Short timeout so the test waits for the latch to expire rather than for shutdown to complete.
        final TimeValue timeout = TimeValue.timeValueMillis(100);
        final String nodeId = randomAlphaOfLength(10);

        final var testThreadPool = new TestThreadPool(this.getTestName());
        try {
            final var client = mock(Client.class);
            when(client.threadPool()).thenReturn(testThreadPool);
            doAnswer(invocation -> {
                ActionListener<AcknowledgedResponse> listener = invocation.getArgument(2);
                listener.onResponse(AcknowledgedResponse.TRUE);
                return null;
            }).when(client).execute(eq(PutShutdownNodeAction.INSTANCE), any(), any());

            doAnswer(invocation -> {
                ActionListener<GetShutdownStatusAction.Response> listener = invocation.getArgument(2);
                listener.onResponse(
                    new GetShutdownStatusAction.Response(
                        Collections.singletonList(
                            new SingleNodeShutdownStatus(
                                SingleNodeShutdownMetadata.builder()
                                    .setNodeId(nodeId)
                                    .setNodeEphemeralId(nodeId)
                                    .setType(SingleNodeShutdownMetadata.Type.SIGTERM)
                                    .setReason(this.getTestName())
                                    .setStartedAtMillis(randomNonNegativeLong())
                                    .setGracePeriod(timeout)
                                    .build(),
                                // Keep migration incomplete so overall status never reaches COMPLETE before the timeout.
                                new ShutdownShardMigrationStatus(SingleNodeShutdownMetadata.Status.IN_PROGRESS, 0, 0, 0),
                                ShutdownPersistentTasksStatus.fromRemainingTasks(0, 0),
                                new ShutdownPluginsStatus(true),
                                ShutdownShardSnapshotsStatus.fromShardCounts(0, 0, 0)
                            )
                        )
                    )
                );
                return null;
            }).when(client).execute(eq(GetShutdownStatusAction.INSTANCE), any(), any());

            final var meterRegistry = new RecordingMeterRegistry();
            new SigtermTerminationHandler(
                client,
                testThreadPool,
                null,
                null,
                pollInterval,
                timeout,
                nodeId,
                new SigtermShutdownMetrics(meterRegistry),
                ShortShutdownMarker.disabled(),
                SHORT_GRACE_PERIOD,
                MARKER_CHECK_DELAY
            ).handleTermination();

            assertShutdownAndMigrationMetrics(meterRegistry, SigtermTerminationHandler.ShutdownStatus.IN_PROGRESS, true, false);

            final List<Measurement> shutdownMeasurements = meterRegistry.getRecorder()
                .getMeasurements(InstrumentType.DOUBLE_HISTOGRAM, SigtermShutdownMetrics.SHUTDOWN_DURATION_HISTOGRAM);
            assertThat(shutdownMeasurements.getFirst().getDouble(), greaterThanOrEqualTo(timeout.millis() / 1000.0));
        } finally {
            testThreadPool.shutdownNow();
        }
    }

    public void testGracePeriodIsTimeoutWithoutMarker() {
        final var fixture = new HandlerFixture(ShortShutdownMarker.disabled(), HandlerFixture.SHORT_GRACE_PERIOD, null);
        fixture.start();
        fixture.advanceTimeBy(HandlerFixture.MARKER_CHECK_DELAY);
        fixture.advanceTimeBy(HandlerFixture.MARKER_CHECK_DELAY);
        fixture.completeShutdown();

        assertThat(fixture.putRequests, hasSize(1));
        assertThat(fixture.putRequests.getFirst().getGracePeriod(), equalTo(HandlerFixture.TIMEOUT));
        fixture.assertShutdownStatus(SigtermTerminationHandler.ShutdownStatus.COMPLETE);
    }

    public void testShortGracePeriodNotShorterThanTimeoutIsNotUsed() throws IOException {
        // the marker is enabled and does not exist, but the short grace period is no shorter than the timeout, so there is nothing to
        // extend
        final var markerFile = createTempDir().resolve("short-shutdown");
        final var fixture = new HandlerFixture(enabledMarker(markerFile), HandlerFixture.TIMEOUT, null);
        fixture.start();
        fixture.advanceTimeBy(HandlerFixture.MARKER_CHECK_DELAY);
        fixture.advanceTimeBy(HandlerFixture.MARKER_CHECK_DELAY);
        fixture.completeShutdown();

        assertThat(fixture.putRequests, hasSize(1));
        assertThat(fixture.putRequests.getFirst().getGracePeriod(), equalTo(HandlerFixture.TIMEOUT));
    }

    @TestLogging(
        reason = "Testing logging at INFO level",
        value = "org.elasticsearch.xpack.stateless.shutdown.SigtermTerminationHandler:INFO"
    )
    public void testShortGracePeriodKeptWhenMarkerExistsAtSigterm() throws IOException {
        final var markerFile = createTempDir().resolve("short-shutdown");
        Files.createFile(markerFile);

        try (var mockLog = MockLog.capture(SigtermTerminationHandler.class)) {
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "marker found",
                    SigtermTerminationHandler.class.getCanonicalName(),
                    Level.INFO,
                    "short shutdown marker [" + markerFile + "] found, keep grace period [" + HandlerFixture.SHORT_GRACE_PERIOD + "]"
                )
            );

            final var fixture = new HandlerFixture(enabledMarker(markerFile), HandlerFixture.SHORT_GRACE_PERIOD, null);
            fixture.start();
            // the marker was found right away, so there is no check that could extend the grace period later
            fixture.advanceTimeBy(HandlerFixture.MARKER_CHECK_DELAY);
            fixture.advanceTimeBy(HandlerFixture.MARKER_CHECK_DELAY);
            fixture.completeShutdown();

            assertThat(fixture.putRequests, hasSize(1));
            assertThat(fixture.putRequests.getFirst().getGracePeriod(), equalTo(HandlerFixture.SHORT_GRACE_PERIOD));
            mockLog.assertAllExpectationsMatched();
        }
    }

    @TestLogging(
        reason = "Testing logging at INFO level",
        value = "org.elasticsearch.xpack.stateless.shutdown.SigtermTerminationHandler:INFO"
    )
    public void testGracePeriodExtendedWhenMarkerIsMissingAfterCheckDelay() throws IOException {
        final var markerFile = createTempDir().resolve("short-shutdown");

        try (var mockLog = MockLog.capture(SigtermTerminationHandler.class)) {
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "grace period extended",
                    SigtermTerminationHandler.class.getCanonicalName(),
                    Level.INFO,
                    "extended shutdown grace period from [" + HandlerFixture.SHORT_GRACE_PERIOD + "] to [" + HandlerFixture.TIMEOUT + "]"
                )
            );

            final var fixture = new HandlerFixture(enabledMarker(markerFile), HandlerFixture.SHORT_GRACE_PERIOD, null);
            fixture.start();
            assertThat(fixture.putRequests, hasSize(1));
            assertThat(fixture.putRequests.getFirst().getGracePeriod(), equalTo(HandlerFixture.SHORT_GRACE_PERIOD));

            // the grace period is not extended before the check delay has passed
            fixture.advanceTimeBy(timeValueMillis(HandlerFixture.MARKER_CHECK_DELAY.millis() - 1));
            assertThat(fixture.putRequests, hasSize(1));

            fixture.advanceTimeBy(timeValueMillis(1));
            assertThat(fixture.putRequests, hasSize(2));
            final var first = fixture.putRequests.get(0);
            final var second = fixture.putRequests.get(1);
            assertThat(second.getGracePeriod(), equalTo(HandlerFixture.TIMEOUT));
            assertThat(second.getType(), equalTo(first.getType()));
            assertThat(second.getReason(), equalTo(first.getReason()));
            assertThat(second.getNodeId(), equalTo(first.getNodeId()));

            fixture.completeShutdown();
            // the marker is only checked once
            assertThat(fixture.putRequests, hasSize(2));
            fixture.assertShutdownStatus(SigtermTerminationHandler.ShutdownStatus.COMPLETE);
            mockLog.assertAllExpectationsMatched();
        }
    }

    @TestLogging(
        reason = "Testing logging at INFO level",
        value = "org.elasticsearch.xpack.stateless.shutdown.SigtermTerminationHandler:INFO"
    )
    public void testShortGracePeriodKeptWhenMarkerAppearsBeforeCheck() throws IOException {
        final var markerFile = createTempDir().resolve("short-shutdown");

        try (var mockLog = MockLog.capture(SigtermTerminationHandler.class)) {
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "marker found",
                    SigtermTerminationHandler.class.getCanonicalName(),
                    Level.INFO,
                    "short shutdown marker [" + markerFile + "] found, keep grace period [" + HandlerFixture.SHORT_GRACE_PERIOD + "]"
                )
            );

            final var fixture = new HandlerFixture(enabledMarker(markerFile), HandlerFixture.SHORT_GRACE_PERIOD, null);
            fixture.start();
            // the control plane writes the marker after the sigterm, but before the check
            fixture.advanceTimeBy(timeValueMillis(HandlerFixture.MARKER_CHECK_DELAY.millis() / 2));
            Files.createFile(markerFile);
            fixture.advanceTimeBy(HandlerFixture.MARKER_CHECK_DELAY);
            fixture.completeShutdown();

            assertThat(fixture.putRequests, hasSize(1));
            assertThat(fixture.putRequests.getFirst().getGracePeriod(), equalTo(HandlerFixture.SHORT_GRACE_PERIOD));
            mockLog.assertAllExpectationsMatched();
        }
    }

    public void testFailedExtensionDoesNotFailShutdown() throws IOException {
        final var markerFile = createTempDir().resolve("short-shutdown");

        try (var mockLog = MockLog.capture(SigtermTerminationHandler.class)) {
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "extension failed",
                    SigtermTerminationHandler.class.getCanonicalName(),
                    Level.WARN,
                    "failed to extend shutdown grace period from ["
                        + HandlerFixture.SHORT_GRACE_PERIOD
                        + "] to ["
                        + HandlerFixture.TIMEOUT
                        + "]"
                )
            );

            final var fixture = new HandlerFixture(
                enabledMarker(markerFile),
                HandlerFixture.SHORT_GRACE_PERIOD,
                new RuntimeException("extension failed")
            );
            fixture.start();
            fixture.advanceTimeBy(HandlerFixture.MARKER_CHECK_DELAY);
            fixture.advanceTimeBy(HandlerFixture.MARKER_CHECK_DELAY);
            assertThat(fixture.putRequests, hasSize(2));

            // the shutdown is still being handled, and it is not counted as failed
            fixture.completeShutdown();
            fixture.assertShutdownStatus(SigtermTerminationHandler.ShutdownStatus.COMPLETE);
            mockLog.assertAllExpectationsMatched();
        }
    }

    public void testFailedExtensionBecauseNodeIsClosingIsNotLoggedAsWarning() throws IOException {
        final var markerFile = createTempDir().resolve("short-shutdown");

        try (var mockLog = MockLog.capture(SigtermTerminationHandler.class)) {
            mockLog.addExpectation(
                new MockLog.UnseenEventExpectation(
                    "no warning for closing node",
                    SigtermTerminationHandler.class.getCanonicalName(),
                    Level.WARN,
                    "failed to extend shutdown grace period"
                )
            );

            final var fixture = new HandlerFixture(
                enabledMarker(markerFile),
                HandlerFixture.SHORT_GRACE_PERIOD,
                new NodeClosedException(DiscoveryNodeUtils.builder("node").build())
            );
            fixture.start();
            fixture.advanceTimeBy(HandlerFixture.MARKER_CHECK_DELAY);
            fixture.advanceTimeBy(HandlerFixture.MARKER_CHECK_DELAY);
            assertThat(fixture.putRequests, hasSize(2));

            fixture.completeShutdown();
            fixture.assertShutdownStatus(SigtermTerminationHandler.ShutdownStatus.COMPLETE);
            mockLog.assertAllExpectationsMatched();
        }
    }

    /**
     * A marker that cannot be read is treated as missing, so the grace period is extended. Failing to read the marker must not abort the
     * handling of the sigterm, which would leave the node with the short grace period and no extension.
     */
    @TestLogging(
        reason = "Testing logging at WARN and INFO level",
        value = "org.elasticsearch.xpack.stateless.shutdown.SigtermTerminationHandler:INFO"
    )
    public void testUnreadableMarkerIsTreatedAsMissing() throws IOException {
        final var markerFile = createTempDir().resolve("short-shutdown");
        final var unreadableMarker = new ShortShutdownMarker(markerFile) {
            @Override
            boolean exists() {
                throw new RuntimeException("failed to check the marker");
            }
        };

        try (var mockLog = MockLog.capture(SigtermTerminationHandler.class)) {
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "marker could not be checked",
                    SigtermTerminationHandler.class.getCanonicalName(),
                    Level.WARN,
                    "failed to check for short shutdown marker [" + markerFile + "]"
                )
            );
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "grace period extended",
                    SigtermTerminationHandler.class.getCanonicalName(),
                    Level.INFO,
                    "extended shutdown grace period from [" + HandlerFixture.SHORT_GRACE_PERIOD + "] to [" + HandlerFixture.TIMEOUT + "]"
                )
            );

            final var fixture = new HandlerFixture(unreadableMarker, HandlerFixture.SHORT_GRACE_PERIOD, null);
            fixture.start();
            fixture.advanceTimeBy(HandlerFixture.MARKER_CHECK_DELAY);
            fixture.advanceTimeBy(HandlerFixture.MARKER_CHECK_DELAY);

            assertThat(fixture.putRequests, hasSize(2));
            assertThat(fixture.putRequests.getFirst().getGracePeriod(), equalTo(HandlerFixture.SHORT_GRACE_PERIOD));
            assertThat(fixture.putRequests.get(1).getGracePeriod(), equalTo(HandlerFixture.TIMEOUT));

            fixture.completeShutdown();
            fixture.assertShutdownStatus(SigtermTerminationHandler.ShutdownStatus.COMPLETE);
            mockLog.assertAllExpectationsMatched();
        }
    }

    public void testNoExtensionWhenShutdownCompletesBeforeCheck() throws IOException {
        final var markerFile = createTempDir().resolve("short-shutdown");
        final var fixture = new HandlerFixture(enabledMarker(markerFile), HandlerFixture.SHORT_GRACE_PERIOD, null);
        fixture.start();
        fixture.completeShutdown();
        fixture.assertShutdownStatus(SigtermTerminationHandler.ShutdownStatus.COMPLETE);

        // the check is not run after the shutdown completed, even when the time for it comes
        fixture.taskQueue.runAllTasksInTimeOrder();
        assertThat(fixture.putRequests, hasSize(1));
        assertThat(fixture.putRequests.getFirst().getGracePeriod(), equalTo(HandlerFixture.SHORT_GRACE_PERIOD));
    }

    private static ShortShutdownMarker enabledMarker(Path markerFile) {
        return ShortShutdownMarker.fromSettings(
            Settings.builder().put(StatelessSigtermPlugin.SHORT_SHUTDOWN_MARKER_FILE_SETTING.getKey(), markerFile.toString()).build(),
            markerFile.getParent()
        );
    }

    /**
     * Runs {@link SigtermTerminationHandler#handleTermination()} on its own thread against a mocked client. Time is controlled by a
     * {@link DeterministicTaskQueue}, so a test decides when the status polls and the marker check run. Only the test thread touches the
     * task queue after {@link #start()}, until the handler thread is joined in {@link #completeShutdown()}.
     */
    private final class HandlerFixture {
        static final TimeValue POLL_INTERVAL = timeValueMillis(25);
        // The handler waits for this long in real time if the shutdown does not complete, so it only needs to be longer than a test.
        static final TimeValue TIMEOUT = timeValueSeconds(10);
        static final TimeValue SHORT_GRACE_PERIOD = timeValueSeconds(5);
        static final TimeValue MARKER_CHECK_DELAY = timeValueMillis(100);

        final DeterministicTaskQueue taskQueue = new DeterministicTaskQueue(new Random(random().nextLong()));
        final List<PutShutdownNodeAction.Request> putRequests = new CopyOnWriteArrayList<>();
        final RecordingMeterRegistry meterRegistry = new RecordingMeterRegistry();
        private final AtomicBoolean shutdownComplete = new AtomicBoolean(false);
        private final CountDownLatch firstPutDone = new CountDownLatch(1);
        private final Thread handlerThread;

        /**
         * @param laterPutFailure how every Put Shutdown request after the first one fails, or {@code null} if they succeed
         */
        HandlerFixture(ShortShutdownMarker marker, TimeValue shortGracePeriod, Exception laterPutFailure) {
            taskQueue.setExecutionDelayVariabilityMillis(0);
            final var fixtureThreadPool = taskQueue.getThreadPool();
            final String nodeId = randomAlphaOfLength(10);

            final var client = mock(Client.class);
            when(client.threadPool()).thenReturn(fixtureThreadPool);
            doAnswer(invocation -> {
                putRequests.add(invocation.getArgument(1, PutShutdownNodeAction.Request.class));
                ActionListener<AcknowledgedResponse> listener = invocation.getArgument(2);
                if (putRequests.size() > 1 && laterPutFailure != null) {
                    listener.onFailure(laterPutFailure);
                } else {
                    listener.onResponse(AcknowledgedResponse.TRUE);
                }
                // The listener of the first request starts the status polling and schedules the marker check, so all of that is done here.
                firstPutDone.countDown();
                return null;
            }).when(client).execute(eq(PutShutdownNodeAction.INSTANCE), any(), any());
            doAnswer(invocation -> {
                final var migrationStatus = shutdownComplete.get()
                    ? SingleNodeShutdownMetadata.Status.COMPLETE
                    : SingleNodeShutdownMetadata.Status.IN_PROGRESS;
                ActionListener<GetShutdownStatusAction.Response> listener = invocation.getArgument(2);
                listener.onResponse(
                    new GetShutdownStatusAction.Response(
                        Collections.singletonList(
                            new SingleNodeShutdownStatus(
                                SingleNodeShutdownMetadata.builder()
                                    .setNodeId(nodeId)
                                    .setNodeEphemeralId(nodeId)
                                    .setType(SingleNodeShutdownMetadata.Type.SIGTERM)
                                    .setReason(getTestName())
                                    .setStartedAtMillis(randomNonNegativeLong())
                                    .setGracePeriod(TIMEOUT)
                                    .build(),
                                new ShutdownShardMigrationStatus(migrationStatus, 0, 0, 0),
                                ShutdownPersistentTasksStatus.fromRemainingTasks(0, 0),
                                new ShutdownPluginsStatus(true),
                                ShutdownShardSnapshotsStatus.fromShardCounts(0, 0, 0)
                            )
                        )
                    )
                );
                return null;
            }).when(client).execute(eq(GetShutdownStatusAction.INSTANCE), any(), any());

            final var handler = new SigtermTerminationHandler(
                client,
                fixtureThreadPool,
                null,
                null,
                POLL_INTERVAL,
                TIMEOUT,
                nodeId,
                new SigtermShutdownMetrics(meterRegistry),
                marker,
                shortGracePeriod,
                MARKER_CHECK_DELAY
            );
            handlerThread = new Thread(handler::handleTermination, "sigterm-handler");
            handlerThread.setDaemon(true);
        }

        /**
         * Starts handling the sigterm and returns once the first Put Shutdown request is answered and the polling and the marker check
         * are set up.
         */
        void start() {
            handlerThread.start();
            safeAwait(firstPutDone);
        }

        /**
         * Moves the simulated time forward and runs everything that is due, which are status polls and the marker check.
         * Tests keep the total below ten polls ({@link #POLL_INTERVAL}), as the handler dumps hot threads from the tenth poll on.
         */
        void advanceTimeBy(TimeValue duration) {
            taskQueue.runTasksUpToTimeInOrder(taskQueue.getCurrentTimeMillis() + duration.millis());
        }

        /**
         * Makes the next status poll report a complete shutdown and waits for the handler to return.
         */
        void completeShutdown() {
            shutdownComplete.set(true);
            advanceTimeBy(POLL_INTERVAL);
            safeJoin(handlerThread);
        }

        void assertShutdownStatus(SigtermTerminationHandler.ShutdownStatus expected) {
            final List<Measurement> shutdownMeasurements = meterRegistry.getRecorder()
                .getMeasurements(InstrumentType.DOUBLE_HISTOGRAM, SigtermShutdownMetrics.SHUTDOWN_DURATION_HISTOGRAM);
            assertThat(shutdownMeasurements, hasSize(1));
            assertThat(
                shutdownMeasurements.getFirst().attributes().get(SigtermShutdownMetrics.ATTRIBUTE_NAME_STATUS),
                equalTo(expected.name().toLowerCase(Locale.ROOT))
            );
        }
    }

    public void testShutdownStalledRequiresPolling() {
        shutdownRequiresPolling(SingleNodeShutdownMetadata.Status.STALLED, SingleNodeShutdownMetadata.Status.COMPLETE);
    }

    public void testShutdownRequestPollingThenCompletes() {
        shutdownRequiresPolling(SingleNodeShutdownMetadata.Status.IN_PROGRESS, SingleNodeShutdownMetadata.Status.COMPLETE);
    }

    public void testShutdownRequestPollingThenStalls() {
        shutdownRequiresPolling(SingleNodeShutdownMetadata.Status.IN_PROGRESS, SingleNodeShutdownMetadata.Status.COMPLETE);
    }

    public void testShutdownNotStartedThenCompletes() {
        shutdownRequiresPolling(SingleNodeShutdownMetadata.Status.NOT_STARTED, SingleNodeShutdownMetadata.Status.COMPLETE);
    }

    public void testShutdownBlockEarly() {
        assertShutdownBlock(ClusterState.EMPTY_STATE, l -> { fail("no listener should be added with no nodes in cluster state"); }, l -> {
            fail("no listener should be removed with no nodes in cluster state");
        }, (node, handler) -> { fail("no readiness requests should be made with no nodes in cluster state"); });
    }

    public void testShutdownBlockAlreadyReady() {
        ClusterState initialState = createClusterState(
            "local-node",
            Map.of("local-node", Set.of(INDEX_ROLE), "other-node", Set.of(INDEX_ROLE))
        );
        assertShutdownBlock(initialState, l -> {}, l -> {}, (node, handler) -> {
            assertThat(node.getId(), equalTo("other-node"));
            handler.handleResponse(ActionResponse.Empty.INSTANCE);
        });
    }

    public void testShutdownBlockIgnoreOtherRoles() {
        ClusterState initialState = createClusterState("local-node", Map.of("local-node", Set.of(INDEX_ROLE)));
        AtomicReference<ClusterStateListener> listenerRef = new AtomicReference<>();
        CountDownLatch listeningLatch = new CountDownLatch(1);
        Thread shutdownThread = new Thread(() -> assertShutdownBlock(initialState, l -> {
            listenerRef.compareAndSet(null, l);
            listeningLatch.countDown();
        }, l -> assertThat(l, is(listenerRef.getAndSet(null))), (node, handler) -> {
            assertThat(node.getId(), not(equalTo("other-node")));
            if (node.getId().equals("ready-node")) {
                handler.handleResponse(ActionResponse.Empty.INSTANCE);
            }

        }));
        shutdownThread.start();
        safeAwait(listeningLatch);

        ClusterState newClusterState = createClusterState(
            "local-node",
            Map.of("local-node", Set.of(INDEX_ROLE), "other-node", Set.of(SEARCH_ROLE), "ready-node", Set.of(INDEX_ROLE))
        );
        ClusterChangedEvent otherNodeAdded = new ClusterChangedEvent("test", newClusterState, initialState);
        listenerRef.get().clusterChanged(otherNodeAdded);

        safeJoin(shutdownThread);
    }

    public void testShutdownBlockMultipleMatchingNodesFirstReadyWins() {
        ClusterState initialState = createClusterState(
            "local-node",
            Map.of("local-node", Set.of(INDEX_ROLE), "speedy-node-1", Set.of(INDEX_ROLE), "pokey-node-2", Set.of(INDEX_ROLE))
        );

        AtomicInteger node1Counter = new AtomicInteger();
        AtomicInteger node2Counter = new AtomicInteger();

        CountDownLatch node1Responded = new CountDownLatch(1);
        CountDownLatch node2Responded = new CountDownLatch(1);

        assertShutdownBlock(initialState, l -> {}, l -> {}, (node, handler) -> {
            if (node.getId().equals("speedy-node-1")) {
                node1Counter.incrementAndGet();
                handler.handleResponse(ActionResponse.Empty.INSTANCE);
                node1Responded.countDown();
            } else if (node.getId().equals("pokey-node-2")) {
                node2Counter.incrementAndGet();
                // Simulate a very long delay by just never calling the handler
                node2Responded.countDown();
            } else {
                fail("Unexpected node id " + node.getId());
            }
        });

        safeAwait(node1Responded);
        safeAwait(node2Responded);

        // At least one request to each node
        assertThat(node1Counter.get(), greaterThanOrEqualTo(1));
        assertThat(node2Counter.get(), greaterThanOrEqualTo(1));
    }

    public void testShutdownBlockReadinessFailureThenSuccess() {
        // Single replacement node that fails first readiness, then succeeds on retry
        ClusterState initialState = createClusterState(
            "local-node",
            Map.of("local-node", Set.of(INDEX_ROLE), "replacement", Set.of(INDEX_ROLE))
        );

        AtomicInteger attempts = new AtomicInteger();

        assertShutdownBlock(initialState, l -> {}, l -> {}, (node, handler) -> {
            assertThat(node.getId(), equalTo("replacement"));
            int attempt = attempts.incrementAndGet();
            if (attempt == 1) {
                handler.handleException(new TransportException("simulated failure"));
            } else {
                handler.handleResponse(ActionResponse.Empty.INSTANCE);
            }
        });

        assertThat(attempts.get(), greaterThanOrEqualTo(2));
    }

    public void testShutdownBlockNoDuplicateReadinessRequestsPerNode() {
        // Same node appears as added multiple times; pendingNodes should prevent duplicate outstanding checks
        ClusterState initialState = createClusterState("local-node", Map.of("local-node", Set.of(INDEX_ROLE)));

        AtomicReference<ClusterStateListener> listenerRef = new AtomicReference<>();
        CountDownLatch listenerAdded = new CountDownLatch(1);
        AtomicInteger readinessRequests = new AtomicInteger();

        Thread shutdownThread = new Thread(() -> assertShutdownBlock(initialState, l -> {
            assertTrue(listenerRef.compareAndSet(null, l));
            listenerAdded.countDown();
        }, l -> assertThat(l, is(listenerRef.getAndSet(null))), (node, handler) -> {
            assertThat(node.getId(), equalTo("ready-node"));
            int count = readinessRequests.incrementAndGet();
            // Only respond once, but we assert below that there was only one outstanding request
            if (count == 1) {
                handler.handleResponse(ActionResponse.Empty.INSTANCE);
            }
        }));
        shutdownThread.start();
        safeAwait(listenerAdded);
        ClusterStateListener listener = listenerRef.get();

        // First cluster state with ready-node
        ClusterState withReadyNode = createClusterState(
            "local-node",
            Map.of("local-node", Set.of(INDEX_ROLE), "ready-node", Set.of(INDEX_ROLE))
        );
        ClusterChangedEvent event1 = new ClusterChangedEvent("add ready-node", withReadyNode, initialState);

        listener.clusterChanged(event1);
        // Simulate another nodesChanged event that again "adds" the same node
        listener.clusterChanged(event1);

        safeJoin(shutdownThread);

        // Only a single readiness check should be in flight per node id
        assertThat(readinessRequests.get(), equalTo(1));
    }

    public void testShutdownBlockScaleToZero() {
        // ML nodes are allowed to scale to zero, so shutdown should not be blocked even without a replacement
        ClusterState initialState = createClusterState("local-node", Map.of("local-node", Set.of(ML_ROLE)));
        assertShutdownBlock(initialState, l -> fail("no listener should be added when scaling to zero"), l -> {
            fail("no listener should be removed when scaling to zero");
        }, (node, handler) -> fail("no readiness requests should be made when scaling to zero"));
    }

    private ClusterState createClusterState(String localNode, Map<String, Set<DiscoveryNodeRole>> nodeToRole) {
        ClusterState.Builder builder = ClusterState.builder(ClusterState.EMPTY_STATE);
        DiscoveryNodes.Builder nodesBuilder = DiscoveryNodes.builder();
        nodesBuilder.localNodeId(localNode);
        for (var entry : nodeToRole.entrySet()) {
            nodesBuilder.add(DiscoveryNodeUtils.builder(entry.getKey()).roles(entry.getValue()).build());
        }
        builder.nodes(nodesBuilder);
        return builder.build();
    }

    private void assertShutdownBlock(
        ClusterState initialState,
        Consumer<ClusterStateListener> addListener,
        Consumer<ClusterStateListener> removeListener,
        BiConsumer<DiscoveryNode, TransportResponseHandler<ActionResponse.Empty>> readinessCheck
    ) {
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.getSettings()).thenReturn(Settings.builder().put(ReadinessService.PORT.getKey(), "0").build());
        when(clusterService.state()).thenAnswer(invocation -> initialState);
        doAnswer(answer -> {
            var listener = answer.getArgument(0, ClusterStateListener.class);
            addListener.accept(listener);
            return null;
        }).when(clusterService).addListener(any());
        doAnswer(answer -> {
            var listener = answer.getArgument(0, ClusterStateListener.class);
            removeListener.accept(listener);
            return null;
        }).when(clusterService).removeListener(any());
        RemoteTransportClient remoteClient = new RemoteTransportClient() {
            @Override
            public <T extends TransportResponse> void sendRequest(
                DiscoveryNode node,
                String action,
                TransportRequest request,
                TransportResponseHandler<T> handler
            ) {
                assertThat(action, equalTo(TransportReadinessAction.TYPE.name()));
                assertThat(request.getClass(), equalTo(ReadinessRequest.class));
                @SuppressWarnings("unchecked")
                var readinessHandler = (TransportResponseHandler<ActionResponse.Empty>) handler;
                readinessCheck.accept(node, readinessHandler);
            }
        };
        Client client = mock(Client.class);

        SigtermTerminationHandler handler = new SigtermTerminationHandler(
            client,
            threadPool,
            clusterService,
            remoteClient,
            timeValueSeconds(1),
            null,
            "local-node",
            SigtermShutdownMetrics.NOOP,
            ShortShutdownMarker.disabled(),
            SHORT_GRACE_PERIOD,
            MARKER_CHECK_DELAY
        );
        handler.blockTermination();
    }

    private static void assertShutdownAndMigrationMetrics(
        RecordingMeterRegistry meterRegistry,
        SigtermTerminationHandler.ShutdownStatus expectedShutdownStatus,
        boolean expectedTimedOut,
        boolean expectedMigrationCompleted
    ) {
        final List<Measurement> shutdownMeasurements = meterRegistry.getRecorder()
            .getMeasurements(InstrumentType.DOUBLE_HISTOGRAM, SigtermShutdownMetrics.SHUTDOWN_DURATION_HISTOGRAM);
        assertThat(shutdownMeasurements, hasSize(1));
        assertThat(
            shutdownMeasurements.getFirst().attributes().get(SigtermShutdownMetrics.ATTRIBUTE_NAME_STATUS),
            equalTo(expectedShutdownStatus.name().toLowerCase(Locale.ROOT))
        );
        assertThat(
            shutdownMeasurements.getFirst().attributes().get(SigtermShutdownMetrics.ATTRIBUTE_NAME_TIMED_OUT),
            equalTo(expectedTimedOut)
        );
        assertThat(shutdownMeasurements.getFirst().getDouble(), greaterThanOrEqualTo(0.0));

        final List<Measurement> migrationMeasurements = meterRegistry.getRecorder()
            .getMeasurements(InstrumentType.DOUBLE_HISTOGRAM, SigtermShutdownMetrics.SHARD_MIGRATION_DURATION_HISTOGRAM);
        assertThat(migrationMeasurements, hasSize(1));
        assertThat(
            migrationMeasurements.getFirst().attributes().get(SigtermShutdownMetrics.ATTRIBUTE_NAME_MIGRATION_COMPLETED),
            equalTo(expectedMigrationCompleted)
        );
        assertThat(migrationMeasurements.getFirst().getDouble(), greaterThanOrEqualTo(0.0));
    }

    public void shutdownRequiresPolling(
        SingleNodeShutdownMetadata.Status incompleteStatus,
        SingleNodeShutdownMetadata.Status finishedStatus
    ) {
        final TimeValue pollInterval = TimeValue.timeValueMillis(5);
        // Should be way more than enough to allow rounds of iteration but not block tests forever if something breaks
        final TimeValue timeout = timeValueSeconds(10);
        final String nodeId = randomAlphaOfLength(10);

        int initialRounds = randomIntBetween(2, 5);
        AtomicInteger rounds = new AtomicInteger(initialRounds);

        TestThreadPool threadPool = new TestThreadPool(this.getTestName());
        try {
            Client client = mock(Client.class);
            when(client.threadPool()).thenReturn(threadPool);
            doAnswer(invocation -> {
                PutShutdownNodeAction.Request putRequest = invocation.getArgument(1, PutShutdownNodeAction.Request.class);
                assertThat(putRequest.getNodeId(), equalTo(nodeId));
                assertThat(putRequest.getType(), equalTo(SingleNodeShutdownMetadata.Type.SIGTERM));
                ActionListener<AcknowledgedResponse> listener = invocation.getArgument(2);
                listener.onResponse(randomFrom(AcknowledgedResponse.TRUE, AcknowledgedResponse.FALSE));
                return null; // real method is void
            }).when(client).execute(eq(PutShutdownNodeAction.INSTANCE), any(), any());

            doAnswer(invocation -> {
                int thisRound = rounds.decrementAndGet();
                assertThat(thisRound, greaterThanOrEqualTo(0));
                SingleNodeShutdownMetadata.Status status = thisRound == 0 ? finishedStatus : incompleteStatus;
                GetShutdownStatusAction.Request getRequest = invocation.getArgument(1, GetShutdownStatusAction.Request.class);
                assertThat(getRequest.getNodeIds()[0], equalTo(nodeId));
                ActionListener<GetShutdownStatusAction.Response> listener = invocation.getArgument(2);
                listener.onResponse(
                    new GetShutdownStatusAction.Response(
                        Collections.singletonList(
                            new SingleNodeShutdownStatus(
                                SingleNodeShutdownMetadata.builder()
                                    .setNodeId(nodeId)
                                    .setNodeEphemeralId(nodeId)
                                    .setType(SingleNodeShutdownMetadata.Type.SIGTERM)
                                    .setReason(this.getTestName())
                                    .setStartedAtMillis(randomNonNegativeLong())
                                    .setGracePeriod(timeout)
                                    .build(),
                                new ShutdownShardMigrationStatus(status, 0, 0, 0),
                                ShutdownPersistentTasksStatus.fromRemainingTasks(0, 0),
                                new ShutdownPluginsStatus(true),
                                ShutdownShardSnapshotsStatus.fromShardCounts(0, 0, 0)
                            )
                        )
                    )
                );
                return null; // real method is void
            }).when(client).execute(eq(GetShutdownStatusAction.INSTANCE), any(), any());

            RecordingMeterRegistry meterRegistry = new RecordingMeterRegistry();
            SigtermTerminationHandler handler = new SigtermTerminationHandler(
                client,
                threadPool,
                null,
                null,
                pollInterval,
                timeout,
                nodeId,
                new SigtermShutdownMetrics(meterRegistry),
                ShortShutdownMarker.disabled(),
                SHORT_GRACE_PERIOD,
                MARKER_CHECK_DELAY
            );
            handler.handleTermination();

            verify(client, times(initialRounds)).execute(eq(GetShutdownStatusAction.INSTANCE), any(), any());
            assertShutdownAndMigrationMetrics(meterRegistry, SigtermTerminationHandler.ShutdownStatus.COMPLETE, false, true);
        } finally {
            threadPool.shutdownNow();
        }
    }

}
