/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.recovery.phantom;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.LogEvent;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.recovery.RecoveryCancelledException;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.telemetry.InstrumentType;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.RecordingMeterRegistry;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.stateless.cache.SearchRecoveryWarmingWindows;
import org.elasticsearch.xpack.stateless.recovery.phantom.PhantomQueryMetrics.QueryOutcome;
import org.elasticsearch.xpack.stateless.recovery.phantom.PhantomQueryMetrics.RecoveryType;
import org.elasticsearch.xpack.stateless.recovery.phantom.PhantomQueryMetrics.ReplayOutcome;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.IntStream;

import static org.elasticsearch.xpack.stateless.recovery.phantom.PhantomQueryTestUtils.clusterStateWithInitializingSearchReplica;
import static org.elasticsearch.xpack.stateless.recovery.phantom.PhantomQueryTestUtils.clusterStateWithRelocatingSearchReplica;
import static org.elasticsearch.xpack.stateless.recovery.phantom.PhantomQueryTestUtils.counterBy;
import static org.elasticsearch.xpack.stateless.recovery.phantom.PhantomQueryTestUtils.counterTotal;
import static org.elasticsearch.xpack.stateless.recovery.phantom.PhantomQueryTestUtils.encode;
import static org.elasticsearch.xpack.stateless.recovery.phantom.PhantomQueryTestUtils.queryRequest;
import static org.elasticsearch.xpack.stateless.recovery.phantom.PhantomQueryTestUtils.shards;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PhantomQueryReplayerTests extends ESTestCase {

    private static final String SECRET = "a-value-that-must-never-be-logged";
    private static final long WINDOW_MILLIS = 10_000L;
    private static final long GIB = 1024L * 1024L * 1024L;

    private final ShardId shardId = PhantomQueryTestUtils.shardId();
    private DeterministicTaskQueue taskQueue;
    private ThreadPool threadPool;
    private TaskManager taskManager;
    private ClusterSettings clusterSettings;
    private SearchRecoveryWarmingWindows windows;
    private RecordingMeterRegistry meterRegistry;
    private IndexShard shard;
    private FakeQueryExecution execution;
    private final AtomicLong dataSetSizeInBytes = new AtomicLong(5 * GIB);
    private PhantomQueryReplayer replayer;
    private Task recoveryTask;
    private ShardRouting newCopy;
    private ShardRouting relocationTarget;

    @Before
    public void createReplayer() {
        taskQueue = new DeterministicTaskQueue();
        threadPool = taskQueue.getThreadPool();
        taskManager = new TaskManager(Settings.EMPTY, threadPool, Set.of());
        clusterSettings = new ClusterSettings(settings(2), Set.copyOf(PhantomQueriesSettings.settings()));
        windows = new SearchRecoveryWarmingWindows();
        meterRegistry = new RecordingMeterRegistry();
        shard = mock(IndexShard.class);
        execution = new FakeQueryExecution();
        replayer = new PhantomQueryReplayer(
            threadPool,
            taskManager,
            new PhantomQueriesSettings(clusterSettings),
            windows,
            new PhantomQueryMetrics(meterRegistry),
            PhantomQueryTestUtils.namedWriteableRegistry(),
            execution,
            replayedShard -> dataSetSizeInBytes.get()
        );
        recoveryTask = new Task(randomNonNegativeLong(), "transport", "recovery", "", TaskId.EMPTY_TASK_ID, Map.of());
        newCopy = shards(clusterStateWithInitializingSearchReplica()).stream().filter(ShardRouting::initializing).findFirst().orElseThrow();
        relocationTarget = shards(clusterStateWithRelocatingSearchReplica()).stream()
            .filter(ShardRouting::relocating)
            .findFirst()
            .orElseThrow()
            .getTargetRelocatingShard();
        assertThat(newCopy.relocatingNodeId(), nullValue());
        assertThat(relocationTarget.relocatingNodeId(), notNullValue());
    }

    private static Settings settings(int maxConcurrent) {
        return Settings.builder()
            .put(PhantomQueriesSettings.ENABLED_SETTING.getKey(), true)
            .put(PhantomQueriesSettings.MAX_CONCURRENT_PER_NODE_SETTING.getKey(), maxConcurrent)
            .build();
    }

    private void setMaxConcurrent(int maxConcurrent) {
        clusterSettings.applySettings(settings(maxConcurrent));
    }

    private void disablePhantomQueries() {
        clusterSettings.applySettings(Settings.builder().put(PhantomQueriesSettings.ENABLED_SETTING.getKey(), false).build());
    }

    // ---- fixtures

    private static String value(int i) {
        return String.format(Locale.ROOT, "value-%03d", i);
    }

    private static PhantomQuerySample sample(int size) {
        final ShardId shardId = PhantomQueryTestUtils.shardId();
        return new PhantomQuerySample(IntStream.range(0, size).mapToObj(i -> encode(queryRequest(shardId, value(i)))).toList(), null);
    }

    private RecoveryCancelledException recoveryCancelled() {
        return new RecoveryCancelledException(shardId, null, DiscoveryNodeUtils.create("target"));
    }

    private void startWindow(ShardRouting copy, long millisFromNow) {
        windows.startWindow(shardId, copy.allocationId().getId(), threadPool.relativeTimeInMillis() + millisFromNow);
    }

    private PlainActionFuture<Void> replay(ShardRouting copy, PhantomQuerySample sample) {
        final var future = new PlainActionFuture<Void>();
        replayer.replay(recoveryTask, shard, copy, sample, future);
        return future;
    }

    private void runUntilIdle() {
        taskQueue.runAllRunnableTasks();
    }

    private void advanceTime(long millis) {
        taskQueue.runTasksUpToTimeInOrder(taskQueue.getCurrentTimeMillis() + millis);
    }

    private record Started(ShardSearchRequest request, CancellableTask task, ActionListener<Void> listener) {

        String value() {
            return ((TermQueryBuilder) request.source().query()).value().toString();
        }

        void succeed() {
            listener.onResponse(null);
        }

        void fail(Exception e) {
            listener.onFailure(e);
        }
    }

    /** Starts nothing by itself: every query holds until the test completes it, unless the test says how it ends. */
    private final class FakeQueryExecution implements PhantomQueryReplayer.QueryExecution {

        final List<Started> started = new ArrayList<>();
        int running;
        int maxRunning;
        Consumer<Started> reaction = s -> {};

        @Override
        public void execute(IndexShard executingShard, ShardSearchRequest request, CancellableTask task, ActionListener<Void> listener) {
            assertSame(shard, executingShard);
            final var context = threadPool.getThreadContext();
            assertTrue("phantom queries run in the system context", context.isSystemContext());
            assertThat("the headers of the recovery are not kept", context.getHeader("Authorization"), nullValue());
            final var query = new Started(request, task, new ActionListener<>() {
                @Override
                public void onResponse(Void unused) {
                    running--;
                    listener.onResponse(null);
                }

                @Override
                public void onFailure(Exception e) {
                    running--;
                    listener.onFailure(e);
                }
            });
            started.add(query);
            running++;
            maxRunning = Math.max(maxRunning, running);
            try {
                reaction.accept(query);
            } catch (RuntimeException e) {
                running--;
                throw e;
            }
        }

        List<String> values() {
            return started.stream().map(Started::value).toList();
        }

        List<Started> unfinished() {
            return started.stream().filter(s -> taskManager.getCancellableTask(s.task().getId()) != null).toList();
        }
    }

    private List<Measurement> replays() {
        return meterRegistry.getRecorder().getMeasurements(InstrumentType.DOUBLE_HISTOGRAM, PhantomQueryMetrics.REPLAY_DURATION_HISTOGRAM);
    }

    private Measurement onlyReplay() {
        assertThat(replays(), hasSize(1));
        return replays().get(0);
    }

    private void assertOnlyReplayOutcome(ReplayOutcome outcome, RecoveryType recoveryType) {
        final Measurement replay = onlyReplay();
        assertThat(replay.attributes().get(PhantomQueryMetrics.REPLAY_OUTCOME_ATTRIBUTE), equalTo(outcome.name()));
        assertThat(replay.attributes().get(PhantomQueryMetrics.RECOVERY_TYPE_ATTRIBUTE), equalTo(recoveryType.name()));
    }

    private Map<String, Long> executedByOutcome() {
        final Map<String, Long> executed = new HashMap<>();
        for (Measurement measurement : meterRegistry.getRecorder()
            .getMeasurements(InstrumentType.LONG_COUNTER, PhantomQueryMetrics.EXECUTED_COUNTER)) {
            executed.merge(
                (String) measurement.attributes().get(PhantomQueryMetrics.QUERY_OUTCOME_ATTRIBUTE),
                measurement.getLong(),
                Long::sum
            );
        }
        return executed;
    }

    private long executed(QueryOutcome outcome) {
        return executedByOutcome().getOrDefault(outcome.name(), 0L);
    }

    private void assertNothingExecuted() {
        assertThat(execution.started, empty());
        assertThat(executedByOutcome(), equalTo(Map.of()));
    }

    // ---- NO_* outcomes and DISABLED

    public void testNothingIsReplayedWhilePhantomQueriesAreDisabled() {
        startWindow(newCopy, WINDOW_MILLIS);
        disablePhantomQueries();

        final var done = replay(newCopy, sample(3));

        assertThat("the recovery does not wait", done.isDone(), is(true));
        assertNothingExecuted();
        assertOnlyReplayOutcome(ReplayOutcome.DISABLED, RecoveryType.NEW_COPY);
        assertThat(taskQueue.hasRunnableTasks() || taskQueue.hasDeferredTasks(), is(false));
    }

    public void testNothingIsReplayedWhenTheRecoveryHasNoWarmingWindow() {
        final var done = replay(newCopy, sample(3));

        assertThat(done.isDone(), is(true));
        assertNothingExecuted();
        assertOnlyReplayOutcome(ReplayOutcome.NO_WINDOW, RecoveryType.NEW_COPY);
    }

    public void testNothingIsReplayedWhenTheWarmingWindowIsUsedUp() {
        // a wait that timed out ends its window at once, and the sample may only have been collected before
        startWindow(newCopy, -randomLongBetween(0, WINDOW_MILLIS));

        final var done = replay(newCopy, sample(3));

        assertThat(done.isDone(), is(true));
        assertNothingExecuted();
        assertOnlyReplayOutcome(ReplayOutcome.NO_TIME_LEFT, RecoveryType.NEW_COPY);
    }

    public void testNothingIsReplayedWhenThereIsNoSample() {
        startWindow(newCopy, WINDOW_MILLIS);

        // what the recovering copy gets when starting or collecting the sample failed, the peer declined, or there was nothing to sample
        final var done = replay(newCopy, PhantomQuerySample.EMPTY);

        assertThat("an empty sample does not wait", done.isDone(), is(true));
        assertNothingExecuted();
        assertOnlyReplayOutcome(ReplayOutcome.NO_SAMPLE, RecoveryType.NEW_COPY);
        assertThat("no deadline is left behind", taskQueue.hasDeferredTasks(), is(false));
    }

    public void testReasonsToSkipAreCheckedInOrder() {
        // disabled wins over everything, because the setting may have been switched off after the sample was requested
        disablePhantomQueries();
        replay(newCopy, PhantomQuerySample.EMPTY);
        assertThat(replays().get(0).attributes().get(PhantomQueryMetrics.REPLAY_OUTCOME_ATTRIBUTE), equalTo("DISABLED"));

        // then the window, because a recovery without one never collects a sample
        clusterSettings.applySettings(settings(2));
        replay(newCopy, PhantomQuerySample.EMPTY);
        assertThat(replays().get(1).attributes().get(PhantomQueryMetrics.REPLAY_OUTCOME_ATTRIBUTE), equalTo("NO_WINDOW"));

        startWindow(newCopy, 0);
        replay(newCopy, PhantomQuerySample.EMPTY);
        assertThat(replays().get(2).attributes().get(PhantomQueryMetrics.REPLAY_OUTCOME_ATTRIBUTE), equalTo("NO_TIME_LEFT"));

        startWindow(newCopy, WINDOW_MILLIS);
        replay(newCopy, PhantomQuerySample.EMPTY);
        assertThat(replays().get(3).attributes().get(PhantomQueryMetrics.REPLAY_OUTCOME_ATTRIBUTE), equalTo("NO_SAMPLE"));
        assertNothingExecuted();
    }

    public void testSkippedReplaysAreRecordedWithTheRecoveryType() {
        replay(relocationTarget, PhantomQuerySample.EMPTY);
        assertOnlyReplayOutcome(ReplayOutcome.NO_WINDOW, RecoveryType.RELOCATION);
    }

    public void testAnUnexpectedFailureDoesNotFailTheRecovery() {
        final PhantomQueriesSettings brokenSettings = mock(PhantomQueriesSettings.class);
        when(brokenSettings.enabled()).thenThrow(new IllegalStateException("simulated"));
        final var brokenReplayer = new PhantomQueryReplayer(
            threadPool,
            taskManager,
            brokenSettings,
            windows,
            new PhantomQueryMetrics(new RecordingMeterRegistry()),
            PhantomQueryTestUtils.namedWriteableRegistry(),
            execution,
            replayedShard -> dataSetSizeInBytes.get()
        );

        final var done = new PlainActionFuture<Void>();
        brokenReplayer.replay(recoveryTask, shard, newCopy, sample(1), done);

        assertThat(done.isDone(), is(true));
        assertThat(safeGet(done), nullValue());
        assertNothingExecuted();
    }

    // ---- replaying

    public void testTheWholeSampleRunsInOrderAndTheReplayCompletes() {
        setMaxConcurrent(1);
        startWindow(newCopy, WINDOW_MILLIS);
        execution.reaction = Started::succeed;
        final var sample = sample(5);

        final var done = replay(newCopy, sample);
        runUntilIdle();

        assertThat(done.isDone(), is(true));
        assertThat(execution.values(), equalTo(IntStream.range(0, 5).mapToObj(PhantomQueryReplayerTests::value).toList()));
        assertThat("the queries run one by one", execution.maxRunning, equalTo(1));
        assertThat(executedByOutcome(), equalTo(Map.of("SUCCESS", 5L)));
        assertOnlyReplayOutcome(ReplayOutcome.COMPLETED, RecoveryType.NEW_COPY);
        assertThat(
            "the queries are given as they were retained",
            execution.started.get(2).request().source().query(),
            equalTo(new TermQueryBuilder(PhantomQueryTestUtils.FIELD, value(2)))
        );
        assertThat("the tasks are unregistered", execution.unfinished(), empty());

        advanceTime(WINDOW_MILLIS);
        assertThat("the deadline does not fire after the replay completed", replays(), hasSize(1));
    }

    public void testReplayDurationIsTheTimeTheReplayTook() {
        startWindow(relocationTarget, WINDOW_MILLIS);
        execution.reaction = s -> {};
        final var done = replay(relocationTarget, sample(1));
        runUntilIdle();
        advanceTime(1_500);

        execution.started.get(0).succeed();
        runUntilIdle();

        assertThat(done.isDone(), is(true));
        assertOnlyReplayOutcome(ReplayOutcome.COMPLETED, RecoveryType.RELOCATION);
        assertThat(onlyReplay().getDouble(), closeTo(1.5, 1e-9));
    }

    public void testRecoveryContinuesWithItsOwnThreadContextAndNotOnTheWarmerThread() {
        startWindow(newCopy, WINDOW_MILLIS);
        execution.reaction = Started::succeed;
        threadPool.getThreadContext().putHeader("Authorization", "secret");
        final var seenByRecovery = new AtomicReference<String>();
        final var seenThread = new AtomicReference<Boolean>();

        replayer.replay(recoveryTask, shard, newCopy, sample(1), ActionListener.running(() -> {
            seenByRecovery.set(threadPool.getThreadContext().getHeader("Authorization"));
            seenThread.set(threadPool.getThreadContext().isSystemContext());
        }));
        runUntilIdle();

        assertThat(seenByRecovery.get(), equalTo("secret"));
        assertThat(seenThread.get(), is(false));
        assertThat("the caller keeps its context", threadPool.getThreadContext().getHeader("Authorization"), equalTo("secret"));
    }

    public void testQueriesAreCancellableChildTasksOfTheRecoveryTask() {
        startWindow(newCopy, WINDOW_MILLIS);
        final var seen = new ArrayList<CancellableTask>();
        execution.reaction = query -> {
            seen.add(query.task());
            assertThat(taskManager.getCancellableTask(query.task().getId()), equalTo(query.task()));
            assertThat(query.task().getParentTaskId(), equalTo(new TaskId(taskManager.getNodeId(), recoveryTask.getId())));
            assertThat(query.task().getAction(), equalTo("internal:index/shard/recovery/stateless_unpromotable_relocation/phantom_query"));
            assertThat("the description never has the query", query.task().getDescription(), not(containsString(value(0))));
            assertThat(query.task().isCancelled(), is(false));
            query.succeed();
        };

        final var done = replay(newCopy, sample(2));
        runUntilIdle();

        assertThat(done.isDone(), is(true));
        assertThat(seen, hasSize(2));
        for (CancellableTask task : seen) {
            assertThat("unregistered when done", taskManager.getCancellableTask(task.getId()), nullValue());
        }
    }

    // ---- concurrency

    public void testNoMoreQueriesThanTheLimitRunAtOnce() {
        setMaxConcurrent(2);
        startWindow(newCopy, WINDOW_MILLIS);
        final var done = replay(newCopy, sample(6));
        runUntilIdle();

        assertThat(execution.started, hasSize(2));
        for (int finished = 0; finished < 6; finished++) {
            assertThat(execution.running, equalTo(Math.min(2, 6 - finished)));
            execution.unfinished().get(0).succeed();
            runUntilIdle();
        }

        assertThat(done.isDone(), is(true));
        assertThat(execution.maxRunning, equalTo(2));
        // the first two take their turn at the same time, and the queue keeps the order of the sample for the others
        assertThat(execution.values().subList(0, 2), containsInAnyOrder(value(0), value(1)));
        assertThat(execution.values().subList(2, 6), equalTo(IntStream.range(2, 6).mapToObj(PhantomQueryReplayerTests::value).toList()));
        assertOnlyReplayOutcome(ReplayOutcome.COMPLETED, RecoveryType.NEW_COPY);
    }

    public void testTheLimitIsSharedByAllReplaysOnTheNode() {
        setMaxConcurrent(2);
        final ShardRouting otherCopy = relocationTarget;
        startWindow(newCopy, WINDOW_MILLIS);
        startWindow(otherCopy, WINDOW_MILLIS);

        final var first = replay(newCopy, sample(4));
        final var second = replay(otherCopy, sample(4));
        runUntilIdle();
        assertThat("both replays share two slots", execution.started, hasSize(2));

        while (first.isDone() == false || second.isDone() == false) {
            assertThat(execution.running, lessThanOrEqualTo(2));
            execution.unfinished().get(0).succeed();
            runUntilIdle();
        }

        assertThat(execution.maxRunning, equalTo(2));
        assertThat(execution.started, hasSize(8));
        assertThat(replays(), hasSize(2));
    }

    public void testTheLimitThatAReplayStartsWithAppliesToIt() {
        setMaxConcurrent(1);
        startWindow(newCopy, WINDOW_MILLIS);
        replay(newCopy, sample(5));
        runUntilIdle();
        assertThat(execution.started, hasSize(1));

        setMaxConcurrent(3);
        startWindow(relocationTarget, WINDOW_MILLIS);
        replay(relocationTarget, sample(5));
        runUntilIdle();

        assertThat("the first replay still runs one at a time, and the second three", execution.started, hasSize(1 + 3));
    }

    // ---- the deadline

    public void testTheDeadlineCancelsRunningQueriesAndEndsTheReplayWithoutWaitingForThem() {
        setMaxConcurrent(2);
        startWindow(newCopy, 1_000);
        final var done = replay(newCopy, sample(5));
        runUntilIdle();
        final List<Started> running = List.copyOf(execution.started);
        assertThat(running, hasSize(2));

        advanceTime(999);
        assertThat("the window is not over", done.isDone(), is(false));
        running.forEach(query -> assertThat(query.task().isCancelled(), is(false)));

        advanceTime(1);
        assertThat("the recovery goes on at the deadline, and does not wait for the queries to stop", done.isDone(), is(true));
        running.forEach(query -> assertThat(query.task().isCancelled(), is(true)));
        assertOnlyReplayOutcome(ReplayOutcome.DEADLINE, RecoveryType.NEW_COPY);
        assertThat(onlyReplay().getDouble(), closeTo(1.0, 1e-9));

        // they stop later, and the queued queries never start
        running.forEach(query -> query.fail(new TaskCancelledException("cancelled")));
        runUntilIdle();
        assertThat(execution.started, hasSize(2));
        assertThat(execution.unfinished(), empty());
        assertThat(executedByOutcome(), equalTo(Map.of("CANCELLED", 2L)));
        assertThat("the replay is recorded once", replays(), hasSize(1));
    }

    public void testTheDeadlineIsTheEndOfTheWindowThatWasLeftAfterWarming() {
        // warming ended early, so there is time up to the deadline that was recorded
        startWindow(newCopy, 5_000);
        advanceTime(3_000);
        final var done = replay(newCopy, sample(1));
        runUntilIdle();
        assertThat(done.isDone(), is(false));

        advanceTime(1_999);
        assertThat(done.isDone(), is(false));
        advanceTime(1);

        assertThat(done.isDone(), is(true));
        assertOnlyReplayOutcome(ReplayOutcome.DEADLINE, RecoveryType.NEW_COPY);
        assertThat(onlyReplay().getDouble(), closeTo(2.0, 1e-9));
    }

    public void testAReplayThatEndsBeforeTheDeadlineLeavesNothingToFire() {
        startWindow(newCopy, 1_000);
        execution.reaction = Started::succeed;
        replay(newCopy, sample(2));
        runUntilIdle();
        assertOnlyReplayOutcome(ReplayOutcome.COMPLETED, RecoveryType.NEW_COPY);

        advanceTime(5_000);

        assertThat(replays(), hasSize(1));
        assertThat(executedByOutcome(), equalTo(Map.of("SUCCESS", 2L)));
    }

    // ---- circuit breakers and other failures

    public void testACircuitBreakerStopsTheReplayOfTheShard() {
        setMaxConcurrent(2);
        startWindow(newCopy, WINDOW_MILLIS);
        final var done = replay(newCopy, sample(5));
        runUntilIdle();
        final List<Started> running = List.copyOf(execution.started);
        assertThat(running, hasSize(2));

        running.get(0).fail(new CircuitBreakingException("too much", CircuitBreaker.Durability.TRANSIENT));
        runUntilIdle();

        assertThat("the recovery continues", done.isDone(), is(true));
        assertOnlyReplayOutcome(ReplayOutcome.BREAKER, RecoveryType.NEW_COPY);
        assertThat("the query that is still running is cancelled", running.get(1).task().isCancelled(), is(true));
        running.get(1).fail(new TaskCancelledException("cancelled"));
        runUntilIdle();
        assertThat("the rest of the sample does not start", execution.started, hasSize(2));
        assertThat(executedByOutcome(), equalTo(Map.of("FAILURE", 1L, "CANCELLED", 1L)));
        assertThat(replays(), hasSize(1));

        advanceTime(WINDOW_MILLIS);
        assertOnlyReplayOutcome(ReplayOutcome.BREAKER, RecoveryType.NEW_COPY);
    }

    public void testACircuitBreakerThatIsTheCauseStopsTheReplayToo() {
        startWindow(newCopy, WINDOW_MILLIS);
        execution.reaction = query -> query.fail(
            new ElasticsearchException("wrapped", new CircuitBreakingException("too much", CircuitBreaker.Durability.TRANSIENT))
        );

        final var done = replay(newCopy, sample(4));
        runUntilIdle();

        assertThat(done.isDone(), is(true));
        assertOnlyReplayOutcome(ReplayOutcome.BREAKER, RecoveryType.NEW_COPY);
        assertThat(execution.started, hasSize(1));
    }

    public void testFailuresOfQueriesAreIsolated() {
        setMaxConcurrent(1);
        startWindow(newCopy, WINDOW_MILLIS);
        execution.reaction = query -> {
            switch (query.value()) {
                case "value-000" -> query.succeed();
                // what a query that needs remote work, such as a terms lookup, fails with
                case "value-001" -> query.fail(new IllegalArgumentException("the query needs remote work to be rewritten"));
                // a failure that does not even reach the listener
                case "value-002" -> throw new IllegalStateException("simulated");
                case "value-003" -> query.fail(new ElasticsearchException("simulated", new IOException("disk")));
                case "value-004" -> query.succeed();
                case "value-005" -> query.fail(new TaskCancelledException("cancelled"));
                default -> throw new AssertionError(query.value());
            }
        };

        final var done = replay(newCopy, sample(6));
        runUntilIdle();

        assertThat("a failing query never fails the recovery", safeGet(done), nullValue());
        assertThat("every query ran", execution.started, hasSize(6));
        assertThat(executedByOutcome(), equalTo(Map.of("SUCCESS", 2L, "FAILURE", 3L, "CANCELLED", 1L)));
        assertOnlyReplayOutcome(ReplayOutcome.COMPLETED, RecoveryType.NEW_COPY);
        assertThat(execution.unfinished(), empty());
    }

    public void testAQueryThatCompletesTwiceIsCountedOnce() {
        setMaxConcurrent(1);
        startWindow(newCopy, WINDOW_MILLIS);
        execution.reaction = query -> {
            query.succeed();
            query.fail(new IllegalStateException("simulated"));
        };

        final var done = replay(newCopy, sample(2));
        runUntilIdle();

        assertThat(done.isDone(), is(true));
        assertThat(executedByOutcome(), equalTo(Map.of("SUCCESS", 2L)));
    }

    public void testAQueryThatCannotBeDecodedIsAFailure() {
        setMaxConcurrent(1);
        startWindow(newCopy, WINDOW_MILLIS);
        execution.reaction = Started::succeed;
        final var garbage = new EncodedQueryRequest(new BytesArray(randomByteArrayOfLength(16)), TransportVersion.current());
        final var requests = new ArrayList<>(sample(2).requests());
        requests.add(1, garbage);

        final var done = replay(newCopy, new PhantomQuerySample(requests, null));
        runUntilIdle();

        assertThat(safeGet(done), nullValue());
        assertThat(execution.started, hasSize(2));
        assertThat(executedByOutcome(), equalTo(Map.of("SUCCESS", 2L, "FAILURE", 1L)));
        assertOnlyReplayOutcome(ReplayOutcome.COMPLETED, RecoveryType.NEW_COPY);
    }

    // ---- recovery cancellation

    public void testACancelledRecoveryStopsTheQueriesThatDidNotStartAndRecordsNoOutcome() {
        setMaxConcurrent(1);
        startWindow(newCopy, WINDOW_MILLIS);
        final var done = replay(newCopy, sample(4));
        runUntilIdle();
        assertThat(execution.started, hasSize(1));

        doThrow(recoveryCancelled()).when(shard).ensureRecoveryNotCancelled();
        execution.started.get(0).succeed();
        runUntilIdle();

        assertThat("the recovery gets control back to find that it was cancelled", done.isDone(), is(true));
        assertThat(execution.started, hasSize(1));
        assertThat("the replay has no outcome, but its query was counted", replays(), empty());
        assertThat(executedByOutcome(), equalTo(Map.of("SUCCESS", 1L)));

        advanceTime(WINDOW_MILLIS);
        assertThat("the deadline does not fire after the replay was abandoned", replays(), empty());
    }

    public void testACancelledRecoveryCancelsTheQueriesThatRun() {
        setMaxConcurrent(2);
        startWindow(newCopy, WINDOW_MILLIS);
        final var done = replay(newCopy, sample(3));
        runUntilIdle();
        assertThat(execution.started, hasSize(2));

        doThrow(recoveryCancelled()).when(shard).ensureRecoveryNotCancelled();
        execution.started.get(0).succeed();
        runUntilIdle();

        assertThat(done.isDone(), is(true));
        assertThat("the other query that runs is cancelled", execution.started.get(1).task().isCancelled(), is(true));
        assertThat("the third never starts", execution.started, hasSize(2));
        assertThat(replays(), empty());
    }

    // ---- shard size

    private String shardSizeOf(Measurement replay) {
        return (String) replay.attributes().get(PhantomQueryMetrics.SHARD_SIZE_BUCKET_ATTRIBUTE);
    }

    public void testReplayIsRecordedWithTheBucketOfTheShardSize() {
        dataSetSizeInBytes.set(5 * GIB);
        startWindow(newCopy, WINDOW_MILLIS);
        execution.reaction = Started::succeed;

        replay(newCopy, sample(1));
        runUntilIdle();

        assertOnlyReplayOutcome(ReplayOutcome.COMPLETED, RecoveryType.NEW_COPY);
        assertThat(shardSizeOf(onlyReplay()), equalTo("<=10GiB"));
        assertThat(
            onlyReplay().attributes().keySet(),
            equalTo(
                Set.of(
                    PhantomQueryMetrics.REPLAY_OUTCOME_ATTRIBUTE,
                    PhantomQueryMetrics.RECOVERY_TYPE_ATTRIBUTE,
                    PhantomQueryMetrics.SHARD_SIZE_BUCKET_ATTRIBUTE
                )
            )
        );
    }

    public void testEveryBucketOfTheShardSizeIsRecorded() {
        final Map<Long, String> expected = new LinkedHashMap<>();
        expected.put(1L, "<=1GiB");
        expected.put(GIB, "<=1GiB");
        expected.put(GIB + 1, "<=10GiB");
        expected.put(10 * GIB, "<=10GiB");
        expected.put(10 * GIB + 1, "<=50GiB");
        expected.put(50 * GIB, "<=50GiB");
        expected.put(50 * GIB + 1, ">50GiB");
        execution.reaction = Started::succeed;

        for (long size : expected.keySet()) {
            dataSetSizeInBytes.set(size);
            startWindow(newCopy, WINDOW_MILLIS);
            replay(newCopy, sample(1));
            runUntilIdle();
        }

        assertThat(replays().stream().map(this::shardSizeOf).toList(), equalTo(List.copyOf(expected.values())));
    }

    public void testSkippedReplaysAreRecordedWithTheShardSizeToo() {
        dataSetSizeInBytes.set(60 * GIB);

        // no window
        replay(newCopy, sample(1));
        // a window that is used up
        startWindow(newCopy, -1);
        replay(newCopy, sample(1));
        // no sample
        startWindow(newCopy, WINDOW_MILLIS);
        replay(newCopy, PhantomQuerySample.EMPTY);
        // disabled
        disablePhantomQueries();
        replay(newCopy, sample(1));

        assertThat(
            replays().stream().map(replay -> replay.attributes().get(PhantomQueryMetrics.REPLAY_OUTCOME_ATTRIBUTE)).toList(),
            equalTo(List.of("NO_WINDOW", "NO_TIME_LEFT", "NO_SAMPLE", "DISABLED"))
        );
        assertThat(replays().stream().map(this::shardSizeOf).toList(), equalTo(List.of(">50GiB", ">50GiB", ">50GiB", ">50GiB")));
    }

    public void testASizeThatCannotBeReadIsUnknownAndTheReplayGoesOn() {
        final var registry = new RecordingMeterRegistry();
        final var unreadable = new PhantomQueryReplayer(
            threadPool,
            taskManager,
            new PhantomQueriesSettings(clusterSettings),
            windows,
            new PhantomQueryMetrics(registry),
            PhantomQueryTestUtils.namedWriteableRegistry(),
            execution,
            replayedShard -> {
                throw new IllegalStateException("simulated: the store is closing");
            }
        );
        startWindow(newCopy, WINDOW_MILLIS);
        execution.reaction = Started::succeed;
        final var done = new PlainActionFuture<Void>();

        unreadable.replay(recoveryTask, shard, newCopy, sample(2), done);
        runUntilIdle();

        assertThat(safeGet(done), nullValue());
        assertThat("the queries ran", execution.started, hasSize(2));
        final List<Measurement> replays = registry.getRecorder()
            .getMeasurements(InstrumentType.DOUBLE_HISTOGRAM, PhantomQueryMetrics.REPLAY_DURATION_HISTOGRAM);
        assertThat(replays, hasSize(1));
        assertThat(replays.get(0).attributes().get(PhantomQueryMetrics.REPLAY_OUTCOME_ATTRIBUTE), equalTo("COMPLETED"));
        assertThat(shardSizeOf(replays.get(0)), equalTo("UNKNOWN"));
    }

    public void testASizeOfZeroIsUnknown() {
        dataSetSizeInBytes.set(0);
        startWindow(newCopy, WINDOW_MILLIS);

        replay(newCopy, PhantomQuerySample.EMPTY);

        assertThat(shardSizeOf(onlyReplay()), equalTo("UNKNOWN"));
    }

    public void testTheSizeIsTheOneWhenTheReplayStarted() {
        dataSetSizeInBytes.set(5 * GIB);
        startWindow(newCopy, WINDOW_MILLIS);
        final var done = replay(newCopy, sample(1));
        runUntilIdle();

        dataSetSizeInBytes.set(60 * GIB);
        execution.started.get(0).succeed();
        runUntilIdle();

        assertThat(done.isDone(), is(true));
        assertThat(shardSizeOf(onlyReplay()), equalTo("<=10GiB"));
    }

    public void testEveryOutcomeOfAReplayCarriesTheShardSize() {
        dataSetSizeInBytes.set(20 * GIB);
        setMaxConcurrent(1);

        // the deadline
        startWindow(newCopy, 1_000);
        replay(newCopy, sample(2));
        runUntilIdle();
        advanceTime(1_000);
        execution.started.get(0).fail(new TaskCancelledException("cancelled"));
        runUntilIdle();
        // a circuit breaker
        startWindow(relocationTarget, WINDOW_MILLIS);
        execution.reaction = query -> query.fail(new CircuitBreakingException("too much", CircuitBreaker.Durability.TRANSIENT));
        replay(relocationTarget, sample(2));
        runUntilIdle();

        assertThat(
            replays().stream().map(replay -> replay.attributes().get(PhantomQueryMetrics.REPLAY_OUTCOME_ATTRIBUTE)).toList(),
            containsInAnyOrder("DEADLINE", "BREAKER")
        );
        for (Measurement replay : replays()) {
            assertThat(shardSizeOf(replay), equalTo("<=50GiB"));
        }
    }

    // ---- the duration of each query

    private List<Measurement> executionDurations() {
        return meterRegistry.getRecorder()
            .getMeasurements(InstrumentType.DOUBLE_HISTOGRAM, PhantomQueryMetrics.EXECUTION_DURATION_HISTOGRAM);
    }

    public void testEachQueryThatStartedRecordsItsDurationInSecondsWithItsOutcome() {
        setMaxConcurrent(3);
        startWindow(newCopy, WINDOW_MILLIS);
        replay(newCopy, sample(3));
        runUntilIdle();
        assertThat("all three start at the same time", execution.started, hasSize(3));
        assertThat("nothing is recorded while they run", executionDurations(), empty());

        advanceTime(100);
        execution.started.get(0).succeed();
        advanceTime(400);
        execution.started.get(1).fail(new IllegalArgumentException("the query needs remote work to be rewritten"));
        advanceTime(500);
        execution.started.get(2).fail(new TaskCancelledException("cancelled"));
        runUntilIdle();

        assertThat(executionDurations(), hasSize(3));
        assertThat(executionDurations().get(0).getDouble(), closeTo(0.1, 1e-9));
        assertThat(executionDurations().get(0).attributes(), equalTo(Map.of(PhantomQueryMetrics.QUERY_OUTCOME_ATTRIBUTE, "SUCCESS")));
        assertThat(executionDurations().get(1).getDouble(), closeTo(0.5, 1e-9));
        assertThat(executionDurations().get(1).attributes(), equalTo(Map.of(PhantomQueryMetrics.QUERY_OUTCOME_ATTRIBUTE, "FAILURE")));
        assertThat(executionDurations().get(2).getDouble(), closeTo(1.0, 1e-9));
        assertThat(executionDurations().get(2).attributes(), equalTo(Map.of(PhantomQueryMetrics.QUERY_OUTCOME_ATTRIBUTE, "CANCELLED")));
    }

    public void testTheDurationOfAQueryDoesNotIncludeTheTimeItWaitedForItsTurn() {
        setMaxConcurrent(1);
        startWindow(newCopy, WINDOW_MILLIS);
        replay(newCopy, sample(2));
        runUntilIdle();

        advanceTime(3_000);
        execution.started.get(0).succeed();
        runUntilIdle();
        advanceTime(200);
        execution.started.get(1).succeed();
        runUntilIdle();

        assertThat(executionDurations().stream().map(Measurement::getDouble).toList(), hasSize(2));
        assertThat(executionDurations().get(0).getDouble(), closeTo(3.0, 1e-9));
        assertThat("the second one waited three seconds, and ran for 0.2", executionDurations().get(1).getDouble(), closeTo(0.2, 1e-9));
    }

    public void testAQueryThatCannotBeDecodedHasNoDuration() {
        setMaxConcurrent(1);
        startWindow(newCopy, WINDOW_MILLIS);
        execution.reaction = Started::succeed;
        final var garbage = new EncodedQueryRequest(new BytesArray(randomByteArrayOfLength(16)), TransportVersion.current());
        final var requests = new ArrayList<>(sample(2).requests());
        requests.add(1, garbage);

        replay(newCopy, new PhantomQuerySample(requests, null));
        runUntilIdle();

        assertThat(executedByOutcome(), equalTo(Map.of("SUCCESS", 2L, "FAILURE", 1L)));
        assertThat("only the two that started executing have a duration", executionDurations(), hasSize(2));
        for (Measurement duration : executionDurations()) {
            assertThat(duration.attributes(), equalTo(Map.of(PhantomQueryMetrics.QUERY_OUTCOME_ATTRIBUTE, "SUCCESS")));
        }
    }

    public void testAQueryThatCompletesTwiceHasOneDuration() {
        setMaxConcurrent(1);
        startWindow(newCopy, WINDOW_MILLIS);
        execution.reaction = query -> {
            query.succeed();
            query.fail(new IllegalStateException("simulated"));
        };

        replay(newCopy, sample(2));
        runUntilIdle();

        assertThat(executionDurations(), hasSize(2));
    }

    public void testQueriesThatNeverStartedHaveNoDuration() {
        setMaxConcurrent(1);
        startWindow(newCopy, 1_000);
        replay(newCopy, sample(4));
        runUntilIdle();
        advanceTime(1_000);

        execution.started.get(0).fail(new TaskCancelledException("cancelled"));
        runUntilIdle();

        assertThat("the three that were queued never started", executionDurations(), hasSize(1));
    }

    // ---- the queries that never started

    private long executedInTotal() {
        return counterTotal(meterRegistry, PhantomQueryMetrics.EXECUTED_COUNTER);
    }

    private Map<String, Long> notExecuted() {
        return counterBy(meterRegistry, PhantomQueryMetrics.NOT_EXECUTED_COUNTER, PhantomQueryMetrics.REPLAY_OUTCOME_ATTRIBUTE);
    }

    public void testQueriesThatNeverStartedBecauseOfTheDeadlineAreCountedWithItsOutcome() {
        setMaxConcurrent(2);
        startWindow(newCopy, 1_000);
        replay(newCopy, sample(5));
        runUntilIdle();
        final List<Started> running = List.copyOf(execution.started);
        advanceTime(1_000);
        assertOnlyReplayOutcome(ReplayOutcome.DEADLINE, RecoveryType.NEW_COPY);

        running.forEach(query -> query.fail(new TaskCancelledException("cancelled")));
        runUntilIdle();

        assertThat("the three that were queued never started", notExecuted(), equalTo(Map.of("DEADLINE", 3L)));
        assertThat("the two that ran were cancelled", executedByOutcome(), equalTo(Map.of("CANCELLED", 2L)));
        assertThat(
            "they add up to the sample",
            executedInTotal() + counterTotal(meterRegistry, PhantomQueryMetrics.NOT_EXECUTED_COUNTER),
            equalTo(5L)
        );
        for (Measurement measurement : meterRegistry.getRecorder()
            .getMeasurements(InstrumentType.LONG_COUNTER, PhantomQueryMetrics.NOT_EXECUTED_COUNTER)) {
            assertThat(measurement.attributes().keySet(), equalTo(Set.of(PhantomQueryMetrics.REPLAY_OUTCOME_ATTRIBUTE)));
        }
    }

    public void testQueriesThatNeverStartedBecauseOfACircuitBreakerAreCountedWithItsOutcome() {
        setMaxConcurrent(2);
        startWindow(newCopy, WINDOW_MILLIS);
        replay(newCopy, sample(5));
        runUntilIdle();
        final List<Started> running = List.copyOf(execution.started);

        running.get(0).fail(new CircuitBreakingException("too much", CircuitBreaker.Durability.TRANSIENT));
        runUntilIdle();
        running.get(1).fail(new TaskCancelledException("cancelled"));
        runUntilIdle();

        assertOnlyReplayOutcome(ReplayOutcome.BREAKER, RecoveryType.NEW_COPY);
        assertThat(notExecuted(), equalTo(Map.of("BREAKER", 3L)));
        assertThat(executedByOutcome(), equalTo(Map.of("FAILURE", 1L, "CANCELLED", 1L)));
        assertThat(executedInTotal() + counterTotal(meterRegistry, PhantomQueryMetrics.NOT_EXECUTED_COUNTER), equalTo(5L));
    }

    public void testTheCountsOfAReplayThatIsCutOffAddUpToItsSample() {
        final int sampleSize = randomIntBetween(1, 30);
        final int concurrency = randomIntBetween(1, 5);
        final int finishedBeforeTheDeadline = randomIntBetween(0, sampleSize);
        setMaxConcurrent(concurrency);
        startWindow(newCopy, 1_000);
        replay(newCopy, sample(sampleSize));
        runUntilIdle();
        for (int i = 0; i < finishedBeforeTheDeadline; i++) {
            execution.unfinished().get(0).succeed();
            runUntilIdle();
        }
        final boolean replayWasOver = replays().isEmpty() == false;

        advanceTime(1_000);
        for (Started query : List.copyOf(execution.unfinished())) {
            query.fail(new TaskCancelledException("cancelled"));
        }
        runUntilIdle();

        assertThat(replays(), hasSize(1));
        if (replayWasOver) {
            assertThat("it completed before the deadline", notExecuted(), equalTo(Map.of()));
        } else {
            assertThat(
                "every query of the sample either started or was counted as one that did not",
                executedInTotal() + counterTotal(meterRegistry, PhantomQueryMetrics.NOT_EXECUTED_COUNTER),
                equalTo((long) sampleSize)
            );
        }
    }

    public void testACompletedReplayHasNoQueriesThatNeverStarted() {
        setMaxConcurrent(2);
        startWindow(newCopy, WINDOW_MILLIS);
        execution.reaction = Started::succeed;

        replay(newCopy, sample(5));
        runUntilIdle();

        assertOnlyReplayOutcome(ReplayOutcome.COMPLETED, RecoveryType.NEW_COPY);
        assertThat(notExecuted(), equalTo(Map.of()));
        advanceTime(WINDOW_MILLIS);
        assertThat(notExecuted(), equalTo(Map.of()));
    }

    public void testASkippedReplayHasNoQueriesThatNeverStarted() {
        replay(newCopy, sample(3));
        startWindow(newCopy, -1);
        replay(newCopy, sample(3));
        startWindow(newCopy, WINDOW_MILLIS);
        replay(newCopy, PhantomQuerySample.EMPTY);
        disablePhantomQueries();
        replay(newCopy, sample(3));

        assertThat("four replays were skipped", replays(), hasSize(4));
        assertThat(notExecuted(), equalTo(Map.of()));
    }

    public void testAnAbandonedReplayHasNoQueriesThatNeverStarted() {
        setMaxConcurrent(1);
        startWindow(newCopy, WINDOW_MILLIS);
        replay(newCopy, sample(4));
        runUntilIdle();
        doThrow(recoveryCancelled()).when(shard).ensureRecoveryNotCancelled();

        execution.started.get(0).succeed();
        runUntilIdle();

        assertThat(replays(), empty());
        assertThat("the replay has no outcome to attribute them to", notExecuted(), equalTo(Map.of()));
        assertThat(executedByOutcome(), equalTo(Map.of("SUCCESS", 1L)));
    }

    public void testQueriesThatCannotBeDecodedAreExecutedAndNotCountedAsNeverStarted() {
        setMaxConcurrent(1);
        startWindow(newCopy, WINDOW_MILLIS);
        execution.reaction = Started::succeed;
        final var garbage = new EncodedQueryRequest(new BytesArray(randomByteArrayOfLength(16)), TransportVersion.current());

        replay(newCopy, new PhantomQuerySample(List.of(garbage, garbage), null));
        runUntilIdle();

        assertThat(executedByOutcome(), equalTo(Map.of("FAILURE", 2L)));
        assertThat(notExecuted(), equalTo(Map.of()));
    }

    // ---- what is logged

    @TestLogging(
        value = "org.elasticsearch.xpack.stateless.recovery.phantom.PhantomQueryReplayer:DEBUG",
        reason = "the failures of queries are logged at DEBUG"
    )
    public void testNothingDerivedFromAQueryIsLogged() {
        setMaxConcurrent(1);
        startWindow(newCopy, WINDOW_MILLIS);
        execution.reaction = query -> query.fail(new IllegalArgumentException("failed on [" + SECRET + "]"));

        MockLog.assertThatLogger(() -> {
            replay(newCopy, sample(2));
            runUntilIdle();
        },
            PhantomQueryReplayer.class,
            new MockLog.SeenEventExpectation(
                "the failure is logged without its message",
                PhantomQueryReplayer.class.getCanonicalName(),
                Level.DEBUG,
                "a phantom query failed with [IllegalArgumentException]"
            ),
            new NeverLogs(SECRET)
        );
        assertThat(executed(QueryOutcome.FAILURE), equalTo(2L));
    }

    private static final class NeverLogs implements MockLog.LoggingExpectation {
        private final String text;
        private boolean logged;

        NeverLogs(String text) {
            this.text = text;
        }

        @Override
        public void match(LogEvent event) {
            if (event.getMessage().getFormattedMessage().contains(text)
                || (event.getThrown() != null && String.valueOf(event.getThrown().getMessage()).contains(text))) {
                logged = true;
            }
        }

        @Override
        public void assertMatched() {
            assertFalse("[" + text + "] was logged", logged);
        }
    }
}
