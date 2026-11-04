/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.recovery.phantom;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ContextPreservingActionListener;
import org.elasticsearch.action.support.ThreadedActionListener;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.util.concurrent.ThrottledTaskRunner;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.recovery.RecoveryCancelledException;
import org.elasticsearch.indices.recovery.StatelessUnpromotableRelocationAction;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskAwareRequest;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.stateless.cache.SearchRecoveryWarmingWindows;
import org.elasticsearch.xpack.stateless.recovery.phantom.PhantomQueryMetrics.QueryOutcome;
import org.elasticsearch.xpack.stateless.recovery.phantom.PhantomQueryMetrics.RecoveryType;
import org.elasticsearch.xpack.stateless.recovery.phantom.PhantomQueryMetrics.ReplayOutcome;

import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static org.elasticsearch.core.Strings.format;

/**
 * Executes the sample that a recovering search shard copy collected from its peer, against its own copy, to warm what the queries and
 * the fetch of their top hits read. The results are discarded.
 *
 * <p>
 * <b>Where:</b> the copy is still {@code RECOVERING} with its engine open, so the queries cannot take the normal search path. Each one
 * takes a searcher straight from the engine, without a reader wrapper, and runs through {@link SearchService#executeQueryPhaseForWarming}
 * on the system thread context, which keeps all search side effects out.
 *
 * <p>
 * <b>How much:</b> a node-wide {@link ThrottledTaskRunner} on the {@code warmer} pool runs at most
 * {@link PhantomQueriesSettings#maxConcurrentPerNode()} queries at a time for all the shards that replay. A change of that setting applies
 * to the replays that start afterwards, because the limit of a runner is fixed. Queries that a replay already queued finish under the
 * previous limit.
 *
 * <p>
 * <b>Until when:</b> a replay ends at the deadline of the warming wait that the recovery already spent time on. At the deadline, queries
 * that still run are cancelled and the recovery continues at once, without waiting for them to stop. A query that trips a circuit breaker
 * ends the replay of its shard too. Queries are decoded when their turn comes, so only their serialized form waits in the queue.
 *
 * <p>
 * <b>What never happens:</b> a failing, rejected or cancelled query never fails the recovery, and nothing derived from a query, its
 * results or the documents it fetched is logged. The recovery only learns that replay is over.
 *
 * <p>
 * A recovery that is cancelled while it replays is the one case that has no replay outcome. The cancellation is only visible at the
 * boundaries of the queries, because recovery cancellation is polled, so it stops the queries that did not start yet and ends the replay
 * without a metric.
 */
public class PhantomQueryReplayer {

    private static final Logger logger = LogManager.getLogger(PhantomQueryReplayer.class);

    static final String TASK_TYPE = "direct";
    /** What the task management API lists the phantom queries under, as children of the recovery action. */
    public static final String TASK_ACTION = StatelessUnpromotableRelocationAction.TYPE.name() + "/phantom_query";

    /**
     * Executes one query against the shard and completes the listener when it is done. It must release the searcher that it acquired,
     * whatever the outcome.
     */
    @FunctionalInterface
    interface QueryExecution {
        void execute(IndexShard shard, ShardSearchRequest request, CancellableTask task, ActionListener<Void> listener);
    }

    private record Throttle(int limit, ThrottledTaskRunner runner) {}

    /**
     * How a replay ends. {@link #ABANDONED} is the recovery being cancelled, or the replay not being able to start, and has no metric.
     */
    private enum End {
        COMPLETED(ReplayOutcome.COMPLETED),
        DEADLINE(ReplayOutcome.DEADLINE),
        BREAKER(ReplayOutcome.BREAKER),
        ABANDONED(null);

        final ReplayOutcome outcome;

        End(ReplayOutcome outcome) {
            this.outcome = outcome;
        }
    }

    private final ThreadPool threadPool;
    private final TaskManager taskManager;
    private final PhantomQueriesSettings settings;
    private final SearchRecoveryWarmingWindows warmingWindows;
    private final PhantomQueryMetrics metrics;
    private final NamedWriteableRegistry namedWriteableRegistry;
    private final QueryExecution queryExecution;
    private volatile Throttle throttle;

    public PhantomQueryReplayer(
        ThreadPool threadPool,
        TaskManager taskManager,
        SearchService searchService,
        PhantomQueriesSettings settings,
        SearchRecoveryWarmingWindows warmingWindows,
        PhantomQueryMetrics metrics,
        NamedWriteableRegistry namedWriteableRegistry
    ) {
        this(threadPool, taskManager, settings, warmingWindows, metrics, namedWriteableRegistry, executeWith(searchService));
    }

    PhantomQueryReplayer(
        ThreadPool threadPool,
        TaskManager taskManager,
        PhantomQueriesSettings settings,
        SearchRecoveryWarmingWindows warmingWindows,
        PhantomQueryMetrics metrics,
        NamedWriteableRegistry namedWriteableRegistry,
        QueryExecution queryExecution
    ) {
        this.threadPool = threadPool;
        this.taskManager = taskManager;
        this.settings = settings;
        this.warmingWindows = warmingWindows;
        this.metrics = metrics;
        this.namedWriteableRegistry = namedWriteableRegistry;
        this.queryExecution = queryExecution;
    }

    private static QueryExecution executeWith(SearchService searchService) {
        return (shard, request, task, listener) -> {
            // Straight from the engine: the readiness check of the shard would refuse a copy that is still recovering, and the reader
            // wrapper would apply the document and field level security of a user that no phantom query has. The searcher is filtered
            // like the one that the peer used for the same request, when the shard is being split.
            final Engine.SearcherSupplier searcher = shard.withEngine(
                engine -> engine.acquireSearcherSupplier(
                    Function.identity(),
                    Engine.SearcherScope.EXTERNAL,
                    request.getSplitShardCountSummary()
                )
            );
            // takes ownership of the searcher, and runs on the warmer thread of the throttle, which is the one that is calling
            searchService.executeQueryPhaseForWarming(request, shard, searcher, task, EsExecutors.DIRECT_EXECUTOR_SERVICE, listener);
        };
    }

    /**
     * Replays {@code sample} on {@code shard} if there is a reason to and time to, and completes {@code listener} once the replay is over.
     * Never fails: a replay that cannot run is a recovery without one.
     *
     * @param recoveringCopy the routing entry of the copy when the recovery started, which tells a relocation target from a new copy
     */
    public void replay(
        Task parentTask,
        IndexShard shard,
        ShardRouting recoveringCopy,
        PhantomQuerySample sample,
        ActionListener<Void> listener
    ) {
        // The raw clock, because the cached one only moves every 200ms by default, which is more than a replay may take. For the time that
        // is left it can only be ahead of the cached clock that the window was recorded with, so a replay never outlasts its window.
        final long startMillis = threadPool.rawRelativeTimeInMillis();
        final RecoveryType recoveryType = recoveringCopy.relocatingNodeId() != null ? RecoveryType.RELOCATION : RecoveryType.NEW_COPY;
        final ShardId shardId = recoveringCopy.shardId();
        final String allocationId = recoveringCopy.allocationId().getId();
        try {
            final OptionalLong deadlineMillis = warmingWindows.relativeDeadlineMillis(shardId, allocationId);
            final long remainingMillis = deadlineMillis.isPresent() ? deadlineMillis.getAsLong() - startMillis : 0L;
            final ReplayOutcome skipped = reasonToSkip(deadlineMillis, remainingMillis, sample);
            if (skipped != null) {
                logger.debug("{} not replaying a sample for the copy [{}]: {}", shardId, allocationId, skipped);
                metrics.recordReplay(skipped, recoveryType, threadPool.rawRelativeTimeInMillis() - startMillis);
                listener.onResponse(null);
                return;
            }
            new Replay(parentTask, shard, shardId, allocationId, recoveryType, sample.requests(), startMillis, listener).start(
                TimeValue.timeValueMillis(remainingMillis)
            );
        } catch (Exception e) {
            // Replay is an optimization, and the recovery goes on without it. Replay completes the listener itself once it started.
            logger.debug(() -> format("%s failed to replay a sample for the copy [%s]", shardId, allocationId), e);
            listener.onResponse(null);
        }
    }

    /**
     * Whether to replay, in the order in which the reasons are checked: the setting may have been disabled since the sample was
     * requested, and a recovery that waited for no warming, or all its window, never collected a sample worth replaying.
     */
    private ReplayOutcome reasonToSkip(OptionalLong deadlineMillis, long remainingMillis, PhantomQuerySample sample) {
        if (settings.enabled() == false) {
            return ReplayOutcome.DISABLED;
        }
        if (deadlineMillis.isEmpty()) {
            return ReplayOutcome.NO_WINDOW;
        }
        if (remainingMillis <= 0) {
            return ReplayOutcome.NO_TIME_LEFT;
        }
        if (sample.isEmpty()) {
            return ReplayOutcome.NO_SAMPLE;
        }
        return null;
    }

    private ThrottledTaskRunner runnerForNewReplay() {
        final int limit = Math.max(1, settings.maxConcurrentPerNode());
        Throttle current = throttle;
        if (current == null || current.limit() != limit) {
            synchronized (this) {
                current = throttle;
                if (current == null || current.limit() != limit) {
                    current = new Throttle(
                        limit,
                        new ThrottledTaskRunner("phantom_queries", limit, threadPool.executor(ThreadPool.Names.WARMER))
                    );
                    throttle = current;
                }
            }
        }
        return current.runner();
    }

    /**
     * The replay of one sample on one recovering copy.
     */
    private final class Replay {

        private final Task parentTask;
        private final IndexShard shard;
        private final ShardId shardId;
        private final String allocationId;
        private final RecoveryType recoveryType;
        private final List<EncodedQueryRequest> requests;
        private final long startMillis;
        private final ActionListener<Void> listener;
        private final ThreadContext threadContext = threadPool.getThreadContext();
        private final AtomicInteger unfinishedQueries;
        private final AtomicReference<End> end = new AtomicReference<>();
        private final Set<CancellableTask> runningQueries = ConcurrentCollections.newConcurrentSet();
        private volatile Scheduler.ScheduledCancellable deadlineTask;

        Replay(
            Task parentTask,
            IndexShard shard,
            ShardId shardId,
            String allocationId,
            RecoveryType recoveryType,
            List<EncodedQueryRequest> requests,
            long startMillis,
            ActionListener<Void> recoveryListener
        ) {
            this.parentTask = parentTask;
            this.shard = shard;
            this.shardId = shardId;
            this.allocationId = allocationId;
            this.recoveryType = recoveryType;
            this.requests = requests;
            this.startMillis = startMillis;
            this.unfinishedQueries = new AtomicInteger(requests.size());
            // The recovery goes on with its own thread context, and not on a warmer thread, which it must not block, or the thread of the
            // deadline.
            this.listener = new ThreadedActionListener<>(
                threadPool.generic(),
                new ContextPreservingActionListener<>(threadContext.newRestorableContext(false), recoveryListener)
            );
        }

        void start(TimeValue remaining) {
            final ThrottledTaskRunner runner = runnerForNewReplay();
            try (ThreadContext.StoredContext ignored = threadContext.stashContext()) {
                threadContext.markAsSystemContext();
                // Scheduled before the first query is queued, so that whichever query ends the replay finds it to cancel.
                deadlineTask = threadPool.schedule(() -> end(End.DEADLINE), remaining, threadPool.generic());
                for (EncodedQueryRequest request : requests) {
                    runner.enqueueTask(new ActionListener<>() {
                        @Override
                        public void onResponse(Releasable slot) {
                            runQuery(request, slot);
                        }

                        @Override
                        public void onFailure(Exception e) {
                            // rejected by the warmer pool, so the query never ran
                            logger.debug(() -> format("%s a phantom query was rejected", shardId), e);
                            queryDone();
                        }
                    });
                }
            } catch (Exception e) {
                logger.debug(() -> format("%s failed to start replaying a sample for the copy [%s]", shardId, allocationId), e);
                end(End.ABANDONED);
            }
        }

        /**
         * Runs when the query gets its turn in the throttle. Nothing may escape, because the throttle runner does not expect its tasks to
         * fail.
         */
        private void runQuery(EncodedQueryRequest encoded, Releasable slot) {
            try (ThreadContext.StoredContext ignored = threadContext.stashContext()) {
                // the thread context that the query runs in, whichever thread gave it its turn
                threadContext.markAsSystemContext();
                runQueryInSystemContext(encoded, slot);
            } catch (Exception e) {
                logger.debug(() -> format("%s unexpected failure while replaying a sample for the copy [%s]", shardId, allocationId), e);
                slot.close();
                end(End.ABANDONED);
            }
        }

        private void runQueryInSystemContext(EncodedQueryRequest encoded, Releasable slot) {
            if (end.get() != null) {
                // The replay ended while this query waited for its turn, which is why the queries are not decoded before.
                slot.close();
                queryDone();
                return;
            }
            try {
                shard.ensureRecoveryNotCancelled();
            } catch (RecoveryCancelledException e) {
                slot.close();
                end(End.ABANDONED);
                return;
            }
            final ShardSearchRequest request;
            final CancellableTask task;
            try {
                request = encoded.decode(namedWriteableRegistry);
                task = registerChildTask();
            } catch (Exception e) {
                slot.close();
                queryExecuted(e);
                return;
            }
            runningQueries.add(task);
            if (end.get() != null) {
                // the replay ended after the check above, so it did not see this task when it cancelled the running ones
                cancel(task, "the replay of phantom queries has ended");
            }
            final var completed = new AtomicBoolean();
            final ActionListener<Void> done = ActionListener.wrap(
                ignored -> complete(task, slot, completed, null),
                e -> complete(task, slot, completed, e)
            );
            try {
                queryExecution.execute(shard, request, task, done);
            } catch (Exception e) {
                done.onFailure(e);
            }
        }

        private void complete(CancellableTask task, Releasable slot, AtomicBoolean completed, Exception failure) {
            if (completed.compareAndSet(false, true) == false) {
                return;
            }
            // the slot goes first, so that the next query can start before the work that this one's end may trigger
            runningQueries.remove(task);
            taskManager.unregister(task);
            slot.close();
            queryExecuted(failure);
        }

        private void queryExecuted(Exception failure) {
            final QueryOutcome outcome;
            boolean trippedBreaker = false;
            if (failure == null) {
                outcome = QueryOutcome.SUCCESS;
            } else if (ExceptionsHelper.unwrap(failure, TaskCancelledException.class) != null) {
                outcome = QueryOutcome.CANCELLED;
            } else {
                outcome = QueryOutcome.FAILURE;
                trippedBreaker = ExceptionsHelper.unwrap(failure, CircuitBreakingException.class) != null;
                // Only the type: the message of a failure that a query causes may quote what the query asked for.
                logger.debug("{} a phantom query failed with [{}]", shardId, failure.getClass().getSimpleName());
            }
            metrics.recordExecuted(outcome);
            if (trippedBreaker) {
                end(End.BREAKER);
            } else {
                queryDone();
            }
        }

        private void queryDone() {
            if (unfinishedQueries.decrementAndGet() == 0) {
                end(End.COMPLETED);
            }
        }

        private void end(End reason) {
            if (end.compareAndSet(null, reason) == false) {
                return;
            }
            final Scheduler.ScheduledCancellable deadline = deadlineTask;
            if (deadline != null) {
                deadline.cancel();
            }
            if (reason != End.COMPLETED) {
                // Not waited for: the recovery goes on, and the searchers are given back when the queries notice and stop.
                final String cancellationReason = reason == End.DEADLINE
                    ? "the warming window of the recovery has ended"
                    : "the replay of phantom queries was stopped";
                runningQueries.forEach(task -> cancel(task, cancellationReason));
            }
            if (reason.outcome != null) {
                metrics.recordReplay(reason.outcome, recoveryType, threadPool.rawRelativeTimeInMillis() - startMillis);
            }
            logger.debug("{} replayed a sample of [{}] queries for the copy [{}]: {}", shardId, requests.size(), allocationId, reason);
            listener.onResponse(null);
        }

        private void cancel(CancellableTask task, String reason) {
            try {
                taskManager.cancel(task, reason, () -> {});
            } catch (Exception e) {
                logger.debug(() -> format("%s failed to cancel a phantom query", shardId), e);
            }
        }

        private CancellableTask registerChildTask() {
            final var taskRequest = new ChildTaskRequest(
                new TaskId(taskManager.getNodeId(), parentTask.getId()),
                // never the query
                "phantom query of the recovering copy [" + allocationId + "] of " + shardId
            );
            return (CancellableTask) taskManager.register(TASK_TYPE, TASK_ACTION, taskRequest);
        }
    }

    /**
     * Makes the task of a phantom query a cancellable child of the recovery task, so that it is listed under it by the task management
     * API.
     */
    private static final class ChildTaskRequest implements TaskAwareRequest {

        private TaskId parentTaskId;
        private final String description;

        ChildTaskRequest(TaskId parentTaskId, String description) {
            this.parentTaskId = parentTaskId;
            this.description = description;
        }

        @Override
        public void setParentTask(TaskId taskId) {
            this.parentTaskId = taskId;
        }

        @Override
        public void setRequestId(long requestId) {}

        @Override
        public TaskId getParentTask() {
            return parentTaskId;
        }

        @Override
        public String getDescription() {
            return description;
        }

        @Override
        public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
            return new CancellableTask(id, type, action, description, parentTaskId, headers);
        }
    }
}
