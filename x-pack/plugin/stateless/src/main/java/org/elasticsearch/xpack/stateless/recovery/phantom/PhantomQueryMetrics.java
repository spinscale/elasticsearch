/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.recovery.phantom;

import org.elasticsearch.telemetry.metric.DoubleHistogram;
import org.elasticsearch.telemetry.metric.LongCounter;
import org.elasticsearch.telemetry.metric.MeterRegistry;

import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Sampling is measured on the peer that retains the requests, and replay on the recovering node that executes them.
 */
public class PhantomQueryMetrics {

    public static final String SAMPLED_COUNTER = "es.search_recovery.phantom_queries.sampled.total";
    public static final String EXECUTED_COUNTER = "es.search_recovery.phantom_queries.executed.total";
    public static final String REPLAY_DURATION_HISTOGRAM = "es.search_recovery.phantom_queries.replay_duration.histogram";

    public static final String QUERY_OUTCOME_ATTRIBUTE = "es_phantom_query_outcome";
    public static final String REPLAY_OUTCOME_ATTRIBUTE = "es_phantom_replay_outcome";
    public static final String RECOVERY_TYPE_ATTRIBUTE = "es_search_recovery_type";

    /**
     * How one executed phantom query ended.
     */
    public enum QueryOutcome {
        SUCCESS,
        FAILURE,
        CANCELLED
    }

    /**
     * Why a replay ended, or why there was none.
     */
    public enum ReplayOutcome {
        /** Every query of the sample ran, and none of them tripped a circuit breaker. */
        COMPLETED,
        /** The warming window ended while queries were still queued or running. */
        DEADLINE,
        /** A query tripped a circuit breaker, which stops the replay of that shard. */
        BREAKER,
        /** The recovery has no warming wait, so there is no time to use. */
        NO_WINDOW,
        /** The warming wait used up its window. */
        NO_TIME_LEFT,
        /** The sample is empty, or could not be obtained. */
        NO_SAMPLE,
        /** Phantom queries are disabled on the recovering node. */
        DISABLED
    }

    /**
     * What recovers: a relocation target takes over the traffic of its source, a new copy does not.
     */
    public enum RecoveryType {
        RELOCATION,
        NEW_COPY
    }

    private final LongCounter sampledCounter;
    private final LongCounter executedCounter;
    private final DoubleHistogram replayDurationHistogram;

    public PhantomQueryMetrics(MeterRegistry meterRegistry) {
        this.sampledCounter = meterRegistry.registerLongCounter(
            SAMPLED_COUNTER,
            "Number of query-phase shard requests retained by a peer copy to be replayed on a recovering search shard copy",
            "unit"
        );
        this.executedCounter = meterRegistry.registerLongCounter(
            EXECUTED_COUNTER,
            "Number of phantom queries executed on a recovering search shard copy, broken down by [" + QUERY_OUTCOME_ATTRIBUTE + "]",
            "unit"
        );
        this.replayDurationHistogram = meterRegistry.registerDoubleHistogram(
            REPLAY_DURATION_HISTOGRAM,
            "Time a recovering search shard copy spent replaying its sample of phantom queries, broken down by ["
                + REPLAY_OUTCOME_ATTRIBUTE
                + "] and ["
                + RECOVERY_TYPE_ATTRIBUTE
                + "]",
            "s"
        );
    }

    public void recordSampled() {
        sampledCounter.increment();
    }

    public void recordExecuted(QueryOutcome outcome) {
        executedCounter.incrementBy(1L, Map.of(QUERY_OUTCOME_ATTRIBUTE, outcome.name()));
    }

    public void recordReplay(ReplayOutcome outcome, RecoveryType recoveryType, long durationMillis) {
        replayDurationHistogram.record(
            durationMillis / (double) TimeUnit.SECONDS.toMillis(1),
            Map.of(REPLAY_OUTCOME_ATTRIBUTE, outcome.name(), RECOVERY_TYPE_ATTRIBUTE, recoveryType.name())
        );
    }
}
