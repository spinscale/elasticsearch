/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.recovery.phantom;

import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.telemetry.metric.DoubleHistogram;
import org.elasticsearch.telemetry.metric.LongCounter;
import org.elasticsearch.telemetry.metric.LongHistogram;
import org.elasticsearch.telemetry.metric.MeterRegistry;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

/**
 * Sampling is measured on the peer that retains the requests, and replay on the recovering node that executes them. The attributes only
 * take the values of the enumerations below, never an identifier of a shard, index, node or allocation, so their cardinality is bounded.
 */
public class PhantomQueryMetrics {

    public static final String SAMPLED_COUNTER = "es.search_recovery.phantom_queries.sampled.total";
    public static final String EXECUTED_COUNTER = "es.search_recovery.phantom_queries.executed.total";
    public static final String REPLAY_DURATION_HISTOGRAM = "es.search_recovery.phantom_queries.replay_duration.histogram";
    public static final String START_SAMPLING_COUNTER = "es.search_recovery.phantom_queries.start_sampling.total";
    public static final String DROPPED_COUNTER = "es.search_recovery.phantom_queries.dropped.total";
    public static final String SAMPLERS_ENDED_COUNTER = "es.search_recovery.phantom_queries.samplers_ended.total";
    public static final String SAMPLE_COLLECTION_COUNTER = "es.search_recovery.phantom_queries.sample_collection.total";
    public static final String NOT_EXECUTED_COUNTER = "es.search_recovery.phantom_queries.not_executed.total";
    public static final String EXECUTION_DURATION_HISTOGRAM = "es.search_recovery.phantom_queries.execution_duration.histogram";
    public static final String SAMPLE_QUERIES_HISTOGRAM = "es.search_recovery.phantom_queries.sample_queries.histogram";
    public static final String SAMPLE_SIZE_HISTOGRAM = "es.search_recovery.phantom_queries.sample_size.histogram";
    public static final String SAMPLERS_GAUGE = "es.search_recovery.phantom_queries.samplers.current";
    public static final String SAMPLE_BYTES_GAUGE = "es.search_recovery.phantom_queries.sample_bytes.current";

    public static final String QUERY_OUTCOME_ATTRIBUTE = "es_phantom_query_outcome";
    public static final String REPLAY_OUTCOME_ATTRIBUTE = "es_phantom_replay_outcome";
    public static final String RECOVERY_TYPE_ATTRIBUTE = "es_search_recovery_type";
    public static final String SAMPLING_START_OUTCOME_ATTRIBUTE = "es_phantom_sampling_start_outcome";
    public static final String COLLECTION_OUTCOME_ATTRIBUTE = "es_phantom_collection_outcome";
    public static final String DROP_REASON_ATTRIBUTE = "es_phantom_drop_reason";
    public static final String SAMPLER_END_ATTRIBUTE = "es_phantom_sampler_end";
    public static final String SHARD_SIZE_BUCKET_ATTRIBUTE = "es_shard_size_bucket";

    private static final long BYTES_PER_KILOBYTE = 1024L;

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
        NEW_COPY;

        /**
         * @param recoveringCopy the routing entry of the recovering copy, which has a relocation source when it is a relocation target
         */
        public static RecoveryType of(ShardRouting recoveringCopy) {
            return recoveringCopy.relocatingNodeId() != null ? RELOCATION : NEW_COPY;
        }
    }

    /**
     * What a peer did with a request to start sampling. The two {@code DECLINED_TARGET_*} outcomes tell a peer that does not know the
     * recovering copy yet, which is what a peer that lags behind in cluster state looks like, from one that knows it but finds it is not an
     * initializing search replica.
     */
    public enum SamplingStartOutcome {
        /** The peer registered a sampler. */
        STARTED,
        /** A repeated request found the sampler that is already there and kept it. */
        ALREADY_STARTED,
        /** Phantom queries are disabled on the peer. */
        DECLINED_DISABLED,
        /** The peer holds no started search replica of the shard. */
        DECLINED_NO_ACTIVE_REPLICA,
        /** The peer's cluster state has no copy with the allocation id of the target. */
        DECLINED_TARGET_UNKNOWN,
        /** The peer knows the copy, but it is not an initializing search replica: it started, is a primary, or is a relocation source. */
        DECLINED_TARGET_NOT_INITIALIZING;

        private final Map<String, Object> attributes = Map.of(SAMPLING_START_OUTCOME_ATTRIBUTE, name());

        /** Whether the peer samples for the copy after the request. */
        public boolean sampling() {
            return this == STARTED || this == ALREADY_STARTED;
        }
    }

    /**
     * How a recovering copy got its sample, or why it got none.
     */
    public enum CollectionOutcome {
        /** The setting is enabled, but no started copy on another node supports sampling. */
        NO_PEER,
        /** The request to collect was not sent: the setting was disabled meanwhile, or the warming window was missing or used up. */
        SKIPPED,
        /** Selecting the peer or the request to collect failed or timed out. */
        FAILED,
        /** The peer returned no requests: it declined, had no sampler, or saw no eligible request. */
        EMPTY,
        /** The sample has requests. */
        COLLECTED
    }

    /**
     * Why a sampler did not retain an eligible request that reached it while it was still accepting requests. Reaching the limit of
     * queries per sample is not one of them: it ends sampling.
     */
    public enum DropReason {
        /** The shared budget of the node had no room. The sampler stays open, because other samples may free budget. */
        NODE_BUDGET,
        /** The request would exceed the byte limit of the sample, which also stops that sampler. */
        SAMPLE_BYTES,
        /** The request could not be serialized. */
        ENCODING_FAILED;

        private final Map<String, Object> attributes = Map.of(DROP_REASON_ATTRIBUTE, name());
    }

    /**
     * Why a sampler ended.
     */
    public enum SamplerEnd {
        /** The recovering copy collected it. */
        COLLECTED,
        /** The routing table no longer shows the recovering copy as initializing: it started without collecting, failed, or was removed. */
        TARGET_GONE,
        /** The copy of the peer closed. */
        SHARD_CLOSED,
        /** Nobody collected it within the lease. */
        LEASE_EXPIRED;

        private final Map<String, Object> attributes = Map.of(SAMPLER_END_ATTRIBUTE, name());
    }

    /**
     * The size of the data set of the recovering copy, in buckets, so that outcomes can be compared by shard size without an attribute of
     * unbounded cardinality.
     */
    public enum ShardSizeBucket {
        UP_TO_1_GIB("<=1GiB"),
        UP_TO_10_GIB("<=10GiB"),
        UP_TO_50_GIB("<=50GiB"),
        OVER_50_GIB(">50GiB"),
        /** The size could not be read, or is reported as zero. */
        UNKNOWN("UNKNOWN");

        private static final long GIB = 1024L * 1024L * 1024L;

        private final String label;

        ShardSizeBucket(String label) {
            this.label = label;
        }

        public String label() {
            return label;
        }

        public static ShardSizeBucket of(long dataSetSizeInBytes) {
            if (dataSetSizeInBytes <= 0) {
                return UNKNOWN;
            } else if (dataSetSizeInBytes <= GIB) {
                return UP_TO_1_GIB;
            } else if (dataSetSizeInBytes <= 10 * GIB) {
                return UP_TO_10_GIB;
            } else if (dataSetSizeInBytes <= 50 * GIB) {
                return UP_TO_50_GIB;
            } else {
                return OVER_50_GIB;
            }
        }
    }

    private final MeterRegistry meterRegistry;
    private final LongCounter sampledCounter;
    private final LongCounter executedCounter;
    private final DoubleHistogram replayDurationHistogram;
    private final LongCounter startSamplingCounter;
    private final LongCounter droppedCounter;
    private final LongCounter samplersEndedCounter;
    private final LongCounter sampleCollectionCounter;
    private final LongCounter notExecutedCounter;
    private final DoubleHistogram executionDurationHistogram;
    private final LongHistogram sampleQueriesHistogram;
    private final LongHistogram sampleSizeHistogram;

    public PhantomQueryMetrics(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
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
                + "], ["
                + RECOVERY_TYPE_ATTRIBUTE
                + "] and ["
                + SHARD_SIZE_BUCKET_ATTRIBUTE
                + "]",
            "s"
        );
        this.startSamplingCounter = meterRegistry.registerLongCounter(
            START_SAMPLING_COUNTER,
            "Number of requests to start sampling that a peer copy handled, broken down by [" + SAMPLING_START_OUTCOME_ATTRIBUTE + "]",
            "unit"
        );
        this.droppedCounter = meterRegistry.registerLongCounter(
            DROPPED_COUNTER,
            "Number of eligible query-phase shard requests that a sampler still accepting requests did not retain, broken down by ["
                + DROP_REASON_ATTRIBUTE
                + "]",
            "unit"
        );
        this.samplersEndedCounter = meterRegistry.registerLongCounter(
            SAMPLERS_ENDED_COUNTER,
            "Number of samplers that ended on a peer copy, broken down by [" + SAMPLER_END_ATTRIBUTE + "]",
            "unit"
        );
        this.sampleCollectionCounter = meterRegistry.registerLongCounter(
            SAMPLE_COLLECTION_COUNTER,
            "Number of times a recovering search shard copy obtained, or failed to obtain, its sample, broken down by ["
                + COLLECTION_OUTCOME_ATTRIBUTE
                + "] and ["
                + RECOVERY_TYPE_ATTRIBUTE
                + "]",
            "unit"
        );
        this.notExecutedCounter = meterRegistry.registerLongCounter(
            NOT_EXECUTED_COUNTER,
            "Number of phantom queries of a sample that never started because the replay had already ended, broken down by the ["
                + REPLAY_OUTCOME_ATTRIBUTE
                + "] that ended it",
            "unit"
        );
        this.executionDurationHistogram = meterRegistry.registerDoubleHistogram(
            EXECUTION_DURATION_HISTOGRAM,
            "Time a phantom query took from the moment it started executing, broken down by [" + QUERY_OUTCOME_ATTRIBUTE + "]",
            "s"
        );
        this.sampleQueriesHistogram = meterRegistry.registerLongHistogram(
            SAMPLE_QUERIES_HISTOGRAM,
            "Number of query-phase shard requests in each sample that a peer copy handed to a recovering copy",
            "unit"
        );
        this.sampleSizeHistogram = meterRegistry.registerLongHistogram(
            SAMPLE_SIZE_HISTOGRAM,
            // in kilobytes so that the default bucket boundaries, which stop at 131072, cover the default limit of 1 MB per sample
            "Serialized size in kilobytes, rounded up, of each sample that a peer copy handed to a recovering copy",
            "kilobytes"
        );
    }

    /**
     * Registers the gauges over the samplers that a node holds. They are read when metrics are reported, and a node that samples for no
     * copy reports zero.
     *
     * @param samplers      how many samplers the node holds
     * @param retainedBytes how many bytes the retained requests of all its samplers hold
     */
    public void registerSamplerGauges(LongSupplier samplers, LongSupplier retainedBytes) {
        meterRegistry.registerLongAsyncGauge(
            SAMPLERS_GAUGE,
            "Number of samplers that a node holds for recovering copies",
            "unit",
            samplers
        );
        meterRegistry.registerLongAsyncGauge(
            SAMPLE_BYTES_GAUGE,
            "Serialized bytes of the query-phase shard requests that the samplers of a node hold",
            "bytes",
            retainedBytes
        );
    }

    public void recordSampled() {
        sampledCounter.increment();
    }

    public void recordExecuted(QueryOutcome outcome) {
        executedCounter.incrementBy(1L, Map.of(QUERY_OUTCOME_ATTRIBUTE, outcome.name()));
    }

    public void recordReplay(ReplayOutcome outcome, RecoveryType recoveryType, ShardSizeBucket shardSize, long durationMillis) {
        replayDurationHistogram.record(
            durationMillis / (double) TimeUnit.SECONDS.toMillis(1),
            Map.of(
                REPLAY_OUTCOME_ATTRIBUTE,
                outcome.name(),
                RECOVERY_TYPE_ATTRIBUTE,
                recoveryType.name(),
                SHARD_SIZE_BUCKET_ATTRIBUTE,
                shardSize.label()
            )
        );
    }

    public void recordStartSampling(SamplingStartOutcome outcome) {
        startSamplingCounter.incrementBy(1L, outcome.attributes);
    }

    /** Not allocating, because a peer records it on the thread that receives a query, for every request that a full node budget drops. */
    public void recordDropped(DropReason reason) {
        droppedCounter.incrementBy(1L, reason.attributes);
    }

    public void recordSamplerEnded(SamplerEnd end) {
        samplersEndedCounter.incrementBy(1L, end.attributes);
    }

    public void recordSampleCollection(CollectionOutcome outcome, RecoveryType recoveryType) {
        sampleCollectionCounter.incrementBy(
            1L,
            Map.of(COLLECTION_OUTCOME_ATTRIBUTE, outcome.name(), RECOVERY_TYPE_ATTRIBUTE, recoveryType.name())
        );
    }

    /**
     * @param outcome how the replay ended, which is the deadline or a circuit breaker, because only those leave queries that never started
     */
    public void recordNotExecuted(ReplayOutcome outcome) {
        assert outcome == ReplayOutcome.DEADLINE || outcome == ReplayOutcome.BREAKER : outcome;
        notExecutedCounter.incrementBy(1L, Map.of(REPLAY_OUTCOME_ATTRIBUTE, outcome.name()));
    }

    public void recordExecutionDuration(QueryOutcome outcome, long durationMillis) {
        executionDurationHistogram.record(
            durationMillis / (double) TimeUnit.SECONDS.toMillis(1),
            Map.of(QUERY_OUTCOME_ATTRIBUTE, outcome.name())
        );
    }

    /**
     * Records a sample that a peer hands over, an empty one included, so that shards without traffic show up.
     *
     * @param queries the number of requests in the sample
     * @param bytes   their serialized size
     */
    public void recordSampleCollected(int queries, long bytes) {
        sampleQueriesHistogram.record(queries);
        sampleSizeHistogram.record(toKilobytesRoundedUp(bytes));
    }

    // a sample with requests never records zero
    static long toKilobytesRoundedUp(long bytes) {
        return bytes <= 0 ? 0L : (bytes - 1) / BYTES_PER_KILOBYTE + 1;
    }
}
