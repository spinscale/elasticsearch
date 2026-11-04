/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.recovery.phantom;

import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.telemetry.InstrumentType;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.RecordingMeterRegistry;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.stateless.recovery.phantom.PhantomQueryMetrics.CollectionOutcome;
import org.elasticsearch.xpack.stateless.recovery.phantom.PhantomQueryMetrics.DropReason;
import org.elasticsearch.xpack.stateless.recovery.phantom.PhantomQueryMetrics.QueryOutcome;
import org.elasticsearch.xpack.stateless.recovery.phantom.PhantomQueryMetrics.RecoveryType;
import org.elasticsearch.xpack.stateless.recovery.phantom.PhantomQueryMetrics.ReplayOutcome;
import org.elasticsearch.xpack.stateless.recovery.phantom.PhantomQueryMetrics.SamplerEnd;
import org.elasticsearch.xpack.stateless.recovery.phantom.PhantomQueryMetrics.SamplingStartOutcome;
import org.elasticsearch.xpack.stateless.recovery.phantom.PhantomQueryMetrics.ShardSizeBucket;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.stateless.recovery.phantom.PhantomQueryTestUtils.clusterStateWithInitializingSearchReplica;
import static org.elasticsearch.xpack.stateless.recovery.phantom.PhantomQueryTestUtils.clusterStateWithRelocatingSearchReplica;
import static org.elasticsearch.xpack.stateless.recovery.phantom.PhantomQueryTestUtils.shards;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

public class PhantomQueryMetricsTests extends ESTestCase {

    private static final long GIB = 1024L * 1024L * 1024L;

    public void testSampledCounterName() {
        assertThat(PhantomQueryMetrics.SAMPLED_COUNTER, equalTo("es.search_recovery.phantom_queries.sampled.total"));
    }

    public void testMetricNames() {
        assertThat(PhantomQueryMetrics.EXECUTED_COUNTER, equalTo("es.search_recovery.phantom_queries.executed.total"));
        assertThat(PhantomQueryMetrics.REPLAY_DURATION_HISTOGRAM, equalTo("es.search_recovery.phantom_queries.replay_duration.histogram"));
        assertThat(PhantomQueryMetrics.START_SAMPLING_COUNTER, equalTo("es.search_recovery.phantom_queries.start_sampling.total"));
        assertThat(PhantomQueryMetrics.DROPPED_COUNTER, equalTo("es.search_recovery.phantom_queries.dropped.total"));
        assertThat(PhantomQueryMetrics.SAMPLERS_ENDED_COUNTER, equalTo("es.search_recovery.phantom_queries.samplers_ended.total"));
        assertThat(PhantomQueryMetrics.SAMPLE_COLLECTION_COUNTER, equalTo("es.search_recovery.phantom_queries.sample_collection.total"));
        assertThat(PhantomQueryMetrics.NOT_EXECUTED_COUNTER, equalTo("es.search_recovery.phantom_queries.not_executed.total"));
        assertThat(
            PhantomQueryMetrics.EXECUTION_DURATION_HISTOGRAM,
            equalTo("es.search_recovery.phantom_queries.execution_duration.histogram")
        );
        assertThat(PhantomQueryMetrics.SAMPLE_QUERIES_HISTOGRAM, equalTo("es.search_recovery.phantom_queries.sample_queries.histogram"));
        assertThat(PhantomQueryMetrics.SAMPLE_SIZE_HISTOGRAM, equalTo("es.search_recovery.phantom_queries.sample_size.histogram"));
        assertThat(PhantomQueryMetrics.SAMPLERS_GAUGE, equalTo("es.search_recovery.phantom_queries.samplers.current"));
        assertThat(PhantomQueryMetrics.SAMPLE_BYTES_GAUGE, equalTo("es.search_recovery.phantom_queries.sample_bytes.current"));
    }

    public void testAttributeKeys() {
        assertThat(PhantomQueryMetrics.QUERY_OUTCOME_ATTRIBUTE, equalTo("es_phantom_query_outcome"));
        assertThat(PhantomQueryMetrics.REPLAY_OUTCOME_ATTRIBUTE, equalTo("es_phantom_replay_outcome"));
        assertThat(PhantomQueryMetrics.RECOVERY_TYPE_ATTRIBUTE, equalTo("es_search_recovery_type"));
        assertThat(PhantomQueryMetrics.SAMPLING_START_OUTCOME_ATTRIBUTE, equalTo("es_phantom_sampling_start_outcome"));
        assertThat(PhantomQueryMetrics.COLLECTION_OUTCOME_ATTRIBUTE, equalTo("es_phantom_collection_outcome"));
        assertThat(PhantomQueryMetrics.DROP_REASON_ATTRIBUTE, equalTo("es_phantom_drop_reason"));
        assertThat(PhantomQueryMetrics.SAMPLER_END_ATTRIBUTE, equalTo("es_phantom_sampler_end"));
        assertThat(PhantomQueryMetrics.SHARD_SIZE_BUCKET_ATTRIBUTE, equalTo("es_shard_size_bucket"));
    }

    public void testAttributeValues() {
        assertThat(names(QueryOutcome.values()), containsInAnyOrder("SUCCESS", "FAILURE", "CANCELLED"));
        assertThat(
            names(ReplayOutcome.values()),
            containsInAnyOrder("COMPLETED", "DEADLINE", "BREAKER", "NO_WINDOW", "NO_TIME_LEFT", "NO_SAMPLE", "DISABLED")
        );
        assertThat(names(RecoveryType.values()), containsInAnyOrder("RELOCATION", "NEW_COPY"));
        assertThat(
            names(SamplingStartOutcome.values()),
            containsInAnyOrder(
                "STARTED",
                "ALREADY_STARTED",
                "DECLINED_DISABLED",
                "DECLINED_NO_ACTIVE_REPLICA",
                "DECLINED_TARGET_UNKNOWN",
                "DECLINED_TARGET_NOT_INITIALIZING"
            )
        );
        assertThat(names(CollectionOutcome.values()), containsInAnyOrder("NO_PEER", "SKIPPED", "FAILED", "EMPTY", "COLLECTED"));
        assertThat(names(DropReason.values()), containsInAnyOrder("NODE_BUDGET", "SAMPLE_BYTES", "ENCODING_FAILED"));
        assertThat(names(SamplerEnd.values()), containsInAnyOrder("COLLECTED", "TARGET_GONE", "SHARD_CLOSED", "LEASE_EXPIRED"));
        assertThat(
            Arrays.stream(ShardSizeBucket.values()).map(ShardSizeBucket::label).collect(Collectors.toSet()),
            equalTo(Set.of("<=1GiB", "<=10GiB", "<=50GiB", ">50GiB", "UNKNOWN"))
        );
    }

    public void testMetricsAreRegisteredWithTheirInstrumentType() {
        final var meterRegistry = new RecordingMeterRegistry();
        new PhantomQueryMetrics(meterRegistry);

        assertThat(
            meterRegistry.getRecorder().getRegisteredMetrics(InstrumentType.LONG_COUNTER),
            containsInAnyOrder(
                PhantomQueryMetrics.SAMPLED_COUNTER,
                PhantomQueryMetrics.EXECUTED_COUNTER,
                PhantomQueryMetrics.START_SAMPLING_COUNTER,
                PhantomQueryMetrics.DROPPED_COUNTER,
                PhantomQueryMetrics.SAMPLERS_ENDED_COUNTER,
                PhantomQueryMetrics.SAMPLE_COLLECTION_COUNTER,
                PhantomQueryMetrics.NOT_EXECUTED_COUNTER
            )
        );
        assertThat(
            meterRegistry.getRecorder().getRegisteredMetrics(InstrumentType.DOUBLE_HISTOGRAM),
            containsInAnyOrder(PhantomQueryMetrics.REPLAY_DURATION_HISTOGRAM, PhantomQueryMetrics.EXECUTION_DURATION_HISTOGRAM)
        );
        assertThat(
            meterRegistry.getRecorder().getRegisteredMetrics(InstrumentType.LONG_HISTOGRAM),
            containsInAnyOrder(PhantomQueryMetrics.SAMPLE_QUERIES_HISTOGRAM, PhantomQueryMetrics.SAMPLE_SIZE_HISTOGRAM)
        );
        assertThat(
            "the gauges need the samplers of the node",
            meterRegistry.getRecorder().getRegisteredMetrics(InstrumentType.LONG_ASYNC_GAUGE),
            empty()
        );
        for (String counter : meterRegistry.getRecorder().getRegisteredMetrics(InstrumentType.LONG_COUNTER)) {
            assertThat(counter, meterRegistry.getRecorder().getMeasurements(InstrumentType.LONG_COUNTER, counter), empty());
        }
        for (String histogram : meterRegistry.getRecorder().getRegisteredMetrics(InstrumentType.DOUBLE_HISTOGRAM)) {
            assertThat(histogram, meterRegistry.getRecorder().getMeasurements(InstrumentType.DOUBLE_HISTOGRAM, histogram), empty());
        }
        for (String histogram : meterRegistry.getRecorder().getRegisteredMetrics(InstrumentType.LONG_HISTOGRAM)) {
            assertThat(histogram, meterRegistry.getRecorder().getMeasurements(InstrumentType.LONG_HISTOGRAM, histogram), empty());
        }
    }

    public void testSamplerGaugesReportWhatTheirSuppliersReturn() {
        final var meterRegistry = new RecordingMeterRegistry();
        final var metrics = new PhantomQueryMetrics(meterRegistry);
        final var samplers = new AtomicLong();
        final var bytes = new AtomicLong();

        metrics.registerSamplerGauges(samplers::get, bytes::get);

        assertThat(
            meterRegistry.getRecorder().getRegisteredMetrics(InstrumentType.LONG_ASYNC_GAUGE),
            containsInAnyOrder(PhantomQueryMetrics.SAMPLERS_GAUGE, PhantomQueryMetrics.SAMPLE_BYTES_GAUGE)
        );
        assertThat(
            "a node that samples for no copy reports zero",
            PhantomQueryTestUtils.gauge(meterRegistry, PhantomQueryMetrics.SAMPLERS_GAUGE),
            equalTo(0L)
        );
        assertThat(PhantomQueryTestUtils.gauge(meterRegistry, PhantomQueryMetrics.SAMPLE_BYTES_GAUGE), equalTo(0L));

        final long expectedSamplers = randomLongBetween(1, 50);
        final long expectedBytes = randomLongBetween(1, 1L << 30);
        samplers.set(expectedSamplers);
        bytes.set(expectedBytes);
        assertThat(PhantomQueryTestUtils.gauge(meterRegistry, PhantomQueryMetrics.SAMPLERS_GAUGE), equalTo(expectedSamplers));
        assertThat(PhantomQueryTestUtils.gauge(meterRegistry, PhantomQueryMetrics.SAMPLE_BYTES_GAUGE), equalTo(expectedBytes));

        samplers.set(0);
        bytes.set(0);
        assertThat(PhantomQueryTestUtils.gauge(meterRegistry, PhantomQueryMetrics.SAMPLERS_GAUGE), equalTo(0L));
        assertThat(PhantomQueryTestUtils.gauge(meterRegistry, PhantomQueryMetrics.SAMPLE_BYTES_GAUGE), equalTo(0L));
        for (Measurement measurement : meterRegistry.getRecorder().getAllMeasurements()) {
            assertThat("the gauges have no attributes", measurement.attributes(), equalTo(Map.of()));
        }
    }

    public void testRecordSampledIncrementsTheCounter() {
        final var meterRegistry = new RecordingMeterRegistry();
        final var metrics = new PhantomQueryMetrics(meterRegistry);

        final int sampled = randomIntBetween(1, 20);
        for (int i = 0; i < sampled; i++) {
            metrics.recordSampled();
        }

        assertThat(PhantomQueryTestUtils.counterTotal(meterRegistry, PhantomQueryMetrics.SAMPLED_COUNTER), equalTo((long) sampled));
    }

    public void testRecordExecutedCountsEachOutcomeWithItsAttribute() {
        final var meterRegistry = new RecordingMeterRegistry();
        final var metrics = new PhantomQueryMetrics(meterRegistry);

        final int successes = randomIntBetween(1, 5);
        final int failures = randomIntBetween(1, 5);
        final int cancellations = randomIntBetween(1, 5);
        for (int i = 0; i < successes; i++) {
            metrics.recordExecuted(QueryOutcome.SUCCESS);
        }
        for (int i = 0; i < failures; i++) {
            metrics.recordExecuted(QueryOutcome.FAILURE);
        }
        for (int i = 0; i < cancellations; i++) {
            metrics.recordExecuted(QueryOutcome.CANCELLED);
        }

        final List<Measurement> measurements = meterRegistry.getRecorder()
            .getMeasurements(InstrumentType.LONG_COUNTER, PhantomQueryMetrics.EXECUTED_COUNTER);
        assertThat(measurements, hasSize(successes + failures + cancellations));
        for (Measurement measurement : measurements) {
            assertThat(measurement.getLong(), equalTo(1L));
            assertThat(measurement.attributes().keySet(), equalTo(Set.of(PhantomQueryMetrics.QUERY_OUTCOME_ATTRIBUTE)));
        }
        assertThat(
            PhantomQueryTestUtils.counterBy(
                meterRegistry,
                PhantomQueryMetrics.EXECUTED_COUNTER,
                PhantomQueryMetrics.QUERY_OUTCOME_ATTRIBUTE
            ),
            equalTo(Map.of("SUCCESS", (long) successes, "FAILURE", (long) failures, "CANCELLED", (long) cancellations))
        );
    }

    public void testRecordReplayRecordsSecondsWithOutcomeRecoveryTypeAndShardSize() {
        final var meterRegistry = new RecordingMeterRegistry();
        final var metrics = new PhantomQueryMetrics(meterRegistry);

        final ReplayOutcome outcome = randomFrom(ReplayOutcome.values());
        final RecoveryType recoveryType = randomFrom(RecoveryType.values());
        final ShardSizeBucket shardSize = randomFrom(ShardSizeBucket.values());
        metrics.recordReplay(outcome, recoveryType, shardSize, 1500L);

        final List<Measurement> measurements = meterRegistry.getRecorder()
            .getMeasurements(InstrumentType.DOUBLE_HISTOGRAM, PhantomQueryMetrics.REPLAY_DURATION_HISTOGRAM);
        assertThat(measurements, hasSize(1));
        assertThat("the unit is seconds", measurements.get(0).getDouble(), closeTo(1.5, 1e-9));
        assertThat(
            measurements.get(0).attributes(),
            equalTo(
                Map.of(
                    PhantomQueryMetrics.REPLAY_OUTCOME_ATTRIBUTE,
                    outcome.name(),
                    PhantomQueryMetrics.RECOVERY_TYPE_ATTRIBUTE,
                    recoveryType.name(),
                    PhantomQueryMetrics.SHARD_SIZE_BUCKET_ATTRIBUTE,
                    shardSize.label()
                )
            )
        );
    }

    public void testRecordStartSamplingCountsEachOutcomeWithItsAttribute() {
        final var meterRegistry = new RecordingMeterRegistry();
        final var metrics = new PhantomQueryMetrics(meterRegistry);

        for (SamplingStartOutcome outcome : SamplingStartOutcome.values()) {
            for (int i = 0; i <= outcome.ordinal(); i++) {
                metrics.recordStartSampling(outcome);
            }
        }

        for (Measurement measurement : meterRegistry.getRecorder()
            .getMeasurements(InstrumentType.LONG_COUNTER, PhantomQueryMetrics.START_SAMPLING_COUNTER)) {
            assertThat(measurement.getLong(), equalTo(1L));
            assertThat(measurement.attributes().keySet(), equalTo(Set.of(PhantomQueryMetrics.SAMPLING_START_OUTCOME_ATTRIBUTE)));
        }
        final Map<String, Long> counted = PhantomQueryTestUtils.counterBy(
            meterRegistry,
            PhantomQueryMetrics.START_SAMPLING_COUNTER,
            PhantomQueryMetrics.SAMPLING_START_OUTCOME_ATTRIBUTE
        );
        for (SamplingStartOutcome outcome : SamplingStartOutcome.values()) {
            assertThat(outcome.name(), counted.get(outcome.name()), equalTo((long) outcome.ordinal() + 1));
        }
    }

    public void testOnlyTheOutcomesThatSampleAreSampling() {
        assertThat(
            Arrays.stream(SamplingStartOutcome.values()).filter(SamplingStartOutcome::sampling).collect(Collectors.toSet()),
            equalTo(Set.of(SamplingStartOutcome.STARTED, SamplingStartOutcome.ALREADY_STARTED))
        );
    }

    public void testRecordDroppedCountsEachReasonWithItsAttribute() {
        final var meterRegistry = new RecordingMeterRegistry();
        final var metrics = new PhantomQueryMetrics(meterRegistry);

        for (DropReason reason : DropReason.values()) {
            for (int i = 0; i <= reason.ordinal(); i++) {
                metrics.recordDropped(reason);
            }
        }

        for (Measurement measurement : meterRegistry.getRecorder()
            .getMeasurements(InstrumentType.LONG_COUNTER, PhantomQueryMetrics.DROPPED_COUNTER)) {
            assertThat(measurement.getLong(), equalTo(1L));
            assertThat(measurement.attributes().keySet(), equalTo(Set.of(PhantomQueryMetrics.DROP_REASON_ATTRIBUTE)));
        }
        final Map<String, Long> counted = PhantomQueryTestUtils.counterBy(
            meterRegistry,
            PhantomQueryMetrics.DROPPED_COUNTER,
            PhantomQueryMetrics.DROP_REASON_ATTRIBUTE
        );
        for (DropReason reason : DropReason.values()) {
            assertThat(reason.name(), counted.get(reason.name()), equalTo((long) reason.ordinal() + 1));
        }
    }

    public void testRecordSamplerEndedCountsEachEndWithItsAttribute() {
        final var meterRegistry = new RecordingMeterRegistry();
        final var metrics = new PhantomQueryMetrics(meterRegistry);

        for (SamplerEnd end : SamplerEnd.values()) {
            for (int i = 0; i <= end.ordinal(); i++) {
                metrics.recordSamplerEnded(end);
            }
        }

        for (Measurement measurement : meterRegistry.getRecorder()
            .getMeasurements(InstrumentType.LONG_COUNTER, PhantomQueryMetrics.SAMPLERS_ENDED_COUNTER)) {
            assertThat(measurement.getLong(), equalTo(1L));
            assertThat(measurement.attributes().keySet(), equalTo(Set.of(PhantomQueryMetrics.SAMPLER_END_ATTRIBUTE)));
        }
        final Map<String, Long> counted = PhantomQueryTestUtils.counterBy(
            meterRegistry,
            PhantomQueryMetrics.SAMPLERS_ENDED_COUNTER,
            PhantomQueryMetrics.SAMPLER_END_ATTRIBUTE
        );
        for (SamplerEnd end : SamplerEnd.values()) {
            assertThat(end.name(), counted.get(end.name()), equalTo((long) end.ordinal() + 1));
        }
    }

    public void testRecordSampleCollectionCarriesTheOutcomeAndTheRecoveryType() {
        final var meterRegistry = new RecordingMeterRegistry();
        final var metrics = new PhantomQueryMetrics(meterRegistry);

        final CollectionOutcome outcome = randomFrom(CollectionOutcome.values());
        final RecoveryType recoveryType = randomFrom(RecoveryType.values());
        metrics.recordSampleCollection(outcome, recoveryType);

        final List<Measurement> measurements = meterRegistry.getRecorder()
            .getMeasurements(InstrumentType.LONG_COUNTER, PhantomQueryMetrics.SAMPLE_COLLECTION_COUNTER);
        assertThat(measurements, hasSize(1));
        assertThat(measurements.get(0).getLong(), equalTo(1L));
        assertThat(
            measurements.get(0).attributes(),
            equalTo(
                Map.of(
                    PhantomQueryMetrics.COLLECTION_OUTCOME_ATTRIBUTE,
                    outcome.name(),
                    PhantomQueryMetrics.RECOVERY_TYPE_ATTRIBUTE,
                    recoveryType.name()
                )
            )
        );
    }

    public void testRecordNotExecutedCarriesTheOutcomeThatEndedTheReplay() {
        final var meterRegistry = new RecordingMeterRegistry();
        final var metrics = new PhantomQueryMetrics(meterRegistry);

        metrics.recordNotExecuted(ReplayOutcome.DEADLINE);
        metrics.recordNotExecuted(ReplayOutcome.DEADLINE);
        metrics.recordNotExecuted(ReplayOutcome.BREAKER);

        assertThat(
            PhantomQueryTestUtils.counterBy(
                meterRegistry,
                PhantomQueryMetrics.NOT_EXECUTED_COUNTER,
                PhantomQueryMetrics.REPLAY_OUTCOME_ATTRIBUTE
            ),
            equalTo(Map.of("DEADLINE", 2L, "BREAKER", 1L))
        );
        for (Measurement measurement : meterRegistry.getRecorder()
            .getMeasurements(InstrumentType.LONG_COUNTER, PhantomQueryMetrics.NOT_EXECUTED_COUNTER)) {
            assertThat(measurement.attributes().keySet(), equalTo(Set.of(PhantomQueryMetrics.REPLAY_OUTCOME_ATTRIBUTE)));
        }
    }

    public void testOnlyTheDeadlineAndACircuitBreakerLeaveQueriesThatNeverStarted() {
        final var metrics = new PhantomQueryMetrics(new RecordingMeterRegistry());
        for (ReplayOutcome outcome : ReplayOutcome.values()) {
            if (outcome == ReplayOutcome.DEADLINE || outcome == ReplayOutcome.BREAKER) {
                continue;
            }
            expectThrows(AssertionError.class, outcome.name(), () -> metrics.recordNotExecuted(outcome));
        }
    }

    public void testRecordExecutionDurationRecordsSecondsWithTheOutcome() {
        final var meterRegistry = new RecordingMeterRegistry();
        final var metrics = new PhantomQueryMetrics(meterRegistry);

        final QueryOutcome outcome = randomFrom(QueryOutcome.values());
        metrics.recordExecutionDuration(outcome, 250L);

        final List<Measurement> measurements = meterRegistry.getRecorder()
            .getMeasurements(InstrumentType.DOUBLE_HISTOGRAM, PhantomQueryMetrics.EXECUTION_DURATION_HISTOGRAM);
        assertThat(measurements, hasSize(1));
        assertThat("the unit is seconds", measurements.get(0).getDouble(), closeTo(0.25, 1e-9));
        assertThat(measurements.get(0).attributes(), equalTo(Map.of(PhantomQueryMetrics.QUERY_OUTCOME_ATTRIBUTE, outcome.name())));
    }

    public void testRecordSampleCollectedRecordsTheQueriesAndTheKilobytesRoundedUp() {
        final var meterRegistry = new RecordingMeterRegistry();
        final var metrics = new PhantomQueryMetrics(meterRegistry);

        metrics.recordSampleCollected(12, 30 * 1024L);
        metrics.recordSampleCollected(3, 30 * 1024L + 1);
        metrics.recordSampleCollected(1, 1);
        metrics.recordSampleCollected(0, 0);

        assertThat(
            PhantomQueryTestUtils.longHistogram(meterRegistry, PhantomQueryMetrics.SAMPLE_QUERIES_HISTOGRAM),
            equalTo(List.of(12L, 3L, 1L, 0L))
        );
        assertThat(
            PhantomQueryTestUtils.longHistogram(meterRegistry, PhantomQueryMetrics.SAMPLE_SIZE_HISTOGRAM),
            equalTo(List.of(30L, 31L, 1L, 0L))
        );
        for (Measurement measurement : meterRegistry.getRecorder().getAllMeasurements()) {
            assertThat("the histograms have no attributes", measurement.attributes(), equalTo(Map.of()));
        }
    }

    public void testKilobytesAreRoundedUpAndOnlyAnEmptySampleRecordsZero() {
        assertThat(PhantomQueryMetrics.toKilobytesRoundedUp(0), equalTo(0L));
        assertThat(PhantomQueryMetrics.toKilobytesRoundedUp(1), equalTo(1L));
        assertThat(PhantomQueryMetrics.toKilobytesRoundedUp(1024), equalTo(1L));
        assertThat(PhantomQueryMetrics.toKilobytesRoundedUp(1025), equalTo(2L));
        assertThat(PhantomQueryMetrics.toKilobytesRoundedUp(1024L * 1024L), equalTo(1024L));
        assertThat(
            "the default limit of 1mb stays inside the default histogram boundaries, which stop at 131072",
            PhantomQueryMetrics.toKilobytesRoundedUp(randomLongBetween(1, 16L * 1024L * 1024L)) <= 16L * 1024L,
            is(true)
        );
    }

    public void testShardSizeBucketBoundaries() {
        assertThat(ShardSizeBucket.of(1), equalTo(ShardSizeBucket.UP_TO_1_GIB));
        assertThat(ShardSizeBucket.of(GIB), equalTo(ShardSizeBucket.UP_TO_1_GIB));
        assertThat(ShardSizeBucket.of(GIB + 1), equalTo(ShardSizeBucket.UP_TO_10_GIB));
        assertThat(ShardSizeBucket.of(10 * GIB), equalTo(ShardSizeBucket.UP_TO_10_GIB));
        assertThat(ShardSizeBucket.of(10 * GIB + 1), equalTo(ShardSizeBucket.UP_TO_50_GIB));
        assertThat(ShardSizeBucket.of(50 * GIB), equalTo(ShardSizeBucket.UP_TO_50_GIB));
        assertThat(ShardSizeBucket.of(50 * GIB + 1), equalTo(ShardSizeBucket.OVER_50_GIB));
        assertThat(ShardSizeBucket.of(Long.MAX_VALUE), equalTo(ShardSizeBucket.OVER_50_GIB));
    }

    public void testASizeThatCannotBeReadIsUnknown() {
        assertThat(ShardSizeBucket.of(0), equalTo(ShardSizeBucket.UNKNOWN));
        assertThat(ShardSizeBucket.of(-1), equalTo(ShardSizeBucket.UNKNOWN));
        assertThat(ShardSizeBucket.of(Long.MIN_VALUE), equalTo(ShardSizeBucket.UNKNOWN));
    }

    public void testTheLabelsOfTheBucketsAreTheAttributeValues() {
        assertThat(ShardSizeBucket.UP_TO_1_GIB.label(), equalTo("<=1GiB"));
        assertThat(ShardSizeBucket.UP_TO_10_GIB.label(), equalTo("<=10GiB"));
        assertThat(ShardSizeBucket.UP_TO_50_GIB.label(), equalTo("<=50GiB"));
        assertThat(ShardSizeBucket.OVER_50_GIB.label(), equalTo(">50GiB"));
        assertThat(ShardSizeBucket.UNKNOWN.label(), equalTo("UNKNOWN"));
    }

    public void testRecoveryTypeIsRelocationOnlyForARelocationTarget() {
        final ShardRouting newCopy = shards(clusterStateWithInitializingSearchReplica()).stream()
            .filter(ShardRouting::initializing)
            .findFirst()
            .orElseThrow();
        final ShardRouting relocationTarget = shards(clusterStateWithRelocatingSearchReplica()).stream()
            .filter(ShardRouting::relocating)
            .findFirst()
            .orElseThrow()
            .getTargetRelocatingShard();

        assertThat(RecoveryType.of(newCopy), equalTo(RecoveryType.NEW_COPY));
        assertThat(RecoveryType.of(relocationTarget), equalTo(RecoveryType.RELOCATION));
    }

    private static Set<String> names(Enum<?>[] values) {
        return Arrays.stream(values).map(Enum::name).collect(Collectors.toSet());
    }
}
