/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.recovery.phantom;

import org.elasticsearch.telemetry.InstrumentType;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.RecordingMeterRegistry;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.stateless.recovery.phantom.PhantomQueryMetrics.QueryOutcome;
import org.elasticsearch.xpack.stateless.recovery.phantom.PhantomQueryMetrics.RecoveryType;
import org.elasticsearch.xpack.stateless.recovery.phantom.PhantomQueryMetrics.ReplayOutcome;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class PhantomQueryMetricsTests extends ESTestCase {

    public void testSampledCounterName() {
        assertThat(PhantomQueryMetrics.SAMPLED_COUNTER, equalTo("es.search_recovery.phantom_queries.sampled.total"));
    }

    public void testMetricNames() {
        assertThat(PhantomQueryMetrics.EXECUTED_COUNTER, equalTo("es.search_recovery.phantom_queries.executed.total"));
        assertThat(PhantomQueryMetrics.REPLAY_DURATION_HISTOGRAM, equalTo("es.search_recovery.phantom_queries.replay_duration.histogram"));
    }

    public void testAttributeKeys() {
        assertThat(PhantomQueryMetrics.QUERY_OUTCOME_ATTRIBUTE, equalTo("es_phantom_query_outcome"));
        assertThat(PhantomQueryMetrics.REPLAY_OUTCOME_ATTRIBUTE, equalTo("es_phantom_replay_outcome"));
        assertThat(PhantomQueryMetrics.RECOVERY_TYPE_ATTRIBUTE, equalTo("es_search_recovery_type"));
    }

    public void testAttributeValues() {
        assertThat(names(QueryOutcome.values()), containsInAnyOrder("SUCCESS", "FAILURE", "CANCELLED"));
        assertThat(
            names(ReplayOutcome.values()),
            containsInAnyOrder("COMPLETED", "DEADLINE", "BREAKER", "NO_WINDOW", "NO_TIME_LEFT", "NO_SAMPLE", "DISABLED")
        );
        assertThat(names(RecoveryType.values()), containsInAnyOrder("RELOCATION", "NEW_COPY"));
    }

    public void testMetricsAreRegisteredWithTheirInstrumentType() {
        final var meterRegistry = new RecordingMeterRegistry();
        new PhantomQueryMetrics(meterRegistry);

        assertThat(
            meterRegistry.getRecorder().getRegisteredMetrics(InstrumentType.LONG_COUNTER),
            containsInAnyOrder(PhantomQueryMetrics.SAMPLED_COUNTER, PhantomQueryMetrics.EXECUTED_COUNTER)
        );
        assertThat(
            meterRegistry.getRecorder().getRegisteredMetrics(InstrumentType.DOUBLE_HISTOGRAM),
            containsInAnyOrder(PhantomQueryMetrics.REPLAY_DURATION_HISTOGRAM)
        );
        assertThat(meterRegistry.getRecorder().getMeasurements(InstrumentType.LONG_COUNTER, PhantomQueryMetrics.SAMPLED_COUNTER), empty());
        assertThat(meterRegistry.getRecorder().getMeasurements(InstrumentType.LONG_COUNTER, PhantomQueryMetrics.EXECUTED_COUNTER), empty());
        assertThat(
            meterRegistry.getRecorder().getMeasurements(InstrumentType.DOUBLE_HISTOGRAM, PhantomQueryMetrics.REPLAY_DURATION_HISTOGRAM),
            empty()
        );
    }

    public void testRecordSampledIncrementsTheCounter() {
        final var meterRegistry = new RecordingMeterRegistry();
        final var metrics = new PhantomQueryMetrics(meterRegistry);

        final int sampled = randomIntBetween(1, 20);
        for (int i = 0; i < sampled; i++) {
            metrics.recordSampled();
        }

        final long total = meterRegistry.getRecorder()
            .getMeasurements(InstrumentType.LONG_COUNTER, PhantomQueryMetrics.SAMPLED_COUNTER)
            .stream()
            .mapToLong(Measurement::getLong)
            .sum();
        assertThat(total, equalTo((long) sampled));
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
        assertThat(countByOutcome(measurements, PhantomQueryMetrics.QUERY_OUTCOME_ATTRIBUTE, "SUCCESS"), equalTo(successes));
        assertThat(countByOutcome(measurements, PhantomQueryMetrics.QUERY_OUTCOME_ATTRIBUTE, "FAILURE"), equalTo(failures));
        assertThat(countByOutcome(measurements, PhantomQueryMetrics.QUERY_OUTCOME_ATTRIBUTE, "CANCELLED"), equalTo(cancellations));
    }

    public void testRecordReplayRecordsSecondsWithOutcomeAndRecoveryType() {
        final var meterRegistry = new RecordingMeterRegistry();
        final var metrics = new PhantomQueryMetrics(meterRegistry);

        final ReplayOutcome outcome = randomFrom(ReplayOutcome.values());
        final RecoveryType recoveryType = randomFrom(RecoveryType.values());
        metrics.recordReplay(outcome, recoveryType, 1500L);

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
                    recoveryType.name()
                )
            )
        );
    }

    private static int countByOutcome(List<Measurement> measurements, String attribute, String value) {
        return (int) measurements.stream().filter(m -> value.equals(m.attributes().get(attribute))).count();
    }

    private static Set<String> names(Enum<?>[] values) {
        return Arrays.stream(values).map(Enum::name).collect(Collectors.toSet());
    }
}
