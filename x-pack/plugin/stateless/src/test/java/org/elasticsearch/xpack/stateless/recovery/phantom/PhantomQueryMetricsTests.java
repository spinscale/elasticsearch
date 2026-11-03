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

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;

public class PhantomQueryMetricsTests extends ESTestCase {

    public void testSampledCounterName() {
        assertThat(PhantomQueryMetrics.SAMPLED_COUNTER, equalTo("es.search_recovery.phantom_queries.sampled.total"));
    }

    public void testSampledCounterIsRegisteredAsLongCounter() {
        final var meterRegistry = new RecordingMeterRegistry();
        new PhantomQueryMetrics(meterRegistry);

        assertThat(
            meterRegistry.getRecorder().getRegisteredMetrics(InstrumentType.LONG_COUNTER),
            contains("es.search_recovery.phantom_queries.sampled.total")
        );
        assertThat(meterRegistry.getRecorder().getMeasurements(InstrumentType.LONG_COUNTER, PhantomQueryMetrics.SAMPLED_COUNTER), empty());
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
}
