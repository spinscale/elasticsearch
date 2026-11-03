/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.recovery.phantom;

import org.elasticsearch.telemetry.metric.LongCounter;
import org.elasticsearch.telemetry.metric.MeterRegistry;

public class PhantomQueryMetrics {

    public static final String SAMPLED_COUNTER = "es.search_recovery.phantom_queries.sampled.total";

    private final LongCounter sampledCounter;

    public PhantomQueryMetrics(MeterRegistry meterRegistry) {
        this.sampledCounter = meterRegistry.registerLongCounter(
            SAMPLED_COUNTER,
            "Number of query-phase shard requests retained by a peer copy to be replayed on a recovering search shard copy",
            "unit"
        );
    }

    public void recordSampled() {
        sampledCounter.increment();
    }
}
