/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.recovery.shardinfo;

import org.elasticsearch.telemetry.TelemetryProvider;
import org.elasticsearch.telemetry.metric.LongCounter;
import org.elasticsearch.telemetry.metric.LongHistogram;
import org.elasticsearch.telemetry.metric.MeterRegistry;

import java.util.Map;

/**
 * This collector is collecting runtime metrics in the SearchShardInformationIndexListener for the
 * TransportFetchSearchShardInformationAction
 * It collects the number of successful calls, error and request duration, as this is supposed to be a fast request.
 * This allows to monitor mainly if these requests sent correctly
 * and with the right attributes one can measure if the right shards were queried
 *
 * It also collects how long recoveries waited for global ordinals to be built, how many fields were warmed and how often the
 * warming timed out, which allows to tune the warming timeout.
 */
public class SearchShardInformationMetricsCollector {

    public static final String REQUESTS_DURATION_HISTOGRAM = "es.search.shards.information.retrieval.requests.duration.histogram";
    public static final String REQUESTS_SUCCESS_TOTAL = "es.search.shards.information.retrieval.requests.success.total";
    public static final String REQUESTS_ERRORS_TOTAL = "es.search.shards.information.retrieval.requests.errors.total";
    public static final String REQUESTS_SHARD_MOVED_TOTAL = "es.search.shards.information.retrieval.requests.shard_moved.total";
    public static final String GLOBAL_ORDINALS_WARMING_DURATION_HISTOGRAM =
        "es.search.shards.information.global_ordinals.warming.duration.histogram";
    public static final String GLOBAL_ORDINALS_WARMED_FIELDS_TOTAL = "es.search.shards.information.global_ordinals.warmed_fields.total";
    public static final String GLOBAL_ORDINALS_WARMING_TIMEOUTS_TOTAL =
        "es.search.shards.information.global_ordinals.warming.timeouts.total";

    private final LongHistogram histogram;
    private final LongCounter successes;
    private final LongCounter errors;
    private final LongCounter shardMoved;
    private final LongHistogram globalOrdinalsWarmingDuration;
    private final LongCounter globalOrdinalsWarmedFields;
    private final LongCounter globalOrdinalsWarmingTimeouts;

    public SearchShardInformationMetricsCollector(TelemetryProvider telemetryProvider) {
        final MeterRegistry meterRegistry = telemetryProvider.getMeterRegistry();

        this.histogram = meterRegistry.registerLongHistogram(REQUESTS_DURATION_HISTOGRAM, "duration of shard information calls", "ms");
        this.successes = meterRegistry.registerLongCounter(REQUESTS_SUCCESS_TOTAL, "successful requests", "count");
        this.errors = meterRegistry.registerLongCounter(REQUESTS_ERRORS_TOTAL, "failed requests", "count");
        this.shardMoved = meterRegistry.registerLongCounter(REQUESTS_SHARD_MOVED_TOTAL, "shard moved requests", "count");
        this.globalOrdinalsWarmingDuration = meterRegistry.registerLongHistogram(
            GLOBAL_ORDINALS_WARMING_DURATION_HISTOGRAM,
            "duration a search shard recovery waited for global ordinals to be built",
            "ms"
        );
        this.globalOrdinalsWarmedFields = meterRegistry.registerLongCounter(
            GLOBAL_ORDINALS_WARMED_FIELDS_TOTAL,
            "fields whose global ordinals were built during search shard recovery",
            "count"
        );
        this.globalOrdinalsWarmingTimeouts = meterRegistry.registerLongCounter(
            GLOBAL_ORDINALS_WARMING_TIMEOUTS_TOTAL,
            "search shard recoveries that stopped waiting for global ordinals to be built",
            "count"
        );
    }

    public void recordSuccess(long duration, Map<String, Object> attributes) {
        this.successes.incrementBy(1, attributes);
        this.histogram.record(duration, attributes);
    }

    public void recordError() {
        this.errors.increment();
    }

    public void shardMoved() {
        this.shardMoved.increment();
    }

    public void recordGlobalOrdinalsWarmed(long duration, int warmedFields) {
        this.globalOrdinalsWarmingDuration.record(duration);
        this.globalOrdinalsWarmedFields.incrementBy(warmedFields);
    }

    public void recordGlobalOrdinalsWarmingTimeout(long duration) {
        this.globalOrdinalsWarmingDuration.record(duration);
        this.globalOrdinalsWarmingTimeouts.increment();
    }
}
