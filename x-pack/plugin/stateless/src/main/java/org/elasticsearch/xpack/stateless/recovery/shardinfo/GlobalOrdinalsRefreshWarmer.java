/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.recovery.shardinfo;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.common.lucene.index.ElasticsearchDirectoryReader;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.telemetry.metric.LongCounter;
import org.elasticsearch.telemetry.metric.LongHistogram;
import org.elasticsearch.telemetry.metric.MeterRegistry;

import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.LongSupplier;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xpack.stateless.recovery.shardinfo.SearchShardInformationIndexListener.GLOBAL_ORDINALS_MAX_AGE_SETTING;

/**
 * Builds the global ordinals that were recently used on a search shard for a new reader of that shard, before the reader is exposed to
 * searches.
 *
 * Global ordinals are cached per reader, so every refresh drops them and the first search on the new reader that needs them has to build
 * them again, while all concurrent searches needing them wait. Building them while searches still use the previous reader avoids that,
 * at the cost of delaying the refresh by up to {@link #GLOBAL_ORDINALS_REFRESH_WARMING_TIMEOUT_SETTING}.
 */
public class GlobalOrdinalsRefreshWarmer {

    /**
     * Whether a search shard builds the global ordinals used within {@link SearchShardInformationIndexListener#GLOBAL_ORDINALS_MAX_AGE_SETTING}
     * for a new reader before exposing it.
     */
    public static final Setting<Boolean> GLOBAL_ORDINALS_REFRESH_ENABLED_SETTING = Setting.boolSetting(
        "stateless.search.query_search_shard_information.global_ordinals.refresh.enabled",
        false,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * How long a refresh waits for the global ordinals of the new reader to be built before exposing it. Zero builds them in the
     * background without delaying the refresh.
     */
    public static final Setting<TimeValue> GLOBAL_ORDINALS_REFRESH_WARMING_TIMEOUT_SETTING = Setting.timeSetting(
        "stateless.search.query_search_shard_information.global_ordinals.refresh.warming_timeout",
        TimeValue.timeValueSeconds(5),
        TimeValue.ZERO,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    public static final String REFRESH_WARMING_DURATION_HISTOGRAM =
        "es.search.shards.information.global_ordinals.refresh_warming.duration.histogram";
    public static final String REFRESH_WARMING_TIMEOUTS_TOTAL =
        "es.search.shards.information.global_ordinals.refresh_warming.timeouts.total";

    private static final Logger logger = LogManager.getLogger(GlobalOrdinalsRefreshWarmer.class);

    private final IndicesService indicesService;
    private final GlobalOrdinalsWarmer globalOrdinalsWarmer;
    private final LongSupplier relativeTimeInMillisSupplier;
    private final LongHistogram warmingDuration;
    private final LongCounter warmingTimeouts;
    private volatile boolean enabled;
    private volatile TimeValue maxAge;
    private volatile TimeValue warmingTimeout;

    /**
     * @param relativeTimeInMillisSupplier must be the clock that {@link org.elasticsearch.index.fielddata.ShardFieldData} tracks global
     *                                     ordinals usage with
     */
    @SuppressWarnings("this-escape")
    public GlobalOrdinalsRefreshWarmer(
        ClusterSettings clusterSettings,
        IndicesService indicesService,
        GlobalOrdinalsWarmer globalOrdinalsWarmer,
        LongSupplier relativeTimeInMillisSupplier,
        MeterRegistry meterRegistry
    ) {
        this.indicesService = indicesService;
        this.globalOrdinalsWarmer = globalOrdinalsWarmer;
        this.relativeTimeInMillisSupplier = relativeTimeInMillisSupplier;
        this.warmingDuration = meterRegistry.registerLongHistogram(
            REFRESH_WARMING_DURATION_HISTOGRAM,
            "duration a search shard refresh waited for global ordinals to be built",
            "ms"
        );
        this.warmingTimeouts = meterRegistry.registerLongCounter(
            REFRESH_WARMING_TIMEOUTS_TOTAL,
            "search shard refreshes that stopped waiting for global ordinals to be built",
            "count"
        );
        clusterSettings.initializeAndWatch(GLOBAL_ORDINALS_REFRESH_ENABLED_SETTING, value -> this.enabled = value);
        clusterSettings.initializeAndWatch(GLOBAL_ORDINALS_MAX_AGE_SETTING, value -> this.maxAge = value);
        clusterSettings.initializeAndWatch(GLOBAL_ORDINALS_REFRESH_WARMING_TIMEOUT_SETTING, value -> this.warmingTimeout = value);
    }

    /**
     * Builds the global ordinals of the fields recently used on the shard for its new reader, blocking up to the warming timeout. Never
     * throws, as failing to build global ordinals must not fail the refresh, searches build them on demand anyway.
     */
    public void warmBeforeExposing(ShardId shardId, ElasticsearchDirectoryReader newReader) {
        try {
            final TimeValue maxAge = this.maxAge;
            // global ordinals of single segment readers are the segment ordinals, there is nothing to build
            if (enabled == false || maxAge.millis() <= 0 || newReader.leaves().size() <= 1) {
                return;
            }
            final IndexService indexService = indicesService.indexService(shardId.getIndex());
            final IndexShard indexShard = indexService == null ? null : indexService.getShardOrNull(shardId.id());
            if (indexShard == null) {
                return;
            }
            final Set<String> fields = indexShard.fieldData()
                .globalOrdinalsFieldsAccessedSince(relativeTimeInMillisSupplier.getAsLong() - maxAge.millis());
            if (fields.isEmpty()) {
                return;
            }
            warm(shardId, indexService, newReader, fields);
        } catch (Exception e) {
            logger.warn(() -> format("%s failed to build global ordinals before exposing a new reader", shardId), e);
        }
    }

    private void warm(ShardId shardId, IndexService indexService, ElasticsearchDirectoryReader newReader, Set<String> fields) {
        final TimeValue timeout = this.warmingTimeout;
        final long startNanos = System.nanoTime();
        final AtomicBoolean abandoned = new AtomicBoolean();
        final SubscribableListener<Integer> warming = new SubscribableListener<>();
        globalOrdinalsWarmer.warmReader(shardId, indexService, newReader, fields, abandoned::get, warming);

        final ActionListener<Integer> recordCompletion = ActionListener.wrap(
            warmedFields -> warmingDuration.record(TimeValue.nsecToMSec(System.nanoTime() - startNanos)),
            e -> logger.warn(() -> format("%s failed to build global ordinals before exposing a new reader", shardId), e)
        );
        if (timeout.millis() <= 0) {
            warming.addListener(recordCompletion);
            return;
        }

        final CountDownLatch latch = new CountDownLatch(1);
        warming.addListener(ActionListener.running(latch::countDown));
        try {
            if (latch.await(timeout.millis(), TimeUnit.MILLISECONDS)) {
                warming.addListener(recordCompletion);
                return;
            }
            abandoned.set(true);
            warmingTimeouts.increment();
            warmingDuration.record(TimeValue.nsecToMSec(System.nanoTime() - startNanos));
            logger.debug("{} exposing new reader after waiting [{}] for global ordinals of fields {}", shardId, timeout, fields);
        } catch (InterruptedException e) {
            abandoned.set(true);
            Thread.currentThread().interrupt();
        }
    }
}
