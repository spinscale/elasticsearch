/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.recovery.shardinfo;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.FeatureFlag;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.shard.IndexEventListener;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.stateless.engine.SearchEngine;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.LongSupplier;

import static org.elasticsearch.xpack.stateless.recovery.shardinfo.TransportFetchSearchShardInformationAction.NO_OTHER_SHARDS_FOUND_RESPONSE;
import static org.elasticsearch.xpack.stateless.recovery.shardinfo.TransportFetchSearchShardInformationAction.SHARD_HAS_MOVED_RESPONSE;

/**
 * An IndexEventListener to retrieve state from other shard copies
 *
 * When a shard is moved around in the cluster, the search commit prefetcher stops working on the freshly copied shard, because no
 * searcher has been acquired yet - or unless a new search is executed.
 * This is unwanted behavior. A simple solution is to query other shards about their last time when a searcher was acquired and use that
 * time locally as well.
 * This is exactly the idea of this index listener, which takes care of two cases:
 *
 * Relocation of a shard from one node to another: A request is sent to the node the relocation is coming from.
 * Relocation information is not set when adding a replica, so all nodes with shard copies are queried
 *
 * If {@link #GLOBAL_ORDINALS_ENABLED_SETTING} is set, the other shard copy also reports the fields whose global ordinals it recently used.
 * Stateless search shards do not load eager global ordinals, so these are built before the recovered shard starts (bounded by
 * {@link #GLOBAL_ORDINALS_WARMING_TIMEOUT_SETTING}), instead of having the first searches on the new shard copy wait for them.
 *
 * As only a single instance of this class exists, the only state kept in here are the fields to warm for each recovering shard, from the
 * time the shard information is requested before the recovery until the recovery is done or the shard is closed.
 */
public class SearchShardInformationIndexListener implements IndexEventListener {

    private static final FeatureFlag FEATURE_FLAG_QUERY_SEARCH_SHARD_INFORMATION = new FeatureFlag("query_search_shard_information");

    public static final Setting<Boolean> QUERY_SEARCH_SHARD_INFORMATION_SETTING = Setting.boolSetting(
        "stateless.search.query_search_shard_information.enabled",
        true,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Whether a recovering search shard builds the global ordinals that were recently used on another copy of the shard before it starts.
     */
    public static final Setting<Boolean> GLOBAL_ORDINALS_ENABLED_SETTING = Setting.boolSetting(
        "stateless.search.query_search_shard_information.global_ordinals.enabled",
        false,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Fields whose global ordinals have been used on another shard copy within this time are built on a recovering shard copy.
     * Zero disables building global ordinals during recovery.
     */
    public static final Setting<TimeValue> GLOBAL_ORDINALS_MAX_AGE_SETTING = Setting.timeSetting(
        "stateless.search.query_search_shard_information.global_ordinals.max_age",
        TimeValue.timeValueMinutes(5),
        TimeValue.ZERO,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * How long a search shard recovery waits for the shard information and for global ordinals to be built before the shard starts.
     * Zero builds global ordinals in the background without delaying the recovery.
     */
    public static final Setting<TimeValue> GLOBAL_ORDINALS_WARMING_TIMEOUT_SETTING = Setting.timeSetting(
        "stateless.search.query_search_shard_information.global_ordinals.warming_timeout",
        TimeValue.timeValueSeconds(30),
        TimeValue.ZERO,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    private static final Logger logger = LogManager.getLogger(SearchShardInformationIndexListener.class);

    private final Client client;
    private final SearchShardInformationMetricsCollector collector;
    private final LongSupplier nowSupplier;
    private final ThreadPool threadPool;
    private final GlobalOrdinalsWarmer globalOrdinalsWarmer;
    private final Map<ShardId, SubscribableListener<Set<String>>> pendingGlobalOrdinalsFields = ConcurrentCollections.newConcurrentMap();
    private volatile boolean active = FEATURE_FLAG_QUERY_SEARCH_SHARD_INFORMATION.isEnabled();
    private volatile boolean globalOrdinalsEnabled;
    private volatile TimeValue globalOrdinalsWarmingTimeout;

    @SuppressWarnings("this-escape")
    public SearchShardInformationIndexListener(
        Client client,
        SearchShardInformationMetricsCollector collector,
        ClusterSettings clusterSettings,
        LongSupplier nowSupplier,
        ThreadPool threadPool,
        GlobalOrdinalsWarmer globalOrdinalsWarmer
    ) {
        this.client = client;
        this.collector = collector;
        this.nowSupplier = nowSupplier;
        this.threadPool = threadPool;
        this.globalOrdinalsWarmer = globalOrdinalsWarmer;
        clusterSettings.initializeAndWatch(QUERY_SEARCH_SHARD_INFORMATION_SETTING, active -> this.active = active);
        clusterSettings.initializeAndWatch(GLOBAL_ORDINALS_ENABLED_SETTING, enabled -> this.globalOrdinalsEnabled = enabled);
        clusterSettings.initializeAndWatch(GLOBAL_ORDINALS_WARMING_TIMEOUT_SETTING, timeout -> this.globalOrdinalsWarmingTimeout = timeout);
    }

    @Override
    public void beforeIndexShardRecovery(IndexShard indexShard, IndexSettings indexSettings, ActionListener<Void> listener) {
        ActionListener.completeWith(listener, () -> {
            if (active == false) {
                return null;
            }

            // if relocation from another node is in the routing entry, this is the best source of information, no need to ask other shards
            String relocatingNodeId = indexShard.routingEntry().relocatingNodeId();

            final SubscribableListener<Set<String>> globalOrdinalsFields = new SubscribableListener<>();
            if (globalOrdinalsEnabled) {
                pendingGlobalOrdinalsFields.put(indexShard.shardId(), globalOrdinalsFields);
            }

            final long start = nowSupplier.getAsLong();
            TransportFetchSearchShardInformationAction.Request request = new TransportFetchSearchShardInformationAction.Request(
                relocatingNodeId,
                indexShard.shardId()
            );

            client.execute(TransportFetchSearchShardInformationAction.TYPE, request, ActionListener.wrap(response -> {
                globalOrdinalsFields.onResponse(response.getGlobalOrdinalsFields());

                if (NO_OTHER_SHARDS_FOUND_RESPONSE.equals(response)) {
                    return;
                }

                if (SHARD_HAS_MOVED_RESPONSE.equals(response)) {
                    collector.shardMoved();
                    logger.trace("shard was moved before searcher could be acquired for shard [{}]", indexShard.shardId());
                    return;
                }

                long lastSearcherAcquiredTime = response.getLastSearcherAcquiredTime();

                var attributes = Map.<String, Object>of("es_search_last_searcher_acquired_greater_zero", lastSearcherAcquiredTime > 0);
                collector.recordSuccess(nowSupplier.getAsLong() - start, attributes);

                if (lastSearcherAcquiredTime <= 0) {
                    return;
                }

                indexShard.waitForEngineOrClosedShard(ActionListener.wrap(r -> {
                    if (indexShard.state() != IndexShardState.CLOSED) {
                        indexShard.tryWithEngineOrNull(engine -> {
                            if (engine instanceof SearchEngine searchEngine) {
                                searchEngine.setLastSearcherAcquiredTime(lastSearcherAcquiredTime);
                            }
                            return null;
                        });
                    }
                }, e -> { logger.warn("could not set last acquired searcher data for shard [" + indexShard.shardId() + "]", e); }));

            }, e -> {
                globalOrdinalsFields.onResponse(Set.of());
                logger.warn("could not retrieve search shard information data for shard [" + indexShard.shardId() + "]", e);
                collector.recordError();
            }));

            return null;
        });
    }

    /**
     * Builds the global ordinals reported by the other shard copy before the recovered shard starts. This must run after the search
     * engine refreshed its reader at the end of the recovery, otherwise the global ordinals would be built for a reader that searches
     * will never use.
     */
    @Override
    public void afterIndexShardRecovery(IndexShard indexShard, ActionListener<Void> listener) {
        final SubscribableListener<Set<String>> globalOrdinalsFields = pendingGlobalOrdinalsFields.remove(indexShard.shardId());
        if (globalOrdinalsFields == null || globalOrdinalsEnabled == false) {
            listener.onResponse(null);
            return;
        }

        final TimeValue timeout = globalOrdinalsWarmingTimeout;
        final long start = nowSupplier.getAsLong();
        final AtomicBoolean hasFieldsToWarm = new AtomicBoolean();
        final SubscribableListener<Integer> warming = new SubscribableListener<>();
        globalOrdinalsFields.addListener(ActionListener.wrap(fields -> {
            if (fields.isEmpty()) {
                warming.onResponse(0);
            } else {
                hasFieldsToWarm.set(true);
                globalOrdinalsWarmer.warm(indexShard, fields, warming);
            }
        }, warming::onFailure));

        final ActionListener<Void> recoveryListener;
        if (timeout.millis() > 0) {
            warming.addTimeout(timeout, threadPool, threadPool.generic());
            recoveryListener = listener;
        } else {
            listener.onResponse(null);
            recoveryListener = ActionListener.noop();
        }

        warming.addListener(ActionListener.wrap(warmedFields -> {
            if (hasFieldsToWarm.get()) {
                collector.recordGlobalOrdinalsWarmed(nowSupplier.getAsLong() - start, warmedFields);
            }
            recoveryListener.onResponse(null);
        }, e -> {
            if (e instanceof ElasticsearchTimeoutException) {
                collector.recordGlobalOrdinalsWarmingTimeout(nowSupplier.getAsLong() - start);
                logger.debug("timed out after [{}] waiting for global ordinals to be built for shard [{}]", timeout, indexShard.shardId());
            } else {
                logger.warn("could not build global ordinals for shard [" + indexShard.shardId() + "]", e);
            }
            // never fail the recovery, searches will build the global ordinals
            recoveryListener.onResponse(null);
        }));
    }

    @Override
    public void afterIndexShardClosed(ShardId shardId, @Nullable IndexShard indexShard, Settings indexSettings) {
        pendingGlobalOrdinalsFields.remove(shardId);
    }
}
