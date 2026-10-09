/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.fielddata;

import org.apache.lucene.util.Accountable;
import org.elasticsearch.common.FieldMemoryStats;
import org.elasticsearch.common.metrics.CounterMetric;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.index.fielddata.ordinals.GlobalOrdinalsAccounting;
import org.elasticsearch.index.shard.ShardId;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

public class ShardFieldData implements IndexFieldDataCache.Listener {

    private final CounterMetric evictionsMetric = new CounterMetric();
    private final CounterMetric totalMetric = new CounterMetric();
    private final ConcurrentMap<String, CounterMetric> perFieldTotals = ConcurrentCollections.newConcurrentMap();
    private final CounterMetric buildTime = new CounterMetric();
    private final ConcurrentMap<String, GlobalOrdinalFieldStats> perFieldGlobalOrdinalStats = ConcurrentCollections.newConcurrentMap();
    private final ConcurrentMap<String, AtomicLong> perFieldGlobalOrdinalsLastAccess = ConcurrentCollections.newConcurrentMap();
    private final LongSupplier relativeTimeInMillisSupplier;

    public ShardFieldData(LongSupplier relativeTimeInMillisSupplier) {
        this.relativeTimeInMillisSupplier = relativeTimeInMillisSupplier;
    }

    public FieldDataStats stats(String... fields) {
        Map<String, Long> fieldTotals = null;
        Map<String, FieldDataStats.GlobalOrdinalsStats.GlobalOrdinalFieldStats> fieldGlobalOrdinalsStats = null;
        if (CollectionUtils.isEmpty(fields) == false) {
            fieldTotals = new HashMap<>();
            for (Map.Entry<String, CounterMetric> entry : perFieldTotals.entrySet()) {
                if (Regex.simpleMatch(fields, entry.getKey())) {
                    fieldTotals.put(entry.getKey(), entry.getValue().count());
                }
            }
            for (var entry : perFieldGlobalOrdinalStats.entrySet()) {
                if (Regex.simpleMatch(fields, entry.getKey())) {
                    if (fieldGlobalOrdinalsStats == null) {
                        fieldGlobalOrdinalsStats = new HashMap<>();
                    }
                    fieldGlobalOrdinalsStats.put(
                        entry.getKey(),
                        new FieldDataStats.GlobalOrdinalsStats.GlobalOrdinalFieldStats(
                            entry.getValue().totalBuildTime.count(),
                            entry.getValue().valueCount.get()
                        )
                    );
                }
            }
        }
        return new FieldDataStats(
            totalMetric.count(),
            evictionsMetric.count(),
            fieldTotals == null ? null : new FieldMemoryStats(fieldTotals),
            new FieldDataStats.GlobalOrdinalsStats(buildTime.count(), fieldGlobalOrdinalsStats)
        );
    }

    /**
     * Return names of the fields whose global ordinals were accessed since
     *
     * @param relativeTimeInMillis a timestamp of the same clock this instance was created with
     */
    public Set<String> globalOrdinalsFieldsAccessedSince(long relativeTimeInMillis) {
        Set<String> fields = new HashSet<>();
        for (var entry : perFieldGlobalOrdinalsLastAccess.entrySet()) {
            if (entry.getValue().get() >= relativeTimeInMillis) {
                fields.add(entry.getKey());
            }
        }
        return fields;
    }

    @Override
    public void onCache(ShardId shardId, String fieldName, Accountable ramUsage) {
        totalMetric.inc(ramUsage.ramBytesUsed());
        CounterMetric total = perFieldTotals.get(fieldName);
        if (total != null) {
            total.inc(ramUsage.ramBytesUsed());
        } else {
            total = new CounterMetric();
            total.inc(ramUsage.ramBytesUsed());
            CounterMetric prev = perFieldTotals.putIfAbsent(fieldName, total);
            if (prev != null) {
                prev.inc(ramUsage.ramBytesUsed());
            }
        }
    }

    @Override
    public void onCache(ShardId shardId, String fieldName, GlobalOrdinalsAccounting info) {
        buildTime.inc(info.getBuildingTime().millis());
        perFieldGlobalOrdinalStats.compute(fieldName, (f, globalOrdinalFieldStats) -> {
            if (globalOrdinalFieldStats == null) {
                globalOrdinalFieldStats = new GlobalOrdinalFieldStats();
            }
            globalOrdinalFieldStats.totalBuildTime.inc(info.getBuildingTime().millis());
            if (globalOrdinalFieldStats.valueCount.get() < info.getValueCount()) {
                globalOrdinalFieldStats.valueCount.set(info.getValueCount());
            }
            return globalOrdinalFieldStats;
        });
    }

    @Override
    public void onGlobalOrdinalsAccess(ShardId shardId, String fieldName) {
        final long now = relativeTimeInMillisSupplier.getAsLong();
        final AtomicLong lastAccess = perFieldGlobalOrdinalsLastAccess.get(fieldName);
        if (lastAccess == null) {
            perFieldGlobalOrdinalsLastAccess.computeIfAbsent(fieldName, f -> new AtomicLong()).accumulateAndGet(now, Math::max);
        } else if (lastAccess.get() < now) {
            // skip the write when the cached clock has not advanced, which is the common case on the hot path
            lastAccess.accumulateAndGet(now, Math::max);
        }
    }

    @Override
    public void onRemoval(ShardId shardId, String fieldName, boolean wasEvicted, long sizeInBytes) {
        if (wasEvicted) {
            evictionsMetric.inc();
        }
        if (sizeInBytes != -1) {
            totalMetric.dec(sizeInBytes);

            CounterMetric total = perFieldTotals.get(fieldName);
            if (total != null) {
                total.dec(sizeInBytes);
            }
        }
    }

    static class GlobalOrdinalFieldStats {

        private final CounterMetric totalBuildTime;
        private final AtomicLong valueCount;

        GlobalOrdinalFieldStats() {
            this.totalBuildTime = new CounterMetric();
            this.valueCount = new AtomicLong();
        }
    }
}
