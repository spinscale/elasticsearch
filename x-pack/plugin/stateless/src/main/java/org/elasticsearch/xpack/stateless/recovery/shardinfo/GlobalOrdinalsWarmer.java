/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.recovery.shardinfo;

import org.apache.lucene.index.DirectoryReader;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.RefCountingRunnable;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.fielddata.FieldDataContext;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.stateless.engine.SearchEngine;

import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.core.Strings.format;

/**
 * Builds the global ordinals of a set of fields on the current reader of a search shard.
 *
 * Stateless search shards do not load eager global ordinals on refresh, so the first search on a freshly recovered shard copy that needs
 * global ordinals has to build them, and all concurrent searches needing them wait for that. Building the global ordinals that were in
 * use on another copy of the shard before the recovered copy starts avoids that penalty when shards move around in the cluster.
 */
public class GlobalOrdinalsWarmer {

    static final String WARMING_SOURCE = "global_ordinals_warming";

    private static final Logger logger = LogManager.getLogger(GlobalOrdinalsWarmer.class);

    private final IndicesService indicesService;
    private final Executor executor;

    /**
     * @param executor the executor to build the global ordinals of each field on, fields are built in parallel
     */
    public GlobalOrdinalsWarmer(IndicesService indicesService, Executor executor) {
        this.indicesService = indicesService;
        this.executor = executor;
    }

    /**
     * Builds the global ordinals of the given fields on the current reader of the shard. Fields that are not mapped (anymore) or that
     * cannot be loaded are skipped, so the listener is never completed exceptionally because of an individual field.
     *
     * @param listener completed with the number of fields whose global ordinals were built
     */
    public void warm(IndexShard indexShard, Set<String> fieldNames, ActionListener<Integer> listener) {
        final IndexService indexService;
        final Engine.Searcher searcher;
        try {
            indexService = indicesService.indexService(indexShard.shardId().getIndex());
            if (indexService == null || indexShard.state() == IndexShardState.CLOSED) {
                listener.onResponse(0);
                return;
            }
            searcher = indexShard.tryWithEngineOrNull(
                engine -> engine instanceof SearchEngine ? engine.acquireSearcher(WARMING_SOURCE, Engine.SearcherScope.EXTERNAL) : null
            );
        } catch (Exception e) {
            listener.onFailure(e);
            return;
        }
        if (searcher == null) {
            listener.onResponse(0);
            return;
        }

        final AtomicInteger fieldsWarmed = new AtomicInteger();
        try (var refs = new RefCountingRunnable(() -> ActionListener.completeWith(listener, () -> {
            searcher.close();
            return fieldsWarmed.get();
        }))) {
            for (String fieldName : fieldNames) {
                final Releasable ref = refs.acquire();
                executor.execute(new AbstractRunnable() {
                    @Override
                    protected void doRun() {
                        if (warmField(indexService, indexShard, fieldName, searcher.getDirectoryReader())) {
                            fieldsWarmed.incrementAndGet();
                        }
                    }

                    @Override
                    public void onFailure(Exception e) {
                        logger.warn(() -> format("%s failed to warm global ordinals for field [%s]", indexShard.shardId(), fieldName), e);
                    }

                    @Override
                    public void onAfter() {
                        ref.close();
                    }
                });
            }
        }
    }

    private static boolean warmField(IndexService indexService, IndexShard indexShard, String fieldName, DirectoryReader reader) {
        final MappedFieldType fieldType = indexShard.mapperService().fieldType(fieldName);
        if (fieldType == null) {
            return false;
        }
        final long start = System.nanoTime();
        final IndexFieldData<?> fieldData = indexService.loadFielddata(
            fieldType,
            FieldDataContext.noRuntimeFields(indexShard.shardId().getIndexName(), "global ordinals warming")
        );
        if (fieldData instanceof IndexFieldData.Global<?> global) {
            global.loadGlobal(reader);
            logger.trace(
                "{} warmed global ordinals for field [{}], took [{}]",
                indexShard.shardId(),
                fieldName,
                TimeValue.timeValueNanos(System.nanoTime() - start)
            );
            return true;
        }
        return false;
    }
}
