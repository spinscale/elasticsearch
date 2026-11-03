/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.recovery.phantom;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.ShardQueryRequestObserver;
import org.elasticsearch.search.internal.ShardSearchRequest;

import static org.elasticsearch.core.Strings.format;

/**
 * Runs on the receiving thread of every query-phase request of the node, so a shard that nobody samples for must cost one map lookup.
 */
public class PhantomQueryRequestObserver implements ShardQueryRequestObserver {

    private static final Logger logger = LogManager.getLogger(PhantomQueryRequestObserver.class);

    private final PhantomQuerySamplers samplers;
    private final PhantomQueriesSettings settings;

    public PhantomQueryRequestObserver(PhantomQuerySamplers samplers, PhantomQueriesSettings settings) {
        this.samplers = samplers;
        this.settings = settings;
    }

    @Override
    public void onQueryPhaseRequest(IndexShard indexShard, ShardSearchRequest request) {
        final ShardId shardId = indexShard.shardId();
        if (samplers.isSampling(shardId) == false || settings.enabled() == false) {
            return;
        }
        try {
            final var observed = new ObservedQueryRequest(request);
            samplers.forEachSampler(shardId, sampler -> sampler.offer(observed));
        } catch (Exception e) {
            // sampling is best effort and must never affect the search
            logger.debug(() -> format("%s: failed to sample a query request", shardId), e);
        }
    }
}
