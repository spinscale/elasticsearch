/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search;

import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.search.internal.ShardSearchRequest;

/**
 * Sees every query-phase shard request that reaches this node, as the coordinating node sent it, before the data node rewrites or queues
 * it. Both the per-shard and the batched {@code [query][n]} actions are observed, while DFS, query-by-id, fetch and scroll-continuation
 * requests are not. Implementations decide themselves which of the observed requests are eligible for their purpose.
 * <p>
 * The observer runs on the receiving thread and delays the search, so it must be cheap, must not block and must not throw: failures are
 * logged and ignored. The request is rewritten in place afterwards, so implementations must neither modify nor retain it, only copy what
 * they need.
 */
public interface ShardQueryRequestObserver {

    ShardQueryRequestObserver NOOP = (indexShard, request) -> {};

    void onQueryPhaseRequest(IndexShard indexShard, ShardSearchRequest request);
}
