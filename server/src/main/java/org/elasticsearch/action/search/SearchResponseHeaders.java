/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.search;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.threadpool.ThreadPool;

/**
 * Shared helpers for emitting HTTP response headers attached to search responses. Response
 * headers do not cross transport boundaries, so they must be added in the
 * {@link org.elasticsearch.common.util.concurrent.ThreadContext} of the coordinator node right
 * before the final {@link SearchResponse} is handed to the REST layer.
 */
public final class SearchResponseHeaders {

    public static final String BYTES_READ_HEADER = "Elasticsearch-Bytes-Read";

    private SearchResponseHeaders() {}

    /**
     * Wraps the given listener to publish {@link SearchResponse#getBytesRead()} as the
     * {@value #BYTES_READ_HEADER} HTTP response header. The header is only emitted when the
     * {@link Store#DIRECTORY_METRICS_FEATURE_FLAG directory-metrics feature flag} is enabled and
     * the response total is greater than zero: this avoids surfacing a misleading zero header
     * in mixed-version CCS clusters where older remote nodes do not populate the value.
     */
    public static ActionListener<SearchResponse> wrapWithBytesReadHeader(ActionListener<SearchResponse> inner, ThreadPool threadPool) {
        if (Store.DIRECTORY_METRICS_FEATURE_FLAG.isEnabled() == false) {
            return inner;
        }
        var threadContext = threadPool.getThreadContext();
        return inner.delegateFailureAndWrap((delegate, response) -> {
            addBytesReadHeader(threadContext, response.getBytesRead());
            delegate.onResponse(response);
        });
    }

    /**
     * Adds the {@value #BYTES_READ_HEADER} response header to the given {@link ThreadContext} when
     * the {@link Store#DIRECTORY_METRICS_FEATURE_FLAG directory-metrics feature flag} is enabled
     * and {@code bytes > 0}. Used by response paths (such as async-search) that do not have direct
     * access to a listener wrapping point.
     */
    public static void addBytesReadHeader(ThreadContext threadContext, long bytes) {
        if (bytes > 0L && Store.DIRECTORY_METRICS_FEATURE_FLAG.isEnabled()) {
            threadContext.addResponseHeader(BYTES_READ_HEADER, Long.toString(bytes));
        }
    }
}
