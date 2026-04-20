/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.internal;

import org.elasticsearch.core.CheckedRunnable;
import org.elasticsearch.core.CheckedSupplier;
import org.elasticsearch.index.store.PluggableDirectoryMetricsHolder;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.store.StoreMetrics;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

/**
 * Accumulates the number of bytes read from the Lucene {@link org.apache.lucene.store.Directory}
 * during a single search request. Reads are recorded per-thread by {@link StoreMetrics} via the
 * thread-local holder; {@link #trackOnCurrentThread} captures the delta observed on the calling
 * thread across a block of work and atomically adds it to a shared total that can span multiple
 * slice threads.
 *
 * <p>Calls to {@link #trackOnCurrentThread} must run their block on the current thread (they are
 * synchronous). When wrapping work that itself dispatches to worker threads — such as slice
 * callables executed via Lucene's {@code TaskExecutor} — each piece of thread-local work must
 * have its own {@code trackOnCurrentThread} scope, so that each scope captures only the reads
 * performed on its thread. Non-overlapping scopes avoid double counting when the executor runs
 * one task inline on the caller.</p>
 *
 * <p>{@link #NOOP} is used when the {@link Store#DIRECTORY_METRICS_FEATURE_FLAG} is disabled or
 * no holder is available; it runs blocks without recording anything and reports zero.</p>
 */
public final class BytesReadTracker {

    public static final BytesReadTracker NOOP = new BytesReadTracker(null, null);

    private final PluggableDirectoryMetricsHolder<StoreMetrics> holder;
    private final AtomicLong total;

    private BytesReadTracker(PluggableDirectoryMetricsHolder<StoreMetrics> holder, AtomicLong total) {
        this.holder = holder;
        this.total = total;
    }

    /**
     * Returns a tracker backed by the supplied holder, or {@link #NOOP} when the directory
     * metrics feature flag is disabled or no holder is provided.
     */
    public static BytesReadTracker create(PluggableDirectoryMetricsHolder<StoreMetrics> holder) {
        if (holder == null || Store.DIRECTORY_METRICS_FEATURE_FLAG.isEnabled() == false) {
            return NOOP;
        }
        return new BytesReadTracker(holder, new AtomicLong());
    }

    public <E extends Exception> void trackOnCurrentThread(CheckedRunnable<E> block) throws E {
        if (total == null) {
            block.run();
            return;
        }
        Supplier<StoreMetrics> delta = holder.instance().delta();
        try {
            block.run();
        } finally {
            total.addAndGet(delta.get().getBytesRead());
        }
    }

    public <T, E extends Exception> T trackOnCurrentThread(CheckedSupplier<T, E> block) throws E {
        if (total == null) {
            return block.get();
        }
        Supplier<StoreMetrics> delta = holder.instance().delta();
        try {
            return block.get();
        } finally {
            total.addAndGet(delta.get().getBytesRead());
        }
    }

    /**
     * @return the total bytes read observed across all {@link #trackOnCurrentThread} scopes on
     *         this tracker, or zero for {@link #NOOP}.
     */
    public long getTotalBytesRead() {
        return total == null ? 0L : total.get();
    }
}
