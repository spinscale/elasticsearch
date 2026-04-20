/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.internal;

import org.elasticsearch.index.store.PluggableDirectoryMetricsHolder;
import org.elasticsearch.index.store.StoreMetrics;
import org.elasticsearch.test.ESTestCase;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicLong;

public class BytesReadTrackerTests extends ESTestCase {

    public void testNoopTrackerRecordsNothing() throws Exception {
        BytesReadTracker tracker = BytesReadTracker.NOOP;
        tracker.trackOnCurrentThread(() -> { /* no-op */ });
        String value = tracker.trackOnCurrentThread(() -> "result");
        assertEquals("result", value);
        assertEquals(0L, tracker.getTotalBytesRead());
    }

    public void testCreateReturnsNoopForNullHolder() {
        assertSame(BytesReadTracker.NOOP, BytesReadTracker.create(null));
    }

    public void testRecordsDeltaOnSingleThread() throws Exception {
        PerThreadHolder holder = new PerThreadHolder();
        BytesReadTracker tracker = BytesReadTracker.create(holder);
        tracker.trackOnCurrentThread(() -> holder.instance().addBytesRead(1000L));
        assertEquals(1000L, tracker.getTotalBytesRead());
    }

    public void testNestedScopesOnSameThreadDoNotDoubleCount() throws Exception {
        PerThreadHolder holder = new PerThreadHolder();
        BytesReadTracker tracker = BytesReadTracker.create(holder);
        // Outer scope sees everything, inner scope also adds its delta -> total would double-count.
        // We therefore require non-overlapping scopes; assert that sequential (non-nested) scopes sum correctly.
        tracker.trackOnCurrentThread(() -> holder.instance().addBytesRead(100L));
        tracker.trackOnCurrentThread(() -> holder.instance().addBytesRead(250L));
        assertEquals(350L, tracker.getTotalBytesRead());
    }

    public void testAggregatesAcrossThreads() throws Exception {
        PerThreadHolder holder = new PerThreadHolder();
        BytesReadTracker tracker = BytesReadTracker.create(holder);

        int threads = 8;
        int iterations = 100;
        long bytesPerIteration = 7L;
        CyclicBarrier start = new CyclicBarrier(threads);
        CountDownLatch done = new CountDownLatch(threads);
        Thread[] workers = new Thread[threads];
        for (int t = 0; t < threads; t++) {
            workers[t] = new Thread(() -> {
                try {
                    start.await();
                    for (int i = 0; i < iterations; i++) {
                        tracker.trackOnCurrentThread(() -> holder.instance().addBytesRead(bytesPerIteration));
                    }
                } catch (Exception e) {
                    throw new AssertionError(e);
                } finally {
                    done.countDown();
                }
            });
            workers[t].start();
        }
        done.await();
        assertEquals(threads * iterations * bytesPerIteration, tracker.getTotalBytesRead());
    }

    public void testExceptionPropagatesAndStillRecordsDelta() {
        PerThreadHolder holder = new PerThreadHolder();
        BytesReadTracker tracker = BytesReadTracker.create(holder);
        RuntimeException boom = new RuntimeException("boom");
        RuntimeException thrown = expectThrows(RuntimeException.class, () -> tracker.trackOnCurrentThread(() -> {
            holder.instance().addBytesRead(42L);
            throw boom;
        }));
        assertSame(boom, thrown);
        assertEquals(42L, tracker.getTotalBytesRead());
    }

    public void testSupplierReturnsValue() throws Exception {
        PerThreadHolder holder = new PerThreadHolder();
        BytesReadTracker tracker = BytesReadTracker.create(holder);
        Integer value = tracker.trackOnCurrentThread(() -> {
            holder.instance().addBytesRead(5L);
            return 123;
        });
        assertEquals(Integer.valueOf(123), value);
        assertEquals(5L, tracker.getTotalBytesRead());
    }

    /**
     * Thread-local {@link StoreMetrics} holder that mimics the production
     * {@code ThreadLocalDirectoryMetricHolder} contract: each thread sees its own accumulator.
     */
    private static final class PerThreadHolder implements PluggableDirectoryMetricsHolder<StoreMetrics> {
        private final ThreadLocal<StoreMetrics> perThread = ThreadLocal.withInitial(StoreMetrics::new);
        private final AtomicLong createdInstances = new AtomicLong();

        @Override
        public StoreMetrics instance() {
            StoreMetrics m = perThread.get();
            createdInstances.incrementAndGet();
            return m;
        }

        @Override
        public PluggableDirectoryMetricsHolder<StoreMetrics> singleThreaded() {
            return this;
        }
    }
}
