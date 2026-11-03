/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.recovery.phantom;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

/**
 * The bytes that all the samples of a node may hold together. One peer can sample for many recovering copies at once, so bounding each
 * sample is not enough. It is not part of the request circuit breaker so that sampling can never make user requests trip it.
 */
public class PhantomQuerySampleBudget {

    private final LongSupplier limitInBytes;
    private final AtomicLong usedInBytes = new AtomicLong();

    public PhantomQuerySampleBudget(LongSupplier limitInBytes) {
        this.limitInBytes = limitInBytes;
    }

    boolean tryReserve(long bytes) {
        assert bytes >= 0 : bytes;
        final long limit = limitInBytes.getAsLong();
        while (true) {
            final long used = usedInBytes.get();
            // a subtraction, so that a limit near Long.MAX_VALUE cannot overflow the sum
            if (bytes > limit - used) {
                return false;
            }
            if (usedInBytes.compareAndSet(used, used + bytes)) {
                return true;
            }
        }
    }

    void release(long bytes) {
        assert bytes >= 0 : bytes;
        final long used = usedInBytes.addAndGet(-bytes);
        assert used >= 0 : "released more bytes than were reserved: " + used;
    }

    public long usedInBytes() {
        return usedInBytes.get();
    }
}
