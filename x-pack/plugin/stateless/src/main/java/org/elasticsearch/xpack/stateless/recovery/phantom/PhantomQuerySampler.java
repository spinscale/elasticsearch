/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.recovery.phantom;

import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.xpack.stateless.recovery.phantom.PhantomQueryMetrics.DropReason;

import java.util.ArrayList;
import java.util.List;

/**
 * The sample that one peer copy keeps for one recovering copy of its shard. It keeps the first eligible requests rather than the most
 * recent ones: they warm as well, and the peer only pays for a fixed number of serializations however busy the shard is.
 */
final class PhantomQuerySampler {

    private final int maxQueries;
    private final long maxBytes;
    private final PhantomQuerySampleBudget nodeBudget;
    private final PhantomQueryMetrics metrics;

    // read without the lock, so that once the sample is full a request only costs this check
    private volatile boolean accepting;

    // guarded by this
    private final List<EncodedQueryRequest> retained = new ArrayList<>();
    private long retainedBytes;
    private boolean closed;

    PhantomQuerySampler(int maxQueries, long maxBytes, PhantomQuerySampleBudget nodeBudget, PhantomQueryMetrics metrics) {
        this.maxQueries = maxQueries;
        this.maxBytes = maxBytes;
        this.nodeBudget = nodeBudget;
        this.metrics = metrics;
        this.accepting = maxQueries > 0 && maxBytes > 0;
    }

    static boolean isEligible(ShardSearchRequest request) {
        return request.searchType() == SearchType.QUERY_THEN_FETCH
            && request.readerId() == null
            && request.scroll() == null
            && request.waitForCheckpoint() <= SequenceNumbers.UNASSIGNED_SEQ_NO;
    }

    boolean offer(ObservedQueryRequest observed) {
        if (accepting == false || isEligible(observed.request()) == false) {
            return false;
        }
        // serialized outside the lock, and only for requests that this sampler may still retain
        final EncodedQueryRequest encoded = observed.encoded();
        if (encoded == null) {
            metrics.recordDropped(DropReason.ENCODING_FAILED);
            return false;
        }
        final long size = encoded.sizeInBytes();
        DropReason dropped = null;
        synchronized (this) {
            if (closed || accepting == false) {
                return false;
            }
            // a subtraction, so that a limit near Long.MAX_VALUE cannot overflow the sum
            if (size > maxBytes - retainedBytes) {
                accepting = false;
                dropped = DropReason.SAMPLE_BYTES;
            } else if (nodeBudget.tryReserve(size) == false) {
                // only drops the request and keeps sampling, since other samples may free budget before the next request arrives
                dropped = DropReason.NODE_BUDGET;
            } else {
                retained.add(encoded);
                retainedBytes += size;
                if (retained.size() >= maxQueries) {
                    accepting = false;
                }
            }
        }
        // only a request that reached a sampler that was still accepting is counted, so a full sample keeps costing one flag check
        if (dropped != null) {
            metrics.recordDropped(dropped);
            return false;
        }
        metrics.recordSampled();
        return true;
    }

    synchronized List<EncodedQueryRequest> collect() {
        final List<EncodedQueryRequest> sample = List.copyOf(retained);
        close();
        return sample;
    }

    synchronized void discard() {
        close();
    }

    private void close() {
        assert Thread.holdsLock(this);
        accepting = false;
        closed = true;
        retained.clear();
        nodeBudget.release(retainedBytes);
        retainedBytes = 0;
    }

    boolean accepting() {
        return accepting;
    }

    synchronized int retainedCount() {
        return retained.size();
    }

    synchronized long retainedBytes() {
        return retainedBytes;
    }
}
