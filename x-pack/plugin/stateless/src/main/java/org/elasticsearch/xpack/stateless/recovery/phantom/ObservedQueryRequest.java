/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.recovery.phantom;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.search.internal.ShardSearchRequest;

import java.io.IOException;

import static org.elasticsearch.core.Strings.format;

/**
 * A request that the observer sees, serialized lazily and at most once however many samplers retain it. The data node rewrites the
 * request in place after the observer returns, so samplers keep the serialized copy and never the request.
 *
 * <p>
 * Not thread safe: the thread that observes the request offers it to the samplers one after the other.
 */
final class ObservedQueryRequest {

    private static final Logger logger = LogManager.getLogger(ObservedQueryRequest.class);

    private final ShardSearchRequest request;
    private EncodedQueryRequest encoded;
    private boolean encodingFailed;

    ObservedQueryRequest(ShardSearchRequest request) {
        this.request = request;
    }

    ShardSearchRequest request() {
        return request;
    }

    @Nullable
    EncodedQueryRequest encoded() {
        if (encoded == null && encodingFailed == false) {
            try {
                encoded = encode(request);
            } catch (Exception e) {
                encodingFailed = true;
                logger.debug(() -> format("%s: failed to serialize a query request for sampling", request.shardId()), e);
            }
        }
        return encoded;
    }

    private static EncodedQueryRequest encode(ShardSearchRequest request) throws IOException {
        final TransportVersion version = TransportVersion.current();
        try (var out = new BytesStreamOutput()) {
            out.setTransportVersion(version);
            request.writeTo(out);
            // the stream grows in pages: copying to an exact size makes the bytes that the limits count the bytes that stay on the heap
            return new EncodedQueryRequest(new BytesArray(BytesReference.toBytes(out.bytes())), version);
        }
    }
}
