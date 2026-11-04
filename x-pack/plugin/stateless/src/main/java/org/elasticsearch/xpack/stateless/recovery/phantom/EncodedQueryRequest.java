/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.recovery.phantom;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.search.internal.ShardSearchRequest;

import java.io.IOException;

/**
 * A serialized request together with the version it was serialized with, which is needed to read it back, or to re-encode it for a node
 * on another version.
 */
public record EncodedQueryRequest(BytesReference bytes, TransportVersion version) {

    public long sizeInBytes() {
        return bytes.length();
    }

    EncodedQueryRequest reencode(TransportVersion targetVersion, NamedWriteableRegistry namedWriteableRegistry) throws IOException {
        if (version.equals(targetVersion)) {
            return this;
        }
        final ShardSearchRequest request;
        try (var in = new NamedWriteableAwareStreamInput(bytes.streamInput(), namedWriteableRegistry)) {
            in.setTransportVersion(version);
            request = new ShardSearchRequest(in);
        }
        try (var out = new BytesStreamOutput()) {
            out.setTransportVersion(targetVersion);
            request.writeTo(out);
            // exact-size copy, so that its size is what the sample limits counted
            return new EncodedQueryRequest(new BytesArray(BytesReference.toBytes(out.bytes())), targetVersion);
        }
    }
}
