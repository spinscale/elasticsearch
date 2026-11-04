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
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.core.Strings.format;

/**
 * Sampled requests stay serialized with the version of the peer, which an older recovering node may not be able to read. Writing the sample
 * to a channel of another version therefore re-encodes them. Requests that cannot be re-encoded are dropped, because a smaller sample still
 * warms.
 */
public final class PhantomQuerySample implements Writeable {

    private static final Logger logger = LogManager.getLogger(PhantomQuerySample.class);

    public static final PhantomQuerySample EMPTY = new PhantomQuerySample(List.of(), null);

    private final List<EncodedQueryRequest> requests;
    @Nullable
    private final NamedWriteableRegistry reencodingRegistry;

    public PhantomQuerySample(List<EncodedQueryRequest> requests, @Nullable NamedWriteableRegistry reencodingRegistry) {
        this.requests = List.copyOf(requests);
        this.reencodingRegistry = reencodingRegistry;
    }

    public PhantomQuerySample(StreamInput in) throws IOException {
        final TransportVersion version = in.getTransportVersion();
        // copied, because the network buffer is released once the message was handled
        this.requests = in.readCollectionAsImmutableList(i -> new EncodedQueryRequest(new BytesArray(i.readByteArray()), version));
        this.reencodingRegistry = null;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        final TransportVersion version = out.getTransportVersion();
        final List<EncodedQueryRequest> toWrite = new ArrayList<>(requests.size());
        for (EncodedQueryRequest request : requests) {
            if (request.version().equals(version)) {
                toWrite.add(request);
                continue;
            }
            assert reencodingRegistry != null : "a sample that was read from the wire is not written for another version";
            try {
                toWrite.add(request.reencode(version, reencodingRegistry));
            } catch (Exception e) {
                // query contents are never logged
                logger.debug(
                    () -> format("dropping a sampled query request that cannot be written for [%s] from [%s]", version, request.version()),
                    e
                );
            }
        }
        out.writeCollection(toWrite, (o, request) -> o.writeBytesReference(request.bytes()));
    }

    public List<EncodedQueryRequest> requests() {
        return requests;
    }

    public boolean isEmpty() {
        return requests.isEmpty();
    }

    public int size() {
        return requests.size();
    }
}
