/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.recovery.phantom;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.bytes.BytesReference;

/**
 * A serialized request together with the version it was serialized with, which is needed to read it back, or to re-encode it for a node
 * on another version.
 */
public record EncodedQueryRequest(BytesReference bytes, TransportVersion version) {

    public long sizeInBytes() {
        return bytes.length();
    }
}
