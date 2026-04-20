/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.fetch;

import org.apache.lucene.search.TotalHits;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

public class FetchSearchResultTests extends ESTestCase {

    public void testBytesReadRoundTrip() throws IOException {
        FetchSearchResult original = newEmptyResult();
        long expected = randomNonNegativeLong();
        original.setBytesRead(expected);
        try {
            FetchSearchResult deserialized = copy(original);
            try {
                assertEquals(expected, deserialized.getBytesRead());
            } finally {
                deserialized.decRef();
            }
        } finally {
            original.decRef();
        }
    }

    private static FetchSearchResult newEmptyResult() {
        ShardId shardId = new ShardId("index", "uuid", randomInt());
        FetchSearchResult result = new FetchSearchResult(
            new ShardSearchContextId(UUIDs.base64UUID(), randomLong()),
            new SearchShardTarget("node", shardId, null)
        );
        SearchHits hits = SearchHits.empty(new TotalHits(0, TotalHits.Relation.EQUAL_TO), Float.NaN);
        try {
            result.shardResult(hits, null);
        } finally {
            hits.decRef();
        }
        return result;
    }

    private static FetchSearchResult copy(FetchSearchResult original) throws IOException {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            original.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                return new FetchSearchResult(in);
            }
        }
    }
}
