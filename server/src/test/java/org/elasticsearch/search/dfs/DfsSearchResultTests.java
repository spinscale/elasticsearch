/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.dfs;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.TermStatistics;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

public class DfsSearchResultTests extends ESTestCase {

    public void testBytesReadRoundTrip() throws IOException {
        ShardId shardId = new ShardId("index", "uuid", randomInt());
        DfsSearchResult original = new DfsSearchResult(
            new ShardSearchContextId(UUIDs.base64UUID(), randomLong()),
            new SearchShardTarget("node", shardId, null),
            null
        );
        original.termsStatistics(new Term[0], new TermStatistics[0]);
        long expected = randomNonNegativeLong();
        original.setBytesRead(expected);

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            original.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                DfsSearchResult deserialized = new DfsSearchResult(in);
                assertEquals(expected, deserialized.getBytesRead());
            }
        }
    }
}
