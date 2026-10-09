/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.fielddata;

import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;

import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;

public class ShardFieldDataTests extends ESTestCase {

    private final ShardId shardId = new ShardId("index", "_na_", 0);

    public void testGlobalOrdinalsFieldsAccessedSince() {
        final AtomicLong clock = new AtomicLong(randomLongBetween(0, 1000));
        final ShardFieldData shardFieldData = new ShardFieldData(clock::get);
        assertThat(shardFieldData.globalOrdinalsFieldsAccessedSince(Long.MIN_VALUE), empty());

        final long firstAccess = clock.get();
        shardFieldData.onGlobalOrdinalsAccess(shardId, "field1");
        final long secondAccess = clock.addAndGet(randomLongBetween(1, 1000));
        shardFieldData.onGlobalOrdinalsAccess(shardId, "field2");

        assertThat(shardFieldData.globalOrdinalsFieldsAccessedSince(firstAccess), containsInAnyOrder("field1", "field2"));
        assertThat(shardFieldData.globalOrdinalsFieldsAccessedSince(firstAccess + 1), containsInAnyOrder("field2"));
        assertThat(shardFieldData.globalOrdinalsFieldsAccessedSince(secondAccess + 1), empty());

        final long thirdAccess = clock.addAndGet(randomLongBetween(1, 1000));
        shardFieldData.onGlobalOrdinalsAccess(shardId, "field1");
        assertThat(shardFieldData.globalOrdinalsFieldsAccessedSince(secondAccess + 1), containsInAnyOrder("field1"));
        assertThat(shardFieldData.globalOrdinalsFieldsAccessedSince(thirdAccess + 1), empty());
    }

    public void testLastAccessNeverMovesBackwards() {
        final AtomicLong clock = new AtomicLong(randomLongBetween(1000, 2000));
        final ShardFieldData shardFieldData = new ShardFieldData(clock::get);
        final long lastAccess = clock.get();
        shardFieldData.onGlobalOrdinalsAccess(shardId, "field");

        clock.addAndGet(-randomLongBetween(1, 1000));
        shardFieldData.onGlobalOrdinalsAccess(shardId, "field");

        assertThat(shardFieldData.globalOrdinalsFieldsAccessedSince(lastAccess), containsInAnyOrder("field"));
    }

    public void testRemovalKeepsLastAccess() {
        final AtomicLong clock = new AtomicLong(randomLongBetween(0, 1000));
        final ShardFieldData shardFieldData = new ShardFieldData(clock::get);
        shardFieldData.onGlobalOrdinalsAccess(shardId, "field");

        // global ordinals are removed from the cache on every refresh, which must not reset when they were last used
        shardFieldData.onRemoval(shardId, "field", randomBoolean(), -1);

        assertThat(shardFieldData.globalOrdinalsFieldsAccessedSince(clock.get()), containsInAnyOrder("field"));
    }
}
