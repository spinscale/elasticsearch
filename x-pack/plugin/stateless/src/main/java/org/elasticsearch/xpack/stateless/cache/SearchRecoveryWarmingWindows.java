/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.cache;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.shard.IndexEventListener;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;

import java.util.OptionalLong;
import java.util.concurrent.ConcurrentMap;

/**
 * The deadline of the warming wait of each search shard copy that is recovering on this node, which is the "warming window".
 *
 * <p>
 * <b>What:</b> a recovering search shard copy waits for offline cache warming before its engine opens.
 * {@link SharedBlobCacheWarmingService} decides for how long, and may extend the wait while it runs. The deadline is only known inside
 * its warming listener, which is invisible to the rest of the recovery, so it is recorded here, keyed by the shard and the allocation id
 * of the recovering copy. Deadlines are in the milliseconds of {@code ThreadPool#relativeTimeInMillis()}.
 *
 * <p>
 * <b>Why:</b> work that the recovery does after the wait, such as replaying sampled queries on the new copy, has to fit into the time
 * the recovery already spends waiting for warming. A fresh budget computed at that point could double that time, so such work reads
 * the deadline that the wait itself used.
 *
 * <p>
 * <b>Meaning:</b> a recovery without a window does not wait for warming, so there is no time to use. A deadline in the past means
 * the wait timed out, and a deadline in the future means warming completed early and that much time is left.
 *
 * <p>
 * <b>Lifetime:</b> a window outlives its wait, because that is when it is read. The recovery action removes it when it completes,
 * whatever the outcome, and it is removed when the shard closes.
 */
public class SearchRecoveryWarmingWindows implements IndexEventListener {

    private record Key(ShardId shardId, String allocationId) {}

    private final ConcurrentMap<Key, Long> relativeDeadlinesMillis = ConcurrentCollections.newConcurrentMap();

    public void startWindow(ShardId shardId, String allocationId, long relativeDeadlineMillis) {
        relativeDeadlinesMillis.put(new Key(shardId, allocationId), relativeDeadlineMillis);
    }

    public void updateDeadlineIfPresent(ShardId shardId, String allocationId, long relativeDeadlineMillis) {
        relativeDeadlinesMillis.computeIfPresent(new Key(shardId, allocationId), (key, current) -> relativeDeadlineMillis);
    }

    public OptionalLong relativeDeadlineMillis(ShardId shardId, String allocationId) {
        final Long relativeDeadlineMillis = relativeDeadlinesMillis.get(new Key(shardId, allocationId));
        return relativeDeadlineMillis == null ? OptionalLong.empty() : OptionalLong.of(relativeDeadlineMillis);
    }

    public void removeWindow(ShardId shardId, String allocationId) {
        relativeDeadlinesMillis.remove(new Key(shardId, allocationId));
    }

    @Override
    public void afterIndexShardClosed(ShardId shardId, @Nullable IndexShard indexShard, Settings indexSettings) {
        relativeDeadlinesMillis.keySet().removeIf(key -> key.shardId().equals(shardId));
    }
}
