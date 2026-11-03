/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.cache;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;

import java.util.OptionalLong;

import static org.hamcrest.Matchers.equalTo;

public class SearchRecoveryWarmingWindowsTests extends ESTestCase {

    private static ShardId randomShardId() {
        return new ShardId(randomIdentifier(), randomUUID(), randomIntBetween(0, 10));
    }

    public void testNoWindowByDefault() {
        final var windows = new SearchRecoveryWarmingWindows();
        assertThat(windows.relativeDeadlineMillis(randomShardId(), randomIdentifier()), equalTo(OptionalLong.empty()));
    }

    public void testStartAndRemove() {
        final var windows = new SearchRecoveryWarmingWindows();
        final var shardId = randomShardId();
        final var allocationId = randomIdentifier();
        final long deadline = randomLongBetween(0, Long.MAX_VALUE);

        windows.startWindow(shardId, allocationId, deadline);
        assertThat(windows.relativeDeadlineMillis(shardId, allocationId), equalTo(OptionalLong.of(deadline)));

        windows.removeWindow(shardId, allocationId);
        assertThat(windows.relativeDeadlineMillis(shardId, allocationId), equalTo(OptionalLong.empty()));

        windows.removeWindow(shardId, allocationId);
        windows.removeWindow(randomShardId(), randomIdentifier());
    }

    public void testWindowsAreKeyedByShardAndAllocationId() {
        final var windows = new SearchRecoveryWarmingWindows();
        final var shardId = randomShardId();
        final var otherShardId = randomValueOtherThan(shardId, SearchRecoveryWarmingWindowsTests::randomShardId);
        final var allocationId = randomIdentifier();
        final var otherAllocationId = randomValueOtherThan(allocationId, ESTestCase::randomIdentifier);

        windows.startWindow(shardId, allocationId, 1L);
        windows.startWindow(shardId, otherAllocationId, 2L);
        windows.startWindow(otherShardId, allocationId, 3L);

        assertThat(windows.relativeDeadlineMillis(shardId, allocationId), equalTo(OptionalLong.of(1L)));
        assertThat(windows.relativeDeadlineMillis(shardId, otherAllocationId), equalTo(OptionalLong.of(2L)));
        assertThat(windows.relativeDeadlineMillis(otherShardId, allocationId), equalTo(OptionalLong.of(3L)));
        assertThat(windows.relativeDeadlineMillis(otherShardId, otherAllocationId), equalTo(OptionalLong.empty()));

        windows.removeWindow(shardId, allocationId);
        assertThat(windows.relativeDeadlineMillis(shardId, allocationId), equalTo(OptionalLong.empty()));
        assertThat(windows.relativeDeadlineMillis(shardId, otherAllocationId), equalTo(OptionalLong.of(2L)));
        assertThat(windows.relativeDeadlineMillis(otherShardId, allocationId), equalTo(OptionalLong.of(3L)));
    }

    public void testStartReplacesTheWindowOfAnEarlierAttempt() {
        final var windows = new SearchRecoveryWarmingWindows();
        final var shardId = randomShardId();
        final var allocationId = randomIdentifier();

        windows.startWindow(shardId, allocationId, 100L);
        windows.startWindow(shardId, allocationId, 50L);

        assertThat(windows.relativeDeadlineMillis(shardId, allocationId), equalTo(OptionalLong.of(50L)));
    }

    public void testUpdateMovesTheDeadlineOfAnExistingWindow() {
        final var windows = new SearchRecoveryWarmingWindows();
        final var shardId = randomShardId();
        final var allocationId = randomIdentifier();

        windows.startWindow(shardId, allocationId, 100L);
        windows.updateDeadlineIfPresent(shardId, allocationId, 300L);
        assertThat(windows.relativeDeadlineMillis(shardId, allocationId), equalTo(OptionalLong.of(300L)));

        windows.updateDeadlineIfPresent(shardId, allocationId, 200L);
        assertThat(windows.relativeDeadlineMillis(shardId, allocationId), equalTo(OptionalLong.of(200L)));
    }

    public void testUpdateDoesNotCreateAWindow() {
        final var windows = new SearchRecoveryWarmingWindows();
        final var shardId = randomShardId();
        final var allocationId = randomIdentifier();

        windows.updateDeadlineIfPresent(shardId, allocationId, randomLong());
        assertThat(windows.relativeDeadlineMillis(shardId, allocationId), equalTo(OptionalLong.empty()));

        windows.startWindow(shardId, allocationId, 100L);
        windows.removeWindow(shardId, allocationId);
        windows.updateDeadlineIfPresent(shardId, allocationId, randomLong());
        assertThat("a removed window stays removed", windows.relativeDeadlineMillis(shardId, allocationId), equalTo(OptionalLong.empty()));
    }

    public void testClosingTheShardRemovesItsWindows() {
        final var windows = new SearchRecoveryWarmingWindows();
        final var shardId = randomShardId();
        final var otherShardId = randomValueOtherThan(shardId, SearchRecoveryWarmingWindowsTests::randomShardId);
        final var allocationId = randomIdentifier();
        final var otherAllocationId = randomValueOtherThan(allocationId, ESTestCase::randomIdentifier);

        windows.startWindow(shardId, allocationId, 1L);
        windows.startWindow(shardId, otherAllocationId, 2L);
        windows.startWindow(otherShardId, allocationId, 3L);

        windows.afterIndexShardClosed(shardId, null, Settings.EMPTY);

        assertThat(windows.relativeDeadlineMillis(shardId, allocationId), equalTo(OptionalLong.empty()));
        assertThat(windows.relativeDeadlineMillis(shardId, otherAllocationId), equalTo(OptionalLong.empty()));
        assertThat(windows.relativeDeadlineMillis(otherShardId, allocationId), equalTo(OptionalLong.of(3L)));
    }

    public void testClosingAShardWithoutWindowsIsFine() {
        final var windows = new SearchRecoveryWarmingWindows();
        final var shardIdWithWindow = randomShardId();
        final var allocationId = randomIdentifier();
        windows.startWindow(shardIdWithWindow, allocationId, 1L);

        final var shardIdWithoutWindow = randomValueOtherThan(shardIdWithWindow, SearchRecoveryWarmingWindowsTests::randomShardId);

        windows.afterIndexShardClosed(shardIdWithoutWindow, null, Settings.EMPTY);
        assertThat(windows.relativeDeadlineMillis(shardIdWithWindow, allocationId), equalTo(OptionalLong.of(1L)));

        windows.afterIndexShardClosed(shardIdWithWindow, null, Settings.EMPTY);
        windows.afterIndexShardClosed(shardIdWithWindow, null, Settings.EMPTY);
        assertThat(windows.relativeDeadlineMillis(shardIdWithWindow, allocationId), equalTo(OptionalLong.empty()));
    }
}
