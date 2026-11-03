/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.recovery.phantom;

import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.telemetry.RecordingMeterRegistry;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.stateless.cache.SearchRecoveryTimeoutCalculationService;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.stateless.recovery.phantom.PhantomQueryTestUtils.clusterStateWithInitializingSearchReplica;
import static org.elasticsearch.xpack.stateless.recovery.phantom.PhantomQueryTestUtils.clusterStateWithInitializingSearchReplicas;
import static org.elasticsearch.xpack.stateless.recovery.phantom.PhantomQueryTestUtils.clusterStateWithRelocatingSearchReplica;
import static org.elasticsearch.xpack.stateless.recovery.phantom.PhantomQueryTestUtils.decode;
import static org.elasticsearch.xpack.stateless.recovery.phantom.PhantomQueryTestUtils.initializingAllocationId;
import static org.elasticsearch.xpack.stateless.recovery.phantom.PhantomQueryTestUtils.initializingAllocationIds;
import static org.elasticsearch.xpack.stateless.recovery.phantom.PhantomQueryTestUtils.queryRequest;
import static org.elasticsearch.xpack.stateless.recovery.phantom.PhantomQueryTestUtils.shards;
import static org.elasticsearch.xpack.stateless.recovery.phantom.PhantomQueryTestUtils.withCopyStarted;
import static org.elasticsearch.xpack.stateless.recovery.phantom.PhantomQueryTestUtils.withShards;
import static org.elasticsearch.xpack.stateless.recovery.phantom.PhantomQueryTestUtils.withoutCopy;
import static org.elasticsearch.xpack.stateless.recovery.phantom.PhantomQueryTestUtils.withoutIndex;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PhantomQuerySamplersTests extends ESTestCase {

    private static final TimeValue LEASE = TimeValue.timeValueMinutes(5);

    private final ShardId shardId = PhantomQueryTestUtils.shardId();
    private DeterministicTaskQueue taskQueue;
    private ClusterSettings clusterSettings;
    private AtomicReference<ClusterState> state;
    private volatile Supplier<ClusterState> stateSupplier;
    private PhantomQuerySamplers samplers;

    @Before
    public void createSamplers() {
        taskQueue = new DeterministicTaskQueue();
        state = new AtomicReference<>(clusterStateWithInitializingSearchReplica());
        stateSupplier = state::get;
        final Set<Setting<?>> registeredSettings = new HashSet<>(PhantomQueriesSettings.settings());
        registeredSettings.add(SearchRecoveryTimeoutCalculationService.OFFLINE_WARMING_TOTAL_TIMEOUT_CAP_SETTING);
        clusterSettings = new ClusterSettings(settings(true, LEASE), registeredSettings);
        final ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        when(clusterService.state()).thenAnswer(invocation -> stateSupplier.get());
        samplers = new PhantomQuerySamplers(
            taskQueue.getThreadPool(),
            clusterService,
            new PhantomQueriesSettings(clusterSettings),
            new PhantomQueryMetrics(new RecordingMeterRegistry())
        );
    }

    private static Settings settings(boolean enabled, TimeValue lease) {
        return Settings.builder()
            .put(PhantomQueriesSettings.ENABLED_SETTING.getKey(), enabled)
            .put(SearchRecoveryTimeoutCalculationService.OFFLINE_WARMING_TOTAL_TIMEOUT_CAP_SETTING.getKey(), lease.getStringRep())
            .build();
    }

    private void setEnabled(boolean enabled) {
        clusterSettings.applySettings(settings(enabled, LEASE));
    }

    private ShardSearchRequest request(int i) {
        return queryRequest(shardId, String.format(Locale.ROOT, "value-%03d", i));
    }

    private void offerToAllSamplers(ShardSearchRequest request) {
        final var observed = new ObservedQueryRequest(request);
        samplers.forEachSampler(shardId, sampler -> sampler.offer(observed));
    }

    private List<PhantomQuerySampler> currentSamplers() {
        final List<PhantomQuerySampler> current = new ArrayList<>();
        samplers.forEachSampler(shardId, current::add);
        return current;
    }

    private static long sizeOf(ShardSearchRequest request) {
        return new ObservedQueryRequest(request).encoded().sizeInBytes();
    }

    private void advanceTime(long millis) {
        taskQueue.runTasksUpToTimeInOrder(taskQueue.getCurrentTimeMillis() + millis);
    }

    /** As in the cluster applier, the new state is already the current one when the listener runs. */
    private void applyClusterState(ClusterState next) {
        final ClusterState previous = state.getAndSet(next);
        samplers.clusterChanged(new ClusterChangedEvent("test", next, previous));
    }

    private void assertNothingIsLeftBehind() {
        assertThat(samplers.samplerCount(), equalTo(0));
        assertThat(samplers.isSampling(shardId), is(false));
        assertThat(samplers.usedInBytes(), equalTo(0L));
    }

    public void testStartSamplingForAnInitializingCopy() {
        final String target = initializingAllocationId(state.get());
        assertThat(samplers.isSampling(shardId), is(false));

        assertThat(samplers.startSampling(shardId, target), is(true));

        assertThat(samplers.isSampling(shardId), is(true));
        assertThat(samplers.samplerCount(), equalTo(1));
        assertThat("the sampler has a lease", taskQueue.hasDeferredTasks(), is(true));
    }

    public void testStartSamplingIsDeclinedWhileDisabled() {
        setEnabled(false);

        assertThat(samplers.startSampling(shardId, initializingAllocationId(state.get())), is(false));

        assertNothingIsLeftBehind();
        assertThat(taskQueue.hasDeferredTasks(), is(false));
    }

    public void testStartSamplingIsDeclinedForACopyThatIsNotInitializing() {
        final List<String> startedAllocationIds = shards(state.get()).stream()
            .filter(ShardRouting::started)
            .map(routing -> routing.allocationId().getId())
            .toList();
        assertThat(startedAllocationIds, hasSize(2));

        for (String allocationId : startedAllocationIds) {
            assertThat(samplers.startSampling(shardId, allocationId), is(false));
        }
        assertThat(samplers.startSampling(shardId, randomAlphaOfLength(10)), is(false));

        assertNothingIsLeftBehind();
        assertThat(taskQueue.hasDeferredTasks(), is(false));
    }

    public void testStartSamplingIsDeclinedForAShardThatDoesNotExist() {
        assertThat(
            samplers.startSampling(new ShardId(randomAlphaOfLength(8), randomAlphaOfLength(8), 0), initializingAllocationId(state.get())),
            is(false)
        );
        assertThat(samplers.startSampling(new ShardId(shardId.getIndex(), 7), initializingAllocationId(state.get())), is(false));

        assertThat(samplers.samplerCount(), equalTo(0));
    }

    public void testStartSamplingForTheTargetOfARelocation() {
        state.set(clusterStateWithRelocatingSearchReplica());
        final String target = initializingAllocationId(state.get());
        final ShardRouting source = shards(state.get()).stream().filter(ShardRouting::relocating).findFirst().orElseThrow();
        assertThat(source.getTargetRelocatingShard().allocationId().getId(), equalTo(target));

        assertThat("the source is relocating, not recovering", samplers.startSampling(shardId, source.allocationId().getId()), is(false));
        assertThat(samplers.startSampling(shardId, target), is(true));

        assertThat(samplers.samplerCount(), equalTo(1));
    }

    public void testStartSamplingAppliesTheLimitsInEffectWhenItStarts() {
        clusterSettings.applySettings(
            Settings.builder().put(settings(true, LEASE)).put(PhantomQueriesSettings.MAX_QUERIES_PER_SAMPLE_SETTING.getKey(), 1).build()
        );
        final String target = initializingAllocationId(state.get());
        assertThat(samplers.startSampling(shardId, target), is(true));

        clusterSettings.applySettings(
            Settings.builder().put(settings(true, LEASE)).put(PhantomQueriesSettings.MAX_QUERIES_PER_SAMPLE_SETTING.getKey(), 5).build()
        );
        offerToAllSamplers(request(0));
        offerToAllSamplers(request(1));

        assertThat("a later change does not apply to a sampler that started", samplers.collect(shardId, target), hasSize(1));
    }

    public void testRepeatedStartKeepsTheSamplerAndItsRequests() {
        final String target = initializingAllocationId(state.get());
        assertThat(samplers.startSampling(shardId, target), is(true));
        offerToAllSamplers(request(0));
        final long used = samplers.usedInBytes();
        assertThat(used, greaterThan(0L));
        final List<PhantomQuerySampler> before = currentSamplers();

        assertThat(samplers.startSampling(shardId, target), is(true));

        final List<PhantomQuerySampler> after = currentSamplers();
        assertThat(after, hasSize(1));
        assertThat(after.get(0), sameInstance(before.get(0)));
        assertThat(samplers.samplerCount(), equalTo(1));
        assertThat(samplers.usedInBytes(), equalTo(used));
        assertThat(after.get(0).retainedCount(), equalTo(1));
    }

    public void testRepeatedStartDoesNotExtendTheLease() {
        final String target = initializingAllocationId(state.get());
        assertThat(samplers.startSampling(shardId, target), is(true));
        advanceTime(LEASE.millis() - 1);

        assertThat(samplers.startSampling(shardId, target), is(true));
        assertThat(samplers.samplerCount(), equalTo(1));
        advanceTime(1);

        assertNothingIsLeftBehind();
    }

    public void testRepeatedStartIsDeclinedOnceDisabled() {
        final String target = initializingAllocationId(state.get());
        assertThat(samplers.startSampling(shardId, target), is(true));

        setEnabled(false);

        assertThat(samplers.startSampling(shardId, target), is(false));
        assertThat("the sampler that exists ends as any other would", samplers.samplerCount(), equalTo(1));
        advanceTime(LEASE.millis());
        assertNothingIsLeftBehind();
    }

    public void testSeveralCopiesOfTheSameShardGetSeparateSamplers() throws IOException {
        state.set(clusterStateWithInitializingSearchReplicas(2));
        final List<String> targets = initializingAllocationIds(state.get());
        assertThat(targets, hasSize(2));

        assertThat(samplers.startSampling(shardId, targets.get(0)), is(true));
        assertThat(samplers.startSampling(shardId, targets.get(1)), is(true));
        assertThat(samplers.samplerCount(), equalTo(2));

        final var request = request(0);
        final var original = request.source().query();
        offerToAllSamplers(request);
        assertThat("each sampler counts the request against the node", samplers.usedInBytes(), equalTo(2 * sizeOf(request)));

        for (String target : targets) {
            final var sample = samplers.collect(shardId, target);
            assertThat(sample, hasSize(1));
            assertThat(decode(sample.get(0)).source().query(), equalTo(original));
        }
        assertNothingIsLeftBehind();
    }

    public void testStartSamplingDeclinesACopyThatStartedWhileItWasRegistering() {
        final ClusterState initializing = state.get();
        final String target = initializingAllocationId(initializing);
        final ClusterState started = withCopyStarted(initializing, target);
        // the copy is initializing when the request is checked, and has started by the time that the sampler is in place
        final var reads = new AtomicInteger();
        stateSupplier = () -> reads.incrementAndGet() == 1 ? initializing : started;

        assertThat(samplers.startSampling(shardId, target), is(false));

        assertThat(reads.get(), equalTo(2));
        assertNothingIsLeftBehind();
        advanceTime(LEASE.millis());
    }

    public void testCollectReturnsTheSampleInOrderAndEndsSampling() throws IOException {
        final String target = initializingAllocationId(state.get());
        assertThat(samplers.startSampling(shardId, target), is(true));
        final int requests = randomIntBetween(1, 10);
        final List<ShardSearchRequest> sent = new ArrayList<>();
        for (int i = 0; i < requests; i++) {
            sent.add(request(i));
            offerToAllSamplers(sent.get(i));
        }

        final var sample = samplers.collect(shardId, target);

        assertThat(sample, hasSize(requests));
        for (int i = 0; i < requests; i++) {
            assertThat(decode(sample.get(i)).source().query(), equalTo(sent.get(i).source().query()));
        }
        assertNothingIsLeftBehind();
        offerToAllSamplers(request(requests));
        assertThat("nothing is retained after collection", samplers.usedInBytes(), equalTo(0L));
        advanceTime(LEASE.millis());
        assertNothingIsLeftBehind();
    }

    public void testCollectWithoutASamplerReturnsAnEmptySample() {
        assertThat(samplers.collect(shardId, initializingAllocationId(state.get())), empty());
        assertThat(samplers.collect(shardId, randomAlphaOfLength(10)), empty());
        assertThat(samplers.collect(new ShardId(randomAlphaOfLength(8), randomAlphaOfLength(8), 0), randomAlphaOfLength(10)), empty());
    }

    public void testCollectingTheSameCopyTwiceReturnsAnEmptySampleTheSecondTime() {
        final String target = initializingAllocationId(state.get());
        assertThat(samplers.startSampling(shardId, target), is(true));
        offerToAllSamplers(request(0));

        assertThat(samplers.collect(shardId, target), hasSize(1));
        assertThat(samplers.collect(shardId, target), empty());
    }

    public void testCollectingOneCopyKeepsTheSamplersOfTheOthers() {
        state.set(clusterStateWithInitializingSearchReplicas(2));
        final List<String> targets = initializingAllocationIds(state.get());
        assertThat(samplers.startSampling(shardId, targets.get(0)), is(true));
        assertThat(samplers.startSampling(shardId, targets.get(1)), is(true));
        offerToAllSamplers(request(0));
        final long sizeOfOne = sizeOf(request(0));

        assertThat(samplers.collect(shardId, targets.get(0)), hasSize(1));

        assertThat(samplers.samplerCount(), equalTo(1));
        assertThat(samplers.isSampling(shardId), is(true));
        assertThat(samplers.usedInBytes(), equalTo(sizeOfOne));
        assertThat(samplers.collect(shardId, targets.get(1)), hasSize(1));
        assertNothingIsLeftBehind();
    }

    public void testTheLeaseOfACollectedSamplerDoesNotRemoveANewSampler() {
        final String target = initializingAllocationId(state.get());
        assertThat(samplers.startSampling(shardId, target), is(true));
        final long firstStart = taskQueue.getCurrentTimeMillis();
        advanceTime(60_000);
        assertThat(samplers.collect(shardId, target), empty());

        assertThat(samplers.startSampling(shardId, target), is(true));
        taskQueue.runTasksUpToTimeInOrder(firstStart + LEASE.millis());

        assertThat("the first lease is gone, and the second one is not due yet", samplers.samplerCount(), equalTo(1));
        taskQueue.runTasksUpToTimeInOrder(firstStart + 60_000 + LEASE.millis());
        assertNothingIsLeftBehind();
    }

    public void testSamplerIsRemovedWhenTheCopyStarts() {
        final String target = initializingAllocationId(state.get());
        assertThat(samplers.startSampling(shardId, target), is(true));
        offerToAllSamplers(request(0));
        assertThat(samplers.usedInBytes(), greaterThan(0L));

        applyClusterState(withCopyStarted(state.get(), target));

        assertNothingIsLeftBehind();
        advanceTime(LEASE.millis());
    }

    public void testSamplerIsRemovedWhenTheCopyIsRemovedFromTheRoutingTable() {
        final String target = initializingAllocationId(state.get());
        assertThat(samplers.startSampling(shardId, target), is(true));
        offerToAllSamplers(request(0));

        applyClusterState(withoutCopy(state.get(), target));

        assertNothingIsLeftBehind();
    }

    public void testSamplerIsRemovedWhenTheIndexIsRemoved() {
        final String target = initializingAllocationId(state.get());
        assertThat(samplers.startSampling(shardId, target), is(true));
        offerToAllSamplers(request(0));

        applyClusterState(withoutIndex());

        assertNothingIsLeftBehind();
    }

    public void testSamplerOfARelocationTargetIsRemovedWhenTheRelocationIsCancelled() {
        state.set(clusterStateWithRelocatingSearchReplica());
        final String target = initializingAllocationId(state.get());
        assertThat(samplers.startSampling(shardId, target), is(true));
        offerToAllSamplers(request(0));

        applyClusterState(
            withShards(state.get(), shards(state.get()).stream().map(r -> r.relocating() ? r.cancelRelocation() : r).toList())
        );

        assertNothingIsLeftBehind();
    }

    public void testSamplerOfARelocationTargetIsRemovedWhenTheRelocationCompletes() {
        state.set(clusterStateWithRelocatingSearchReplica());
        final String target = initializingAllocationId(state.get());
        assertThat(samplers.startSampling(shardId, target), is(true));
        offerToAllSamplers(request(0));
        final List<ShardRouting> completed = new ArrayList<>();
        for (ShardRouting routing : shards(state.get())) {
            if (routing.relocating()) {
                completed.add(routing.getTargetRelocatingShard().moveToStarted(ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE));
            } else {
                completed.add(routing);
            }
        }

        applyClusterState(withShards(state.get(), completed));

        assertNothingIsLeftBehind();
    }

    public void testSamplerIsKeptWhileTheCopyIsStillInitializing() {
        final String target = initializingAllocationId(state.get());
        assertThat(samplers.startSampling(shardId, target), is(true));
        offerToAllSamplers(request(0));
        final long used = samplers.usedInBytes();

        // a routing change that does not concern the copy
        applyClusterState(withShards(state.get(), shards(state.get())));

        assertThat(samplers.samplerCount(), equalTo(1));
        assertThat(samplers.usedInBytes(), equalTo(used));
        assertThat(samplers.collect(shardId, target), hasSize(1));
    }

    public void testOnlyTheSamplersOfCopiesThatAreNoLongerInitializingAreRemoved() {
        state.set(clusterStateWithInitializingSearchReplicas(2));
        final List<String> targets = initializingAllocationIds(state.get());
        assertThat(samplers.startSampling(shardId, targets.get(0)), is(true));
        assertThat(samplers.startSampling(shardId, targets.get(1)), is(true));
        offerToAllSamplers(request(0));

        applyClusterState(withCopyStarted(state.get(), targets.get(0)));

        assertThat(samplers.samplerCount(), equalTo(1));
        assertThat(samplers.usedInBytes(), equalTo(sizeOf(request(0))));
        assertThat(samplers.collect(shardId, targets.get(0)), empty());
        assertThat(samplers.collect(shardId, targets.get(1)), hasSize(1));
    }

    public void testClusterStateChangesAreIgnoredWhileThereAreNoSamplers() {
        applyClusterState(withoutIndex());
        applyClusterState(clusterStateWithInitializingSearchReplica());

        assertThat(samplers.samplerCount(), equalTo(0));
    }

    public void testSamplersAreRemovedWhenTheLocalCopyCloses() {
        state.set(clusterStateWithInitializingSearchReplicas(2));
        final List<String> targets = initializingAllocationIds(state.get());
        assertThat(samplers.startSampling(shardId, targets.get(0)), is(true));
        assertThat(samplers.startSampling(shardId, targets.get(1)), is(true));
        offerToAllSamplers(request(0));

        samplers.afterIndexShardClosed(new ShardId(shardId.getIndex(), 1), null, Settings.EMPTY);
        samplers.afterIndexShardClosed(new ShardId(randomAlphaOfLength(8), randomAlphaOfLength(8), 0), null, Settings.EMPTY);
        assertThat("other shards closing does not matter", samplers.samplerCount(), equalTo(2));

        samplers.afterIndexShardClosed(shardId, null, Settings.EMPTY);

        assertNothingIsLeftBehind();
        advanceTime(LEASE.millis());
        assertNothingIsLeftBehind();
    }

    public void testSamplerIsRemovedWhenItsLeaseExpires() {
        final String target = initializingAllocationId(state.get());
        assertThat(samplers.startSampling(shardId, target), is(true));
        offerToAllSamplers(request(0));

        advanceTime(LEASE.millis() - 1);
        assertThat("the lease has not expired yet", samplers.samplerCount(), equalTo(1));
        assertThat(samplers.usedInBytes(), greaterThan(0L));

        advanceTime(1);
        assertNothingIsLeftBehind();
        assertThat(samplers.collect(shardId, target), empty());
    }

    public void testLeaseFollowsTheWarmingTimeoutCap() {
        final TimeValue shorter = TimeValue.timeValueMinutes(1);
        clusterSettings.applySettings(settings(true, shorter));
        final String target = initializingAllocationId(state.get());
        assertThat(samplers.startSampling(shardId, target), is(true));

        advanceTime(shorter.millis() - 1);
        assertThat(samplers.samplerCount(), equalTo(1));
        advanceTime(1);

        assertNothingIsLeftBehind();
    }

    public void testBudgetIsBackToZeroAfterEveryWayOfEndingASample() {
        state.set(clusterStateWithInitializingSearchReplicas(4));
        final List<String> targets = initializingAllocationIds(state.get());
        for (String target : targets) {
            assertThat(samplers.startSampling(shardId, target), is(true));
        }
        offerToAllSamplers(request(0));
        assertThat(samplers.usedInBytes(), equalTo(4 * sizeOf(request(0))));

        samplers.collect(shardId, targets.get(0));
        assertThat(samplers.usedInBytes(), equalTo(3 * sizeOf(request(0))));
        applyClusterState(withCopyStarted(state.get(), targets.get(1)));
        assertThat(samplers.usedInBytes(), equalTo(2 * sizeOf(request(0))));
        advanceTime(LEASE.millis());

        assertNothingIsLeftBehind();
    }

    public void testCurrentSamplersAreAnImmutableSnapshot() {
        state.set(clusterStateWithInitializingSearchReplicas(2));
        final List<String> targets = initializingAllocationIds(state.get());
        assertThat(samplers.startSampling(shardId, targets.get(0)), is(true));

        final var seen = new ArrayList<PhantomQuerySampler>();
        samplers.forEachSampler(shardId, sampler -> {
            seen.add(sampler);
            // a sampler registered while the observer iterates must neither fail nor be visited by this iteration
            assertThat(samplers.startSampling(shardId, targets.get(1)), is(true));
        });

        assertThat(seen, hasSize(1));
        assertThat(samplers.samplerCount(), equalTo(2));
    }
}
