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
import org.elasticsearch.xpack.stateless.recovery.phantom.PhantomQueryMetrics.SamplingStartOutcome;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.stateless.recovery.phantom.PhantomQueryTestUtils.clusterStateWithInitializingSearchReplica;
import static org.elasticsearch.xpack.stateless.recovery.phantom.PhantomQueryTestUtils.clusterStateWithInitializingSearchReplicas;
import static org.elasticsearch.xpack.stateless.recovery.phantom.PhantomQueryTestUtils.clusterStateWithRelocatingSearchReplica;
import static org.elasticsearch.xpack.stateless.recovery.phantom.PhantomQueryTestUtils.counterBy;
import static org.elasticsearch.xpack.stateless.recovery.phantom.PhantomQueryTestUtils.decode;
import static org.elasticsearch.xpack.stateless.recovery.phantom.PhantomQueryTestUtils.gauge;
import static org.elasticsearch.xpack.stateless.recovery.phantom.PhantomQueryTestUtils.initializingAllocationId;
import static org.elasticsearch.xpack.stateless.recovery.phantom.PhantomQueryTestUtils.initializingAllocationIds;
import static org.elasticsearch.xpack.stateless.recovery.phantom.PhantomQueryTestUtils.longHistogram;
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
    private RecordingMeterRegistry meterRegistry;
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
        meterRegistry = new RecordingMeterRegistry();
        samplers = new PhantomQuerySamplers(
            taskQueue.getThreadPool(),
            clusterService,
            new PhantomQueriesSettings(clusterSettings),
            new PhantomQueryMetrics(meterRegistry)
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

        assertThat(samplers.startSampling(shardId, target).sampling(), is(true));

        assertThat(samplers.isSampling(shardId), is(true));
        assertThat(samplers.samplerCount(), equalTo(1));
        assertThat("the sampler has a lease", taskQueue.hasDeferredTasks(), is(true));
    }

    public void testStartSamplingIsDeclinedWhileDisabled() {
        setEnabled(false);

        assertThat(samplers.startSampling(shardId, initializingAllocationId(state.get())).sampling(), is(false));

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
            assertThat(samplers.startSampling(shardId, allocationId).sampling(), is(false));
        }
        assertThat(samplers.startSampling(shardId, randomAlphaOfLength(10)).sampling(), is(false));

        assertNothingIsLeftBehind();
        assertThat(taskQueue.hasDeferredTasks(), is(false));
    }

    public void testStartSamplingIsDeclinedForAPrimary() {
        final ClusterState withInitializingPrimary = PhantomQueryTestUtils.clusterStateWithInitializingPrimary(
            randomFrom(ShardRouting.Role.INDEX_ONLY, ShardRouting.Role.DEFAULT)
        );
        state.set(withInitializingPrimary);
        final String primary = initializingAllocationId(withInitializingPrimary);

        assertThat(samplers.startSampling(shardId, primary).sampling(), is(false));

        assertNothingIsLeftBehind();
        assertThat(taskQueue.hasDeferredTasks(), is(false));
    }

    public void testReplicaLookupNeverFindsThePrimary() {
        final ClusterState clusterState = clusterStateWithInitializingSearchReplica();
        int replicas = 0;
        for (ShardRouting routing : shards(clusterState)) {
            final ShardRouting found = PhantomQuerySamplers.findReplica(clusterState, shardId, routing.allocationId().getId());
            if (routing.primary()) {
                assertThat(found, is((ShardRouting) null));
                assertThat(PhantomQuerySamplers.isInitializingReplica(clusterState, shardId, routing.allocationId().getId()), is(false));
            } else {
                replicas++;
                assertThat(found.allocationId().getId(), equalTo(routing.allocationId().getId()));
            }
        }
        assertThat(replicas, equalTo(2));

        final ClusterState relocating = clusterStateWithRelocatingSearchReplica();
        final ShardRouting source = shards(relocating).stream().filter(ShardRouting::relocating).findFirst().orElseThrow();
        final ShardRouting target = source.getTargetRelocatingShard();
        assertThat(
            "the target of a relocation of a replica is a replica",
            PhantomQuerySamplers.findReplica(relocating, shardId, target.allocationId().getId()).allocationId().getId(),
            equalTo(target.allocationId().getId())
        );
        assertThat(PhantomQuerySamplers.isInitializingReplica(relocating, shardId, target.allocationId().getId()), is(true));
    }

    public void testStartSamplingIsDeclinedForAShardThatDoesNotExist() {
        assertThat(
            samplers.startSampling(new ShardId(randomAlphaOfLength(8), randomAlphaOfLength(8), 0), initializingAllocationId(state.get()))
                .sampling(),
            is(false)
        );
        assertThat(samplers.startSampling(new ShardId(shardId.getIndex(), 7), initializingAllocationId(state.get())).sampling(), is(false));

        assertThat(samplers.samplerCount(), equalTo(0));
    }

    public void testStartSamplingForTheTargetOfARelocation() {
        state.set(clusterStateWithRelocatingSearchReplica());
        final String target = initializingAllocationId(state.get());
        final ShardRouting source = shards(state.get()).stream().filter(ShardRouting::relocating).findFirst().orElseThrow();
        assertThat(source.getTargetRelocatingShard().allocationId().getId(), equalTo(target));

        assertThat(
            "the source is relocating, not recovering",
            samplers.startSampling(shardId, source.allocationId().getId()).sampling(),
            is(false)
        );
        assertThat(samplers.startSampling(shardId, target).sampling(), is(true));

        assertThat(samplers.samplerCount(), equalTo(1));
    }

    public void testStartSamplingAppliesTheLimitsInEffectWhenItStarts() {
        clusterSettings.applySettings(
            Settings.builder().put(settings(true, LEASE)).put(PhantomQueriesSettings.MAX_QUERIES_PER_SAMPLE_SETTING.getKey(), 1).build()
        );
        final String target = initializingAllocationId(state.get());
        assertThat(samplers.startSampling(shardId, target).sampling(), is(true));

        clusterSettings.applySettings(
            Settings.builder().put(settings(true, LEASE)).put(PhantomQueriesSettings.MAX_QUERIES_PER_SAMPLE_SETTING.getKey(), 5).build()
        );
        offerToAllSamplers(request(0));
        offerToAllSamplers(request(1));

        assertThat("a later change does not apply to a sampler that started", samplers.collect(shardId, target), hasSize(1));
    }

    public void testRepeatedStartKeepsTheSamplerAndItsRequests() {
        final String target = initializingAllocationId(state.get());
        assertThat(samplers.startSampling(shardId, target).sampling(), is(true));
        offerToAllSamplers(request(0));
        final long used = samplers.usedInBytes();
        assertThat(used, greaterThan(0L));
        final List<PhantomQuerySampler> before = currentSamplers();

        assertThat(samplers.startSampling(shardId, target).sampling(), is(true));

        final List<PhantomQuerySampler> after = currentSamplers();
        assertThat(after, hasSize(1));
        assertThat(after.get(0), sameInstance(before.get(0)));
        assertThat(samplers.samplerCount(), equalTo(1));
        assertThat(samplers.usedInBytes(), equalTo(used));
        assertThat(after.get(0).retainedCount(), equalTo(1));
    }

    public void testRepeatedStartDoesNotExtendTheLease() {
        final String target = initializingAllocationId(state.get());
        assertThat(samplers.startSampling(shardId, target).sampling(), is(true));
        advanceTime(LEASE.millis() - 1);

        assertThat(samplers.startSampling(shardId, target).sampling(), is(true));
        assertThat(samplers.samplerCount(), equalTo(1));
        advanceTime(1);

        assertNothingIsLeftBehind();
    }

    public void testRepeatedStartIsDeclinedOnceDisabled() {
        final String target = initializingAllocationId(state.get());
        assertThat(samplers.startSampling(shardId, target).sampling(), is(true));

        setEnabled(false);

        assertThat(samplers.startSampling(shardId, target).sampling(), is(false));
        assertThat("the sampler that exists ends as any other would", samplers.samplerCount(), equalTo(1));
        advanceTime(LEASE.millis());
        assertNothingIsLeftBehind();
    }

    public void testSeveralCopiesOfTheSameShardGetSeparateSamplers() throws IOException {
        state.set(clusterStateWithInitializingSearchReplicas(2));
        final List<String> targets = initializingAllocationIds(state.get());
        assertThat(targets, hasSize(2));

        assertThat(samplers.startSampling(shardId, targets.get(0)).sampling(), is(true));
        assertThat(samplers.startSampling(shardId, targets.get(1)).sampling(), is(true));
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

        assertThat(samplers.startSampling(shardId, target).sampling(), is(false));

        assertThat(reads.get(), equalTo(2));
        assertNothingIsLeftBehind();
        advanceTime(LEASE.millis());
    }

    public void testCollectReturnsTheSampleInOrderAndEndsSampling() throws IOException {
        final String target = initializingAllocationId(state.get());
        assertThat(samplers.startSampling(shardId, target).sampling(), is(true));
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
        assertThat(samplers.startSampling(shardId, target).sampling(), is(true));
        offerToAllSamplers(request(0));

        assertThat(samplers.collect(shardId, target), hasSize(1));
        assertThat(samplers.collect(shardId, target), empty());
    }

    public void testCollectingOneCopyKeepsTheSamplersOfTheOthers() {
        state.set(clusterStateWithInitializingSearchReplicas(2));
        final List<String> targets = initializingAllocationIds(state.get());
        assertThat(samplers.startSampling(shardId, targets.get(0)).sampling(), is(true));
        assertThat(samplers.startSampling(shardId, targets.get(1)).sampling(), is(true));
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
        assertThat(samplers.startSampling(shardId, target).sampling(), is(true));
        final long firstStart = taskQueue.getCurrentTimeMillis();
        advanceTime(60_000);
        assertThat(samplers.collect(shardId, target), empty());

        assertThat(samplers.startSampling(shardId, target).sampling(), is(true));
        taskQueue.runTasksUpToTimeInOrder(firstStart + LEASE.millis());

        assertThat("the first lease is gone, and the second one is not due yet", samplers.samplerCount(), equalTo(1));
        taskQueue.runTasksUpToTimeInOrder(firstStart + 60_000 + LEASE.millis());
        assertNothingIsLeftBehind();
    }

    public void testSamplerIsRemovedWhenTheCopyStarts() {
        final String target = initializingAllocationId(state.get());
        assertThat(samplers.startSampling(shardId, target).sampling(), is(true));
        offerToAllSamplers(request(0));
        assertThat(samplers.usedInBytes(), greaterThan(0L));

        applyClusterState(withCopyStarted(state.get(), target));

        assertNothingIsLeftBehind();
        advanceTime(LEASE.millis());
    }

    public void testSamplerIsRemovedWhenTheCopyIsRemovedFromTheRoutingTable() {
        final String target = initializingAllocationId(state.get());
        assertThat(samplers.startSampling(shardId, target).sampling(), is(true));
        offerToAllSamplers(request(0));

        applyClusterState(withoutCopy(state.get(), target));

        assertNothingIsLeftBehind();
    }

    public void testSamplerIsRemovedWhenTheIndexIsRemoved() {
        final String target = initializingAllocationId(state.get());
        assertThat(samplers.startSampling(shardId, target).sampling(), is(true));
        offerToAllSamplers(request(0));

        applyClusterState(withoutIndex());

        assertNothingIsLeftBehind();
    }

    public void testSamplerOfARelocationTargetIsRemovedWhenTheRelocationIsCancelled() {
        state.set(clusterStateWithRelocatingSearchReplica());
        final String target = initializingAllocationId(state.get());
        assertThat(samplers.startSampling(shardId, target).sampling(), is(true));
        offerToAllSamplers(request(0));

        applyClusterState(
            withShards(state.get(), shards(state.get()).stream().map(r -> r.relocating() ? r.cancelRelocation() : r).toList())
        );

        assertNothingIsLeftBehind();
    }

    public void testSamplerOfARelocationTargetIsRemovedWhenTheRelocationCompletes() {
        state.set(clusterStateWithRelocatingSearchReplica());
        final String target = initializingAllocationId(state.get());
        assertThat(samplers.startSampling(shardId, target).sampling(), is(true));
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
        assertThat(samplers.startSampling(shardId, target).sampling(), is(true));
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
        assertThat(samplers.startSampling(shardId, targets.get(0)).sampling(), is(true));
        assertThat(samplers.startSampling(shardId, targets.get(1)).sampling(), is(true));
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
        assertThat(samplers.startSampling(shardId, targets.get(0)).sampling(), is(true));
        assertThat(samplers.startSampling(shardId, targets.get(1)).sampling(), is(true));
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
        assertThat(samplers.startSampling(shardId, target).sampling(), is(true));
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
        assertThat(samplers.startSampling(shardId, target).sampling(), is(true));

        advanceTime(shorter.millis() - 1);
        assertThat(samplers.samplerCount(), equalTo(1));
        advanceTime(1);

        assertNothingIsLeftBehind();
    }

    public void testBudgetIsBackToZeroAfterEveryWayOfEndingASample() {
        state.set(clusterStateWithInitializingSearchReplicas(4));
        final List<String> targets = initializingAllocationIds(state.get());
        for (String target : targets) {
            assertThat(samplers.startSampling(shardId, target).sampling(), is(true));
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

    // ---- metrics

    private Map<String, Long> ended() {
        return counterBy(meterRegistry, PhantomQueryMetrics.SAMPLERS_ENDED_COUNTER, PhantomQueryMetrics.SAMPLER_END_ATTRIBUTE);
    }

    public void testStartSamplingSaysThatItStartedAndThenThatItWasAlreadyStarted() {
        final String target = initializingAllocationId(state.get());

        assertThat(samplers.startSampling(shardId, target), equalTo(SamplingStartOutcome.STARTED));
        assertThat(samplers.startSampling(shardId, target), equalTo(SamplingStartOutcome.ALREADY_STARTED));
        assertThat(samplers.startSampling(shardId, target), equalTo(SamplingStartOutcome.ALREADY_STARTED));

        assertThat(samplers.samplerCount(), equalTo(1));
    }

    public void testStartSamplingSaysThatTheSettingIsDisabled() {
        setEnabled(false);

        assertThat(samplers.startSampling(shardId, initializingAllocationId(state.get())), equalTo(SamplingStartOutcome.DECLINED_DISABLED));

        assertNothingIsLeftBehind();
    }

    public void testStartSamplingTellsACopyThatTheClusterStateDoesNotHaveFromOneThatItDoes() {
        final ClusterState initializing = state.get();
        final String target = initializingAllocationId(initializing);
        final String started = shards(initializing).stream()
            .filter(ShardRouting::started)
            .filter(routing -> routing.primary() == false)
            .findFirst()
            .orElseThrow()
            .allocationId()
            .getId();
        final String primary = shards(initializing).stream().filter(ShardRouting::primary).findFirst().orElseThrow().allocationId().getId();

        // what a peer that has not applied the cluster state that adds the copy yet looks like
        assertThat(samplers.startSampling(shardId, randomUUID()), equalTo(SamplingStartOutcome.DECLINED_TARGET_UNKNOWN));
        assertThat(
            samplers.startSampling(new ShardId(shardId.getIndex(), 7), target),
            equalTo(SamplingStartOutcome.DECLINED_TARGET_UNKNOWN)
        );
        assertThat(
            samplers.startSampling(new ShardId(randomAlphaOfLength(8), randomAlphaOfLength(8), 0), target),
            equalTo(SamplingStartOutcome.DECLINED_TARGET_UNKNOWN)
        );
        applyClusterState(withoutCopy(initializing, target));
        assertThat(samplers.startSampling(shardId, target), equalTo(SamplingStartOutcome.DECLINED_TARGET_UNKNOWN));
        applyClusterState(withoutIndex());
        assertThat(samplers.startSampling(shardId, target), equalTo(SamplingStartOutcome.DECLINED_TARGET_UNKNOWN));

        // a copy that it has, and that cannot be sampled for
        applyClusterState(initializing);
        assertThat(samplers.startSampling(shardId, started), equalTo(SamplingStartOutcome.DECLINED_TARGET_NOT_INITIALIZING));
        assertThat(samplers.startSampling(shardId, primary), equalTo(SamplingStartOutcome.DECLINED_TARGET_NOT_INITIALIZING));
        applyClusterState(withCopyStarted(initializing, target));
        assertThat(samplers.startSampling(shardId, target), equalTo(SamplingStartOutcome.DECLINED_TARGET_NOT_INITIALIZING));

        assertNothingIsLeftBehind();
    }

    public void testStartSamplingForTheSourceOfARelocationIsNotInitializing() {
        state.set(clusterStateWithRelocatingSearchReplica());
        final ShardRouting source = shards(state.get()).stream().filter(ShardRouting::relocating).findFirst().orElseThrow();

        assertThat(
            samplers.startSampling(shardId, source.allocationId().getId()),
            equalTo(SamplingStartOutcome.DECLINED_TARGET_NOT_INITIALIZING)
        );
        assertThat(samplers.startSampling(shardId, initializingAllocationId(state.get())), equalTo(SamplingStartOutcome.STARTED));
    }

    public void testStartSamplingSaysWhyACopyThatStoppedInitializingWhileItWasRegisteringIsDeclined() {
        final ClusterState initializing = state.get();
        final String target = initializingAllocationId(initializing);
        final ClusterState started = withCopyStarted(initializing, target);
        final var reads = new AtomicInteger();
        stateSupplier = () -> reads.incrementAndGet() == 1 ? initializing : started;

        assertThat(samplers.startSampling(shardId, target), equalTo(SamplingStartOutcome.DECLINED_TARGET_NOT_INITIALIZING));

        assertThat("it was registered and then ended", ended(), equalTo(Map.of("TARGET_GONE", 1L)));
        assertNothingIsLeftBehind();

        final ClusterState removed = withoutCopy(initializing, target);
        reads.set(0);
        stateSupplier = () -> reads.incrementAndGet() == 1 ? initializing : removed;

        assertThat(samplers.startSampling(shardId, target), equalTo(SamplingStartOutcome.DECLINED_TARGET_UNKNOWN));
        assertThat(ended(), equalTo(Map.of("TARGET_GONE", 2L)));
    }

    public void testCollectedSamplerEndsOnceAndItsLeaseDoesNotEndItAgain() {
        final String target = initializingAllocationId(state.get());
        assertThat(samplers.startSampling(shardId, target).sampling(), is(true));
        offerToAllSamplers(request(0));

        assertThat(samplers.collect(shardId, target), hasSize(1));
        applyClusterState(withCopyStarted(state.get(), target));
        samplers.afterIndexShardClosed(shardId, null, Settings.EMPTY);
        advanceTime(LEASE.millis());

        assertThat(ended(), equalTo(Map.of("COLLECTED", 1L)));
    }

    public void testSamplersThatEndBecauseTheCopyIsNoLongerInitializingAreCountedAsTargetGone() {
        state.set(clusterStateWithInitializingSearchReplicas(4));
        final List<String> targets = initializingAllocationIds(state.get());
        for (String target : targets) {
            assertThat(samplers.startSampling(shardId, target).sampling(), is(true));
        }

        applyClusterState(withCopyStarted(state.get(), targets.get(0)));
        assertThat("it started without collecting", ended(), equalTo(Map.of("TARGET_GONE", 1L)));
        applyClusterState(withoutCopy(state.get(), targets.get(1)));
        assertThat("it was removed", ended(), equalTo(Map.of("TARGET_GONE", 2L)));
        applyClusterState(withShards(state.get(), shards(state.get())));
        assertThat("a routing change that does not concern the others ends nothing", ended(), equalTo(Map.of("TARGET_GONE", 2L)));
        applyClusterState(withoutIndex());
        assertThat("the index was removed, and with it the other two", ended(), equalTo(Map.of("TARGET_GONE", 4L)));
        advanceTime(LEASE.millis());

        assertThat("the leases of ended samplers do not end them again", ended(), equalTo(Map.of("TARGET_GONE", 4L)));
        assertNothingIsLeftBehind();
    }

    public void testSamplerOfARelocationTargetThatIsCancelledIsCountedAsTargetGone() {
        state.set(clusterStateWithRelocatingSearchReplica());
        assertThat(samplers.startSampling(shardId, initializingAllocationId(state.get())).sampling(), is(true));

        applyClusterState(
            withShards(state.get(), shards(state.get()).stream().map(r -> r.relocating() ? r.cancelRelocation() : r).toList())
        );

        assertThat(ended(), equalTo(Map.of("TARGET_GONE", 1L)));
    }

    public void testEverySamplerOfAClosingShardIsCountedAsShardClosed() {
        state.set(clusterStateWithInitializingSearchReplicas(2));
        for (String target : initializingAllocationIds(state.get())) {
            assertThat(samplers.startSampling(shardId, target).sampling(), is(true));
        }

        samplers.afterIndexShardClosed(new ShardId(shardId.getIndex(), 1), null, Settings.EMPTY);
        assertThat("other shards closing does not matter", ended(), equalTo(Map.of()));
        samplers.afterIndexShardClosed(shardId, null, Settings.EMPTY);
        samplers.afterIndexShardClosed(shardId, null, Settings.EMPTY);

        assertThat(ended(), equalTo(Map.of("SHARD_CLOSED", 2L)));
    }

    public void testSamplerWhoseLeaseExpiresIsCountedAsLeaseExpired() {
        final String target = initializingAllocationId(state.get());
        assertThat(samplers.startSampling(shardId, target).sampling(), is(true));

        advanceTime(LEASE.millis() - 1);
        assertThat("the lease has not expired yet", ended(), equalTo(Map.of()));
        advanceTime(1);
        assertThat(ended(), equalTo(Map.of("LEASE_EXPIRED", 1L)));

        assertThat("a collect that finds nothing ends nothing", samplers.collect(shardId, target), empty());
        assertThat(ended(), equalTo(Map.of("LEASE_EXPIRED", 1L)));
    }

    public void testCollectRecordsTheNumberOfRequestsAndTheKilobytesRoundedUp() {
        final String target = initializingAllocationId(state.get());
        assertThat(samplers.startSampling(shardId, target).sampling(), is(true));
        final int requests = randomIntBetween(1, 10);
        long bytes = 0;
        for (int i = 0; i < requests; i++) {
            offerToAllSamplers(request(i));
            bytes += sizeOf(request(i));
        }

        assertThat(samplers.collect(shardId, target), hasSize(requests));

        assertThat(longHistogram(meterRegistry, PhantomQueryMetrics.SAMPLE_QUERIES_HISTOGRAM), equalTo(List.of((long) requests)));
        assertThat(
            longHistogram(meterRegistry, PhantomQueryMetrics.SAMPLE_SIZE_HISTOGRAM),
            equalTo(List.of(PhantomQueryMetrics.toKilobytesRoundedUp(bytes)))
        );
        assertThat(PhantomQueryMetrics.toKilobytesRoundedUp(bytes), greaterThan(0L));
    }

    public void testCollectRecordsAnEmptySampleToo() {
        final String target = initializingAllocationId(state.get());
        assertThat(samplers.startSampling(shardId, target).sampling(), is(true));

        assertThat(samplers.collect(shardId, target), empty());

        assertThat(longHistogram(meterRegistry, PhantomQueryMetrics.SAMPLE_QUERIES_HISTOGRAM), equalTo(List.of(0L)));
        assertThat(longHistogram(meterRegistry, PhantomQueryMetrics.SAMPLE_SIZE_HISTOGRAM), equalTo(List.of(0L)));
        assertThat(ended(), equalTo(Map.of("COLLECTED", 1L)));
    }

    public void testCollectWithoutASamplerRecordsNothing() {
        final String target = initializingAllocationId(state.get());
        assertThat(samplers.startSampling(shardId, target).sampling(), is(true));
        applyClusterState(withCopyStarted(state.get(), target));

        assertThat(samplers.collect(shardId, target), empty());
        assertThat(samplers.collect(shardId, randomUUID()), empty());
        assertThat(samplers.collect(new ShardId(randomAlphaOfLength(8), randomAlphaOfLength(8), 0), target), empty());

        assertThat(longHistogram(meterRegistry, PhantomQueryMetrics.SAMPLE_QUERIES_HISTOGRAM), empty());
        assertThat(longHistogram(meterRegistry, PhantomQueryMetrics.SAMPLE_SIZE_HISTOGRAM), empty());
        assertThat("only the cluster state ended it", ended(), equalTo(Map.of("TARGET_GONE", 1L)));
    }

    public void testGaugesFollowTheSamplersAndTheBytesTheyHold() {
        state.set(clusterStateWithInitializingSearchReplicas(2));
        final List<String> targets = initializingAllocationIds(state.get());
        assertThat("a node that samples for nobody reports zero", gauge(meterRegistry, PhantomQueryMetrics.SAMPLERS_GAUGE), equalTo(0L));
        assertThat(gauge(meterRegistry, PhantomQueryMetrics.SAMPLE_BYTES_GAUGE), equalTo(0L));

        assertThat(samplers.startSampling(shardId, targets.get(0)).sampling(), is(true));
        assertThat(samplers.startSampling(shardId, targets.get(1)).sampling(), is(true));
        offerToAllSamplers(request(0));
        assertThat(gauge(meterRegistry, PhantomQueryMetrics.SAMPLERS_GAUGE), equalTo(2L));
        assertThat(gauge(meterRegistry, PhantomQueryMetrics.SAMPLE_BYTES_GAUGE), equalTo(2 * sizeOf(request(0))));

        samplers.collect(shardId, targets.get(0));
        assertThat(gauge(meterRegistry, PhantomQueryMetrics.SAMPLERS_GAUGE), equalTo(1L));
        assertThat(gauge(meterRegistry, PhantomQueryMetrics.SAMPLE_BYTES_GAUGE), equalTo(sizeOf(request(0))));

        applyClusterState(withCopyStarted(state.get(), targets.get(1)));
        assertThat(gauge(meterRegistry, PhantomQueryMetrics.SAMPLERS_GAUGE), equalTo(0L));
        assertThat(gauge(meterRegistry, PhantomQueryMetrics.SAMPLE_BYTES_GAUGE), equalTo(0L));
        assertThat(
            "the gauges read what the samplers hold now",
            gauge(meterRegistry, PhantomQueryMetrics.SAMPLERS_GAUGE),
            equalTo((long) samplers.samplerCount())
        );
    }

    public void testCurrentSamplersAreAnImmutableSnapshot() {
        state.set(clusterStateWithInitializingSearchReplicas(2));
        final List<String> targets = initializingAllocationIds(state.get());
        assertThat(samplers.startSampling(shardId, targets.get(0)).sampling(), is(true));

        final var seen = new ArrayList<PhantomQuerySampler>();
        samplers.forEachSampler(shardId, sampler -> {
            seen.add(sampler);
            // a sampler registered while the observer iterates must neither fail nor be visited by this iteration
            assertThat(samplers.startSampling(shardId, targets.get(1)).sampling(), is(true));
        });

        assertThat(seen, hasSize(1));
        assertThat(samplers.samplerCount(), equalTo(2));
    }
}
