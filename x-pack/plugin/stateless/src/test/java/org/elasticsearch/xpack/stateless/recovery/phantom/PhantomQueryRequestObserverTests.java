/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.recovery.phantom;

import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.MatchNoneQueryBuilder;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.telemetry.InstrumentType;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.RecordingMeterRegistry;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.stateless.cache.SearchRecoveryTimeoutCalculationService;
import org.junit.Before;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.xpack.stateless.recovery.phantom.PhantomQueryTestUtils.clusterStateWithInitializingSearchReplica;
import static org.elasticsearch.xpack.stateless.recovery.phantom.PhantomQueryTestUtils.clusterStateWithInitializingSearchReplicas;
import static org.elasticsearch.xpack.stateless.recovery.phantom.PhantomQueryTestUtils.decode;
import static org.elasticsearch.xpack.stateless.recovery.phantom.PhantomQueryTestUtils.initializingAllocationId;
import static org.elasticsearch.xpack.stateless.recovery.phantom.PhantomQueryTestUtils.initializingAllocationIds;
import static org.elasticsearch.xpack.stateless.recovery.phantom.PhantomQueryTestUtils.queryRequest;
import static org.elasticsearch.xpack.stateless.recovery.phantom.PhantomQueryTestUtils.scrollRequest;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

public class PhantomQueryRequestObserverTests extends ESTestCase {

    private static final TimeValue LEASE = TimeValue.timeValueMinutes(5);

    private final ShardId shardId = PhantomQueryTestUtils.shardId();
    private ClusterSettings clusterSettings;
    private ClusterState state;
    private RecordingMeterRegistry meterRegistry;
    private PhantomQuerySamplers samplers;
    private PhantomQueryRequestObserver observer;
    private IndexShard indexShard;

    @Before
    public void createObserver() {
        state = clusterStateWithInitializingSearchReplica();
        meterRegistry = new RecordingMeterRegistry();
        final Set<Setting<?>> registeredSettings = new HashSet<>(PhantomQueriesSettings.settings());
        registeredSettings.add(SearchRecoveryTimeoutCalculationService.OFFLINE_WARMING_TOTAL_TIMEOUT_CAP_SETTING);
        clusterSettings = new ClusterSettings(settings(true), registeredSettings);
        final ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        when(clusterService.state()).thenAnswer(invocation -> state);
        final var settings = new PhantomQueriesSettings(clusterSettings);
        samplers = new PhantomQuerySamplers(
            new DeterministicTaskQueue().getThreadPool(),
            clusterService,
            settings,
            new PhantomQueryMetrics(meterRegistry)
        );
        observer = new PhantomQueryRequestObserver(samplers, settings);
        indexShard = indexShard(shardId);
    }

    private static Settings settings(boolean enabled) {
        return Settings.builder()
            .put(PhantomQueriesSettings.ENABLED_SETTING.getKey(), enabled)
            .put(SearchRecoveryTimeoutCalculationService.OFFLINE_WARMING_TOTAL_TIMEOUT_CAP_SETTING.getKey(), LEASE.getStringRep())
            .build();
    }

    private static IndexShard indexShard(ShardId shardId) {
        final IndexShard indexShard = mock(IndexShard.class);
        when(indexShard.shardId()).thenReturn(shardId);
        return indexShard;
    }

    private ShardSearchRequest request(int i) {
        return queryRequest(shardId, String.format(Locale.ROOT, "value-%03d", i));
    }

    private long sampledCount() {
        return meterRegistry.getRecorder()
            .getMeasurements(InstrumentType.LONG_COUNTER, PhantomQueryMetrics.SAMPLED_COUNTER)
            .stream()
            .mapToLong(Measurement::getLong)
            .sum();
    }

    private ShardSearchRequest requestThatFailsToSerialize() throws IOException {
        final ShardSearchRequest request = mock(ShardSearchRequest.class);
        when(request.searchType()).thenReturn(SearchType.QUERY_THEN_FETCH);
        when(request.waitForCheckpoint()).thenReturn(SequenceNumbers.UNASSIGNED_SEQ_NO);
        when(request.shardId()).thenReturn(shardId);
        doThrow(new IOException("cannot serialize")).when(request).writeTo(any(StreamOutput.class));
        return request;
    }

    public void testRequestsAreNotLookedAtWhileNoCopyRecovers() {
        final ShardSearchRequest request = mock(ShardSearchRequest.class);

        observer.onQueryPhaseRequest(indexShard, request);

        verifyNoInteractions(request);
        assertThat(samplers.usedInBytes(), equalTo(0L));
        assertThat(sampledCount(), equalTo(0L));
    }

    public void testRequestsAreNotLookedAtWhileOnlyOtherShardsAreSampled() {
        assertThat(samplers.startSampling(shardId, initializingAllocationId(state)), is(true));
        final ShardSearchRequest request = mock(ShardSearchRequest.class);

        observer.onQueryPhaseRequest(indexShard(new ShardId(shardId.getIndex(), 1)), request);
        observer.onQueryPhaseRequest(indexShard(new ShardId(randomAlphaOfLength(8), randomAlphaOfLength(8), 0)), request);

        verifyNoInteractions(request);
        assertThat(samplers.usedInBytes(), equalTo(0L));
    }

    public void testRequestsAreRetainedForTheRecoveringCopy() throws IOException {
        final String target = initializingAllocationId(state);
        assertThat(samplers.startSampling(shardId, target), is(true));
        final int requests = randomIntBetween(1, 10);

        for (int i = 0; i < requests; i++) {
            observer.onQueryPhaseRequest(indexShard, request(i));
        }

        assertThat(sampledCount(), equalTo((long) requests));
        final var sample = samplers.collect(shardId, target);
        assertThat(sample, hasSize(requests));
        for (int i = 0; i < requests; i++) {
            assertThat(decode(sample.get(i)).source().query(), equalTo(request(i).source().query()));
        }
        assertThat(samplers.usedInBytes(), equalTo(0L));
    }

    public void testRequestsAreRetainedAsTheyArrivedWhateverHappensToThemAfterwards() throws IOException {
        final String target = initializingAllocationId(state);
        assertThat(samplers.startSampling(shardId, target), is(true));
        final ShardSearchRequest request = request(0);
        final var original = request.source().query();

        observer.onQueryPhaseRequest(indexShard, request);
        // the data node rewrites the request in place once the observer has returned
        request.source(new SearchSourceBuilder().query(new MatchNoneQueryBuilder()));

        final var sample = samplers.collect(shardId, target);
        assertThat(sample, hasSize(1));
        assertThat(decode(sample.get(0)).source().query(), equalTo(original));
    }

    public void testEachRecoveringCopyGetsItsOwnSample() throws IOException {
        state = clusterStateWithInitializingSearchReplicas(2);
        final List<String> targets = initializingAllocationIds(state);
        assertThat(samplers.startSampling(shardId, targets.get(0)), is(true));
        assertThat(samplers.startSampling(shardId, targets.get(1)), is(true));

        final ShardSearchRequest request = request(0);
        final long size = new ObservedQueryRequest(request).encoded().sizeInBytes();
        observer.onQueryPhaseRequest(indexShard, request);

        assertThat(sampledCount(), equalTo(2L));
        assertThat(samplers.usedInBytes(), equalTo(2 * size));
        assertThat(samplers.collect(shardId, targets.get(0)), hasSize(1));
        assertThat(samplers.collect(shardId, targets.get(1)), hasSize(1));
    }

    public void testNothingIsRetainedWhileDisabled() {
        final String target = initializingAllocationId(state);
        assertThat(samplers.startSampling(shardId, target), is(true));

        clusterSettings.applySettings(settings(false));
        observer.onQueryPhaseRequest(indexShard, request(0));

        assertThat("a sampler that exists retains nothing while disabled", samplers.usedInBytes(), equalTo(0L));
        assertThat(sampledCount(), equalTo(0L));

        clusterSettings.applySettings(settings(true));
        observer.onQueryPhaseRequest(indexShard, request(1));

        assertThat(sampledCount(), equalTo(1L));
        assertThat(samplers.collect(shardId, target), hasSize(1));
    }

    public void testNothingIsRetainedWhenNoCopyAskedForASample() {
        clusterSettings.applySettings(settings(false));
        assertThat(samplers.startSampling(shardId, initializingAllocationId(state)), is(false));

        observer.onQueryPhaseRequest(indexShard, request(0));

        assertThat(samplers.usedInBytes(), equalTo(0L));
        assertThat(sampledCount(), equalTo(0L));
    }

    public void testIneligibleRequestsAreNotRetained() {
        final String target = initializingAllocationId(state);
        assertThat(samplers.startSampling(shardId, target), is(true));

        observer.onQueryPhaseRequest(indexShard, scrollRequest(shardId, "scroll"));

        assertThat(samplers.usedInBytes(), equalTo(0L));
        assertThat(samplers.collect(shardId, target), empty());
    }

    public void testSamplingStopsWhenTheSampleIsFull() {
        clusterSettings.applySettings(
            Settings.builder().put(settings(true)).put(PhantomQueriesSettings.MAX_QUERIES_PER_SAMPLE_SETTING.getKey(), 2).build()
        );
        final String target = initializingAllocationId(state);
        assertThat(samplers.startSampling(shardId, target), is(true));

        for (int i = 0; i < 10; i++) {
            observer.onQueryPhaseRequest(indexShard, request(i));
        }

        assertThat(sampledCount(), equalTo(2L));
        assertThat(samplers.collect(shardId, target), hasSize(2));
    }

    public void testRequestsThatFailToSerializeAreSkippedWithoutFailing() throws IOException {
        final String target = initializingAllocationId(state);
        assertThat(samplers.startSampling(shardId, target), is(true));

        observer.onQueryPhaseRequest(indexShard, requestThatFailsToSerialize());
        observer.onQueryPhaseRequest(indexShard, request(0));

        assertThat("the sampler is still open", samplers.collect(shardId, target), hasSize(1));
        assertThat(sampledCount(), equalTo(1L));
    }

    public void testTheObserverNeverThrows() {
        assertThat(samplers.startSampling(shardId, initializingAllocationId(state)), is(true));
        final ShardSearchRequest request = mock(ShardSearchRequest.class);
        when(request.searchType()).thenThrow(new IllegalStateException("unexpected"));

        observer.onQueryPhaseRequest(indexShard, request);

        assertThat(samplers.usedInBytes(), equalTo(0L));
        assertThat(samplers.samplerCount(), equalTo(1));
    }

    public void testConcurrentRequestsFillTheSampleExactly() {
        final int limit = randomIntBetween(1, 20);
        clusterSettings.applySettings(
            Settings.builder().put(settings(true)).put(PhantomQueriesSettings.MAX_QUERIES_PER_SAMPLE_SETTING.getKey(), limit).build()
        );
        final String target = initializingAllocationId(state);
        assertThat(samplers.startSampling(shardId, target), is(true));
        final int threads = randomIntBetween(2, 8);
        final AtomicInteger requestNumber = new AtomicInteger();

        runInParallel(threads, thread -> {
            for (int i = 0; i < 50; i++) {
                observer.onQueryPhaseRequest(indexShard, request(requestNumber.getAndIncrement() % 1000));
            }
        });

        assertThat(sampledCount(), equalTo((long) limit));
        assertThat(samplers.collect(shardId, target), hasSize(limit));
        assertThat(samplers.usedInBytes(), equalTo(0L));
    }
}
