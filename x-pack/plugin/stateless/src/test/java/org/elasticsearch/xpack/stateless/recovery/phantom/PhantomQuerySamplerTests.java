/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.recovery.phantom;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.index.query.MatchNoneQueryBuilder;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.telemetry.InstrumentType;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.RecordingMeterRegistry;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;

import static org.elasticsearch.xpack.stateless.recovery.phantom.PhantomQueryTestUtils.decode;
import static org.elasticsearch.xpack.stateless.recovery.phantom.PhantomQueryTestUtils.dfsRequest;
import static org.elasticsearch.xpack.stateless.recovery.phantom.PhantomQueryTestUtils.queryRequest;
import static org.elasticsearch.xpack.stateless.recovery.phantom.PhantomQueryTestUtils.readerRequest;
import static org.elasticsearch.xpack.stateless.recovery.phantom.PhantomQueryTestUtils.scrollRequest;
import static org.elasticsearch.xpack.stateless.recovery.phantom.PhantomQueryTestUtils.waitForCheckpointRequest;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class PhantomQuerySamplerTests extends ESTestCase {

    private final ShardId shardId = PhantomQueryTestUtils.shardId();
    private RecordingMeterRegistry meterRegistry;
    private AtomicLong nodeLimitInBytes;
    private PhantomQuerySampleBudget budget;
    private PhantomQueryMetrics metrics;

    @Before
    public void createMetricsAndBudget() {
        meterRegistry = new RecordingMeterRegistry();
        metrics = new PhantomQueryMetrics(meterRegistry);
        nodeLimitInBytes = new AtomicLong(Long.MAX_VALUE);
        budget = new PhantomQuerySampleBudget(nodeLimitInBytes::get);
    }

    private PhantomQuerySampler newSampler(int maxQueries, long maxBytes) {
        return new PhantomQuerySampler(maxQueries, maxBytes, budget, metrics);
    }

    /** Requests that only differ in a fixed-width value, so they all serialize to the same number of bytes. */
    private ShardSearchRequest request(int i) {
        return queryRequest(shardId, String.format(Locale.ROOT, "value-%03d", i));
    }

    private static boolean offer(PhantomQuerySampler sampler, ShardSearchRequest request) {
        return sampler.offer(new ObservedQueryRequest(request));
    }

    private static long sizeOf(ShardSearchRequest request) {
        return new ObservedQueryRequest(request).encoded().sizeInBytes();
    }

    private long sampledCount() {
        return meterRegistry.getRecorder()
            .getMeasurements(InstrumentType.LONG_COUNTER, PhantomQueryMetrics.SAMPLED_COUNTER)
            .stream()
            .mapToLong(Measurement::getLong)
            .sum();
    }

    public void testKeepsTheFirstRequestsUpToTheCountLimit() throws IOException {
        final int limit = randomIntBetween(1, 10);
        final int total = limit + randomIntBetween(1, 5);
        final List<ShardSearchRequest> requests = IntStream.range(0, total).mapToObj(this::request).toList();
        final var sampler = newSampler(limit, Long.MAX_VALUE);

        for (int i = 0; i < total; i++) {
            assertThat("request " + i, offer(sampler, requests.get(i)), is(i < limit));
        }

        assertThat(sampler.accepting(), is(false));
        assertThat(sampler.retainedCount(), equalTo(limit));
        assertThat(sampledCount(), equalTo((long) limit));
        final var sample = sampler.collect();
        assertThat(sample, hasSize(limit));
        for (int i = 0; i < limit; i++) {
            assertThat("request " + i, decode(sample.get(i)).source().query(), equalTo(requests.get(i).source().query()));
        }
    }

    public void testByteLimitStopsSamplingForTheCopy() {
        final long size = sizeOf(request(0));
        final int fitting = randomIntBetween(1, 5);
        final long limitOneByteShortOfOneMore = (fitting + 1) * size - 1;
        final var sampler = newSampler(1000, limitOneByteShortOfOneMore);

        for (int i = 0; i < fitting; i++) {
            assertThat("request " + i, offer(sampler, request(i)), is(true));
        }
        assertThat("a smaller request could still fit", sampler.accepting(), is(true));

        assertThat("the next request would exceed the limit", offer(sampler, request(fitting)), is(false));
        assertThat(sampler.accepting(), is(false));

        final var small = queryRequest(shardId, "x");
        assertThat("the small request would fit, but sampling has stopped", sizeOf(small) <= size - 1, is(true));
        assertThat(offer(sampler, small), is(false));

        assertThat(sampler.retainedCount(), equalTo(fitting));
        assertThat(sampler.retainedBytes(), equalTo(fitting * size));
        assertThat(budget.usedInBytes(), equalTo(fitting * size));
        assertThat(sampledCount(), equalTo((long) fitting));
    }

    public void testRequestExactlyAtTheByteLimitIsRetained() {
        final long size = sizeOf(request(0));
        final var sampler = newSampler(1000, 2 * size);

        assertThat(offer(sampler, request(0)), is(true));
        assertThat(offer(sampler, request(1)), is(true));
        assertThat(offer(sampler, request(2)), is(false));
        assertThat(sampler.retainedBytes(), equalTo(2 * size));
    }

    public void testZeroQueriesPerSampleRetainsNothing() {
        final var sampler = newSampler(0, Long.MAX_VALUE);

        assertThat(sampler.accepting(), is(false));
        assertThat(offer(sampler, request(0)), is(false));
        assertThat(sampler.collect(), empty());
        assertThat(budget.usedInBytes(), equalTo(0L));
        assertThat(sampledCount(), equalTo(0L));
    }

    public void testZeroBytesPerSampleRetainsNothing() {
        final var sampler = newSampler(10, 0);

        assertThat(sampler.accepting(), is(false));
        assertThat(offer(sampler, request(0)), is(false));
        assertThat(sampler.collect(), empty());
        assertThat(budget.usedInBytes(), equalTo(0L));
        assertThat(sampledCount(), equalTo(0L));
    }

    public void testIneligibleRequestsAreSkippedAndDoNotStopSampling() {
        final var sampler = newSampler(10, Long.MAX_VALUE);
        final var ineligible = List.of(
            scrollRequest(shardId, "scroll"),
            readerRequest(shardId, "point-in-time"),
            waitForCheckpointRequest(shardId, "wait-for-checkpoint"),
            dfsRequest(shardId, "dfs")
        );

        for (ShardSearchRequest request : ineligible) {
            assertThat(PhantomQuerySampler.isEligible(request), is(false));
            assertThat(offer(sampler, request), is(false));
        }

        assertThat(sampler.accepting(), is(true));
        assertThat(sampler.retainedCount(), equalTo(0));
        assertThat(budget.usedInBytes(), equalTo(0L));
        assertThat(sampledCount(), equalTo(0L));
        assertThat("an eligible request after them is retained", offer(sampler, request(0)), is(true));
    }

    public void testPlainRequestsAreEligible() {
        assertThat(PhantomQuerySampler.isEligible(request(0)), is(true));
    }

    public void testIneligibleRequestsAreNotSerialized() throws IOException {
        final var sampler = newSampler(10, Long.MAX_VALUE);
        final var scroll = spy(scrollRequest(shardId, "scroll"));

        assertThat(offer(sampler, scroll), is(false));

        verify(scroll, never()).writeTo(any(StreamOutput.class));
    }

    public void testNodeBudgetFullDropsRequestsWithoutClosingTheSampler() {
        final long size = sizeOf(request(0));
        nodeLimitInBytes.set(2 * size + size / 2);
        final var first = newSampler(100, Long.MAX_VALUE);
        final var second = newSampler(100, Long.MAX_VALUE);

        assertThat(offer(first, request(0)), is(true));
        assertThat(offer(first, request(1)), is(true));
        assertThat(budget.usedInBytes(), equalTo(2 * size));

        assertThat("the node budget is full", offer(second, request(2)), is(false));
        assertThat("the sampler stays open because budget may be freed", second.accepting(), is(true));
        assertThat("what was retained is kept", first.retainedCount(), equalTo(2));
        assertThat(budget.usedInBytes(), equalTo(2 * size));
        assertThat(sampledCount(), equalTo(2L));

        assertThat(first.collect(), hasSize(2));
        assertThat(budget.usedInBytes(), equalTo(0L));

        assertThat("budget is available again", offer(second, request(3)), is(true));
        assertThat(budget.usedInBytes(), equalTo(size));
        assertThat(sampledCount(), equalTo(3L));
    }

    public void testNodeBudgetIsFreedWhenASampleIsDiscarded() {
        final long size = sizeOf(request(0));
        nodeLimitInBytes.set(size);
        final var first = newSampler(100, Long.MAX_VALUE);
        final var second = newSampler(100, Long.MAX_VALUE);

        assertThat(offer(first, request(0)), is(true));
        assertThat(offer(second, request(1)), is(false));

        first.discard();

        assertThat(budget.usedInBytes(), equalTo(0L));
        assertThat(offer(second, request(1)), is(true));
        assertThat(budget.usedInBytes(), equalTo(size));
    }

    public void testLoweringTheNodeLimitDoesNotEvictRetainedRequests() {
        final long size = sizeOf(request(0));
        final var sampler = newSampler(100, Long.MAX_VALUE);
        assertThat(offer(sampler, request(0)), is(true));

        nodeLimitInBytes.set(0);

        assertThat(offer(sampler, request(1)), is(false));
        assertThat(sampler.retainedCount(), equalTo(1));
        assertThat(budget.usedInBytes(), equalTo(size));
        assertThat(sampler.collect(), hasSize(1));
        assertThat(budget.usedInBytes(), equalTo(0L));
    }

    public void testSharedBytesAreCountedByEachSampler() {
        final long size = sizeOf(request(0));
        final var first = newSampler(100, Long.MAX_VALUE);
        final var second = newSampler(100, Long.MAX_VALUE);
        final var tooSmall = newSampler(100, size - 1);
        final var observed = new ObservedQueryRequest(request(0));

        assertThat(first.offer(observed), is(true));
        assertThat(second.offer(observed), is(true));
        assertThat("its own byte limit applies", tooSmall.offer(observed), is(false));

        assertThat(first.retainedBytes(), equalTo(size));
        assertThat(second.retainedBytes(), equalTo(size));
        assertThat(tooSmall.retainedBytes(), equalTo(0L));
        assertThat("each sampler counts the bytes against the node", budget.usedInBytes(), equalTo(2 * size));
        assertThat("each retained request is counted", sampledCount(), equalTo(2L));

        final var firstSample = first.collect();
        assertThat(budget.usedInBytes(), equalTo(size));
        final var secondSample = second.collect();
        assertThat(budget.usedInBytes(), equalTo(0L));
        assertThat("the bytes are shared, not copied", firstSample.get(0), sameInstance(secondSample.get(0)));
    }

    public void testRequestIsSerializedOnceForAllSamplers() throws IOException {
        final var request = spy(request(0));
        final var observed = new ObservedQueryRequest(request);

        for (int i = 0; i < randomIntBetween(2, 5); i++) {
            assertThat(newSampler(100, Long.MAX_VALUE).offer(observed), is(true));
        }

        verify(request, times(1)).writeTo(any(StreamOutput.class));
    }

    public void testRetainedCopyIsNotChangedByALaterRewrite() throws IOException {
        final var request = request(0);
        final var original = request.source().query();
        final var sampler = newSampler(100, Long.MAX_VALUE);
        assertThat(offer(sampler, request), is(true));

        // the data node rewrites the request in place once the observer has returned
        request.source(new SearchSourceBuilder().query(new MatchNoneQueryBuilder()));

        final var retained = decode(sampler.collect().get(0));
        assertThat(retained.source().query(), equalTo(original));
        assertThat(retained.shardId(), equalTo(shardId));
    }

    public void testRetainedCopyStoresTheTransportVersionItWasEncodedWith() {
        final var sampler = newSampler(100, Long.MAX_VALUE);
        assertThat(offer(sampler, request(0)), is(true));

        assertThat(sampler.collect().get(0).version(), equalTo(TransportVersion.current()));
    }

    // request headers carry the credentials of the requester, and are in the thread context, not in the serialization of the request
    public void testRetainedCopyIsExactlyTheSerializedRequest() throws IOException {
        final var request = request(0);
        final var sampler = newSampler(100, Long.MAX_VALUE);

        assertThat(offer(sampler, request), is(true));

        final var retained = sampler.collect().get(0);
        try (var out = new BytesStreamOutput()) {
            out.setTransportVersion(retained.version());
            request.writeTo(out);
            assertThat(retained.bytes(), equalTo(out.bytes()));
        }
    }

    public void testRetainedCopyDoesNotContainHeadersOfTheThreadContext() {
        final var threadContext = new ThreadContext(Settings.EMPTY);
        threadContext.putHeader("Authorization", "ApiKey secret-credentials");
        threadContext.putHeader("X-Opaque-Id", "secret-opaque-id");
        final var sampler = newSampler(100, Long.MAX_VALUE);

        // the observer runs on the thread that received the request, whose thread context holds the headers
        assertThat(offer(sampler, request(0)), is(true));
        assertThat(threadContext.getHeader("Authorization"), equalTo("ApiKey secret-credentials"));

        // the serialization is binary: decoding as Latin-1 maps every byte to a char, so that the text can be searched for
        final var retained = new String(BytesReference.toBytes(sampler.collect().get(0).bytes()), StandardCharsets.ISO_8859_1);
        assertThat(retained, not(containsString("secret-credentials")));
        assertThat(retained, not(containsString("secret-opaque-id")));
        assertThat("the search is able to find text in the serialization", retained, containsString("value-000"));
    }

    public void testRetainedBytesAreExactlySized() {
        final var request = request(0);
        final var sampler = newSampler(100, Long.MAX_VALUE);
        assertThat(offer(sampler, request), is(true));

        final BytesReference bytes = sampler.collect().get(0).bytes();
        assertThat(bytes.length(), equalTo(bytes.toBytesRef().bytes.length));
    }

    public void testCollectEndsSamplingAndIsRepeatable() {
        final var sampler = newSampler(100, Long.MAX_VALUE);
        assertThat(offer(sampler, request(0)), is(true));
        assertThat(offer(sampler, request(1)), is(true));

        assertThat(sampler.collect(), hasSize(2));

        assertThat(sampler.accepting(), is(false));
        assertThat(budget.usedInBytes(), equalTo(0L));
        assertThat("nothing is retained after collection", offer(sampler, request(2)), is(false));
        assertThat(sampler.collect(), empty());
        assertThat(sampler.retainedCount(), equalTo(0));
        assertThat(budget.usedInBytes(), equalTo(0L));
    }

    public void testDiscardEndsSamplingAndFreesTheBudget() {
        final var sampler = newSampler(100, Long.MAX_VALUE);
        assertThat(offer(sampler, request(0)), is(true));
        assertThat(budget.usedInBytes(), is(not(0L)));

        sampler.discard();
        sampler.discard();

        assertThat(sampler.accepting(), is(false));
        assertThat(budget.usedInBytes(), equalTo(0L));
        assertThat(offer(sampler, request(1)), is(false));
        assertThat(sampler.collect(), empty());
    }

    public void testConcurrentOffersRetainExactlyTheCountLimit() {
        final int limit = randomIntBetween(1, 20);
        final int threads = randomIntBetween(2, 8);
        final int requestsPerThread = 50;
        final var sampler = newSampler(limit, Long.MAX_VALUE);
        final var retained = new AtomicInteger();

        runInParallel(threads, thread -> {
            for (int i = 0; i < requestsPerThread; i++) {
                if (offer(sampler, request(thread * requestsPerThread + i))) {
                    retained.incrementAndGet();
                }
            }
        });

        assertThat(retained.get(), equalTo(limit));
        assertThat(sampler.retainedCount(), equalTo(limit));
        assertThat(budget.usedInBytes(), equalTo(sampler.retainedBytes()));
        assertThat(sampledCount(), equalTo((long) limit));
        assertThat(sampler.collect(), hasSize(limit));
        assertThat(budget.usedInBytes(), equalTo(0L));
    }

    public void testConcurrentOffersNeverExceedTheByteLimit() {
        final long size = sizeOf(request(0));
        final int fitting = randomIntBetween(1, 10);
        final var sampler = newSampler(1000, fitting * size);

        runInParallel(randomIntBetween(2, 8), thread -> {
            for (int i = 0; i < 50; i++) {
                offer(sampler, request(thread * 50 + i));
            }
        });

        assertThat(sampler.retainedCount(), equalTo(fitting));
        assertThat(sampler.retainedBytes(), equalTo(fitting * size));
        assertThat(budget.usedInBytes(), equalTo(fitting * size));
    }

    public void testOffersRacingWithCollectionNeverLeakBudget() {
        final var sampler = newSampler(1000, Long.MAX_VALUE);
        final var collected = new AtomicInteger();
        final var offered = new AtomicInteger();
        final var offeringDone = new AtomicBoolean();
        // the random numbers have to be drawn on the test thread
        final int offersBeforeCollection = randomIntBetween(1, 20);

        runInParallel(() -> {
            try {
                for (int i = 0; i < 200; i++) {
                    if (offer(sampler, request(i))) {
                        offered.incrementAndGet();
                    }
                }
            } finally {
                // never leave the collecting thread waiting, whatever happened here
                offeringDone.set(true);
            }
        }, () -> {
            while (offered.get() < offersBeforeCollection && offeringDone.get() == false) {
                Thread.onSpinWait();
            }
            collected.set(sampler.collect().size());
        });

        assertThat("whatever was retained before the collection is in the sample", collected.get(), equalTo(offered.get()));
        assertThat("nothing is left in the budget", budget.usedInBytes(), equalTo(0L));
        assertThat(sampler.retainedCount(), equalTo(0));
    }
}
