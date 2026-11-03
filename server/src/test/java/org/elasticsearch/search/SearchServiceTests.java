/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.Automata;
import org.apache.lucene.util.automaton.TooComplexToDeterminizeException;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.search.OnlinePrewarmingService;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.search.SearchShardTask;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.SplitShardCountSummary;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.component.Lifecycle;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.cache.bitset.BitsetFilterCache;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.fielddata.FieldDataContext;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexFieldDataCache;
import org.elasticsearch.index.fielddata.LeafFieldData;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperBuilderContext;
import org.elasticsearch.index.mapper.MapperMetrics;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.Mapping;
import org.elasticsearch.index.mapper.MappingLookup;
import org.elasticsearch.index.mapper.MetadataFieldMapper;
import org.elasticsearch.index.mapper.RootObjectMapper;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.DataRewriteContext;
import org.elasticsearch.index.query.LeafQueryBuilder;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.MatchNoneQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.query.SearchExecutionContextHelper;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.index.query.WildcardQueryBuilder;
import org.elasticsearch.index.search.stats.SearchStats;
import org.elasticsearch.index.search.stats.ShardSearchStats;
import org.elasticsearch.index.shard.IllegalIndexShardStateException;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.shard.IndexShardTestCase;
import org.elasticsearch.index.shard.SearchOperationListener;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.DirectoryMetrics;
import org.elasticsearch.indices.EmptySystemIndices;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.TermsLookup;
import org.elasticsearch.indices.breaker.CircuitBreakerMetrics;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.script.ScriptCompiler;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.FetchPhase;
import org.elasticsearch.search.fetch.FetchSubPhase;
import org.elasticsearch.search.fetch.FetchSubPhaseProcessor;
import org.elasticsearch.search.fetch.ShardFetchRequest;
import org.elasticsearch.search.fetch.StoredFieldsSpec;
import org.elasticsearch.search.fetch.chunk.FetchPhaseResponseChunk;
import org.elasticsearch.search.fetch.subphase.FetchSourcePhase;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.search.internal.InternalScrollSearchRequest;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.search.query.QueryPhase;
import org.elasticsearch.search.sort.BucketedSort;
import org.elasticsearch.search.sort.MinAndMax;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.TaskCancelHelper;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.telemetry.tracing.Tracer;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.threadpool.DefaultBuiltInExecutorBuilders;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.threadpool.ThreadPoolStats;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParserConfiguration;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.function.Predicate;

import static org.elasticsearch.common.Strings.format;
import static org.elasticsearch.common.breaker.ChildMemoryCircuitBreaker.CATEGORY_FETCH;
import static org.elasticsearch.common.util.concurrent.EsExecutors.DIRECT_EXECUTOR_SERVICE;
import static org.elasticsearch.search.SearchService.isExecutorQueuedBeyondPrewarmingFactor;
import static org.elasticsearch.search.SearchService.isTransientRejection;
import static org.elasticsearch.search.SearchService.wrapFailureListener;
import static org.elasticsearch.search.SearchService.wrapListenerForErrorHandling;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.startsWith;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

public class SearchServiceTests extends IndexShardTestCase {

    public void testCanMatchMatchAll() throws IOException {
        SearchRequest searchRequest = new SearchRequest().allowPartialSearchResults(false)
            .source(new SearchSourceBuilder().query(new MatchAllQueryBuilder()));
        doTestCanMatch(searchRequest, null, true, null, false);
    }

    public void testCanMatchMatchNone() throws IOException {
        SearchRequest searchRequest = new SearchRequest().allowPartialSearchResults(false)
            .source(new SearchSourceBuilder().query(new MatchNoneQueryBuilder()));
        doTestCanMatch(searchRequest, null, false, null, false);
    }

    public void testCanMatchMatchNoneWithException() throws IOException {
        SearchRequest searchRequest = new SearchRequest().allowPartialSearchResults(false)
            .source(new SearchSourceBuilder().query(new MatchNoneQueryBuilder()));
        doTestCanMatch(searchRequest, null, true, null, true);
    }

    public void testCanMatchKeywordSortedQueryMatchNone() throws IOException {
        SearchRequest searchRequest = new SearchRequest().allowPartialSearchResults(false)
            .source(new SearchSourceBuilder().sort("field").query(new MatchNoneQueryBuilder()));
        SortField sortField = new SortField("field", SortField.Type.STRING);
        doTestCanMatch(searchRequest, sortField, false, null, false);
    }

    public void testCanMatchKeywordSortedQueryMatchAll() throws IOException {
        SearchRequest searchRequest = new SearchRequest().allowPartialSearchResults(false)
            .source(new SearchSourceBuilder().sort("field").query(new MatchAllQueryBuilder()));
        SortField sortField = new SortField("field", SortField.Type.STRING);
        MinAndMax<BytesRef> expectedMinAndMax = new MinAndMax<>(new BytesRef("value"), new BytesRef("value"));
        doTestCanMatch(searchRequest, sortField, true, expectedMinAndMax, false);
    }

    public void testCanMatchTimeRangeFilter() throws IOException {
        SearchRequest searchRequest = new SearchRequest().allowPartialSearchResults(false)
            .source(new SearchSourceBuilder().query(new RangeQueryBuilder("@timestamp").from("2025-12-15")));
        SearchService.CanMatchContext canMatchContext = doTestCanMatch(searchRequest, null, false, null, false);
        LocalDateTime localDateTime = LocalDateTime.of(2025, 12, 15, 0, 0);
        assertEquals(
            localDateTime.atZone(ZoneOffset.UTC).toInstant().toEpochMilli(),
            canMatchContext.getTimeRangeFilterFromMillis().longValue()
        );
    }

    // Verify that a range filter nested inside a bool `should` clause does not contribute to
    // the time_range_filter_from metric — only mandatory (must/filter) clauses should be tracked.
    public void testCanMatchTimeRangeFilterInsideShouldClauseIsNotTracked() throws IOException {
        dotestNotTrackedTimeRangeFilter(BoolQueryBuilder::should);
    }

    // Verify that a range filter nested inside a bool `mustNot` clause does not contribute to
    // the time_range_filter_from metric — only mandatory (must/filter) clauses should be tracked.
    public void testCanMatchTimeRangeFilterInsideMustNotClauseIsNotTracked() throws IOException {
        dotestNotTrackedTimeRangeFilter(BoolQueryBuilder::mustNot);
    }

    private void dotestNotTrackedTimeRangeFilter(BiFunction<BoolQueryBuilder, QueryBuilder, BoolQueryBuilder> queryWrapper)
        throws IOException {
        // outer filter: @timestamp >= 2025-12-01 (should match the indexed doc from 2025-12-09)
        // inner should: @timestamp >= 2020-01-01 (old date in an optional clause — must not be tracked)
        SearchRequest searchRequest = new SearchRequest().allowPartialSearchResults(false)
            .source(
                new SearchSourceBuilder().query(
                    queryWrapper.apply(
                        new BoolQueryBuilder().filter(new RangeQueryBuilder("@timestamp").from("2025-12-01")),
                        new BoolQueryBuilder().filter(new RangeQueryBuilder("@timestamp").from("2020-01-01"))
                    )
                )
            );
        SearchService.CanMatchContext canMatchContext = doTestCanMatch(searchRequest, null, true, null, false);
        // Only the outer filter range should be reported; Math.min must not drag it back to 2020-01-01
        LocalDateTime expectedFrom = LocalDateTime.of(2025, 12, 1, 0, 0);
        assertEquals(
            expectedFrom.atZone(ZoneOffset.UTC).toInstant().toEpochMilli(),
            canMatchContext.getTimeRangeFilterFromMillis().longValue()
        );
    }

    public void testCanMatchKeywordSortedQueryMatchNoneWithException() throws IOException {
        SearchRequest searchRequest = new SearchRequest().allowPartialSearchResults(false)
            .source(new SearchSourceBuilder().sort("field").query(new MatchNoneQueryBuilder()));
        // provide a sort field that throws exception
        SortField sortField = new SortField("field", SortField.Type.STRING) {
            @Override
            public Type getType() {
                throw new UnsupportedOperationException();
            }
        };
        doTestCanMatch(searchRequest, sortField, false, null, false);
    }

    public void testCanMatchKeywordSortedQueryMatchAllWithException() throws IOException {
        SearchRequest searchRequest = new SearchRequest().allowPartialSearchResults(false)
            .source(new SearchSourceBuilder().sort("field").query(new MatchAllQueryBuilder()));
        // provide a sort field that throws exception
        SortField sortField = new SortField("field", SortField.Type.STRING) {
            @Override
            public Type getType() {
                throw new UnsupportedOperationException();
            }
        };
        doTestCanMatch(searchRequest, sortField, true, null, false);
    }

    public void testWrapListenerForErrorHandling() {
        ShardId shardId = new ShardId("index", "index", 0);
        // Tests that the same listener has stack trace if is not wrapped or does not have stack trace if it is wrapped.
        AtomicBoolean isWrapped = new AtomicBoolean(false);
        ActionListener<SearchPhaseResult> listener = new ActionListener<>() {
            @Override
            public void onResponse(SearchPhaseResult searchPhaseResult) {
                // noop - we only care about failure scenarios
            }

            @Override
            public void onFailure(Exception e) {
                if (isWrapped.get()) {
                    assertThat(e.getStackTrace().length, is(0));
                } else {
                    assertThat(e.getStackTrace().length, is(not(0)));
                }
            }
        };
        Exception e = new Exception();
        e.fillInStackTrace();
        assertThat(e.getStackTrace().length, is(not(0)));
        listener.onFailure(e);
        listener = wrapListenerForErrorHandling(listener, TransportVersion.current(), "node", shardId, 123L, threadPool, randomLifecycle());
        isWrapped.set(true);
        listener.onFailure(e);
    }

    private static Lifecycle randomLifecycle() {
        return randomBoolean() ? randomInitializedOrStartedLifecycle() : randomStoppedOrClosedLifecycle();
    }

    private static Lifecycle randomInitializedOrStartedLifecycle() {
        Lifecycle lifecycle = new Lifecycle();
        if (randomBoolean()) {
            lifecycle.started();
        }
        return lifecycle;
    }

    private static Lifecycle randomStoppedOrClosedLifecycle() {
        Lifecycle lifecycle = new Lifecycle();
        lifecycle.started();
        lifecycle.stopped();
        if (randomBoolean()) {
            lifecycle.closed();
        }
        return lifecycle;
    }

    public void testBadRequestIfPatternTooComplex() {
        TooComplexToDeterminizeException tooComplex = new TooComplexToDeterminizeException(Automata.makeEmpty(), 10);

        for (Exception e : List.of(tooComplex, new ElasticsearchException("wrapped", tooComplex))) {
            Exception converted = SearchService.badRequestIfPatternTooComplex(e);
            assertThat(converted, instanceOf(IllegalArgumentException.class));
            assertThat(converted.getMessage(), equalTo("Pattern was too complex to determinize"));
            assertThat(converted.getCause(), sameInstance(e));
            assertThat(ExceptionsHelper.status(converted), equalTo(RestStatus.BAD_REQUEST));
        }

        Exception alreadyClientError = new ElasticsearchStatusException("bad", RestStatus.BAD_REQUEST, tooComplex);
        assertThat(SearchService.badRequestIfPatternTooComplex(alreadyClientError), sameInstance(alreadyClientError));
        Exception unrelated = new ElasticsearchException("boom", new IllegalStateException());
        assertThat(SearchService.badRequestIfPatternTooComplex(unrelated), sameInstance(unrelated));
    }

    public void testWrapListenerForErrorHandlingTooComplexPattern() {
        final String nodeId = "node";
        final ShardId shardId = new ShardId("index", "index", 0);
        final long taskId = 123L;
        final String logMessage = format("[%s]%s: failed to execute search request for task [%d]", nodeId, shardId, taskId);
        final TooComplexToDeterminizeException tooComplex = new TooComplexToDeterminizeException(Automata.makeEmpty(), 10);

        for (Exception failure : List.of(tooComplex, new ElasticsearchException("wrapped", tooComplex))) {
            try (var mockLog = MockLog.capture(SearchService.class)) {
                Configurator.setLevel(SearchService.class, Level.DEBUG);
                mockLog.addExpectation(
                    new MockLog.ExceptionSeenEventExpectation(
                        "converted failure logged at debug",
                        SearchService.class.getCanonicalName(),
                        Level.DEBUG,
                        logMessage,
                        IllegalArgumentException.class,
                        "Pattern was too complex to determinize"
                    )
                );
                mockLog.addExpectation(
                    new MockLog.UnseenEventExpectation("no warn log", SearchService.class.getCanonicalName(), Level.WARN, "*")
                );

                AtomicReference<Exception> received = new AtomicReference<>();
                ActionListener<SearchPhaseResult> listener = wrapListenerForErrorHandling(
                    ActionListener.wrap(r -> fail("expected a failure"), received::set),
                    TransportVersion.current(),
                    nodeId,
                    shardId,
                    taskId,
                    threadPool,
                    randomInitializedOrStartedLifecycle()
                );
                listener.onFailure(failure);

                assertThat(received.get(), instanceOf(IllegalArgumentException.class));
                assertThat(received.get().getCause(), sameInstance(failure));
                mockLog.assertAllExpectationsMatched();
            }
        }
    }

    public void testWrapListenerForErrorHandlingDebugLog() {
        final String nodeId = "node";
        final String index = "index";
        ShardId shardId = new ShardId(index, index, 0);
        final long taskId = 123L;

        try (var mockLog = MockLog.capture(SearchService.class)) {
            Configurator.setLevel(SearchService.class, Level.DEBUG);
            final String exceptionMessage = "test exception message";
            mockLog.addExpectation(
                new MockLog.ExceptionSeenEventExpectation(
                    format("\"[%s]%s: failed to execute search request for task [%d]\" and an exception logged", nodeId, shardId, taskId),
                    SearchService.class.getCanonicalName(),
                    Level.DEBUG, // We will throw a 400-level exception, so it should only be logged at the debug level
                    format("[%s]%s: failed to execute search request for task [%d]", nodeId, shardId, taskId),
                    IllegalArgumentException.class,
                    exceptionMessage
                )
            );

            // Tests the listener has logged if it is wrapped
            ActionListener<SearchPhaseResult> listener = new ActionListener<>() {
                @Override
                public void onResponse(SearchPhaseResult searchPhaseResult) {
                    // noop - we only care about failure scenarios
                }

                @Override
                public void onFailure(Exception e) {
                    mockLog.assertAllExpectationsMatched();
                }
            };
            // Default behavior is to use debug level for 400-level exceptions
            IllegalArgumentException iae = new IllegalArgumentException(exceptionMessage); // 400-level exception
            listener = wrapListenerForErrorHandling(
                listener,
                TransportVersion.current(),
                nodeId,
                shardId,
                taskId,
                threadPool,
                randomLifecycle()
            );
            listener.onFailure(iae);

            // Debug logging for a 500-level exception when closing or stopped lifecycle is to log as debug level
            IllegalStateException ise = new IllegalStateException(exceptionMessage);
            listener = wrapListenerForErrorHandling(
                listener,
                TransportVersion.current(),
                nodeId,
                shardId,
                taskId,
                threadPool,
                randomStoppedOrClosedLifecycle()
            );
            listener.onFailure(ise);
        }
    }

    public void testWrapListenerForErrorHandlingWarnLog() {
        final String nodeId = "node";
        final String index = "index";
        ShardId shardId = new ShardId(index, index, 0);
        final long taskId = 123L;

        try (var mockLog = MockLog.capture(SearchService.class)) {
            final String exceptionMessage = "test exception message";
            mockLog.addExpectation(
                new MockLog.ExceptionSeenEventExpectation(
                    format("\"[%s]%s: failed to execute search request for task [%d]\" and an exception logged", nodeId, shardId, taskId),
                    SearchService.class.getCanonicalName(),
                    Level.WARN, // We will throw a 500-level exception, so it should be logged at the warn level
                    format("[%s]%s: failed to execute search request for task [%d]", nodeId, shardId, taskId),
                    IllegalStateException.class,
                    exceptionMessage
                )
            );

            // Tests the listener has logged if it is wrapped
            ActionListener<SearchPhaseResult> listener = new ActionListener<>() {
                @Override
                public void onResponse(SearchPhaseResult searchPhaseResult) {
                    // noop - we only care about failure scenarios
                }

                @Override
                public void onFailure(Exception e) {
                    mockLog.assertAllExpectationsMatched();
                }
            };
            IllegalStateException e = new IllegalStateException(exceptionMessage); // 500-level exception
            listener = wrapListenerForErrorHandling(
                listener,
                TransportVersion.current(),
                nodeId,
                shardId,
                taskId,
                threadPool,
                randomInitializedOrStartedLifecycle()
            );
            listener.onFailure(e);
        }
    }

    public void testWrapFailureListenerOnResponse() {
        var releasableClosed = new AtomicBoolean(false);
        var response = new AtomicReference<String>();

        var wrapped = wrapFailureListener(ActionListener.<String>wrap((r) -> {
            assertTrue("releasable must be closed before listener.onResponse runs", releasableClosed.get());
            response.set(r);
        }, e -> fail("unexpected failure")), () -> releasableClosed.set(true), e -> fail("cleanup must not run on success"));
        wrapped.onResponse("ok");

        assertTrue("releasable must be closed after onResponse", releasableClosed.get());
        assertEquals("ok", response.get());
    }

    public void testWrapFailureListenerOnFailure() {
        var releasableClosed = new AtomicBoolean(false);
        var cleanupRan = new AtomicBoolean(false);
        var failure = new AtomicReference<Exception>();
        var cause = new RuntimeException("search failed");

        var wrapped = wrapFailureListener(
            ActionListener.<String>wrap(r -> fail("unexpected response"), failure::set),
            () -> releasableClosed.set(true),
            e -> cleanupRan.set(true)
        );
        wrapped.onFailure(cause);

        assertTrue("cleanup must run on failure", cleanupRan.get());
        assertTrue("releasable must be closed after onFailure", releasableClosed.get());
        assertSame("original exception must reach the listener", cause, failure.get());
    }

    public void testWrapFailureListenerCleanupThrows() {
        var releasableClosed = new AtomicBoolean(false);
        var failure = new AtomicReference<Exception>();
        var cause = new RuntimeException("search failed");

        var wrapped = wrapFailureListener(
            ActionListener.<String>wrap(r -> fail("unexpected response"), failure::set),
            () -> releasableClosed.set(true),
            e -> {
                throw new RuntimeException("cleanup exploded");
            }
        );

        expectThrows(RuntimeException.class, () -> wrapped.onFailure(cause));
        assertTrue("releasable must be closed even when cleanup throws", releasableClosed.get());
        assertSame("listener.onFailure must be called even when cleanup throws", cause, failure.get());
    }

    public void testIsTransientRejection() {
        assertTrue(isTransientRejection(new EsRejectedExecutionException("rejected", false)));
        assertFalse(isTransientRejection(new EsRejectedExecutionException("shutdown", true)));
        assertFalse(isTransientRejection(new RuntimeException("other")));

        Exception wrapped = new RuntimeException(new EsRejectedExecutionException("rejected", false));
        assertTrue(isTransientRejection(wrapped));

        // suppressed-only rejection must not retain (cause-chain unwrap only)
        RuntimeException primary = new RuntimeException("primary");
        primary.addSuppressed(new EsRejectedExecutionException("rejected", false));
        assertFalse(isTransientRejection(primary));
    }

    public void testIsExecutorQueuedBeyondPrewarmingFactor() throws InterruptedException {
        {
            final String threadPoolName = randomFrom(
                ThreadPool.THREAD_POOL_TYPES.entrySet()
                    .stream()
                    .filter(t -> t.getValue().equals(ThreadPool.ThreadPoolType.FIXED))
                    .map(Map.Entry::getKey)
                    .sorted()
                    .toList()
            );
            final int size = 6;
            final int queueSize = size * 100;

            ThreadPool threadPool = null;
            final Settings nodeSettings = Settings.builder()
                .put("node.name", "testPrewarmingBasedOnQueuedItems")
                .put("thread_pool." + threadPoolName + ".size", size)
                .put("thread_pool." + threadPoolName + ".queue_size", queueSize)
                .build();
            final CountDownLatch blockThreadPoolToQueueItems = new CountDownLatch(1);

            try {
                threadPool = new ThreadPool(nodeSettings, MeterRegistry.NOOP, new DefaultBuiltInExecutorBuilders());
                ExecutorService executor = threadPool.executor(threadPoolName);

                // these tasks will consume the thread pool causing further
                // submissions to queue
                final CountDownLatch occupyAllThreads = new CountDownLatch(size);
                for (int i = 0; i < size; i++) {
                    executor.execute(() -> {
                        try {
                            occupyAllThreads.countDown();
                            blockThreadPoolToQueueItems.await();
                        } catch (InterruptedException e) {
                            fail(e.toString());
                        }
                    });
                }

                // wait for all threads to have an active task in their hands
                occupyAllThreads.await();

                // now on to the fun stuff, let's queue up items - 2 queued items
                // for every thread in the pool (plus one more for one thread)
                for (int i = 0; i < 13; i++) {
                    executor.execute(() -> {});
                }

                // 13 queued up items
                assertThat(isExecutorQueuedBeyondPrewarmingFactor(executor, 10), is(false));
                assertThat(isExecutorQueuedBeyondPrewarmingFactor(executor, 2), is(true));

                // let's get us at the 10 factor (6 threads * 10 + 1= 61 queued up items - at which point we should indicate
                // prewarming should not happen)
                for (int i = 0; i < 48; i++) {
                    executor.execute(() -> {});
                }

                // 61 queued up items
                assertThat(isExecutorQueuedBeyondPrewarmingFactor(executor, 10), is(true));
                blockThreadPoolToQueueItems.countDown();
            } catch (AssertionError e) {
                // terminate more gracefully if there's an assertion error above
                blockThreadPoolToQueueItems.countDown();
                throw e;
            } finally {
                ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
            }
        }

        {
            // executors that are not ThreadPoolExecutor (i.e. no stats available) are always
            // allowing prewarming
            assertThat(isExecutorQueuedBeyondPrewarmingFactor(DIRECT_EXECUTOR_SERVICE, 2), is(false));
        }
    }

    public void testBuildQueryWithCircuitBreakerAccountsMemory() throws IOException {
        IndexShard indexShard = newShard(true);
        try {
            recoverShardFromStore(indexShard);
            try (Engine.Searcher searcher = indexShard.acquireExternalSearcher("test", SplitShardCountSummary.IRRELEVANT)) {
                CircuitBreaker cb = createQueryConstructionBreaker("100mb");
                SearchExecutionContext context = new SearchExecutionContext(
                    createSearchExecutionContext((mappedFieldType, fieldDataContext) -> null, searcher),
                    cb
                );

                try {
                    long cbBefore = cb.getUsed();
                    assertEquals("Freshly created breaker must start at zero", 0L, cbBefore);

                    WildcardQueryBuilder queryBuilder = new WildcardQueryBuilder("field", "*test*pattern*");
                    assertNotNull(queryBuilder.toQuery(context));

                    long cbAfter = cb.getUsed();
                    long tracked = context.getQueryConstructionMemoryUsed();

                    assertTrue("Circuit breaker should have accounted memory", cbAfter > cbBefore);
                    assertTrue("Context should have tracked memory", tracked > 0);
                    assertEquals("CB delta must match tracked memory", cbAfter - cbBefore, tracked);
                } finally {
                    context.releaseQueryConstructionMemory();
                }
            }
        } finally {
            closeShards(indexShard);
        }
    }

    public void testBuildQueryWithCircuitBreakerTripsOnLimitExceeded() throws IOException {
        IndexShard indexShard = newShard(true);
        try {
            recoverShardFromStore(indexShard);
            try (Engine.Searcher searcher = indexShard.acquireExternalSearcher("test", SplitShardCountSummary.IRRELEVANT)) {
                CircuitBreaker cb = createQueryConstructionBreaker("1kb");
                SearchExecutionContext context = new SearchExecutionContext(
                    createSearchExecutionContext((mappedFieldType, fieldDataContext) -> null, searcher),
                    cb
                );

                try {
                    BoolQueryBuilder boolQuery = new BoolQueryBuilder();
                    for (int i = 0; i < 50; i++) {
                        boolQuery.should(new WildcardQueryBuilder("field", "*a*b*c*" + i + "*"));
                    }

                    expectThrows(CircuitBreakingException.class, () -> boolQuery.toQuery(context));
                } finally {
                    context.releaseQueryConstructionMemory();
                }
            }
        } finally {
            closeShards(indexShard);
        }
    }

    public void testQueryConstructionMemoryReleasedOnContextClose() throws IOException {
        IndexShard indexShard = newShard(true);
        try {
            recoverShardFromStore(indexShard);
            try (Engine.Searcher searcher = indexShard.acquireExternalSearcher("test", SplitShardCountSummary.IRRELEVANT)) {
                CircuitBreaker cb = createQueryConstructionBreaker("100mb");
                SearchExecutionContext context = new SearchExecutionContext(
                    createSearchExecutionContext((mappedFieldType, fieldDataContext) -> null, searcher),
                    cb
                );

                for (int i = 0; i < 3; i++) {
                    new WildcardQueryBuilder("field", "*test" + i + "*").toQuery(context);
                }

                long cbAfterBuild = cb.getUsed();
                long tracked = context.getQueryConstructionMemoryUsed();
                assertTrue("Memory should be accounted before release", cbAfterBuild > 0);
                assertEquals("Tracked memory should match CB", cbAfterBuild, tracked);

                context.releaseQueryConstructionMemory();

                assertEquals("CB must be fully released", 0L, cb.getUsed());
                assertEquals("Tracked memory must be zeroed", 0L, context.getQueryConstructionMemoryUsed());
            }
        } finally {
            closeShards(indexShard);
        }
    }

    private CircuitBreaker createQueryConstructionBreaker(String limit) {
        Settings settings = Settings.builder()
            .put(HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING.getKey(), limit)
            .put(HierarchyCircuitBreakerService.USE_REAL_MEMORY_USAGE_SETTING.getKey(), false)
            .build();
        ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        return new HierarchyCircuitBreakerService(CircuitBreakerMetrics.NOOP, settings, Collections.emptyList(), clusterSettings)
            .getBreaker(CircuitBreaker.REQUEST);
    }

    private SearchService.CanMatchContext doTestCanMatch(
        SearchRequest searchRequest,
        SortField sortField,
        boolean expectedCanMatch,
        MinAndMax<?> expectedMinAndMax,
        boolean throwException
    ) throws IOException {
        ShardSearchRequest shardRequest = new ShardSearchRequest(
            OriginalIndices.NONE,
            searchRequest,
            new ShardId("index", "index", 0),
            0,
            5,
            AliasFilter.EMPTY,
            1.0f,
            0,
            null
        );
        IndexFieldData<LeafFieldData> indexFieldData = indexFieldData(sortField);
        IndexShard indexShard = newShard(true);
        try {
            recoverShardFromStore(indexShard);
            assertTrue(indexDoc(indexShard, "_doc", "id", "{\"field\":\"value\", \"@timestamp\":\"2025-12-09\"}").isCreated());
            assertTrue(indexShard.refresh("test").refreshed());
            try (Engine.Searcher searcher = indexShard.acquireExternalSearcher("test", SplitShardCountSummary.IRRELEVANT)) {
                SearchExecutionContext searchExecutionContext = createSearchExecutionContext(
                    (mappedFieldType, fieldDataContext) -> indexFieldData,
                    searcher
                );
                SearchService.CanMatchContext canMatchContext = createCanMatchContext(
                    shardRequest,
                    indexShard,
                    searchExecutionContext,
                    parserConfig(),
                    throwException
                );
                CanMatchShardResponse canMatchShardResponse = SearchService.canMatch(canMatchContext, false);
                assertEquals(expectedCanMatch, canMatchShardResponse.canMatch());
                if (expectedMinAndMax == null) {
                    assertNull(canMatchShardResponse.estimatedMinAndMax());
                } else {
                    MinAndMax<?> minAndMax = canMatchShardResponse.estimatedMinAndMax();
                    assertNotNull(minAndMax);
                    assertEquals(expectedMinAndMax.getMin(), minAndMax.getMin());
                    assertEquals(expectedMinAndMax.getMin(), minAndMax.getMax());
                }
                return canMatchContext;
            }
        } finally {
            closeShards(indexShard);
        }
    }

    private SearchExecutionContext createSearchExecutionContext(
        BiFunction<MappedFieldType, FieldDataContext, IndexFieldData<?>> indexFieldDataLookup,
        IndexSearcher searcher
    ) {
        IndexMetadata indexMetadata = IndexMetadata.builder("index")
            .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current()))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
        IndexSettings indexSettings = new IndexSettings(indexMetadata, Settings.EMPTY);
        Predicate<String> indexNameMatcher = pattern -> Regex.simpleMatch(pattern, "index");

        MapperBuilderContext root = MapperBuilderContext.root(false, false);
        RootObjectMapper.Builder builder = new RootObjectMapper.Builder("_doc");
        Mapping mapping = new Mapping(
            builder.build(MapperBuilderContext.root(false, false)),
            new MetadataFieldMapper[0],
            Collections.emptyMap()
        );
        KeywordFieldMapper keywordFieldMapper = new KeywordFieldMapper.Builder("field", indexSettings).build(root);
        DateFieldMapper dateFieldMapper = new DateFieldMapper.Builder(
            "@timestamp",
            DateFieldMapper.Resolution.MILLISECONDS,
            null,
            ScriptCompiler.NONE,
            indexSettings
        ).build(root);
        MappingLookup mappingLookup = MappingLookup.fromMappers(
            mapping,
            List.of(keywordFieldMapper, dateFieldMapper),
            Collections.emptyList(),
            IndexMode.STANDARD
        );
        MapperService mapperService = mock(MapperService.class);
        when(mapperService.getIdFieldDataEnabled()).thenReturn(() -> false);
        return new SearchExecutionContext(
            0,
            0,
            indexSettings,
            null,
            indexFieldDataLookup,
            mapperService,
            mappingLookup,
            null,
            null,
            parserConfig(),
            writableRegistry(),
            null,
            searcher,
            System::currentTimeMillis,
            null,
            indexNameMatcher,
            () -> true,
            null,
            Collections.emptyMap(),
            null,
            MapperMetrics.NOOP,
            SearchExecutionContextHelper.SHARD_SEARCH_STATS
        );
    }

    private static IndexFieldData<LeafFieldData> indexFieldData(SortField sortField) {
        return new IndexFieldData<>() {
            @Override
            public String getFieldName() {
                return "field";
            }

            @Override
            public ValuesSourceType getValuesSourceType() {
                throw new UnsupportedOperationException();
            }

            @Override
            public LeafFieldData load(LeafReaderContext context) {
                throw new UnsupportedOperationException();
            }

            @Override
            public LeafFieldData loadDirect(LeafReaderContext context) {
                throw new UnsupportedOperationException();
            }

            @Override
            public SortField sortField(
                Object missingValue,
                MultiValueMode sortMode,
                XFieldComparatorSource.Nested nested,
                boolean reverse
            ) {
                return sortField;
            }

            @Override
            public BucketedSort newBucketedSort(
                BigArrays bigArrays,
                Object missingValue,
                MultiValueMode sortMode,
                XFieldComparatorSource.Nested nested,
                SortOrder sortOrder,
                DocValueFormat format,
                int bucketSize,
                BucketedSort.ExtraData extra
            ) {
                throw new UnsupportedOperationException();
            }
        };
    }

    private static SearchService.CanMatchContext createCanMatchContext(
        ShardSearchRequest shardRequest,
        IndexShard indexShard,
        SearchExecutionContext searchExecutionContext,
        XContentParserConfiguration parserConfig,
        boolean throwException
    ) {
        return new SearchService.CanMatchContext(shardRequest, null, null, -1, -1) {
            @Override
            IndexShard getShard() {
                return indexShard;
            }

            @Override
            QueryRewriteContext getQueryRewriteContext(IndexService indexService) {
                if (throwException) {
                    throw new IllegalArgumentException();
                }
                return new QueryRewriteContext(parserConfig, null, System::currentTimeMillis);
            }

            @Override
            SearchExecutionContext getSearchExecutionContext(Engine.Searcher searcher) {
                return searchExecutionContext;
            }

            @Override
            IndexService getIndexService() {
                // it's ok to return null because the three above methods are overridden
                return null;
            }
        };
    }

    public void testAsyncKeepAliveSettingsValidator() {
        ClusterSettings cs = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);

        // max == -1 means unbounded; any default is allowed
        cs.validate(asyncKeepAliveSettings("30d", "-1"), true);
        // default == max is allowed (inclusive)
        cs.validate(asyncKeepAliveSettings("7d", "7d"), true);
        // default < max is allowed
        cs.validate(asyncKeepAliveSettings("1d", "7d"), true);

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> cs.validate(asyncKeepAliveSettings("8d", "7d"), true)
        );
        assertThat(e.getMessage(), containsString("async_search.default_keep_alive"));
        assertThat(e.getMessage(), containsString("async_search.max_keep_alive"));
    }

    public void testAsyncDefaultKeepAliveMinimum() {
        ClusterSettings cs = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        String key = SearchService.ASYNC_SEARCH_DEFAULT_KEEP_ALIVE_SETTING.getKey();

        expectThrows(IllegalArgumentException.class, () -> cs.validate(Settings.builder().put(key, "30s").build(), true));
        cs.validate(Settings.builder().put(key, "1m").build(), true);
    }

    private static Settings asyncKeepAliveSettings(String defaultKeepAlive, String maxKeepAlive) {
        return Settings.builder()
            .put(SearchService.ASYNC_SEARCH_DEFAULT_KEEP_ALIVE_SETTING.getKey(), defaultKeepAlive)
            .put(SearchService.ASYNC_SEARCH_MAX_KEEP_ALIVE_SETTING.getKey(), maxKeepAlive)
            .build();
    }

    private static final String OBSERVED = "observed";
    private static final String REWRITING = "rewriting";

    private static final class StopAtRewrite extends RuntimeException {
        StopAtRewrite() {
            super("stop at rewrite", null, false, false);
        }
    }

    private record ObservedRequest(IndexShard shard, ShardSearchRequest request) {}

    public void testQueryRequestObserverSeesRequestBeforeItIsRewritten() throws IOException {
        IndexShard indexShard = newShard(true);
        try (ClusterService clusterService = ClusterServiceUtils.createClusterService(threadPool)) {
            recoverShardFromStore(indexShard);
            List<String> events = new ArrayList<>();
            List<ObservedRequest> observed = new ArrayList<>();
            SearchService searchService = newSearchServiceStoppingAtRewrite(clusterService, indexShard, events, (shard, request) -> {
                events.add(OBSERVED);
                observed.add(new ObservedRequest(shard, request));
            });
            try {
                ShardSearchRequest request = newQueryPhaseRequest(indexShard.shardId());

                expectThrows(
                    StopAtRewrite.class,
                    () -> searchService.executeQueryPhase(request, newSearchShardTask(), ActionListener.noop())
                );

                assertThat(events, equalTo(List.of(OBSERVED, REWRITING)));
                assertThat(observed, hasSize(1));
                assertThat(observed.get(0).shard(), sameInstance(indexShard));
                assertThat(observed.get(0).request(), sameInstance(request));
            } finally {
                searchService.close();
            }
        } finally {
            closeShards(indexShard);
        }
    }

    public void testFailingQueryRequestObserverDoesNotFailTheSearch() throws IOException {
        IndexShard indexShard = newShard(true);
        try (ClusterService clusterService = ClusterServiceUtils.createClusterService(threadPool)) {
            recoverShardFromStore(indexShard);
            List<String> events = new ArrayList<>();
            SearchService searchService = newSearchServiceStoppingAtRewrite(clusterService, indexShard, events, (shard, request) -> {
                events.add(OBSERVED);
                throw new IllegalStateException("observer failure");
            });
            try {
                ShardSearchRequest request = newQueryPhaseRequest(indexShard.shardId());

                expectThrows(
                    StopAtRewrite.class,
                    () -> searchService.executeQueryPhase(request, newSearchShardTask(), ActionListener.noop())
                );

                assertThat(events, equalTo(List.of(OBSERVED, REWRITING)));
            } finally {
                searchService.close();
            }
        } finally {
            closeShards(indexShard);
        }
    }

    public void testQueryRequestObserverIsNotCalledForDfsScrollOrFetch() throws IOException {
        IndexShard indexShard = newShard(true);
        try (ClusterService clusterService = ClusterServiceUtils.createClusterService(threadPool)) {
            recoverShardFromStore(indexShard);
            List<String> events = new ArrayList<>();
            SearchService searchService = newSearchServiceStoppingAtRewrite(
                clusterService,
                indexShard,
                events,
                (shard, request) -> events.add(OBSERVED)
            );
            try {
                SearchShardTask task = newSearchShardTask();

                expectThrows(
                    StopAtRewrite.class,
                    () -> searchService.executeDfsPhase(newQueryPhaseRequest(indexShard.shardId()), task, ActionListener.noop())
                );
                assertThat("the DFS request reached the rewrite step", events, equalTo(List.of(REWRITING)));

                ShardSearchContextId unknownContextId = new ShardSearchContextId(UUIDs.randomBase64UUID(), randomNonNegativeLong());
                expectThrows(
                    SearchContextMissingException.class,
                    () -> searchService.executeQueryPhase(
                        new InternalScrollSearchRequest(new SearchScrollRequest(), unknownContextId),
                        task,
                        ActionListener.noop(),
                        TransportVersion.current()
                    )
                );
                FetchPhaseResponseChunk.Writer writerNeverUsed = null;
                expectThrows(
                    SearchContextMissingException.class,
                    () -> searchService.executeFetchPhase(
                        new ShardFetchRequest(unknownContextId, List.of(0), null),
                        task,
                        writerNeverUsed,
                        ActionListener.noop()
                    )
                );

                assertThat("nothing was observed", events, equalTo(List.of(REWRITING)));
            } finally {
                searchService.close();
            }
        } finally {
            closeShards(indexShard);
        }
    }

    private SearchService newSearchServiceStoppingAtRewrite(
        ClusterService clusterService,
        IndexShard shard,
        List<String> events,
        ShardQueryRequestObserver observer
    ) {
        // mocked because the real IndicesService and IndexService need a whole node
        IndexService indexService = mock(IndexService.class);
        when(indexService.getShard(anyInt())).thenReturn(shard);
        IndicesService indicesService = mock(IndicesService.class);
        when(indicesService.indexServiceSafe(any(Index.class))).thenReturn(indexService);
        when(indicesService.getDataRewriteContext(any())).thenAnswer(invocation -> {
            events.add(REWRITING);
            throw new StopAtRewrite();
        });
        return new SearchService(
            clusterService,
            indicesService,
            threadPool,
            null,
            null,
            new FetchPhase(Collections.emptyList()),
            new NoneCircuitBreakerService(),
            EmptySystemIndices.INSTANCE.getExecutorSelector(),
            Tracer.NOOP,
            OnlinePrewarmingService.NOOP,
            observer,
            new TaskManager(Settings.EMPTY, threadPool, Collections.emptySet())
        );
    }

    private static ShardSearchRequest newQueryPhaseRequest(ShardId shardId) {
        SearchRequest searchRequest = new SearchRequest().allowPartialSearchResults(false)
            .source(new SearchSourceBuilder().query(new MatchAllQueryBuilder()));
        return new ShardSearchRequest(OriginalIndices.NONE, searchRequest, shardId, 0, 5, AliasFilter.EMPTY, 1.0f, 0, null);
    }

    private static SearchShardTask newSearchShardTask() {
        return new SearchShardTask(123L, "", "", "", null, Collections.emptyMap());
    }

    public void testWarmingQueryPhaseHasNoSearchSideEffects() throws Exception {
        final SearchOperationListener searchOperationListener = mock(SearchOperationListener.class);
        final IndexShard shard = newStartedPrimaryWithThreeSegmentsOfTwoDocuments(List.of(searchOperationListener));
        withFixture(shard, minimumOneDocumentPerSliceSettings(), fixture -> {
            final AtomicInteger searches = new AtomicInteger();
            final ShardSearchRequest request = newRequestCachingShardRequest(
                shard.shardId(),
                new MatchAllCallingBackOnSearchQueryBuilder(searches::incrementAndGet)
            );
            final long searchPoolTasksBefore = completedTasks(ThreadPool.Names.SEARCH);
            final AtomicReference<String> executingThread = new AtomicReference<>();
            final Executor warmer = recordingThreadName(threadPool.executor(ThreadPool.Names.WARMER), executingThread);

            fixture.warm(request, fixture.newSearcherSupplier(), newSearchShardTask(), warmer);

            assertThat(searches.get(), equalTo(1));
            assertThat(executingThread.get(), containsString("[" + ThreadPool.Names.WARMER + "]"));
            assertThat(fixture.searchService().getActiveContexts(), equalTo(0));
            verifyNoInteractions(searchOperationListener);
            verify(fixture.indicesService(), never()).loadIntoContext(any(), any(), any());
            final SearchStats searchStats = shard.searchStats();
            assertThat(searchStats.getTotal().getQueryCount(), equalTo(0L));
            assertThat(searchStats.getTotal().getQueryCurrent(), equalTo(0L));
            assertThat(searchStats.getTotal().getFetchCount(), equalTo(0L));
            assertThat(searchStats.getTotal().getSearchLoadRate(), equalTo(0.0));
            assertThat(searchStats.getOpenContexts(), equalTo(0L));
            assertThat("the search pool ran nothing", completedTasks(ThreadPool.Names.SEARCH), equalTo(searchPoolTasksBefore));
        });
    }

    public void testWarmingQueryPhaseSkipsTheShardWhenBottomSortValuesLetItSkip() throws Exception {
        final IndexShard shard = newStartedPrimaryWithThreeSegmentsOfTwoDocuments(List.of());
        withFixture(shard, Settings.EMPTY, fixture -> {
            final AtomicInteger searches = new AtomicInteger();
            final ShardSearchRequest request = newShardSearchRequest(
                shard.shardId(),
                new SearchRequest().allowPartialSearchResults(false)
                    .source(
                        new SearchSourceBuilder().query(new MatchAllCallingBackOnSearchQueryBuilder(searches::incrementAndGet))
                            .sort("timestamp")
                            .trackTotalHits(false)
                    )
            );
            request.setBottomSortValues(bottomSortValuesBelowEveryTimestamp());

            fixture.warm(request, fixture.newSearcherSupplier(), newSearchShardTask(), DIRECT_EXECUTOR_SERVICE);

            assertThat(searches.get(), equalTo(0));
            assertThat(fixture.fetchedSources(), empty());
        });
    }

    public void testWarmingQueryPhaseFetchesTheTopHits() throws Exception {
        final IndexShard shard = newStartedPrimaryWithThreeSegmentsOfTwoDocuments(List.of());
        withFixture(shard, Settings.EMPTY, fixture -> {
            final SearchRequest searchRequest = requestCachingSearchRequest(new MatchAllQueryBuilder());
            searchRequest.source().from(1).size(2);

            fixture.warm(
                newShardSearchRequest(shard.shardId(), searchRequest),
                fixture.newSearcherSupplier(),
                newSearchShardTask(),
                DIRECT_EXECUTOR_SERVICE
            );

            assertThat(fixture.fetchedSources(), hasSize(2));
            assertThat(fixture.fetchedSources(), everyItem(hasKey("field")));
        });
    }

    public void testWarmingQueryPhaseFetchesNothingForRequestWithoutHits() throws Exception {
        final IndexShard shard = newStartedPrimaryWithThreeSegmentsOfTwoDocuments(List.of());
        withFixture(shard, Settings.EMPTY, fixture -> {
            final AtomicInteger searches = new AtomicInteger();
            final SearchRequest searchRequest = requestCachingSearchRequest(
                new MatchAllCallingBackOnSearchQueryBuilder(searches::incrementAndGet)
            );
            searchRequest.source().size(0);

            fixture.warm(
                newShardSearchRequest(shard.shardId(), searchRequest),
                fixture.newSearcherSupplier(),
                newSearchShardTask(),
                DIRECT_EXECUTOR_SERVICE
            );

            assertThat(searches.get(), equalTo(1));
            assertThat(fixture.fetchedSources(), empty());
        });
    }

    public void testWarmingQueryPhaseGivesBackTheCircuitBreakerBytesOfTheFetch() throws Exception {
        final IndexShard shard = newStartedPrimaryWithThreeSegmentsOfTwoDocuments(List.of());
        withFixture(shard, Settings.EMPTY, fixture -> {
            final long usedBefore = fixture.requestBreaker().getUsed();

            fixture.warm(
                newRequestCachingShardRequest(shard.shardId(), new MatchAllQueryBuilder()),
                fixture.newSearcherSupplier(),
                newSearchShardTask(),
                DIRECT_EXECUTOR_SERVICE
            );

            assertThat(fixture.fetchedSources(), not(empty()));
            verify(fixture.requestBreaker(), atLeastOnce()).addEstimateBytesAndMaybeBreak(anyLong(), startsWith(CATEGORY_FETCH));
            assertThat(fixture.requestBreaker().getUsed(), equalTo(usedBefore));
        });
    }

    public void testCancellingTheTaskWhileFetchingStopsWarmingQueryPhase() throws Exception {
        final IndexShard shard = newStartedPrimaryWithThreeSegmentsOfTwoDocuments(List.of());
        withFixture(shard, Settings.EMPTY, fixture -> {
            final SearchShardTask task = newSearchShardTask();
            fixture.afterEachFetchedHit().set(() -> TaskCancelHelper.cancel(task, "cancelled while fetching"));

            fixture.warmExpectingFailure(
                TaskCancelledException.class,
                newRequestCachingShardRequest(shard.shardId(), new MatchAllQueryBuilder()),
                fixture.newSearcherSupplier(),
                task,
                DIRECT_EXECUTOR_SERVICE
            );

            assertThat("the remaining hits were not fetched", fixture.fetchedSources(), hasSize(1));
            assertThat(fixture.requestBreaker().getUsed(), equalTo(0L));
            assertThat(fixture.searchService().getActiveContexts(), equalTo(0));
        });
    }

    public void testNormalQueryPhaseHasTheSideEffectsThatWarmingHasNot() throws Exception {
        final SearchOperationListener searchOperationListener = mock(SearchOperationListener.class);
        final IndexShard shard = newStartedPrimaryWithThreeSegmentsOfTwoDocuments(List.of(searchOperationListener));
        withFixture(shard, minimumOneDocumentPerSliceSettings(), fixture -> {
            final AtomicInteger searches = new AtomicInteger();
            final ShardSearchRequest request = newRequestCachingShardRequest(
                shard.shardId(),
                new MatchAllCallingBackOnSearchQueryBuilder(searches::incrementAndGet)
            );
            final long searchPoolTasksBefore = completedTasks(ThreadPool.Names.SEARCH);

            safeAwait((ActionListener<SearchPhaseResult> l) -> fixture.searchService().executeQueryPhase(request, newSearchShardTask(), l));

            assertThat(searches.get(), equalTo(1));
            assertThat(shard.searchStats().getTotal().getQueryCount(), equalTo(1L));
            verify(searchOperationListener).onPreQueryPhase(any());
            verify(fixture.indicesService()).loadIntoContext(any(), any(), any());
            assertBusy(() -> assertThat(completedTasks(ThreadPool.Names.SEARCH), greaterThan(searchPoolTasksBefore)));
        });
    }

    public void testWarmingQueryPhaseOnRecoveringShardWithOpenEngine() throws Exception {
        final IndexShard recovering = reinitShard(newStartedPrimaryWithThreeSegmentsOfTwoDocuments(List.of()));
        withFixture(recovering, Settings.EMPTY, fixture -> {
            recoverUpToOpenEngine(recovering);
            assertThat(recovering.state(), equalTo(IndexShardState.RECOVERING));
            assertFalse(recovering.isReadAllowed());
            expectThrows(
                IllegalIndexShardStateException.class,
                () -> recovering.acquireExternalSearcherSupplier(SplitShardCountSummary.IRRELEVANT)
            );
            final AtomicInteger searches = new AtomicInteger();
            final ShardSearchRequest request = newRequestCachingShardRequest(
                recovering.shardId(),
                new MatchAllCallingBackOnSearchQueryBuilder(searches::incrementAndGet)
            );
            final TrackedSearcherSupplier supplier = fixture.newSearcherSupplier();

            fixture.warm(request, supplier, newSearchShardTask(), threadPool.executor(ThreadPool.Names.WARMER));

            assertThat(searches.get(), equalTo(1));
            assertThat(supplier.acquired.get(), equalTo(1));
            assertThat(fixture.searchService().getActiveContexts(), equalTo(0));
            assertThat(recovering.state(), equalTo(IndexShardState.RECOVERING));
            assertThat(recovering.searchStats().getTotal().getQueryCount(), equalTo(0L));
        });
    }

    public void testWarmingQueryPhaseFailsFastWhenRewritingNeedsRemoteWork() throws Exception {
        final SearchOperationListener searchOperationListener = mock(SearchOperationListener.class);
        final IndexShard shard = newStartedPrimaryWithThreeSegmentsOfTwoDocuments(List.of(searchOperationListener));
        withFixture(shard, Settings.EMPTY, fixture -> {
            final ShardSearchRequest request = newRequestCachingShardRequest(
                shard.shardId(),
                new TermsQueryBuilder("field", new TermsLookup("other-index", "doc", "terms"))
            );
            final TrackedSearcherSupplier supplier = fixture.newSearcherSupplier();

            final IllegalArgumentException failure = fixture.warmExpectingFailure(
                IllegalArgumentException.class,
                request,
                supplier,
                newSearchShardTask(),
                DIRECT_EXECUTOR_SERVICE
            );

            assertThat(failure.getMessage(), containsString(shard.shardId().toString()));
            assertThat(failure.getMessage(), containsString("needs remote work"));
            verifyNoInteractions(fixture.client());
            assertThat("the data of the shard was not read", supplier.acquired.get(), equalTo(0));
            assertThat(fixture.searchService().getActiveContexts(), equalTo(0));
            verifyNoInteractions(searchOperationListener);
        });
    }

    public void testCancellingTheTaskWhileSearchingStopsWarmingQueryPhaseEvenWithLowLevelCancellationDisabled() throws Exception {
        final Settings lowLevelCancellationDisabled = Settings.builder()
            .put(SearchService.LOW_LEVEL_CANCELLATION_SETTING.getKey(), false)
            .build();
        final IndexShard shard = newStartedPrimaryWithThreeSegmentsOfTwoDocuments(List.of());
        withFixture(shard, lowLevelCancellationDisabled, fixture -> {
            final SearchShardTask task = newSearchShardTask();
            final AtomicInteger searches = new AtomicInteger();
            final ShardSearchRequest request = newRequestCachingShardRequest(
                shard.shardId(),
                new MatchAllCallingBackOnSearchQueryBuilder(() -> {
                    searches.incrementAndGet();
                    TaskCancelHelper.cancel(task, "cancelled while searching");
                })
            );

            final TaskCancelledException failure = fixture.warmExpectingFailure(
                TaskCancelledException.class,
                request,
                fixture.newSearcherSupplier(),
                task,
                DIRECT_EXECUTOR_SERVICE
            );

            assertThat(failure.getMessage(), containsString("cancelled while searching"));
            assertThat(searches.get(), equalTo(1));
            assertThat(fixture.searchService().getActiveContexts(), equalTo(0));
        });
    }

    public void testWarmingQueryPhaseDoesNotStartForCancelledTask() throws Exception {
        final IndexShard shard = newStartedPrimaryWithThreeSegmentsOfTwoDocuments(List.of());
        withFixture(shard, Settings.EMPTY, fixture -> {
            final SearchShardTask cancelledTask = newSearchShardTask();
            TaskCancelHelper.cancel(cancelledTask, "cancelled before starting");
            final AtomicInteger searches = new AtomicInteger();
            final ShardSearchRequest request = newRequestCachingShardRequest(
                shard.shardId(),
                new MatchAllCallingBackOnSearchQueryBuilder(searches::incrementAndGet)
            );

            fixture.warmExpectingFailure(
                TaskCancelledException.class,
                request,
                fixture.newSearcherSupplier(),
                cancelledTask,
                DIRECT_EXECUTOR_SERVICE
            );

            assertThat(searches.get(), equalTo(0));
        });
    }

    public void testWarmingQueryPhaseRejectedByExecutorReleasesTheSearcher() throws Exception {
        final IndexShard shard = newStartedPrimaryWithThreeSegmentsOfTwoDocuments(List.of());
        withFixture(shard, Settings.EMPTY, fixture -> {
            final AtomicInteger searches = new AtomicInteger();
            final ShardSearchRequest request = newRequestCachingShardRequest(
                shard.shardId(),
                new MatchAllCallingBackOnSearchQueryBuilder(searches::incrementAndGet)
            );
            final Executor rejectingExecutor = command -> ((AbstractRunnable) command).onRejection(
                new EsRejectedExecutionException("test")
            );

            fixture.warmExpectingFailure(
                EsRejectedExecutionException.class,
                request,
                fixture.newSearcherSupplier(),
                newSearchShardTask(),
                rejectingExecutor
            );

            assertThat(searches.get(), equalTo(0));
        });
    }

    public void testWarmingQueryPhaseOfScrollRequestFailsAndReleasesTheSearcher() throws Exception {
        final IndexShard shard = newStartedPrimaryWithThreeSegmentsOfTwoDocuments(List.of());
        withFixture(shard, Settings.EMPTY, fixture -> {
            final ShardSearchRequest scrollRequest = newShardSearchRequest(
                shard.shardId(),
                requestCachingSearchRequest(new MatchAllQueryBuilder()).scroll(TimeValue.timeValueMinutes(1))
            );

            final IllegalArgumentException failure = fixture.warmExpectingFailure(
                IllegalArgumentException.class,
                scrollRequest,
                fixture.newSearcherSupplier(),
                newSearchShardTask(),
                DIRECT_EXECUTOR_SERVICE
            );

            assertThat(failure.getMessage(), containsString("scroll and point-in-time"));
        });
    }

    public void testWarmingQueryPhaseOfPointInTimeRequestFailsAndReleasesTheSearcher() throws Exception {
        final IndexShard shard = newStartedPrimaryWithThreeSegmentsOfTwoDocuments(List.of());
        withFixture(shard, Settings.EMPTY, fixture -> {
            final ShardSearchRequest pointInTimeRequest = new ShardSearchRequest(
                OriginalIndices.NONE,
                requestCachingSearchRequest(new MatchAllQueryBuilder()),
                shard.shardId(),
                0,
                5,
                AliasFilter.EMPTY,
                1.0f,
                0,
                null,
                new ShardSearchContextId(UUIDs.randomBase64UUID(), randomNonNegativeLong()),
                null,
                SplitShardCountSummary.UNSET,
                true
            );

            final IllegalArgumentException failure = fixture.warmExpectingFailure(
                IllegalArgumentException.class,
                pointInTimeRequest,
                fixture.newSearcherSupplier(),
                newSearchShardTask(),
                DIRECT_EXECUTOR_SERVICE
            );

            assertThat(failure.getMessage(), containsString("scroll and point-in-time"));
        });
    }

    public void testWarmingQueryPhaseOfRemovedIndexFailsAndReleasesTheSearcher() throws Exception {
        final IndexShard shard = newStartedPrimaryWithThreeSegmentsOfTwoDocuments(List.of());
        withFixture(shard, Settings.EMPTY, fixture -> {
            when(fixture.indicesService().indexServiceSafe(any(Index.class))).thenThrow(new IndexNotFoundException("removed"));

            fixture.warmExpectingFailure(
                IndexNotFoundException.class,
                newRequestCachingShardRequest(shard.shardId(), new MatchAllQueryBuilder()),
                fixture.newSearcherSupplier(),
                newSearchShardTask(),
                DIRECT_EXECUTOR_SERVICE
            );
        });
    }

    private static final class MatchAllCallingBackOnSearchQueryBuilder extends LeafQueryBuilder<MatchAllCallingBackOnSearchQueryBuilder> {
        private final Runnable onSearch;

        MatchAllCallingBackOnSearchQueryBuilder(Runnable onSearch) {
            this.onSearch = onSearch;
        }

        @Override
        protected Query doToQuery(SearchExecutionContext context) {
            return new Query() {
                @Override
                public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
                    onSearch.run();
                    return Queries.ALL_DOCS_INSTANCE.createWeight(searcher, scoreMode, boost);
                }

                @Override
                public String toString(String field) {
                    return getWriteableName();
                }

                @Override
                public void visit(QueryVisitor visitor) {
                    visitor.visitLeaf(this);
                }

                @Override
                public boolean equals(Object obj) {
                    return this == obj;
                }

                @Override
                public int hashCode() {
                    return System.identityHashCode(this);
                }
            };
        }

        @Override
        protected void doWriteTo(StreamOutput out) {
            throw new UnsupportedOperationException();
        }

        @Override
        protected void doXContent(XContentBuilder builder, Params params) {
            throw new UnsupportedOperationException();
        }

        @Override
        protected boolean doEquals(MatchAllCallingBackOnSearchQueryBuilder other) {
            return this == other;
        }

        @Override
        protected int doHashCode() {
            return System.identityHashCode(this);
        }

        @Override
        public String getWriteableName() {
            return "match_all_calling_back_on_search";
        }

        @Override
        public TransportVersion getMinimalSupportedVersion() {
            return TransportVersion.zero();
        }
    }

    private static final class TrackedSearcherSupplier extends Engine.SearcherSupplier {
        private final Engine.SearcherSupplier delegate;
        final AtomicInteger acquired = new AtomicInteger();
        final AtomicBoolean closed = new AtomicBoolean();
        private final AtomicReference<Boolean> closedWhenListenerWasNotified = new AtomicReference<>();

        TrackedSearcherSupplier(Engine.SearcherSupplier delegate) {
            super(Function.identity());
            this.delegate = delegate;
        }

        @Override
        protected void doClose() {
            closed.set(true);
            delegate.close();
        }

        @Override
        protected Engine.Searcher acquireSearcherInternal(String source) {
            acquired.incrementAndGet();
            return delegate.acquireSearcher(source);
        }

        @Override
        public String getSearcherId() {
            return delegate.getSearcherId();
        }

        <T> ActionListener<T> releaseCheckingListener(ActionListener<T> delegate) {
            return ActionListener.runBefore(delegate, () -> closedWhenListenerWasNotified.set(closed.get()));
        }

        void assertReleasedBeforeListenerWasNotified() {
            assertThat("the searcher is released before the listener is notified", closedWhenListenerWasNotified.get(), equalTo(true));
        }
    }

    private record WarmingFixture(
        SearchService searchService,
        IndicesService indicesService,
        Client client,
        IndexShard shard,
        CircuitBreaker requestBreaker,
        List<Map<String, Object>> fetchedSources,
        AtomicReference<Runnable> afterEachFetchedHit
    ) {

        TrackedSearcherSupplier newSearcherSupplier() {
            return new TrackedSearcherSupplier(acquireSearcherSupplierFromEngine(shard));
        }

        void warm(ShardSearchRequest request, TrackedSearcherSupplier supplier, CancellableTask task, Executor executor) {
            safeAwait(
                (ActionListener<Void> l) -> searchService.executeQueryPhaseForWarming(
                    request,
                    shard,
                    supplier,
                    task,
                    executor,
                    supplier.releaseCheckingListener(l)
                )
            );
            supplier.assertReleasedBeforeListenerWasNotified();
        }

        <E extends Exception> E warmExpectingFailure(
            Class<E> exceptionType,
            ShardSearchRequest request,
            TrackedSearcherSupplier supplier,
            CancellableTask task,
            Executor executor
        ) {
            final E failure = safeAwaitFailure(
                exceptionType,
                Void.class,
                l -> searchService.executeQueryPhaseForWarming(
                    request,
                    shard,
                    supplier,
                    task,
                    executor,
                    supplier.releaseCheckingListener(l)
                )
            );
            supplier.assertReleasedBeforeListenerWasNotified();
            return failure;
        }
    }

    private void withFixture(IndexShard shard, Settings nodeSettings, CheckedConsumer<WarmingFixture, Exception> test) throws Exception {
        try (ClusterService clusterService = ClusterServiceUtils.createClusterService(threadPool, nodeSettings)) {
            final WarmingFixture fixture = newFixtureWithMockedNodeServices(clusterService, shard);
            try {
                test.accept(fixture);
            } finally {
                fixture.searchService().close();
            }
        } finally {
            closeShards(shard);
        }
    }

    private WarmingFixture newFixtureWithMockedNodeServices(ClusterService clusterService, IndexShard shard) throws Exception {
        final Client client = mock(Client.class);
        final CircuitBreaker requestBreaker = spy(createQueryConstructionBreaker("100mb"));
        final CircuitBreakerService circuitBreakerService = mock(CircuitBreakerService.class);
        when(circuitBreakerService.getBreaker(CircuitBreaker.REQUEST)).thenReturn(requestBreaker);
        final List<Map<String, Object>> fetchedSources = Collections.synchronizedList(new ArrayList<>());
        final AtomicReference<Runnable> afterEachFetchedHit = new AtomicReference<>(() -> {});
        final BitsetFilterCache bitsetFilterCache = new BitsetFilterCache(shard.indexSettings(), BitsetFilterCache.Listener.NOOP);
        final IndexService indexService = mock(IndexService.class);
        when(indexService.breakerService()).thenReturn(circuitBreakerService);
        when(indexService.getShard(anyInt())).thenReturn(shard);
        when(indexService.index()).thenReturn(shard.shardId().getIndex());
        when(indexService.getIndexSettings()).thenReturn(shard.indexSettings());
        when(indexService.mapperService()).thenReturn(shard.mapperService());
        when(indexService.newSearchExecutionContext(anyInt(), anyInt(), any(), any(), any(), any(), any(), any())).thenAnswer(
            invocation -> new SearchExecutionContext(
                invocation.<Integer>getArgument(0),
                invocation.<Integer>getArgument(1),
                shard.indexSettings(),
                bitsetFilterCache,
                (fieldType, fieldDataContext) -> fieldType.fielddataBuilder(fieldDataContext)
                    .build(new IndexFieldDataCache.None(), new NoneCircuitBreakerService()),
                shard.mapperService(),
                shard.mapperService().mappingLookup(),
                null,
                null,
                parserConfig(),
                writableRegistry(),
                null,
                invocation.<IndexSearcher>getArgument(2),
                invocation.<LongSupplier>getArgument(3),
                invocation.<String>getArgument(4),
                pattern -> Regex.simpleMatch(pattern, shard.shardId().getIndexName()),
                () -> true,
                null,
                invocation.<Map<String, Object>>getArgument(5),
                invocation.<Integer>getArgument(6),
                MapperMetrics.NOOP,
                invocation.<ShardSearchStats>getArgument(7)
            )
        );
        final IndicesService indicesService = mock(IndicesService.class);
        when(indicesService.indexServiceSafe(any(Index.class))).thenReturn(indexService);
        when(indicesService.hasIndex(any(Index.class))).thenReturn(true);
        when(indicesService.getDataRewriteContext(any())).thenAnswer(
            invocation -> new DataRewriteContext(parserConfig(), client, invocation.<LongSupplier>getArgument(0))
        );
        when(indicesService.directoryMetricsDelta()).thenReturn(() -> DirectoryMetrics.EMPTY);
        doAnswer(invocation -> {
            QueryPhase.execute(invocation.<SearchContext>getArgument(1));
            return null;
        }).when(indicesService).loadIntoContext(any(), any(), any());
        final SearchService searchService = new SearchService(
            clusterService,
            indicesService,
            threadPool,
            null,
            BigArrays.NON_RECYCLING_INSTANCE,
            new FetchPhase(List.of(new FetchSourcePhase(), recordingFetchedSources(fetchedSources, afterEachFetchedHit))),
            circuitBreakerService,
            EmptySystemIndices.INSTANCE.getExecutorSelector(),
            Tracer.NOOP,
            OnlinePrewarmingService.NOOP,
            ShardQueryRequestObserver.NOOP,
            new TaskManager(Settings.EMPTY, threadPool, Collections.emptySet())
        );
        return new WarmingFixture(searchService, indicesService, client, shard, requestBreaker, fetchedSources, afterEachFetchedHit);
    }

    private static SearchSortValuesAndFormats bottomSortValuesBelowEveryTimestamp() {
        return new SearchSortValuesAndFormats(new Object[] { 0L }, new DocValueFormat[] { DocValueFormat.RAW });
    }

    private static FetchSubPhase recordingFetchedSources(List<Map<String, Object>> fetchedSources, AtomicReference<Runnable> afterEachHit) {
        return fetchContext -> new FetchSubPhaseProcessor() {
            @Override
            public void setNextReader(LeafReaderContext readerContext) {}

            @Override
            public void process(FetchSubPhase.HitContext hitContext) {
                fetchedSources.add(hitContext.source().source());
                afterEachHit.get().run();
            }

            @Override
            public StoredFieldsSpec storedFieldsSpec() {
                return StoredFieldsSpec.NEEDS_SOURCE;
            }
        };
    }

    private static Settings minimumOneDocumentPerSliceSettings() {
        return Settings.builder().put(SearchService.MINIMUM_DOCS_PER_SLICE.getKey(), 1).build();
    }

    private IndexShard newStartedPrimaryWithThreeSegmentsOfTwoDocuments(List<SearchOperationListener> searchOperationListeners)
        throws IOException {
        final IndexShard shard = newShard(true, searchOperationListeners);
        recoverShardFromStore(shard);
        for (int segment = 0; segment < 3; segment++) {
            for (int doc = 0; doc < 2; doc++) {
                final int docNumber = segment * 2 + doc;
                final String source = "{\"field\":\"value-" + doc + "\",\"timestamp\":\"2025-01-0" + (docNumber + 1) + "\"}";
                assertTrue(indexDoc(shard, "_doc", "id-" + docNumber, source).isCreated());
            }
            assertTrue(shard.refresh("test").refreshed());
        }
        return shard;
    }

    private void recoverUpToOpenEngine(IndexShard shard) {
        shard.markAsRecovering("for testing");
        shard.prepareForIndexRecovery();
        shard.recoveryState().getIndex().setFileDetailsComplete();
        safeAwait(shard::openEngineAndRecoverFromTranslog);
        refreshExternalReaderOnceForInternalEngine(shard);
    }

    private static void refreshExternalReaderOnceForInternalEngine(IndexShard shard) {
        shard.refresh("external_reader_refreshed_once_for_internal_engine");
    }

    private static Engine.SearcherSupplier acquireSearcherSupplierFromEngine(IndexShard shard) {
        return shard.withEngine(
            engine -> engine.acquireSearcherSupplier(Function.identity(), Engine.SearcherScope.EXTERNAL, SplitShardCountSummary.IRRELEVANT)
        );
    }

    private static SearchRequest requestCachingSearchRequest(QueryBuilder query) {
        return new SearchRequest().allowPartialSearchResults(false).requestCache(true).source(new SearchSourceBuilder().query(query));
    }

    private static ShardSearchRequest newShardSearchRequest(ShardId shardId, SearchRequest searchRequest) {
        return new ShardSearchRequest(OriginalIndices.NONE, searchRequest, shardId, 0, 5, AliasFilter.EMPTY, 1.0f, 0, null);
    }

    private static ShardSearchRequest newRequestCachingShardRequest(ShardId shardId, QueryBuilder query) {
        return newShardSearchRequest(shardId, requestCachingSearchRequest(query));
    }

    private static Executor recordingThreadName(Executor executor, AtomicReference<String> threadName) {
        return command -> executor.execute(() -> {
            threadName.set(Thread.currentThread().getName());
            command.run();
        });
    }

    private long completedTasks(String threadPoolName) {
        for (ThreadPoolStats.Stats stats : threadPool.stats()) {
            if (stats.name().equals(threadPoolName)) {
                return stats.completed();
            }
        }
        throw new AssertionError("no thread pool [" + threadPoolName + "]");
    }
}
