/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.recovery.shardinfo;

import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteRequest;
import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteResponse;
import org.elasticsearch.action.admin.cluster.reroute.TransportClusterRerouteAction;
import org.elasticsearch.action.admin.indices.template.put.TransportPutComposableIndexTemplateAction;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.datastreams.CreateDataStreamAction;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.blobcache.shared.SharedBytes;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.DataStreamTestHelper;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.datastreams.DataStreamsPlugin;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.fielddata.FieldDataStats;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.stateless.AbstractStatelessPluginIntegTestCase;
import org.elasticsearch.xpack.stateless.cache.SearchCommitPrefetcher;
import org.elasticsearch.xpack.stateless.engine.SearchEngine;
import org.hamcrest.Matcher;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.blobcache.shared.SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING;
import static org.elasticsearch.blobcache.shared.SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;

public class SearchShardInformationIndexListenerIT extends AbstractStatelessPluginIntegTestCase {

    private static final String KEYWORD_FIELD = "keyword_field";

    @Override
    protected Settings.Builder settingsForRoles(DiscoveryNodeRole... roles) {
        return super.settingsForRoles(roles).put(SHARED_CACHE_REGION_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(SharedBytes.PAGE_SIZE))
            .put(SHARED_CACHE_SIZE_SETTING.getKey(), ByteSizeValue.ofMb(8));
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(DataStreamsPlugin.class);
        return plugins;
    }

    public void testLastSearcherAcquiredTimeIsSetWhenAddingReplica() throws Exception {
        startMasterAndIndexNode();
        String firstNode = startSearchNode();
        String indexName = "my-index";

        indexRandomDocs(indexName, 100);

        IndexShard searchShard = findSearchShard(indexName);
        final long lastAcquiredTime = searchShard.tryWithEngineOrNull(e -> ((SearchEngine) e).getLastSearcherAcquiredTime());
        assertThat(lastAcquiredTime, greaterThan(0L));

        // increase replica count
        String secondNode = startSearchNode();
        setReplicaCount(2, indexName);
        waitForRelocation(ClusterHealthStatus.GREEN);

        // find all search index shards
        Index index = resolveIndex(indexName);
        ShardId shardId = new ShardId(index, 0);

        assertSearchShardOnNode(firstNode, shardId, lastAcquiredTime);
        assertSearchShardOnNode(secondNode, shardId, lastAcquiredTime);

        var numberOfCommits = randomIntBetween(1, 4);
        for (int j = 0; j < numberOfCommits; j++) {
            // Index enough documents so the initial read happening during refresh doesn't include the complete Lucene files
            indexDocs(indexName, 2_000);
            refresh(indexName);
        }
        flush(indexName);

        assertBusy(() -> { assertPrefetchedBytesOnNode(secondNode, shardId, greaterThan(0L)); }, 10, TimeUnit.SECONDS);
    }

    public void testLastSearcherAcquiredTimeIsSetWhenRecovering() throws Exception {
        startMasterAndIndexNode();
        String firstNode = startSearchNode();
        String indexName = "my-index";

        indexRandomDocs(indexName, 100);

        IndexShard searchShard = findSearchShard(indexName);
        final long lastAcquiredTime = searchShard.tryWithEngineOrNull(e -> ((SearchEngine) e).getLastSearcherAcquiredTime());
        assertThat(lastAcquiredTime, greaterThan(0L));

        String secondNode = startSearchNode();

        // trigger a relocation via reroute
        ClusterRerouteRequest rerouteRequest = new ClusterRerouteRequest(TimeValue.ONE_MINUTE, TimeValue.ONE_MINUTE);
        rerouteRequest.add(new MoveAllocationCommand(indexName, searchShard.shardId().id(), firstNode, secondNode, ProjectId.DEFAULT));
        ClusterRerouteResponse rerouteResponse = client().execute(TransportClusterRerouteAction.TYPE, rerouteRequest)
            .actionGet(TimeValue.timeValueMinutes(5));
        assertAcked(rerouteResponse);
        waitForRelocation(ClusterHealthStatus.GREEN);

        // find all search index shards
        Index index = resolveIndex(indexName);
        ShardId shardId = new ShardId(index, 0);

        // search node does not have any IndexService left as data has moved
        IndicesService indicesService = internalCluster().getInstance(IndicesService.class, firstNode);
        assertThat(indicesService.hasIndex(index), equalTo(false));

        // this one has the same last acquired time than the other node earlier
        assertSearchShardOnNode(secondNode, shardId, lastAcquiredTime);

        var numberOfCommits = randomIntBetween(1, 4);
        for (int j = 0; j < numberOfCommits; j++) {
            // Index enough documents so the initial read happening during refresh doesn't include the complete Lucene files
            indexDocs(indexName, 2_000);
            refresh(indexName);
        }
        flush(indexName);

        assertBusy(() -> { assertPrefetchedBytesOnNode(secondNode, shardId, greaterThan(0L)); }, 10, TimeUnit.SECONDS);
    }

    public void testLastSearcherSetForLatestDatastream() throws ExecutionException, InterruptedException {
        long testStartTime = System.currentTimeMillis();
        startMasterAndIndexNode();
        startSearchNode();

        // create composable index template
        TransportPutComposableIndexTemplateAction.Request request = new TransportPutComposableIndexTemplateAction.Request("my-logs");
        request.indexTemplate(
            ComposableIndexTemplate.builder()
                .indexPatterns(List.of("my-logs*"))
                .template(Template.builder().dataStreamOptions(DataStreamTestHelper.createDataStreamOptionsTemplate(false)))
                .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate())
                .build()
        );
        client().execute(TransportPutComposableIndexTemplateAction.TYPE, request).actionGet();

        // create data stream
        CreateDataStreamAction.Request createDataStreamRequest = new CreateDataStreamAction.Request(
            TEST_REQUEST_TIMEOUT,
            TEST_REQUEST_TIMEOUT,
            "my-logs"
        );
        client().execute(CreateDataStreamAction.INSTANCE, createDataStreamRequest).get();

        // index into datastream
        BulkResponse response = client().prepareBulk("my-logs")
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .add(
                client().prepareIndex("my-logs")
                    .setOpType(DocWriteRequest.OpType.CREATE)
                    .setSource("@timestamp", "2025-01-28T12:34:56.789Z")
            )
            .get();
        assertNoFailures(response);
        assertThat(response.getItems().length, equalTo(1));
        String index = response.getItems()[0].getIndex();

        assertThat(index, not(equalTo("my-logs")));

        // last searcher acquired time should be set, because this data stream is the latest
        // there is also no need for other copies of that shard to be around, it will still be set
        IndexShard searchShard = findSearchShard(index);
        final long lastAcquiredTime = searchShard.tryWithEngineOrNull(e -> ((SearchEngine) e).getLastSearcherAcquiredTime());
        assertThat(lastAcquiredTime, greaterThan(testStartTime));
    }

    public void testGlobalOrdinalsAreBuiltWhenRelocating() throws Exception {
        startMasterAndIndexNode();
        String firstNode = startSearchNode();
        enableGlobalOrdinalsWarming(Settings.builder());
        String indexName = "my-index";
        createIndexWithMultipleSegments(indexName);
        runTermsAggregation(indexName, firstNode);
        assertThat(findSearchShard(indexName).fieldData().globalOrdinalsFieldsAccessedSince(Long.MIN_VALUE), contains(KEYWORD_FIELD));

        String secondNode = startSearchNode();
        relocateSearchShard(indexName, firstNode, secondNode);

        ShardId shardId = new ShardId(resolveIndex(indexName), 0);
        // the global ordinals are built during the recovery, before any search reached the new shard copy
        assertGlobalOrdinalsBuiltOnNode(secondNode, shardId, true);
        assertSearchReusesBuiltGlobalOrdinals(indexName, secondNode);
    }

    public void testGlobalOrdinalsAreBuiltWhenAddingReplica() throws Exception {
        startMasterAndIndexNode();
        String firstNode = startSearchNode();
        enableGlobalOrdinalsWarming(Settings.builder());
        String indexName = "my-index";
        createIndexWithMultipleSegments(indexName);
        runTermsAggregation(indexName, firstNode);

        String secondNode = startSearchNode();
        setReplicaCount(2, indexName);
        waitForRelocation(ClusterHealthStatus.GREEN);

        ShardId shardId = new ShardId(resolveIndex(indexName), 0);
        assertGlobalOrdinalsBuiltOnNode(secondNode, shardId, true);
        assertSearchReusesBuiltGlobalOrdinals(indexName, secondNode);
    }

    public void testGlobalOrdinalsAreNotBuiltWhenUsedLongerAgoThanMaxAge() throws Exception {
        startMasterAndIndexNode();
        String firstNode = startSearchNode();
        TimeValue maxAge = TimeValue.timeValueMillis(randomLongBetween(100, 500));
        enableGlobalOrdinalsWarming(
            Settings.builder().put(SearchShardInformationIndexListener.GLOBAL_ORDINALS_MAX_AGE_SETTING.getKey(), maxAge)
        );
        String indexName = "my-index";
        createIndexWithMultipleSegments(indexName);
        runTermsAggregation(indexName, firstNode);

        IndexShard sourceShard = findSearchShard(indexName);
        ThreadPool sourceThreadPool = internalCluster().getInstance(ThreadPool.class, firstNode);
        assertBusy(
            () -> assertThat(
                sourceShard.fieldData().globalOrdinalsFieldsAccessedSince(sourceThreadPool.relativeTimeInMillis() - maxAge.millis()),
                empty()
            )
        );

        String secondNode = startSearchNode();
        relocateSearchShard(indexName, firstNode, secondNode);

        assertGlobalOrdinalsBuiltOnNode(secondNode, new ShardId(resolveIndex(indexName), 0), false);
    }

    public void testGlobalOrdinalsAreNotBuiltByDefault() throws Exception {
        startMasterAndIndexNode();
        String firstNode = startSearchNode();
        String indexName = "my-index";
        createIndexWithMultipleSegments(indexName);
        runTermsAggregation(indexName, firstNode);

        String secondNode = startSearchNode();
        relocateSearchShard(indexName, firstNode, secondNode);

        assertGlobalOrdinalsBuiltOnNode(secondNode, new ShardId(resolveIndex(indexName), 0), false);
    }

    private static void enableGlobalOrdinalsWarming(Settings.Builder settings) {
        updateClusterSettings(settings.put(SearchShardInformationIndexListener.GLOBAL_ORDINALS_ENABLED_SETTING.getKey(), true));
    }

    private void createIndexWithMultipleSegments(String indexName) throws Exception {
        assertAcked(prepareCreate(indexName).setSettings(indexSettings(1, 1)).setMapping(KEYWORD_FIELD, "type=keyword"));
        // global ordinals are only built for readers with more than one segment
        int segments = randomIntBetween(2, 4);
        for (int i = 0; i < segments; i++) {
            BulkRequestBuilder bulkRequest = client().prepareBulk(indexName);
            for (int j = 0; j < 10; j++) {
                bulkRequest.add(prepareIndex(indexName).setSource(KEYWORD_FIELD, "value-" + i + "-" + j));
            }
            assertNoFailures(bulkRequest.get());
            refresh(indexName);
        }
        try (Engine.Searcher searcher = findSearchShard(indexName).acquireSearcher("test")) {
            assertThat(searcher.getIndexReader().leaves().size(), greaterThan(1));
        }
    }

    private static void runTermsAggregation(String indexName, String nodeName) {
        assertNoFailures(
            prepareSearch(indexName).setPreference("_only_nodes:" + nodeName)
                .setSize(0)
                .setRequestCache(false)
                .addAggregation(AggregationBuilders.terms("terms").field(KEYWORD_FIELD).executionHint("global_ordinals"))
        );
    }

    private void relocateSearchShard(String indexName, String fromNode, String toNode) {
        ClusterRerouteRequest rerouteRequest = new ClusterRerouteRequest(TimeValue.ONE_MINUTE, TimeValue.ONE_MINUTE);
        rerouteRequest.add(new MoveAllocationCommand(indexName, 0, fromNode, toNode, ProjectId.DEFAULT));
        ClusterRerouteResponse rerouteResponse = client().execute(TransportClusterRerouteAction.TYPE, rerouteRequest)
            .actionGet(TimeValue.timeValueMinutes(5));
        assertAcked(rerouteResponse);
        waitForRelocation(ClusterHealthStatus.GREEN);
    }

    private static void assertGlobalOrdinalsBuiltOnNode(String nodeName, ShardId shardId, boolean expectBuilt) {
        IndicesService indicesService = internalCluster().getInstance(IndicesService.class, nodeName);
        IndexShard shard = indicesService.indexServiceSafe(shardId.getIndex()).getShard(shardId.id());
        Map<String, FieldDataStats.GlobalOrdinalsStats.GlobalOrdinalFieldStats> fieldStats = shard.fieldData()
            .stats(KEYWORD_FIELD)
            .getGlobalOrdinalsStats()
            .getFieldGlobalOrdinalsStats();
        if (expectBuilt) {
            assertThat(fieldStats, hasKey(KEYWORD_FIELD));
        } else {
            assertThat(fieldStats == null ? Map.of() : fieldStats, not(hasKey(KEYWORD_FIELD)));
        }
    }

    /**
     * Global ordinals are cached per reader, so a search that does not miss the fielddata cache shows that the global ordinals were
     * built for the reader searches actually use.
     */
    private static void assertSearchReusesBuiltGlobalOrdinals(String indexName, String nodeName) {
        IndicesService indicesService = internalCluster().getInstance(IndicesService.class, nodeName);
        long missesBefore = indicesService.getIndicesFieldDataCache().getCache().stats().getMisses();
        runTermsAggregation(indexName, nodeName);
        assertThat(indicesService.getIndicesFieldDataCache().getCache().stats().getMisses(), equalTo(missesBefore));
    }

    private void assertPrefetchedBytesOnNode(String nodeId, ShardId shardId, Matcher<Long> matcher) {
        IndicesService indicesService = internalCluster().getInstance(IndicesService.class, nodeId);
        IndexService indexService = indicesService.indexService(shardId.getIndex());
        assertThat(indexService, notNullValue());
        IndexShard shard = indexService.getShardOrNull(shardId.id());

        SearchCommitPrefetcher.BCCPreFetchedOffset offset = shard.tryWithEngineOrNull(e -> ((SearchEngine) e).getMaxPrefetchedOffset());
        assertThat(offset, notNullValue());
        assertThat(offset, not(equalTo(SearchCommitPrefetcher.BCCPreFetchedOffset.ZERO)));

        long prefetchedBytes = shard.tryWithEngineOrNull(e -> ((SearchEngine) e).getTotalPrefetchedBytes());
        assertThat(prefetchedBytes, matcher);
    }

    private void assertSearchShardOnNode(String nodeId, ShardId shardId, long expectedLastAcquiredTime) {
        IndicesService indicesService = internalCluster().getInstance(IndicesService.class, nodeId);
        IndexService indexService = indicesService.indexService(shardId.getIndex());
        assertThat(indexService, notNullValue());
        IndexShard shard = indexService.getShardOrNull(shardId.id());

        assertThat(shard, notNullValue());
        assertThat(shard.isActive(), equalTo(true));
        assertThat(shard.routingEntry().role(), equalTo(ShardRouting.Role.SEARCH_ONLY));

        long searchShardLastAcquiredTime = shard.tryWithEngineOrNull(e -> ((SearchEngine) e).getLastSearcherAcquiredTime());
        assertThat(searchShardLastAcquiredTime, greaterThan(0L));
        assertThat(searchShardLastAcquiredTime, equalTo(expectedLastAcquiredTime));
    }

    private void indexRandomDocs(String indexName, int docs) {
        createIndex(indexName, indexSettings(1, 1).build());

        indexDocs(indexName, docs);
        refresh(indexName);
        assertHitCount(prepareSearch(indexName), docs);
    }
}
