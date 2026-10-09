/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.recovery.shardinfo;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.stateless.AbstractStatelessPluginIntegTestCase;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.xpack.stateless.recovery.shardinfo.GlobalOrdinalsRefreshWarmer.GLOBAL_ORDINALS_REFRESH_ENABLED_SETTING;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

/**
 * Global ordinals are cached per reader in the node level fielddata cache, and global ordinals builds are the only fielddata cache
 * misses for keyword fields. The tests therefore use the cache misses on the search node to check whether a refresh built the global
 * ordinals of the new reader before exposing it, or whether the next search had to build them.
 */
public class GlobalOrdinalsRefreshWarmingIT extends AbstractStatelessPluginIntegTestCase {

    private static final String INDEX_NAME = "my-index";
    private static final String KEYWORD_FIELD = "keyword_field";

    public void testGlobalOrdinalsAreBuiltBeforeExposingNewReader() throws Exception {
        startMasterAndIndexNode();
        String searchNode = startSearchNode();
        updateClusterSettings(Settings.builder().put(GLOBAL_ORDINALS_REFRESH_ENABLED_SETTING.getKey(), true));
        createIndexWithMultipleSegments();
        runTermsAggregation(searchNode);

        long missesBeforeRefresh = fielddataCacheMisses(searchNode);
        indexDocsAndRefresh(1);
        assertThat(fielddataCacheMisses(searchNode), greaterThan(missesBeforeRefresh));

        assertSearchDoesNotMissFielddataCache(searchNode);
    }

    public void testGlobalOrdinalsAreNotBuiltByDefault() throws Exception {
        startMasterAndIndexNode();
        String searchNode = startSearchNode();
        createIndexWithMultipleSegments();
        runTermsAggregation(searchNode);

        long missesBeforeRefresh = fielddataCacheMisses(searchNode);
        indexDocsAndRefresh(1);
        assertThat(fielddataCacheMisses(searchNode), equalTo(missesBeforeRefresh));

        runTermsAggregation(searchNode);
        assertThat(fielddataCacheMisses(searchNode), greaterThan(missesBeforeRefresh));
    }

    public void testGlobalOrdinalsAreNotBuiltWhenUsedLongerAgoThanMaxAge() throws Exception {
        startMasterAndIndexNode();
        String searchNode = startSearchNode();
        TimeValue maxAge = TimeValue.timeValueMillis(randomLongBetween(100, 500));
        updateClusterSettings(
            Settings.builder()
                .put(GLOBAL_ORDINALS_REFRESH_ENABLED_SETTING.getKey(), true)
                .put(SearchShardInformationIndexListener.GLOBAL_ORDINALS_MAX_AGE_SETTING.getKey(), maxAge)
        );
        createIndexWithMultipleSegments();
        runTermsAggregation(searchNode);

        IndexShard searchShard = findSearchShard(INDEX_NAME);
        ThreadPool threadPool = internalCluster().getInstance(ThreadPool.class, searchNode);
        assertBusy(
            () -> assertThat(
                searchShard.fieldData().globalOrdinalsFieldsAccessedSince(threadPool.relativeTimeInMillis() - maxAge.millis()),
                empty()
            )
        );

        long missesBeforeRefresh = fielddataCacheMisses(searchNode);
        indexDocsAndRefresh(1);
        assertThat(fielddataCacheMisses(searchNode), equalTo(missesBeforeRefresh));
    }

    public void testWarmingOnRefreshDoesNotCountAsUsage() throws Exception {
        startMasterAndIndexNode();
        String searchNode = startSearchNode();
        updateClusterSettings(Settings.builder().put(GLOBAL_ORDINALS_REFRESH_ENABLED_SETTING.getKey(), true));
        createIndexWithMultipleSegments();
        runTermsAggregation(searchNode);

        // let the cached clock advance past the last search access, so that any later access gets a later timestamp
        ThreadPool threadPool = internalCluster().getInstance(ThreadPool.class, searchNode);
        long lastSearchAccess = threadPool.relativeTimeInMillis();
        assertBusy(() -> assertThat(threadPool.relativeTimeInMillis(), greaterThan(lastSearchAccess)));

        long missesBeforeRefreshes = fielddataCacheMisses(searchNode);
        indexDocsAndRefresh(randomIntBetween(1, 3));
        assertThat("refreshes built global ordinals", fielddataCacheMisses(searchNode), greaterThan(missesBeforeRefreshes));

        // otherwise a field searched once would be warmed on every refresh forever
        assertThat(findSearchShard(INDEX_NAME).fieldData().globalOrdinalsFieldsAccessedSince(lastSearchAccess + 1), empty());
    }

    private void createIndexWithMultipleSegments() throws Exception {
        assertAcked(prepareCreate(INDEX_NAME).setSettings(indexSettings(1, 1)).setMapping(KEYWORD_FIELD, "type=keyword"));
        // global ordinals are only built for readers with more than one segment
        indexDocsAndRefresh(randomIntBetween(2, 4));
        try (Engine.Searcher searcher = findSearchShard(INDEX_NAME).acquireSearcher("test")) {
            assertThat(searcher.getIndexReader().leaves().size(), greaterThan(1));
        }
    }

    private void indexDocsAndRefresh(int refreshes) {
        for (int i = 0; i < refreshes; i++) {
            BulkRequestBuilder bulkRequest = client().prepareBulk(INDEX_NAME);
            for (int j = 0; j < 10; j++) {
                bulkRequest.add(prepareIndex(INDEX_NAME).setSource(KEYWORD_FIELD, randomAlphaOfLength(10)));
            }
            assertNoFailures(bulkRequest.get());
            refresh(INDEX_NAME);
        }
    }

    private static void runTermsAggregation(String nodeName) {
        assertNoFailures(
            prepareSearch(INDEX_NAME).setPreference("_only_nodes:" + nodeName)
                .setSize(0)
                .setRequestCache(false)
                .addAggregation(AggregationBuilders.terms("terms").field(KEYWORD_FIELD).executionHint("global_ordinals"))
        );
    }

    private static void assertSearchDoesNotMissFielddataCache(String nodeName) {
        long missesBefore = fielddataCacheMisses(nodeName);
        runTermsAggregation(nodeName);
        assertThat(fielddataCacheMisses(nodeName), equalTo(missesBefore));
    }

    private static long fielddataCacheMisses(String nodeName) {
        return internalCluster().getInstance(IndicesService.class, nodeName).getIndicesFieldDataCache().getCache().stats().getMisses();
    }
}
