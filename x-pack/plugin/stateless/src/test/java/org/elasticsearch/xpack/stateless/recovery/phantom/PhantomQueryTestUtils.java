/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.recovery.phantom;

import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.support.replication.ClusterStateCreationUtils;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.routing.GlobalRoutingTable;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.SplitShardCountSummary;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.search.internal.ShardSearchRequest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.cluster.metadata.Metadata.DEFAULT_PROJECT_ID;
import static org.elasticsearch.cluster.routing.ShardRoutingState.INITIALIZING;
import static org.elasticsearch.cluster.routing.ShardRoutingState.RELOCATING;
import static org.elasticsearch.cluster.routing.ShardRoutingState.STARTED;

final class PhantomQueryTestUtils {

    static final String INDEX_NAME = "phantom-index";
    static final String FIELD = "field";

    private PhantomQueryTestUtils() {}

    static ShardId shardId() {
        return new ShardId(INDEX_NAME, "_na_", 0);
    }

    static NamedWriteableRegistry namedWriteableRegistry() {
        return new NamedWriteableRegistry(new SearchModule(Settings.EMPTY, List.of()).getNamedWriteables());
    }

    static ShardSearchRequest queryRequest(ShardId shardId, String value) {
        return shardSearchRequest(shardId, searchRequest(value), null);
    }

    static SearchRequest searchRequest(String value) {
        final var searchRequest = new SearchRequest(INDEX_NAME).source(
            new SearchSourceBuilder().query(QueryBuilders.termQuery(FIELD, value))
        );
        searchRequest.allowPartialSearchResults(true);
        return searchRequest;
    }

    static ShardSearchRequest scrollRequest(ShardId shardId, String value) {
        return shardSearchRequest(shardId, searchRequest(value).scroll(TimeValue.timeValueMinutes(1)), null);
    }

    static ShardSearchRequest waitForCheckpointRequest(ShardId shardId, String value) {
        final var searchRequest = searchRequest(value);
        searchRequest.setWaitForCheckpoints(Map.of(shardId.getIndexName(), new long[] { 5L }));
        return shardSearchRequest(shardId, searchRequest, null);
    }

    static ShardSearchRequest readerRequest(ShardId shardId, String value) {
        return shardSearchRequest(shardId, searchRequest(value), new ShardSearchContextId("session", 42L));
    }

    static ShardSearchRequest dfsRequest(ShardId shardId, String value) {
        return shardSearchRequest(shardId, searchRequest(value).searchType(SearchType.DFS_QUERY_THEN_FETCH), null);
    }

    private static ShardSearchRequest shardSearchRequest(ShardId shardId, SearchRequest searchRequest, ShardSearchContextId readerId) {
        return new ShardSearchRequest(
            new OriginalIndices(searchRequest),
            searchRequest,
            shardId,
            0,
            1,
            AliasFilter.EMPTY,
            1.0f,
            System.currentTimeMillis(),
            null,
            readerId,
            readerId == null ? null : TimeValue.timeValueMinutes(1),
            SplitShardCountSummary.UNSET,
            true
        );
    }

    static ShardSearchRequest decode(EncodedQueryRequest encoded) throws IOException {
        try (var in = new NamedWriteableAwareStreamInput(encoded.bytes().streamInput(), namedWriteableRegistry())) {
            in.setTransportVersion(encoded.version());
            return new ShardSearchRequest(in);
        }
    }

    /** Besides the initializing search replica, the shard has a started primary and a started search replica: the peer. */
    static ClusterState clusterStateWithInitializingSearchReplica() {
        return clusterStateWithInitializingSearchReplicas(1);
    }

    static ClusterState clusterStateWithInitializingSearchReplicas(int initializing) {
        final List<Tuple<ShardRoutingState, ShardRouting.Role>> replicas = new ArrayList<>();
        replicas.add(new Tuple<>(STARTED, ShardRouting.Role.SEARCH_ONLY));
        for (int i = 0; i < initializing; i++) {
            replicas.add(new Tuple<>(INITIALIZING, ShardRouting.Role.SEARCH_ONLY));
        }
        return ClusterStateCreationUtils.state(DEFAULT_PROJECT_ID, INDEX_NAME, true, STARTED, ShardRouting.Role.INDEX_ONLY, replicas);
    }

    /** The initializing target of the relocation is not a shard of its own in the routing table: the relocating source implies it. */
    static ClusterState clusterStateWithRelocatingSearchReplica() {
        return ClusterStateCreationUtils.state(
            DEFAULT_PROJECT_ID,
            INDEX_NAME,
            true,
            STARTED,
            ShardRouting.Role.INDEX_ONLY,
            List.of(new Tuple<>(RELOCATING, ShardRouting.Role.SEARCH_ONLY))
        );
    }

    static String initializingAllocationId(ClusterState state) {
        return initializingAllocationIds(state).get(0);
    }

    static List<String> initializingAllocationIds(ClusterState state) {
        return state.routingTable(DEFAULT_PROJECT_ID)
            .shardRoutingTable(shardId())
            .getAllInitializingShards()
            .stream()
            .map(routing -> routing.allocationId().getId())
            .toList();
    }

    /** Without the relocation targets that the routing of a relocating source implies. */
    static List<ShardRouting> shards(ClusterState state) {
        return state.routingTable(DEFAULT_PROJECT_ID).shardRoutingTable(shardId()).allShards().toList();
    }

    static ClusterState withCopyStarted(ClusterState state, String allocationId) {
        return withShards(
            state,
            shards(state).stream()
                .map(
                    routing -> hasAllocationId(routing, allocationId)
                        ? routing.moveToStarted(ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE)
                        : routing
                )
                .toList()
        );
    }

    static ClusterState withoutCopy(ClusterState state, String allocationId) {
        return withShards(state, shards(state).stream().filter(routing -> hasAllocationId(routing, allocationId) == false).toList());
    }

    static ClusterState withoutIndex() {
        return ClusterStateCreationUtils.state(DEFAULT_PROJECT_ID, "another-index", true, STARTED, ShardRoutingState.STARTED);
    }

    static ClusterState withShards(ClusterState state, List<ShardRouting> shards) {
        final var builder = new IndexShardRoutingTable.Builder(shardId());
        shards.forEach(builder::addShard);
        return ClusterState.builder(state)
            .routingTable(
                GlobalRoutingTable.builder()
                    .put(
                        DEFAULT_PROJECT_ID,
                        RoutingTable.builder().add(IndexRoutingTable.builder(shardId().getIndex()).addIndexShard(builder))
                    )
                    .build()
            )
            .build();
    }

    private static boolean hasAllocationId(ShardRouting routing, String allocationId) {
        return routing.allocationId() != null && routing.allocationId().getId().equals(allocationId);
    }
}
