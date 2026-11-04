/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.recovery.phantom;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.function.Predicate;

/**
 * A relocation target takes over the traffic of its source, so the source is its peer. Any other recovering copy picks a started search
 * replica at random, which spreads the sampling load when many copies recover at once, for example after a node left.
 */
final class PhantomQueryPeerSelection {

    private PhantomQueryPeerSelection() {}

    static Optional<DiscoveryNode> selectPeer(
        ClusterState state,
        ShardRouting recoveringCopy,
        String localNodeId,
        Random random,
        Predicate<DiscoveryNode> supportsSampling
    ) {
        if (recoveringCopy.primary()) {
            // a primary is on an indexing node and not recovered through a search peer
            return Optional.empty();
        }
        final Optional<DiscoveryNode> peer = recoveringCopy.relocatingNodeId() != null
            ? relocationSource(state, recoveringCopy)
            : randomStartedSearchReplica(state, recoveringCopy, localNodeId, random);
        // checked for the chosen peer only: a peer without support means no sample, not another peer
        return peer.filter(supportsSampling);
    }

    private static Optional<DiscoveryNode> relocationSource(ClusterState state, ShardRouting recoveringCopy) {
        // empty when the node of the source left, which also ends the relocation
        return Optional.ofNullable(state.nodes().get(recoveringCopy.relocatingNodeId()));
    }

    private static Optional<DiscoveryNode> randomStartedSearchReplica(
        ClusterState state,
        ShardRouting recoveringCopy,
        String localNodeId,
        Random random
    ) {
        final IndexShardRoutingTable shardRoutingTable = PhantomQuerySamplers.findShardRoutingTable(state, recoveringCopy.shardId());
        if (shardRoutingTable == null) {
            return Optional.empty();
        }
        final List<DiscoveryNode> candidates = new ArrayList<>();
        for (ShardRouting replica : shardRoutingTable.replicaShardsWithState(ShardRoutingState.STARTED)) {
            if (replica.isSearchable() && replica.currentNodeId().equals(localNodeId) == false) {
                final DiscoveryNode node = state.nodes().get(replica.currentNodeId());
                if (node != null) {
                    candidates.add(node);
                }
            }
        }
        return candidates.isEmpty() ? Optional.empty() : Optional.of(candidates.get(random.nextInt(candidates.size())));
    }
}
