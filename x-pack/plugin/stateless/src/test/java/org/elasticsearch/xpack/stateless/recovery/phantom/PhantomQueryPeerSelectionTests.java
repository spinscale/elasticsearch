/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.recovery.phantom;

import org.elasticsearch.action.support.replication.ClusterStateCreationUtils;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;

import static org.elasticsearch.cluster.metadata.Metadata.DEFAULT_PROJECT_ID;
import static org.elasticsearch.xpack.stateless.recovery.phantom.PhantomQueryTestUtils.INDEX_NAME;
import static org.elasticsearch.xpack.stateless.recovery.phantom.PhantomQueryTestUtils.shards;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.in;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

public class PhantomQueryPeerSelectionTests extends ESTestCase {

    private static final Predicate<DiscoveryNode> SUPPORTED = node -> true;

    private static ClusterState stateWithReplicas(ShardRoutingState... replicas) {
        final List<Tuple<ShardRoutingState, ShardRouting.Role>> replicaStates = new ArrayList<>();
        for (ShardRoutingState state : replicas) {
            replicaStates.add(new Tuple<>(state, ShardRouting.Role.SEARCH_ONLY));
        }
        return ClusterStateCreationUtils.state(
            DEFAULT_PROJECT_ID,
            INDEX_NAME,
            true,
            ShardRoutingState.STARTED,
            ShardRouting.Role.INDEX_ONLY,
            replicaStates
        );
    }

    private static ShardRouting initializingCopy(ClusterState state) {
        return shards(state).stream().filter(ShardRouting::initializing).findFirst().orElseThrow();
    }

    private static Set<String> nodeIdsOfStartedSearchCopies(ClusterState state) {
        final Set<String> nodeIds = new HashSet<>();
        shards(state).stream().filter(routing -> routing.started() && routing.isSearchable()).forEach(r -> nodeIds.add(r.currentNodeId()));
        return nodeIds;
    }

    private static Optional<DiscoveryNode> select(ClusterState state, ShardRouting recoveringCopy, Predicate<DiscoveryNode> supported) {
        return PhantomQueryPeerSelection.selectPeer(state, recoveringCopy, recoveringCopy.currentNodeId(), random(), supported);
    }

    public void testRelocationTargetSelectsTheRelocationSource() {
        final ClusterState state = PhantomQueryTestUtils.clusterStateWithRelocatingSearchReplica();
        final ShardRouting source = shards(state).stream().filter(ShardRouting::relocating).findFirst().orElseThrow();
        final ShardRouting target = source.getTargetRelocatingShard();

        final Optional<DiscoveryNode> peer = select(state, target, SUPPORTED);

        assertThat(peer.isPresent(), is(true));
        assertThat(peer.get().getId(), equalTo(source.currentNodeId()));
        assertThat(peer.get().getId(), equalTo(target.relocatingNodeId()));
    }

    public void testRelocationTargetSelectsTheSourceWhateverOtherCopiesAreStarted() {
        final ClusterState state = stateWithReplicas(ShardRoutingState.STARTED, ShardRoutingState.STARTED, ShardRoutingState.RELOCATING);
        final ShardRouting source = shards(state).stream().filter(ShardRouting::relocating).findFirst().orElseThrow();
        final ShardRouting target = source.getTargetRelocatingShard();

        for (int i = 0; i < 20; i++) {
            assertThat(select(state, target, SUPPORTED).orElseThrow().getId(), equalTo(source.currentNodeId()));
        }
    }

    public void testRelocationTargetSelectsNobodyWhenTheSourceLeftTheCluster() {
        final ClusterState state = PhantomQueryTestUtils.clusterStateWithRelocatingSearchReplica();
        final ShardRouting source = shards(state).stream().filter(ShardRouting::relocating).findFirst().orElseThrow();
        final DiscoveryNodes.Builder nodes = DiscoveryNodes.builder(state.nodes()).remove(source.currentNodeId());
        // the local and the master node are not a part of what is tested, but the nodes of a state have to know them
        if (source.currentNodeId().equals(state.nodes().getMasterNodeId())) {
            nodes.masterNodeId(null);
        }
        if (source.currentNodeId().equals(state.nodes().getLocalNodeId())) {
            nodes.localNodeId(null);
        }
        final ClusterState withoutSourceNode = ClusterState.builder(state).nodes(nodes).build();

        assertThat(select(withoutSourceNode, source.getTargetRelocatingShard(), SUPPORTED), is(Optional.empty()));
    }

    public void testNewCopySelectsTheOnlyStartedSearchCopy() {
        final ClusterState state = stateWithReplicas(ShardRoutingState.STARTED, ShardRoutingState.INITIALIZING);
        final ShardRouting recovering = initializingCopy(state);
        assertThat(recovering.relocatingNodeId(), equalTo(null));

        final Optional<DiscoveryNode> peer = select(state, recovering, SUPPORTED);

        assertThat(peer.orElseThrow().getId(), is(in(nodeIdsOfStartedSearchCopies(state))));
        assertThat(nodeIdsOfStartedSearchCopies(state), hasSize(1));
    }

    public void testNewCopySelectsOneOfTheStartedSearchCopiesAndSpreadsTheChoice() {
        final int startedCopies = randomIntBetween(2, 4);
        final ShardRoutingState[] replicas = new ShardRoutingState[startedCopies + 1];
        for (int i = 0; i < startedCopies; i++) {
            replicas[i] = ShardRoutingState.STARTED;
        }
        replicas[startedCopies] = ShardRoutingState.INITIALIZING;
        final ClusterState state = stateWithReplicas(replicas);
        final ShardRouting recovering = initializingCopy(state);
        final Set<String> candidates = nodeIdsOfStartedSearchCopies(state);
        assertThat(candidates, hasSize(startedCopies));

        final Set<String> selected = new HashSet<>();
        for (int i = 0; i < 200; i++) {
            final DiscoveryNode peer = select(state, recovering, SUPPORTED).orElseThrow();
            assertThat(peer.getId(), is(in(candidates)));
            assertThat(peer.getId(), is(not(equalTo(recovering.currentNodeId()))));
            selected.add(peer.getId());
        }
        assertThat("many recoveries do not all sample from the same copy", selected, equalTo(candidates));
    }

    // searchable like a stateful primary, so that only it being a primary rules it out
    private static ClusterState stateWithSearchablePrimary(ShardRoutingState... replicas) {
        final List<Tuple<ShardRoutingState, ShardRouting.Role>> replicaStates = new ArrayList<>();
        for (ShardRoutingState state : replicas) {
            replicaStates.add(new Tuple<>(state, ShardRouting.Role.SEARCH_ONLY));
        }
        return ClusterStateCreationUtils.state(
            DEFAULT_PROJECT_ID,
            INDEX_NAME,
            true,
            ShardRoutingState.STARTED,
            ShardRouting.Role.DEFAULT,
            replicaStates
        );
    }

    public void testNewCopyNeverSelectsThePrimary() {
        final ClusterState state = stateWithSearchablePrimary(
            ShardRoutingState.STARTED,
            ShardRoutingState.STARTED,
            ShardRoutingState.INITIALIZING
        );
        final ShardRouting recovering = initializingCopy(state);
        final ShardRouting primary = state.routingTable(DEFAULT_PROJECT_ID)
            .shardRoutingTable(PhantomQueryTestUtils.shardId())
            .primaryShard();
        assertThat(primary.started() && primary.isSearchable(), is(true));
        final Set<String> replicaNodeIds = new HashSet<>();
        shards(state).stream()
            .filter(routing -> routing.primary() == false && routing.started())
            .forEach(r -> replicaNodeIds.add(r.currentNodeId()));
        assertThat(replicaNodeIds, hasSize(2));

        final Set<String> selected = new HashSet<>();
        for (int i = 0; i < 200; i++) {
            selected.add(select(state, recovering, SUPPORTED).orElseThrow().getId());
        }

        assertThat(selected, equalTo(replicaNodeIds));
        assertThat(selected.contains(primary.currentNodeId()), is(false));
    }

    public void testNewCopySelectsNobodyWhenOnlyThePrimaryIsStarted() {
        final ClusterState state = stateWithSearchablePrimary(ShardRoutingState.INITIALIZING);

        assertThat(select(state, initializingCopy(state), SUPPORTED), is(Optional.empty()));
    }

    public void testPrimaryHasNoPeer() {
        final ClusterState state = ClusterStateCreationUtils.state(
            DEFAULT_PROJECT_ID,
            INDEX_NAME,
            true,
            ShardRoutingState.RELOCATING,
            randomFrom(ShardRouting.Role.INDEX_ONLY, ShardRouting.Role.DEFAULT),
            List.of()
        );
        final ShardRouting source = shards(state).stream().filter(ShardRouting::primary).findFirst().orElseThrow();
        final ShardRouting target = source.getTargetRelocatingShard();
        assertThat(target.relocatingNodeId(), equalTo(source.currentNodeId()));

        assertThat("not the relocation source", select(state, target, SUPPORTED), is(Optional.empty()));
        assertThat(select(state, source, SUPPORTED), is(Optional.empty()));
    }

    public void testNewCopyDoesNotSelectCopiesThatAreNotStarted() {
        final ClusterState state = stateWithReplicas(ShardRoutingState.INITIALIZING, ShardRoutingState.INITIALIZING);

        assertThat(shards(state).stream().filter(ShardRouting::started).toList(), hasSize(1));
        for (ShardRouting recovering : shards(state).stream().filter(ShardRouting::initializing).toList()) {
            assertThat(select(state, recovering, SUPPORTED), is(Optional.empty()));
        }
    }

    public void testNewCopyDoesNotSelectARelocatingSource() {
        // it is still serving, but it leaves as soon as its own target started
        final ClusterState state = stateWithReplicas(ShardRoutingState.RELOCATING, ShardRoutingState.INITIALIZING);
        final ShardRouting recovering = shards(state).stream()
            .filter(routing -> routing.initializing() && routing.relocatingNodeId() == null)
            .findFirst()
            .orElseThrow();

        assertThat(select(state, recovering, SUPPORTED), is(Optional.empty()));
    }

    public void testNewCopyOfAShardWithoutAnotherSearchCopySelectsNobody() {
        final ClusterState state = stateWithReplicas(ShardRoutingState.INITIALIZING);

        assertThat(select(state, initializingCopy(state), SUPPORTED), is(Optional.empty()));
    }

    public void testSelectsNobodyForAShardThatIsNotInTheClusterState() {
        final ClusterState state = stateWithReplicas(ShardRoutingState.STARTED, ShardRoutingState.INITIALIZING);
        final ShardRouting recovering = initializingCopy(state);

        assertThat(select(PhantomQueryTestUtils.withoutIndex(), recovering, SUPPORTED), is(Optional.empty()));
    }

    public void testSelectsNobodyWhenTheConnectionToTheRelocationSourceDoesNotSupportSampling() {
        final ClusterState state = PhantomQueryTestUtils.clusterStateWithRelocatingSearchReplica();
        final ShardRouting source = shards(state).stream().filter(ShardRouting::relocating).findFirst().orElseThrow();
        final List<String> asked = new ArrayList<>();

        final Optional<DiscoveryNode> peer = select(state, source.getTargetRelocatingShard(), node -> {
            asked.add(node.getId());
            return false;
        });

        assertThat(peer, is(Optional.empty()));
        assertThat("only the relocation source is asked", asked, equalTo(List.of(source.currentNodeId())));
    }

    public void testSelectsNobodyWhenTheConnectionToTheChosenCopyDoesNotSupportSampling() {
        final ClusterState state = stateWithReplicas(ShardRoutingState.STARTED, ShardRoutingState.STARTED, ShardRoutingState.INITIALIZING);
        final ShardRouting recovering = initializingCopy(state);
        final List<String> asked = new ArrayList<>();

        final Optional<DiscoveryNode> peer = select(state, recovering, node -> {
            asked.add(node.getId());
            return false;
        });

        assertThat(peer, is(Optional.empty()));
        assertThat("the chosen copy is the only one that is asked", asked, hasSize(1));
        assertThat(asked.get(0), is(in(nodeIdsOfStartedSearchCopies(state))));
    }

    public void testSelectsTheChosenCopyThatSupportsSampling() {
        final ClusterState state = stateWithReplicas(ShardRoutingState.STARTED, ShardRoutingState.INITIALIZING);
        final ShardRouting recovering = initializingCopy(state);
        final List<DiscoveryNode> asked = new ArrayList<>();

        final Optional<DiscoveryNode> peer = select(state, recovering, node -> {
            asked.add(node);
            return true;
        });

        assertThat(asked, hasSize(1));
        assertThat(peer, equalTo(Optional.of(asked.get(0))));
    }

    public void testNobodyIsAskedWhenThereIsNoPeer() {
        final ClusterState state = stateWithReplicas(ShardRoutingState.INITIALIZING);
        final List<DiscoveryNode> asked = new ArrayList<>();

        select(state, initializingCopy(state), node -> {
            asked.add(node);
            return true;
        });

        assertThat(asked, empty());
    }
}
