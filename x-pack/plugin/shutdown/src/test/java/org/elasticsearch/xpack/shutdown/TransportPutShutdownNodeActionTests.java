/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.shutdown;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.ClusterStateTaskExecutor.TaskContext;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.NodesShutdownMetadata;
import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata;
import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata.Type;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterServiceTaskQueue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockUtils;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.shutdown.TransportPutShutdownNodeAction.PutShutdownNodeExecutor;
import org.elasticsearch.xpack.shutdown.TransportPutShutdownNodeAction.PutShutdownNodeTask;
import org.junit.Before;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.core.Strings.format;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

public class TransportPutShutdownNodeActionTests extends ESTestCase {

    private ClusterService clusterService;
    private TransportPutShutdownNodeAction action;

    // must use member mock for generic
    @Mock
    private TaskContext<PutShutdownNodeTask> taskContext;

    @Mock
    private MasterServiceTaskQueue<PutShutdownNodeTask> taskQueue;

    @Before
    public void init() {
        MockitoAnnotations.openMocks(this);
        // TODO: it takes almost 2 seconds to create these mocks....WHY?!?
        var threadPool = mock(ThreadPool.class);
        var transportService = MockUtils.setupTransportServiceWithThreadpoolExecutor(threadPool);
        clusterService = mock(ClusterService.class);
        var allocationService = mock(AllocationService.class);
        when(allocationService.reroute(any(ClusterState.class), anyString(), any())).then(invocation -> invocation.getArgument(0));
        var actionFilters = mock(ActionFilters.class);
        when(clusterService.createTaskQueue(any(), any(), Mockito.<ClusterStateTaskExecutor<PutShutdownNodeTask>>any())).thenReturn(
            taskQueue
        );
        action = new TransportPutShutdownNodeAction(transportService, clusterService, allocationService, threadPool, actionFilters);
    }

    public void testNoop() throws Exception {
        var type = randomFrom(Type.REMOVE, Type.REPLACE, Type.RESTART);
        var allocationDelay = type == Type.RESTART ? TimeValue.timeValueMinutes(randomIntBetween(1, 3)) : null;
        var targetNodeName = type == Type.REPLACE ? randomAlphaOfLength(5) : null;
        var request = new PutShutdownNodeAction.Request(
            TEST_REQUEST_TIMEOUT,
            TEST_REQUEST_TIMEOUT,
            "node1",
            type,
            "sunsetting",
            allocationDelay,
            targetNodeName,
            null
        );
        var dummyNode = new DiscoveryNode(targetNodeName, "node1", "eph-node1", "abc", "abc", null, Map.of(), Set.of(), null);
        var state = ClusterState.builder(ClusterState.EMPTY_STATE).nodes(DiscoveryNodes.builder().add(dummyNode).build()).build();
        action.masterOperation(null, request, state, ActionListener.noop());
        var updateTask = ArgumentCaptor.forClass(PutShutdownNodeTask.class);
        var taskExecutor = ArgumentCaptor.forClass(PutShutdownNodeExecutor.class);
        verify(clusterService).createTaskQueue(any(), any(), taskExecutor.capture());
        verify(taskQueue).submitTask(any(), updateTask.capture(), any());
        when(taskContext.getTask()).thenReturn(updateTask.getValue());
        ClusterState stableState = taskExecutor.getValue()
            .execute(new ClusterStateTaskExecutor.BatchExecutionContext<>(state, List.of(taskContext), () -> null));

        // run the request again, there should be no call to submit an update task
        clearTaskQueueInvocations();
        action.masterOperation(null, request, stableState, ActionListener.noop());
        verifyNoInteractions(taskQueue);

        // run the request again with empty state, the update task should return the same state
        action.masterOperation(null, request, ClusterState.EMPTY_STATE, ActionListener.noop());
        verify(taskQueue).submitTask(any(), updateTask.capture(), any());
        when(taskContext.getTask()).thenReturn(updateTask.getValue());
        ClusterState gotState = taskExecutor.getValue()
            .execute(new ClusterStateTaskExecutor.BatchExecutionContext<>(stableState, List.of(taskContext), () -> null));
        assertThat(gotState, sameInstance(stableState));
    }

    @SuppressWarnings("unchecked")
    private void clearTaskQueueInvocations() {
        clearInvocations(taskQueue);
    }

    public void testGracePeriodOnlyForSigterm() throws Exception {
        Arrays.stream(Type.values()).filter(type -> type != Type.SIGTERM).forEach(type -> {
            var allocationDelay = type == Type.RESTART ? TimeValue.timeValueMinutes(randomIntBetween(1, 3)) : null;
            var targetNodeName = type == Type.REPLACE ? randomAlphaOfLength(5) : null;
            assertThat(
                format("type [%s] should work without grace period", type),
                new PutShutdownNodeAction.Request(
                    TEST_REQUEST_TIMEOUT,
                    TEST_REQUEST_TIMEOUT,
                    "node1",
                    type,
                    "test",
                    allocationDelay,
                    targetNodeName,
                    null
                ),
                notNullValue()
            );
            ActionRequestValidationException arve = new PutShutdownNodeAction.Request(
                TEST_REQUEST_TIMEOUT,
                TEST_REQUEST_TIMEOUT,
                "node1",
                type,
                "test",
                allocationDelay,
                targetNodeName,
                TimeValue.timeValueMinutes(5)
            ).validate();
            assertThat(
                format("type [%s] validation message should be due to grace period", type),
                arve.getMessage(),
                containsString("grace period is only valid for SIGTERM type shutdowns")
            );
        });

        assertThat(
            new PutShutdownNodeAction.Request(
                TEST_REQUEST_TIMEOUT,
                TEST_REQUEST_TIMEOUT,
                "node1",
                Type.SIGTERM,
                "test",
                null,
                null,
                TimeValue.timeValueMinutes(5)
            ).validate(),
            nullValue()
        );

        assertThat(
            new PutShutdownNodeAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, "node1", Type.SIGTERM, "test", null, null, null)
                .validate()
                .getMessage(),
            containsString("grace period is required for SIGTERM shutdowns")
        );
    }

    public void testSigtermGracePeriodChangeIsNotNoop() throws Exception {
        var shortGracePeriod = TimeValue.timeValueMinutes(14);
        var longGracePeriod = TimeValue.timeValueHours(1);
        var state = stateWithNode();

        state = executePutShutdown(state, sigtermRequest(shortGracePeriod));
        assertThat(state.metadata().nodeShutdowns().get("node1").getGracePeriod(), equalTo(shortGracePeriod));

        // the same request again is a no-op
        clearTaskQueueInvocations();
        action.masterOperation(null, sigtermRequest(shortGracePeriod), state, ActionListener.noop());
        verifyNoInteractions(taskQueue);

        // a request that only changes the grace period is not a no-op
        action.masterOperation(null, sigtermRequest(longGracePeriod), state, ActionListener.noop());
        verify(taskQueue).submitTask(any(), any(PutShutdownNodeTask.class), any());

        state = executePutShutdown(state, sigtermRequest(longGracePeriod));
        var updated = state.metadata().nodeShutdowns().get("node1");
        assertThat(updated.getType(), equalTo(Type.SIGTERM));
        assertThat(updated.getGracePeriod(), equalTo(longGracePeriod));
    }

    public void testSigtermUpdateKeepsStartedAtForSameEphemeralId() throws Exception {
        var shortGracePeriod = TimeValue.timeValueMinutes(14);
        var longGracePeriod = TimeValue.timeValueHours(1);
        // use a start time in the past so that a reset to the current time is detectable
        var startedAtMillis = System.currentTimeMillis() - TimeValue.timeValueHours(2).millis();
        var existing = shutdownMetadata(Type.SIGTERM, "eph-node1", shortGracePeriod, startedAtMillis);

        var state = executePutShutdown(stateWithNode(existing), sigtermRequest(longGracePeriod));

        var updated = state.metadata().nodeShutdowns().get("node1");
        assertThat(updated.getGracePeriod(), equalTo(longGracePeriod));
        assertThat(updated.getStartedAtMillis(), equalTo(startedAtMillis));
    }

    public void testSigtermUpdateResetsStartedAtForDifferentEphemeralId() throws Exception {
        var shortGracePeriod = TimeValue.timeValueMinutes(14);
        var longGracePeriod = TimeValue.timeValueHours(1);
        var startedAtMillis = System.currentTimeMillis() - TimeValue.timeValueHours(2).millis();
        // the node restarted since the shutdown record was written, so the record belongs to the previous node instance
        var existing = shutdownMetadata(Type.SIGTERM, "eph-previous-node1", shortGracePeriod, startedAtMillis);

        var state = executePutShutdown(stateWithNode(existing), sigtermRequest(longGracePeriod));

        var updated = state.metadata().nodeShutdowns().get("node1");
        assertThat(updated.getNodeEphemeralId(), equalTo("eph-node1"));
        assertThat(updated.getGracePeriod(), equalTo(longGracePeriod));
        assertThat(updated.getStartedAtMillis(), greaterThan(startedAtMillis));
    }

    public void testSigtermRequestResetsStartedAtWhenExistingTypeIsNotSigterm() throws Exception {
        var startedAtMillis = System.currentTimeMillis() - TimeValue.timeValueHours(2).millis();
        var existing = shutdownMetadata(Type.REMOVE, "eph-node1", null, startedAtMillis);

        var state = executePutShutdown(stateWithNode(existing), sigtermRequest(TimeValue.timeValueMinutes(14)));

        var updated = state.metadata().nodeShutdowns().get("node1");
        assertThat(updated.getType(), equalTo(Type.SIGTERM));
        assertThat(updated.getStartedAtMillis(), greaterThan(startedAtMillis));
    }

    private static PutShutdownNodeAction.Request sigtermRequest(TimeValue gracePeriod) {
        return new PutShutdownNodeAction.Request(
            TEST_REQUEST_TIMEOUT,
            TEST_REQUEST_TIMEOUT,
            "node1",
            Type.SIGTERM,
            "node sigterm",
            null,
            null,
            gracePeriod
        );
    }

    private static SingleNodeShutdownMetadata shutdownMetadata(
        Type type,
        String nodeEphemeralId,
        TimeValue gracePeriod,
        long startedAtMillis
    ) {
        return SingleNodeShutdownMetadata.builder()
            .setNodeId("node1")
            .setNodeEphemeralId(nodeEphemeralId)
            .setType(type)
            .setReason("node sigterm")
            .setStartedAtMillis(startedAtMillis)
            .setNodeSeen(true)
            .setGracePeriod(gracePeriod)
            .build();
    }

    private static ClusterState stateWithNode() {
        var node = new DiscoveryNode("node1-name", "node1", "eph-node1", "abc", "abc", null, Map.of(), Set.of(), null);
        return ClusterState.builder(ClusterState.EMPTY_STATE).nodes(DiscoveryNodes.builder().add(node).build()).build();
    }

    private static ClusterState stateWithNode(SingleNodeShutdownMetadata existing) {
        return ClusterState.builder(stateWithNode())
            .metadata(
                Metadata.builder().putCustom(NodesShutdownMetadata.TYPE, new NodesShutdownMetadata(Map.of(existing.getNodeId(), existing)))
            )
            .build();
    }

    /**
     * Runs the cluster state update that {@link TransportPutShutdownNodeAction} would submit for the request, without a master service.
     */
    private ClusterState executePutShutdown(ClusterState state, PutShutdownNodeAction.Request request) throws Exception {
        var taskExecutor = ArgumentCaptor.forClass(PutShutdownNodeExecutor.class);
        verify(clusterService).createTaskQueue(any(), any(), taskExecutor.capture());
        when(taskContext.getTask()).thenReturn(new PutShutdownNodeTask(request, ActionListener.noop()));
        return taskExecutor.getValue()
            .execute(new ClusterStateTaskExecutor.BatchExecutionContext<>(state, List.of(taskContext), () -> null));
    }
}
