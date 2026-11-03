/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.recovery;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.project.TestProjectResolvers;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.recovery.PeerRecoveryTargetService;
import org.elasticsearch.indices.recovery.RecoveriesCollection;
import org.elasticsearch.indices.recovery.RecoveryCancelledException;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.indices.recovery.RecoveryTarget;
import org.elasticsearch.indices.recovery.StatelessUnpromotableRelocationAction;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.IndexSettingsModule;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.stateless.StatelessComponents;
import org.elasticsearch.xpack.stateless.cache.SearchRecoveryWarmingWindows;
import org.elasticsearch.xpack.stateless.engine.translog.TranslogReplicator;
import org.elasticsearch.xpack.stateless.objectstore.ObjectStoreService;
import org.junit.After;
import org.junit.Before;

import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TransportStatelessUnpromotableRelocationActionTests extends ESTestCase {

    private static final long RECOVERY_ID = 42L;
    private static final long WINDOW_DEADLINE_MILLIS = 1_000L;

    private TestThreadPool threadPool;
    private SearchRecoveryWarmingWindows windows;
    private IndexShard indexShard;
    private ShardId shardId;
    private String allocationId;
    private TransportStatelessUnpromotableRelocationAction action;
    private final AtomicReference<ActionListener<Void>> preRecoveryListener = new AtomicReference<>();

    @Before
    public void setUpAction() {
        threadPool = new TestThreadPool(getTestName());
        windows = new SearchRecoveryWarmingWindows();
        shardId = new ShardId(randomIdentifier(), randomUUID(), randomIntBetween(0, 5));
        allocationId = randomIdentifier();

        final var transportService = mock(TransportService.class);
        when(transportService.getTaskManager()).thenReturn(mock(TaskManager.class));
        when(transportService.getThreadPool()).thenReturn(threadPool);

        final var clusterService = mock(ClusterService.class);
        when(clusterService.getSettings()).thenReturn(Settings.EMPTY);
        when(clusterService.getClusterSettings()).thenReturn(ClusterSettings.createBuiltInClusterSettings());
        when(clusterService.state()).thenReturn(ClusterState.EMPTY_STATE);

        final var newCopyRouting = TestShardRouting.newShardRouting(
            shardId,
            randomIdentifier(),
            false,
            ShardRoutingState.INITIALIZING,
            ShardRouting.Role.SEARCH_ONLY
        );
        assertThat(newCopyRouting.relocatingNodeId(), nullValue());

        indexShard = mock(IndexShard.class);
        when(indexShard.indexSettings()).thenReturn(IndexSettingsModule.newIndexSettings(shardId.getIndex(), Settings.EMPTY));
        when(indexShard.routingEntry()).thenReturn(newCopyRouting);
        doAnswer(invocation -> {
            assertTrue("one wait for warming", preRecoveryListener.compareAndSet(null, invocation.getArgument(0)));
            return null;
        }).when(indexShard).preRecovery(any());

        final var indexService = mock(IndexService.class);
        when(indexService.getShard(shardId.id())).thenReturn(indexShard);
        final var indicesService = mock(IndicesService.class);
        when(indicesService.indexServiceSafe(shardId.getIndex())).thenReturn(indexService);

        final var recoveryState = mock(RecoveryState.class);
        when(recoveryState.getIndex()).thenReturn(mock(RecoveryState.Index.class));
        final var recoveryTarget = mock(RecoveryTarget.class);
        when(recoveryTarget.state()).thenReturn(recoveryState);
        when(recoveryTarget.shardId()).thenReturn(shardId);
        final var peerRecoveryTargetService = mock(PeerRecoveryTargetService.class);
        when(peerRecoveryTargetService.getRecoveryRef(RECOVERY_ID, shardId)).thenReturn(new RecoveryRefWithoutRelease(recoveryTarget));

        action = new TransportStatelessUnpromotableRelocationAction(
            transportService,
            clusterService,
            new ActionFilters(Set.of()),
            indicesService,
            peerRecoveryTargetService,
            TestProjectResolvers.DEFAULT_PROJECT_ONLY,
            mock(SearchService.class),
            new PITRelocationService(),
            new StatelessComponents(mock(TranslogReplicator.class), mock(ObjectStoreService.class)),
            new PitRelocationMetrics(MeterRegistry.NOOP),
            windows
        );
    }

    private static class RecoveryRefWithoutRelease extends RecoveriesCollection.RecoveryRef {
        RecoveryRefWithoutRelease(RecoveryTarget recoveryTarget) {
            super(recoveryTarget);
        }

        @Override
        public void close() {}
    }

    @After
    public void terminateThreadPool() {
        terminate(threadPool);
    }

    private void startRecovery(ActionListener<ActionResponse.Empty> listener) {
        action.doExecute(
            mock(Task.class),
            new StatelessUnpromotableRelocationAction.Request(RECOVERY_ID, shardId, allocationId, randomNonNegativeLong()),
            listener
        );
    }

    private PlainActionFuture<ActionResponse.Empty> startRecovery() {
        final var response = new PlainActionFuture<ActionResponse.Empty>();
        startRecovery(response);
        return response;
    }

    private OptionalLong windowDeadline() {
        return windows.relativeDeadlineMillis(shardId, allocationId);
    }

    private void startWarmingWindow() {
        windows.startWindow(shardId, allocationId, WINDOW_DEADLINE_MILLIS);
    }

    public void testWindowIsRemovedWhenTheRecoveryCompletes() {
        startWarmingWindow();

        final var response = startRecovery();
        assertThat("the recovery waits for warming", response.isDone(), is(false));
        assertThat(windowDeadline(), equalTo(OptionalLong.of(WINDOW_DEADLINE_MILLIS)));

        preRecoveryListener.get().onResponse(null);
        assertThat(safeGet(response), equalTo(ActionResponse.Empty.INSTANCE));
        assertThat(windowDeadline(), equalTo(OptionalLong.empty()));
    }

    public void testWindowIsRemovedBeforeTheListenerIsCalled() {
        startWarmingWindow();
        final var seenByListener = new AtomicReference<OptionalLong>();

        startRecovery(ActionListener.running(() -> seenByListener.set(windowDeadline())));
        preRecoveryListener.get().onResponse(null);

        assertThat(seenByListener.get(), equalTo(OptionalLong.empty()));
    }

    public void testWindowIsRemovedWhenTheRecoveryFails() {
        startWarmingWindow();
        final var failure = new ElasticsearchException("simulated");

        final var thrown = safeAwaitFailure(ActionResponse.Empty.class, listener -> {
            startRecovery(listener);
            assertThat("the recovery waits for warming", windowDeadline(), equalTo(OptionalLong.of(WINDOW_DEADLINE_MILLIS)));
            preRecoveryListener.get().onFailure(failure);
        });

        assertSame(failure, thrown);
        assertThat(windowDeadline(), equalTo(OptionalLong.empty()));
    }

    public void testWindowIsRemovedWhenTheRecoveryIsCancelledDuringTheWait() {
        doNothing().doThrow(new RecoveryCancelledException(shardId, null, DiscoveryNodeUtils.create("target")))
            .when(indexShard)
            .ensureRecoveryNotCancelled();
        startWarmingWindow();

        safeAwaitFailure(RecoveryCancelledException.class, ActionResponse.Empty.class, listener -> {
            startRecovery(listener);
            assertThat("the recovery waits for warming", windowDeadline(), equalTo(OptionalLong.of(WINDOW_DEADLINE_MILLIS)));
            preRecoveryListener.get().onResponse(null);
        });

        assertThat(windowDeadline(), equalTo(OptionalLong.empty()));
    }

    public void testOtherWindowsAreKept() {
        final var otherAllocationId = randomValueOtherThan(allocationId, ESTestCase::randomIdentifier);
        final var otherShardId = new ShardId(randomIdentifier(), randomUUID(), randomIntBetween(0, 5));
        startWarmingWindow();
        windows.startWindow(shardId, otherAllocationId, 2_000L);
        windows.startWindow(otherShardId, allocationId, 3_000L);

        final var response = startRecovery();
        preRecoveryListener.get().onResponse(null);
        safeGet(response);

        assertThat(windowDeadline(), equalTo(OptionalLong.empty()));
        assertThat(windows.relativeDeadlineMillis(shardId, otherAllocationId), equalTo(OptionalLong.of(2_000L)));
        assertThat(windows.relativeDeadlineMillis(otherShardId, allocationId), equalTo(OptionalLong.of(3_000L)));
    }
}
