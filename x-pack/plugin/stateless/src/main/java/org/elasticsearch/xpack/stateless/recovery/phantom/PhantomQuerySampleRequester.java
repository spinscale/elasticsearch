/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.recovery.phantom;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.NodeNotConnectedException;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.stateless.cache.SearchRecoveryWarmingWindows;
import org.elasticsearch.xpack.stateless.recovery.phantom.PhantomQuerySamplingActions.CollectSampleRequest;
import org.elasticsearch.xpack.stateless.recovery.phantom.PhantomQuerySamplingActions.CollectSampleResponse;
import org.elasticsearch.xpack.stateless.recovery.phantom.PhantomQuerySamplingActions.StartSamplingRequest;
import org.elasticsearch.xpack.stateless.recovery.phantom.PhantomQuerySamplingActions.StartSamplingResponse;

import java.util.OptionalLong;
import java.util.Random;
import java.util.concurrent.Executor;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import static org.elasticsearch.core.Strings.format;

/**
 * Never fails the recovery: whatever goes wrong ends in no sample and is logged at DEBUG. Sampling starts before the warming wait so that
 * the peer samples while the recovery waits. The sample is only collected while the warming window has time left, because one that cannot
 * be replayed within it would only hold the peer's memory until its lease expires.
 */
public class PhantomQuerySampleRequester {

    private static final Logger logger = LogManager.getLogger(PhantomQuerySampleRequester.class);

    private final TransportService transportService;
    private final ClusterService clusterService;
    private final PhantomQueriesSettings settings;
    private final SearchRecoveryWarmingWindows warmingWindows;
    private final Executor responseExecutor;
    private final TimeValue requestTimeout;
    private final LongSupplier relativeTimeInMillis;
    private final Supplier<Random> random;

    public PhantomQuerySampleRequester(
        TransportService transportService,
        ClusterService clusterService,
        PhantomQueriesSettings settings,
        SearchRecoveryWarmingWindows warmingWindows,
        Executor responseExecutor,
        TimeValue requestTimeout
    ) {
        this(
            transportService,
            clusterService,
            settings,
            warmingWindows,
            responseExecutor,
            requestTimeout,
            transportService.getThreadPool()::relativeTimeInMillis,
            Randomness::get
        );
    }

    PhantomQuerySampleRequester(
        TransportService transportService,
        ClusterService clusterService,
        PhantomQueriesSettings settings,
        SearchRecoveryWarmingWindows warmingWindows,
        Executor responseExecutor,
        TimeValue requestTimeout,
        LongSupplier relativeTimeInMillis,
        Supplier<Random> random
    ) {
        this.transportService = transportService;
        this.clusterService = clusterService;
        this.settings = settings;
        this.warmingWindows = warmingWindows;
        this.responseExecutor = responseExecutor;
        this.requestTimeout = requestTimeout;
        this.relativeTimeInMillis = relativeTimeInMillis;
        this.random = random;
    }

    public Sampling startSampling(Task parentTask, ShardRouting recoveringCopy) {
        try {
            if (settings.enabled() == false) {
                return Sampling.NONE;
            }
            final ShardId shardId = recoveringCopy.shardId();
            final String allocationId = recoveringCopy.allocationId().getId();
            final DiscoveryNode peer = PhantomQueryPeerSelection.selectPeer(
                clusterService.state(),
                recoveringCopy,
                clusterService.localNode().getId(),
                random.get(),
                this::supportsSampling
            ).orElse(null);
            if (peer == null) {
                logger.debug("{} there is no peer to sample for the copy [{}]", shardId, allocationId);
                return Sampling.NONE;
            }
            final ActionListener<StartSamplingResponse> logOutcome = ActionListener.wrap(
                response -> logger.debug(
                    "{} the peer [{}] {} to sample for the copy [{}]",
                    shardId,
                    peer.getName(),
                    response.accepted() ? "agreed" : "declined",
                    allocationId
                ),
                e -> logger.debug(
                    () -> format("%s the peer [%s] did not start to sample for the copy [%s]", shardId, peer.getName(), allocationId),
                    e
                )
            );
            transportService.sendChildRequest(
                transportService.getConnection(peer),
                PhantomQuerySamplingActions.START_SAMPLING_ACTION_NAME,
                new StartSamplingRequest(shardId, allocationId),
                parentTask,
                TransportRequestOptions.timeout(requestTimeout),
                new ActionListenerResponseHandler<>(logOutcome, StartSamplingResponse::new, responseExecutor)
            );
            return new Sampling(this, shardId, allocationId, peer);
        } catch (Exception e) {
            logger.debug(() -> format("%s failed to ask a peer to sample for the copy", recoveringCopy.shardId()), e);
            return Sampling.NONE;
        }
    }

    private boolean supportsSampling(DiscoveryNode node) {
        try {
            return PhantomQuerySamplingActions.isSupportedBy(transportService.getConnection(node).getTransportVersion());
        } catch (NodeNotConnectedException e) {
            return false;
        }
    }

    public static final class Sampling {

        static final Sampling NONE = new Sampling(null, null, null, null);

        @Nullable
        private final PhantomQuerySampleRequester requester;
        private final ShardId shardId;
        private final String allocationId;
        @Nullable
        private final DiscoveryNode peer;

        private Sampling(
            @Nullable PhantomQuerySampleRequester requester,
            ShardId shardId,
            String allocationId,
            @Nullable DiscoveryNode peer
        ) {
            this.requester = requester;
            this.shardId = shardId;
            this.allocationId = allocationId;
            this.peer = peer;
        }

        public void collectSample(Task parentTask, ActionListener<PhantomQuerySample> listener) {
            if (peer == null) {
                listener.onResponse(PhantomQuerySample.EMPTY);
                return;
            }
            try {
                final String reasonToSkip = requester.reasonToSkipCollecting(shardId, allocationId);
                if (reasonToSkip != null) {
                    logger.debug("{} not collecting the sample for the copy [{}]: {}", shardId, allocationId, reasonToSkip);
                    listener.onResponse(PhantomQuerySample.EMPTY);
                    return;
                }
                final ActionListener<PhantomQuerySample> neverFailing = listener.delegateResponse((l, e) -> {
                    logger.debug(
                        () -> format("%s failed to collect the sample of the copy [%s] from [%s]", shardId, allocationId, peer.getName()),
                        e
                    );
                    l.onResponse(PhantomQuerySample.EMPTY);
                });
                requester.transportService.sendChildRequest(
                    requester.transportService.getConnection(peer),
                    PhantomQuerySamplingActions.COLLECT_SAMPLE_ACTION_NAME,
                    new CollectSampleRequest(shardId, allocationId),
                    parentTask,
                    TransportRequestOptions.timeout(requester.requestTimeout),
                    new ActionListenerResponseHandler<>(neverFailing.<CollectSampleResponse>map(response -> {
                        logger.debug(
                            "{} collected a sample of [{}] requests for the copy [{}] from [{}]",
                            shardId,
                            response.sample().size(),
                            allocationId,
                            peer.getName()
                        );
                        return response.sample();
                    }), CollectSampleResponse::new, requester.responseExecutor)
                );
            } catch (Exception e) {
                logger.debug(
                    () -> format("%s failed to ask [%s] for the sample of the copy [%s]", shardId, peer.getName(), allocationId),
                    e
                );
                listener.onResponse(PhantomQuerySample.EMPTY);
            }
        }
    }

    @Nullable
    private String reasonToSkipCollecting(ShardId shardId, String allocationId) {
        if (settings.enabled() == false) {
            return "phantom queries were disabled";
        }
        final OptionalLong deadline = warmingWindows.relativeDeadlineMillis(shardId, allocationId);
        if (deadline.isEmpty()) {
            return "the recovery has no warming window";
        }
        if (deadline.getAsLong() - relativeTimeInMillis.getAsLong() <= 0) {
            return "the warming window has no time left";
        }
        return null;
    }
}
