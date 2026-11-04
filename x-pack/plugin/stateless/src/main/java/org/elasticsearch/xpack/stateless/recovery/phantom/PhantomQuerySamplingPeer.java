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
import org.elasticsearch.action.support.ChannelActionListener;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.stateless.recovery.phantom.PhantomQuerySamplingActions.CollectSampleRequest;
import org.elasticsearch.xpack.stateless.recovery.phantom.PhantomQuerySamplingActions.CollectSampleResponse;
import org.elasticsearch.xpack.stateless.recovery.phantom.PhantomQuerySamplingActions.StartSamplingRequest;
import org.elasticsearch.xpack.stateless.recovery.phantom.PhantomQuerySamplingActions.StartSamplingResponse;

import java.util.List;
import java.util.concurrent.Executor;

/**
 * Declines rather than fails whenever it cannot sample, so that the recovering copy continues without a sample.
 *
 * <p>
 * It checks its current cluster state without waiting for it to catch up with the one that added the recovering copy, unlike
 * {@code start_handoff}, which needs the answer for correctness: here a decline only costs a missed warm-up.
 */
public class PhantomQuerySamplingPeer {

    private static final Logger logger = LogManager.getLogger(PhantomQuerySamplingPeer.class);

    private final ClusterService clusterService;
    private final IndicesService indicesService;
    private final PhantomQuerySamplers samplers;
    private final PhantomQueriesSettings settings;
    private final NamedWriteableRegistry namedWriteableRegistry;

    public PhantomQuerySamplingPeer(
        ClusterService clusterService,
        IndicesService indicesService,
        PhantomQuerySamplers samplers,
        PhantomQueriesSettings settings,
        NamedWriteableRegistry namedWriteableRegistry
    ) {
        this.clusterService = clusterService;
        this.indicesService = indicesService;
        this.samplers = samplers;
        this.settings = settings;
        this.namedWriteableRegistry = namedWriteableRegistry;
    }

    public void registerHandlers(TransportService transportService, Executor executor) {
        transportService.registerRequestHandler(
            PhantomQuerySamplingActions.START_SAMPLING_ACTION_NAME,
            executor,
            false,
            false,
            StartSamplingRequest::new,
            (request, channel, task) -> handleStartSampling(request, new ChannelActionListener<>(channel))
        );
        transportService.registerRequestHandler(
            PhantomQuerySamplingActions.COLLECT_SAMPLE_ACTION_NAME,
            executor,
            false,
            false,
            CollectSampleRequest::new,
            (request, channel, task) -> handleCollectSample(request, new ChannelActionListener<>(channel))
        );
    }

    public void handleStartSampling(StartSamplingRequest request, ActionListener<StartSamplingResponse> listener) {
        ActionListener.completeWith(listener, () -> new StartSamplingResponse(startSampling(request)));
    }

    private boolean startSampling(StartSamplingRequest request) {
        final ShardId shardId = request.shardId();
        final String targetAllocationId = request.targetAllocationId();
        final String declineReason = declineReason(shardId, targetAllocationId);
        if (declineReason != null) {
            logger.debug("{} declined to sample for the copy [{}]: {}", shardId, targetAllocationId, declineReason);
            return false;
        }
        // the samplers check again, because the copy may have started or the setting may have changed since
        return samplers.startSampling(shardId, targetAllocationId);
    }

    @Nullable
    private String declineReason(ShardId shardId, String targetAllocationId) {
        if (settings.enabled() == false) {
            return "phantom queries are disabled";
        }
        if (hasActiveSearchReplica(shardId) == false) {
            return "no active search replica of the shard on this node";
        }
        if (isInitializingSearchReplica(shardId, targetAllocationId) == false) {
            return "it is not an initializing search replica of the shard";
        }
        return null;
    }

    private boolean hasActiveSearchReplica(ShardId shardId) {
        final IndexService indexService = indicesService.indexService(shardId.getIndex());
        final IndexShard indexShard = indexService == null ? null : indexService.getShardOrNull(shardId.id());
        if (indexShard == null) {
            return false;
        }
        final ShardRouting routing = indexShard.routingEntry();
        return routing.active() && isSearchReplica(routing) && indexShard.state() == IndexShardState.STARTED;
    }

    private boolean isInitializingSearchReplica(ShardId shardId, String allocationId) {
        final ShardRouting routing = PhantomQuerySamplers.findReplica(clusterService.state(), shardId, allocationId);
        return routing != null && routing.initializing() && isSearchReplica(routing);
    }

    private static boolean isSearchReplica(ShardRouting routing) {
        return routing.primary() == false && routing.isSearchable() && routing.isPromotableToPrimary() == false;
    }

    public void handleCollectSample(CollectSampleRequest request, ActionListener<CollectSampleResponse> listener) {
        ActionListener.completeWith(listener, () -> {
            final List<EncodedQueryRequest> requests = samplers.collect(request.shardId(), request.targetAllocationId());
            return new CollectSampleResponse(
                requests.isEmpty() ? PhantomQuerySample.EMPTY : new PhantomQuerySample(requests, namedWriteableRegistry)
            );
        });
    }
}
