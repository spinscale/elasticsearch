/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.recovery.phantom;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;

import static org.elasticsearch.indices.recovery.StatelessUnpromotableRelocationAction.TYPE;

/**
 * The requests are only sent over connections that support {@link #TRANSPORT_VERSION}, so no node has to read a request it does not know
 * and a cluster that is being upgraded exchanges no samples.
 */
public final class PhantomQuerySamplingActions {

    public static final TransportVersion TRANSPORT_VERSION = TransportVersion.fromName("phantom_query_sampling");

    public static final String START_SAMPLING_ACTION_NAME = TYPE.name() + "/phantom_queries/start_sampling";

    public static final String COLLECT_SAMPLE_ACTION_NAME = TYPE.name() + "/phantom_queries/collect_sample";

    private PhantomQuerySamplingActions() {}

    public static boolean isSupportedBy(TransportVersion connectionVersion) {
        return connectionVersion.supports(TRANSPORT_VERSION);
    }

    public static final class StartSamplingRequest extends ActionRequest {

        private final ShardId shardId;
        private final String targetAllocationId;

        public StartSamplingRequest(ShardId shardId, String targetAllocationId) {
            this.shardId = shardId;
            this.targetAllocationId = targetAllocationId;
        }

        public StartSamplingRequest(StreamInput in) throws IOException {
            super(in);
            this.shardId = new ShardId(in);
            this.targetAllocationId = in.readString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            shardId.writeTo(out);
            out.writeString(targetAllocationId);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        public ShardId shardId() {
            return shardId;
        }

        public String targetAllocationId() {
            return targetAllocationId;
        }
    }

    public static final class StartSamplingResponse extends ActionResponse {

        private final boolean accepted;

        public StartSamplingResponse(boolean accepted) {
            this.accepted = accepted;
        }

        public StartSamplingResponse(StreamInput in) throws IOException {
            this.accepted = in.readBoolean();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeBoolean(accepted);
        }

        public boolean accepted() {
            return accepted;
        }
    }

    public static final class CollectSampleRequest extends ActionRequest {

        private final ShardId shardId;
        private final String targetAllocationId;

        public CollectSampleRequest(ShardId shardId, String targetAllocationId) {
            this.shardId = shardId;
            this.targetAllocationId = targetAllocationId;
        }

        public CollectSampleRequest(StreamInput in) throws IOException {
            super(in);
            this.shardId = new ShardId(in);
            this.targetAllocationId = in.readString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            shardId.writeTo(out);
            out.writeString(targetAllocationId);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        public ShardId shardId() {
            return shardId;
        }

        public String targetAllocationId() {
            return targetAllocationId;
        }
    }

    public static final class CollectSampleResponse extends ActionResponse {

        private final PhantomQuerySample sample;

        public CollectSampleResponse(PhantomQuerySample sample) {
            this.sample = sample;
        }

        public CollectSampleResponse(StreamInput in) throws IOException {
            this.sample = new PhantomQuerySample(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            sample.writeTo(out);
        }

        public PhantomQuerySample sample() {
            return sample;
        }
    }
}
