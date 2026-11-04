/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.recovery.phantom;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.recovery.StatelessUnpromotableRelocationAction;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xpack.stateless.recovery.phantom.PhantomQuerySamplingActions.CollectSampleRequest;
import org.elasticsearch.xpack.stateless.recovery.phantom.PhantomQuerySamplingActions.CollectSampleResponse;
import org.elasticsearch.xpack.stateless.recovery.phantom.PhantomQuerySamplingActions.StartSamplingRequest;
import org.elasticsearch.xpack.stateless.recovery.phantom.PhantomQuerySamplingActions.StartSamplingResponse;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.xpack.stateless.recovery.phantom.PhantomQueryTestUtils.encode;
import static org.elasticsearch.xpack.stateless.recovery.phantom.PhantomQueryTestUtils.namedWriteableRegistry;
import static org.elasticsearch.xpack.stateless.recovery.phantom.PhantomQueryTestUtils.queryRequest;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;

public class PhantomQuerySampleMessagesTests extends ESTestCase {

    private static ShardId randomShardId() {
        return new ShardId(randomIdentifier(), randomUUID(), randomIntBetween(0, 10));
    }

    public void testOnlyConnectionsOfTheVersionThatAddsSamplingSupportIt() {
        assertThat(PhantomQuerySamplingActions.isSupportedBy(TransportVersion.current()), is(true));
        assertThat(PhantomQuerySamplingActions.isSupportedBy(TransportVersion.minimumCompatible()), is(false));
        assertThat(
            PhantomQuerySamplingActions.isSupportedBy(
                TransportVersionUtils.randomVersionNotSupporting(PhantomQuerySamplingActions.TRANSPORT_VERSION)
            ),
            is(false)
        );
    }

    public void testActionNamesAreNextToStartHandoff() {
        final String prefix = StatelessUnpromotableRelocationAction.TYPE.name() + "/";
        assertThat(PhantomQuerySamplingActions.START_SAMPLING_ACTION_NAME, startsWith(prefix));
        assertThat(PhantomQuerySamplingActions.COLLECT_SAMPLE_ACTION_NAME, startsWith(prefix));
        assertThat(PhantomQuerySamplingActions.START_SAMPLING_ACTION_NAME, equalTo(prefix + "phantom_queries/start_sampling"));
        assertThat(PhantomQuerySamplingActions.COLLECT_SAMPLE_ACTION_NAME, equalTo(prefix + "phantom_queries/collect_sample"));
    }

    public void testStartSamplingRequestRoundTrips() throws IOException {
        final var request = new StartSamplingRequest(randomShardId(), randomUUID());

        final var read = copyWriteable(request, namedWriteableRegistry(), StartSamplingRequest::new);

        assertThat(read.shardId(), equalTo(request.shardId()));
        assertThat(read.targetAllocationId(), equalTo(request.targetAllocationId()));
        assertThat(read.validate(), nullValue());
    }

    public void testStartSamplingResponseRoundTrips() throws IOException {
        for (boolean accepted : new boolean[] { true, false }) {
            final var read = copyWriteable(new StartSamplingResponse(accepted), namedWriteableRegistry(), StartSamplingResponse::new);

            assertThat(read.accepted(), is(accepted));
        }
    }

    public void testCollectSampleRequestRoundTrips() throws IOException {
        final var request = new CollectSampleRequest(randomShardId(), randomUUID());

        final var read = copyWriteable(request, namedWriteableRegistry(), CollectSampleRequest::new);

        assertThat(read.shardId(), equalTo(request.shardId()));
        assertThat(read.targetAllocationId(), equalTo(request.targetAllocationId()));
        assertThat(read.validate(), nullValue());
    }

    public void testCollectSampleResponseRoundTripsTheSample() throws IOException {
        final var shardId = PhantomQueryTestUtils.shardId();
        final List<EncodedQueryRequest> requests = new ArrayList<>();
        for (int i = 0; i < randomIntBetween(0, 5); i++) {
            requests.add(encode(queryRequest(shardId, "value-" + i)));
        }
        final var response = new CollectSampleResponse(new PhantomQuerySample(requests, namedWriteableRegistry()));

        final var read = copyWriteable(response, namedWriteableRegistry(), CollectSampleResponse::new);

        assertThat(read.sample().size(), equalTo(requests.size()));
        for (int i = 0; i < requests.size(); i++) {
            assertThat(read.sample().requests().get(i).bytes(), equalTo(requests.get(i).bytes()));
        }
    }

    public void testCollectSampleResponseReencodesTheSampleForTheVersionOfTheChannel() throws IOException {
        final var shardId = PhantomQueryTestUtils.shardId();
        final var requests = List.of(encode(queryRequest(shardId, "first")), encode(queryRequest(shardId, "second")));
        final var response = new CollectSampleResponse(new PhantomQuerySample(requests, namedWriteableRegistry()));
        final TransportVersion channelVersion = randomValueOtherThan(
            TransportVersion.current(),
            TransportVersionUtils::randomCompatibleVersion
        );

        final var read = copyWriteable(response, namedWriteableRegistry(), CollectSampleResponse::new, channelVersion);

        assertThat(read.sample().size(), equalTo(2));
        assertThat(read.sample().requests().get(0).version(), equalTo(channelVersion));
        assertThat(read.sample().requests().get(1).version(), equalTo(channelVersion));
    }
}
