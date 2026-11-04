/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.recovery.phantom;

import org.apache.lucene.search.Query;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.ByteBufferStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.LeafQueryBuilder;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xcontent.XContentBuilder;
import org.junit.Before;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static org.elasticsearch.xpack.stateless.recovery.phantom.PhantomQueryTestUtils.encode;
import static org.elasticsearch.xpack.stateless.recovery.phantom.PhantomQueryTestUtils.queryRequest;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;

public class PhantomQuerySampleTests extends ESTestCase {

    private static final String FAILING_QUERY = "phantom_test_failing_when_not_current";

    private final ShardId shardId = PhantomQueryTestUtils.shardId();
    private NamedWriteableRegistry registry;

    @Before
    public void createRegistry() {
        final var entries = Stream.concat(
            new SearchModule(Settings.EMPTY, List.of()).getNamedWriteables().stream(),
            Stream.of(new NamedWriteableRegistry.Entry(QueryBuilder.class, FAILING_QUERY, FailingWhenNotCurrentQueryBuilder::new))
        ).toList();
        registry = new NamedWriteableRegistry(entries);
    }

    private static class FailingWhenNotCurrentQueryBuilder extends LeafQueryBuilder<FailingWhenNotCurrentQueryBuilder> {

        FailingWhenNotCurrentQueryBuilder() {}

        FailingWhenNotCurrentQueryBuilder(StreamInput in) throws IOException {
            super(in);
        }

        @Override
        protected void doWriteTo(StreamOutput out) {
            if (out.getTransportVersion().equals(TransportVersion.current()) == false) {
                throw new IllegalArgumentException("cannot be written for [" + out.getTransportVersion() + "]");
            }
        }

        @Override
        protected void doXContent(XContentBuilder builder, Params params) {}

        @Override
        protected Query doToQuery(SearchExecutionContext context) {
            throw new UnsupportedOperationException();
        }

        @Override
        protected boolean doEquals(FailingWhenNotCurrentQueryBuilder other) {
            return true;
        }

        @Override
        protected int doHashCode() {
            return 0;
        }

        @Override
        public String getWriteableName() {
            return FAILING_QUERY;
        }

        @Override
        public TransportVersion getMinimalSupportedVersion() {
            return TransportVersion.zero();
        }
    }

    private List<EncodedQueryRequest> encodedRequests(int count) {
        final List<EncodedQueryRequest> encoded = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            encoded.add(encode(queryRequest(shardId, "value-" + i)));
        }
        return encoded;
    }

    private PhantomQuerySample sample(List<EncodedQueryRequest> requests) {
        return new PhantomQuerySample(requests, registry);
    }

    private PhantomQuerySample sendThrough(PhantomQuerySample sample, TransportVersion channelVersion) throws IOException {
        return copyWriteable(sample, registry, PhantomQuerySample::new, channelVersion);
    }

    private QueryBuilder queryOf(EncodedQueryRequest encoded) throws IOException {
        try (var in = new NamedWriteableAwareStreamInput(encoded.bytes().streamInput(), registry)) {
            in.setTransportVersion(encoded.version());
            final var request = new ShardSearchRequest(in);
            assertThat(request.shardId(), equalTo(shardId));
            return request.source().query();
        }
    }

    private static TransportVersion randomOlderVersion() {
        return randomValueOtherThan(TransportVersion.current(), TransportVersionUtils::randomCompatibleVersion);
    }

    public void testRoundTripAtTheCurrentVersion() throws IOException {
        final List<EncodedQueryRequest> requests = encodedRequests(randomIntBetween(1, 10));

        final PhantomQuerySample read = sendThrough(sample(requests), TransportVersion.current());

        assertThat(read.size(), equalTo(requests.size()));
        for (int i = 0; i < requests.size(); i++) {
            final EncodedQueryRequest sent = requests.get(i);
            final EncodedQueryRequest received = read.requests().get(i);
            assertThat("the order is kept", received.bytes(), equalTo(sent.bytes()));
            assertThat(received.version(), equalTo(TransportVersion.current()));
            assertThat(queryOf(received), equalTo(queryOf(sent)));
        }
    }

    public void testEmptySampleRoundTrips() throws IOException {
        final PhantomQuerySample read = sendThrough(PhantomQuerySample.EMPTY, TransportVersion.current());

        assertThat(read.isEmpty(), is(true));
        assertThat(read.requests(), empty());
    }

    public void testRequestsOfTheVersionOfTheChannelAreWrittenAsTheyAre() throws IOException {
        final EncodedQueryRequest request = encode(queryRequest(shardId, "value"));
        try (var out = new BytesStreamOutput()) {
            out.setTransportVersion(request.version());

            sample(List.of(request)).writeTo(out);

            try (StreamInput in = out.bytes().streamInput()) {
                assertThat(in.readVInt(), equalTo(1));
                assertThat(in.readBytesReference(), equalTo(request.bytes()));
                assertThat(in.available(), equalTo(0));
            }
        }
    }

    public void testRequestsAreReencodedForAnOlderChannel() throws IOException {
        final List<EncodedQueryRequest> requests = encodedRequests(randomIntBetween(1, 10));
        final TransportVersion channelVersion = randomOlderVersion();

        final PhantomQuerySample read = sendThrough(sample(requests), channelVersion);

        assertThat("every request can be written for the other version", read.size(), equalTo(requests.size()));
        for (int i = 0; i < requests.size(); i++) {
            final EncodedQueryRequest received = read.requests().get(i);
            assertThat("the receiver reads a request with the version of the channel", received.version(), equalTo(channelVersion));
            assertThat(queryOf(received), equalTo(queryOf(requests.get(i))));
        }
    }

    public void testRequestsOfAnOlderVersionAreReencodedForANewerChannel() throws IOException {
        final TransportVersion olderVersion = randomOlderVersion();
        final EncodedQueryRequest current = encode(queryRequest(shardId, "value"));
        final EncodedQueryRequest older = current.reencode(olderVersion, registry);
        assertThat(older.version(), equalTo(olderVersion));

        final PhantomQuerySample read = sendThrough(sample(List.of(older)), TransportVersion.current());

        assertThat(read.requests(), hasSize(1));
        assertThat(read.requests().get(0).version(), equalTo(TransportVersion.current()));
        assertThat(queryOf(read.requests().get(0)), equalTo(queryOf(current)));
    }

    public void testReencodingForTheVersionOfTheRequestKeepsTheRequest() throws IOException {
        final EncodedQueryRequest request = encode(queryRequest(shardId, "value"));

        assertThat(request.reencode(request.version(), registry), sameInstance(request));
    }

    public void testRequestThatCannotBeReadIsDropped() throws IOException {
        final List<EncodedQueryRequest> requests = encodedRequests(3);
        final var unreadable = new EncodedQueryRequest(new BytesArray(new byte[] { 1, 2, 3 }), requests.get(0).version());
        final var sample = sample(List.of(requests.get(0), unreadable, requests.get(1), requests.get(2)));

        final PhantomQuerySample read = sendThrough(sample, randomOlderVersion());

        final List<QueryBuilder> queries = new ArrayList<>();
        for (EncodedQueryRequest received : read.requests()) {
            queries.add(queryOf(received));
        }
        assertThat(
            "only the request that cannot be read is dropped, and the order of the others is kept",
            queries,
            contains(queryOf(requests.get(0)), queryOf(requests.get(1)), queryOf(requests.get(2)))
        );
    }

    public void testRequestThatCannotBeWrittenForTheVersionOfTheChannelIsDropped() throws IOException {
        final List<EncodedQueryRequest> requests = encodedRequests(2);
        final EncodedQueryRequest failing = encode(queryRequest(shardId, new FailingWhenNotCurrentQueryBuilder()));
        final var sample = sample(List.of(requests.get(0), failing, requests.get(1)));

        final PhantomQuerySample forTheVersionOfTheRequests = sendThrough(sample, TransportVersion.current());
        assertThat("it is written as it is for the version that it was serialized with", forTheVersionOfTheRequests.size(), equalTo(3));

        final PhantomQuerySample forAnOlderVersion = sendThrough(sample, randomOlderVersion());
        final List<QueryBuilder> queries = new ArrayList<>();
        for (EncodedQueryRequest received : forAnOlderVersion.requests()) {
            queries.add(queryOf(received));
        }
        assertThat(queries, contains(queryOf(requests.get(0)), queryOf(requests.get(1))));
    }

    public void testSampleWithOnlyRequestsThatCannotBeWrittenIsEmpty() throws IOException {
        final EncodedQueryRequest failing = encode(queryRequest(shardId, new FailingWhenNotCurrentQueryBuilder()));

        final PhantomQuerySample read = sendThrough(sample(List.of(failing, failing)), randomOlderVersion());

        assertThat(read.isEmpty(), is(true));
    }

    public void testRequestsOtherThanTermQueriesAreReencoded() throws IOException {
        final EncodedQueryRequest request = encode(queryRequest(shardId, new MatchAllQueryBuilder()));

        final PhantomQuerySample read = sendThrough(sample(List.of(request)), randomOlderVersion());

        assertThat(read.requests(), hasSize(1));
        assertThat(queryOf(read.requests().get(0)), equalTo(new MatchAllQueryBuilder()));
    }

    public void testRequestsDoNotShareTheBufferTheyWereReadFrom() throws IOException {
        final List<EncodedQueryRequest> requests = encodedRequests(2);
        final byte[] buffer;
        try (var out = new BytesStreamOutput()) {
            out.setTransportVersion(TransportVersion.current());
            sample(requests).writeTo(out);
            buffer = BytesReference.toBytes(out.bytes());
        }
        final PhantomQuerySample read;
        try (var in = new ByteBufferStreamInput(ByteBuffer.wrap(buffer))) {
            in.setTransportVersion(TransportVersion.current());
            read = new PhantomQuerySample(in);
        }

        // the network buffer is reused once the message was handled
        Arrays.fill(buffer, (byte) 0);

        assertThat(read.requests().get(0).bytes(), equalTo(requests.get(0).bytes()));
        assertThat(read.requests().get(1).bytes(), equalTo(requests.get(1).bytes()));
    }
}
