/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.recovery.shardinfo;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import static org.elasticsearch.xpack.stateless.recovery.shardinfo.TransportFetchSearchShardInformationAction.Response.SEARCH_SHARD_INFORMATION_GLOBAL_ORDINALS_FIELDS;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class ShardInformationNodeResponseTests extends AbstractWireSerializingTestCase<
    TransportFetchSearchShardInformationAction.Response> {

    @Override
    protected Writeable.Reader<TransportFetchSearchShardInformationAction.Response> instanceReader() {
        return TransportFetchSearchShardInformationAction.Response::new;
    }

    @Override
    protected TransportFetchSearchShardInformationAction.Response createTestInstance() {
        return createLabeledTestInstance("1");
    }

    public static TransportFetchSearchShardInformationAction.Response createLabeledTestInstance(String label) {
        return new TransportFetchSearchShardInformationAction.Response(randomLong(), randomGlobalOrdinalsFields());
    }

    @Override
    protected TransportFetchSearchShardInformationAction.Response mutateInstance(
        TransportFetchSearchShardInformationAction.Response instance
    ) {
        if (randomBoolean()) {
            return new TransportFetchSearchShardInformationAction.Response(
                randomValueOtherThan(instance.getLastSearcherAcquiredTime(), ESTestCase::randomLong),
                instance.getGlobalOrdinalsFields()
            );
        }
        Set<String> fields = new HashSet<>(instance.getGlobalOrdinalsFields());
        if (fields.isEmpty() || randomBoolean()) {
            fields.add(randomValueOtherThanMany(fields::contains, () -> randomAlphaOfLength(10)));
        } else {
            fields.remove(randomFrom(fields));
        }
        return new TransportFetchSearchShardInformationAction.Response(instance.getLastSearcherAcquiredTime(), fields);
    }

    public void testNegativeLongSerialization() throws IOException {
        try (BytesStreamOutput bos = new BytesStreamOutput()) {
            TransportFetchSearchShardInformationAction.SHARD_HAS_MOVED_RESPONSE.writeTo(bos);
            assertThat(bos.size(), greaterThan(0));
        }
        try (BytesStreamOutput bos = new BytesStreamOutput()) {
            TransportFetchSearchShardInformationAction.NO_OTHER_SHARDS_FOUND_RESPONSE.writeTo(bos);
            assertThat(bos.size(), greaterThan(0));
        }
    }

    public void testGlobalOrdinalsFieldsDroppedOnUnsupportedVersion() throws IOException {
        final TransportVersion version = TransportVersionUtils.randomVersionNotSupporting(SEARCH_SHARD_INFORMATION_GLOBAL_ORDINALS_FIELDS);
        final var original = new TransportFetchSearchShardInformationAction.Response(randomLong(), randomGlobalOrdinalsFields());
        final var copy = copyWriteable(original, getNamedWriteableRegistry(), instanceReader(), version);
        assertThat(copy.getLastSearcherAcquiredTime(), equalTo(original.getLastSearcherAcquiredTime()));
        assertThat(copy.getGlobalOrdinalsFields(), empty());
    }

    private static Set<String> randomGlobalOrdinalsFields() {
        return randomSet(0, 5, () -> randomAlphaOfLength(10));
    }
}
