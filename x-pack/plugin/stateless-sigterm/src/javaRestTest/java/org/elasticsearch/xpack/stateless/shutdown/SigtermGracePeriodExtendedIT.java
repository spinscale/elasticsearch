/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.shutdown;

import org.elasticsearch.test.cluster.stateless.StatelessElasticsearchCluster;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestRule;

import java.nio.file.Files;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

/**
 * Without a short shutdown marker file, the shutdown starts with the short grace period and is extended to the timeout.
 */
public class SigtermGracePeriodExtendedIT extends AbstractSigtermGracePeriodTestCase {

    private static final TemporaryFolder markerFolder = new TemporaryFolder();

    private static final StatelessElasticsearchCluster cluster = buildCluster(markerFolder);

    @ClassRule
    public static TestRule ruleChain = RuleChain.outerRule(markerFolder).around(cluster);

    @Override
    StatelessElasticsearchCluster cluster() {
        return cluster;
    }

    public void testGracePeriodExtended() throws Exception {
        assertThat("the marker file must not exist", Files.exists(markerFile(markerFolder)), is(false));
        var nodeId = searchNodeId();
        assertThat(shutdownRecord(nodeId), nullValue());
        createIndexWithShardOnSearchNode();

        var stopThread = stopSearchNodeInBackground();
        try {
            // The shutdown record is created with the short grace period, which is extended once the check for the marker file is due.
            // The extension must not restart the shutdown, so the start time stays the same.
            var startedAtMillis = new AtomicReference<Number>();
            assertBusy(() -> {
                var record = shutdownRecord(nodeId);
                assertThat(record, notNullValue());
                startedAtMillis.compareAndSet(null, record.startedAtMillis());
                assertThat(record.startedAtMillis(), equalTo(startedAtMillis.get()));
                assertThat(record.gracePeriod(), equalTo(SIGTERM_TIMEOUT.getStringRep()));
            }, SHORT_GRACE_PERIOD.getSeconds() + MARKER_CHECK_DELAY.getSeconds() + 10, TimeUnit.SECONDS);
        } finally {
            stopThread.join();
        }
    }
}
