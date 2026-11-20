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

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

/**
 * With a short shutdown marker file, the shutdown keeps the short grace period.
 */
public class SigtermGracePeriodShortIT extends AbstractSigtermGracePeriodTestCase {

    private static final TemporaryFolder markerFolder = new TemporaryFolder();

    private static final StatelessElasticsearchCluster cluster = buildCluster(markerFolder);

    @ClassRule
    public static TestRule ruleChain = RuleChain.outerRule(markerFolder).around(cluster);

    @Override
    StatelessElasticsearchCluster cluster() {
        return cluster;
    }

    public void testShortGracePeriodKept() throws Exception {
        var nodeId = searchNodeId();
        createIndexWithShardOnSearchNode();
        Files.createFile(markerFile(markerFolder));

        var stopThread = stopSearchNodeInBackground();
        try {
            assertBusy(() -> {
                var record = shutdownRecord(nodeId);
                assertThat(record, notNullValue());
                assertThat(record.gracePeriod(), equalTo(SHORT_GRACE_PERIOD.getStringRep()));
            }, SHORT_GRACE_PERIOD.getSeconds() + 5, TimeUnit.SECONDS);

            // the check for the marker file is due after the check delay, so by now the grace period would have been extended if it were
            // going to be
            var startedAtMillis = shutdownRecord(nodeId).startedAtMillis();
            long deadline = System.nanoTime() + 3 * MARKER_CHECK_DELAY.nanos();
            while (System.nanoTime() < deadline) {
                var record = shutdownRecord(nodeId);
                assertThat(record, notNullValue());
                assertThat(record.gracePeriod(), equalTo(SHORT_GRACE_PERIOD.getStringRep()));
                assertThat(record.startedAtMillis(), equalTo(startedAtMillis));
                safeSleep(100);
            }
        } finally {
            stopThread.join();
        }
    }
}
