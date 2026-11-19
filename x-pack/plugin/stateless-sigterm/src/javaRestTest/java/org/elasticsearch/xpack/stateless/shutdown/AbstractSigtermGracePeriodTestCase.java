/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.shutdown;

import org.elasticsearch.client.Request;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.cluster.stateless.StatelessElasticsearchCluster;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;

/**
 * Base class for tests of the grace period of a SIGTERM shutdown when a short shutdown marker file is configured.
 * <p>
 * The marker file is in a temporary folder, which is outside the config directory of the nodes. A node can only read it because of the
 * files entitlement of the stateless-sigterm module, so these tests also cover that entitlement. Without the entitlement, {@code
 * Files.exists} does not throw, it returns {@code false}, so the marker file looks missing and the grace period is extended.
 * <p>
 * The search node is the node that is stopped. It holds the only search copy of the shards of an index, which cannot move to another search
 * node, so the shutdown stays in progress until it times out. That keeps the shutdown record visible for the whole test.
 */
abstract class AbstractSigtermGracePeriodTestCase extends ESRestTestCase {

    /**
     * The grace period of a long shutdown.
     */
    static final TimeValue SIGTERM_TIMEOUT = TimeValue.timeValueSeconds(30);
    static final TimeValue SHORT_GRACE_PERIOD = TimeValue.timeValueSeconds(5);
    static final TimeValue MARKER_CHECK_DELAY = TimeValue.timeValueSeconds(1);

    private static final int SEARCH_NODE_INDEX = 1;
    private static final String INDEX_NAME = "grace-period-test";

    static StatelessElasticsearchCluster buildCluster(TemporaryFolder markerFolder) {
        return StatelessElasticsearchCluster.local()
            .module("stateless")
            .module("stateless-sigterm")
            .module("secure-settings")
            .setting("xpack.ml.enabled", "false")
            .user("admin-user", "x-pack-test-password")
            .setting("xpack.watcher.enabled", "false")
            .setting("stateless.sigterm.timeout", SIGTERM_TIMEOUT.getStringRep())
            .setting("stateless.sigterm.poll_interval", "1s")
            .setting("stateless.sigterm.short_grace_period", SHORT_GRACE_PERIOD.getStringRep())
            .setting("stateless.sigterm.short_shutdown_marker_check_delay", MARKER_CHECK_DELAY.getStringRep())
            .setting("stateless.sigterm.short_shutdown_marker_file", () -> markerFile(markerFolder).toString())
            .build();
    }

    @SuppressForbidden(reason = "TemporaryFolder only has io.File methods, not nio.File")
    static Path markerFile(TemporaryFolder markerFolder) {
        return markerFolder.getRoot().toPath().resolve("short-shutdown");
    }

    abstract StatelessElasticsearchCluster cluster();

    @Override
    protected String getTestRestCluster() {
        return cluster().getHttpAddresses();
    }

    @Override
    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue("admin-user", new SecureString("x-pack-test-password".toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    String searchNodeId() throws IOException {
        var name = cluster().getName(SEARCH_NODE_INDEX);
        var nodeId = assertOKAndCreateObjectPath(client().performRequest(new Request("GET", "_nodes/" + name))).evaluate(
            "nodes._arbitrary_key_"
        );
        assertThat(nodeId, instanceOf(String.class));
        return (String) nodeId;
    }

    /**
     * Creates an index and waits for it to be green, which puts a shard on the search node.
     */
    void createIndexWithShardOnSearchNode() throws IOException {
        createIndex(adminClient(), INDEX_NAME, Settings.EMPTY);
        ensureGreen(adminClient(), INDEX_NAME);
    }

    /**
     * Sends a SIGTERM to the search node. Stopping the node blocks until it exits, which takes until it is forcibly killed here as its
     * shutdown does not complete, so this runs on another thread to let the test look at the shutdown meanwhile.
     */
    Thread stopSearchNodeInBackground() {
        var thread = new Thread(() -> cluster().stopNode(SEARCH_NODE_INDEX, false), "stop-search-node");
        thread.start();
        return thread;
    }

    /**
     * @return the shutdown record of the node as returned by the Get Shutdown Status API, or {@code null} if there is none
     */
    ShutdownRecord shutdownRecord(String nodeId) throws IOException {
        var response = assertOKAndCreateObjectPath(client().performRequest(new Request("GET", "_nodes/" + nodeId + "/shutdown")));
        List<?> nodes = response.evaluate("nodes");
        if (nodes.isEmpty()) {
            return null;
        }
        assertThat(nodes, hasSize(1));
        return new ShutdownRecord(response.evaluate("nodes.0.grace_period"), response.evaluate("nodes.0.shutdown_started_millis"));
    }

    record ShutdownRecord(String gracePeriod, Number startedAtMillis) {}
}
