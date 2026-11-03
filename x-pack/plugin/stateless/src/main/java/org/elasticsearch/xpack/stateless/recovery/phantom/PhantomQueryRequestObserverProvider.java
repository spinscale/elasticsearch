/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.recovery.phantom;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.ShardQueryRequestObserver;
import org.elasticsearch.search.ShardQueryRequestObserverProvider;
import org.elasticsearch.telemetry.TelemetryProvider;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.stateless.StatelessPlugin;

/**
 * Only search nodes sample, because only they hold the started copies that a recovering copy takes its sample from.
 */
public class PhantomQueryRequestObserverProvider implements ShardQueryRequestObserverProvider {

    private final PhantomQuerySamplers samplers;
    private final PhantomQueriesSettings queriesSettings;

    public PhantomQueryRequestObserverProvider() {
        throw new IllegalStateException("This no arg constructor only exists for SPI validation");
    }

    /**
     * The plugin is the only thing that the plugins service can pass to an extension. It builds the extension after it has created the
     * components of all plugins, so the samplers and the settings exist by then.
     */
    public PhantomQueryRequestObserverProvider(StatelessPlugin plugin) {
        this.samplers = plugin.getPhantomQuerySamplers();
        this.queriesSettings = plugin.getPhantomQueriesSettings();
    }

    @Override
    public ShardQueryRequestObserver create(
        Settings settings,
        ThreadPool threadPool,
        ClusterService clusterService,
        TelemetryProvider telemetryProvider
    ) {
        if (DiscoveryNode.hasRole(settings, DiscoveryNodeRole.SEARCH_ROLE)) {
            return new PhantomQueryRequestObserver(samplers, queriesSettings);
        }
        return ShardQueryRequestObserver.NOOP;
    }
}
