/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search;

import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.telemetry.TelemetryProvider;
import org.elasticsearch.threadpool.ThreadPool;

/**
 * Service provider interface through which at most one plugin supplies the {@link ShardQueryRequestObserver} of a node. Without a provider
 * the node uses {@link #DEFAULT}, whose observer does nothing.
 */
public interface ShardQueryRequestObserverProvider {

    ShardQueryRequestObserverProvider DEFAULT = (settings, threadPool, clusterService, telemetryProvider) -> ShardQueryRequestObserver.NOOP;

    ShardQueryRequestObserver create(
        Settings settings,
        ThreadPool threadPool,
        ClusterService clusterService,
        TelemetryProvider telemetryProvider
    );
}
