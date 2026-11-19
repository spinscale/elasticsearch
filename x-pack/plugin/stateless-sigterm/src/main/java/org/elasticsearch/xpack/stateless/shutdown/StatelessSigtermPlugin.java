/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.shutdown;

import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.node.internal.TerminationHandler;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.xpack.shutdown.NodeSeenService;
import org.elasticsearch.xpack.shutdown.RestGetShutdownStatusAction;
import org.elasticsearch.xpack.shutdown.ShutdownPlugin;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * Replaces the Node Shutdown plugin to make it work as desired for stateless, which is primarily converting how k8s wants to talk to
 * Elasticsearch (SIGTERM, you're welcome) into Node Shutdown requests.
 */
public class StatelessSigtermPlugin extends ShutdownPlugin {
    private SigtermTerminationHandler terminationHandler;
    /**
     * The maximum amount of time to wait for an orderly shutdown.
     */
    public static final Setting<TimeValue> TIMEOUT_SETTING = Setting.timeSetting(
        "stateless.sigterm.timeout",
        new TimeValue(1, TimeUnit.HOURS),
        TimeValue.ZERO,
        Setting.Property.NodeScope
    );

    public static final Setting<TimeValue> POLL_INTERVAL_SETTING = Setting.timeSetting(
        "stateless.sigterm.poll_interval",
        new TimeValue(1, TimeUnit.MINUTES),
        TimeValue.ZERO,
        Setting.Property.NodeScope
    );

    /**
     * Path of the marker file that the control plane writes when the shutdown of this node is short, for example during cloud provider
     * maintenance. Only the existence of the file is relevant, its content is ignored. A relative path is resolved against the config
     * directory. When empty, the feature is off and the grace period is always {@link #TIMEOUT_SETTING}.
     */
    public static final Setting<String> SHORT_SHUTDOWN_MARKER_FILE_SETTING = Setting.simpleString(
        "stateless.sigterm.short_shutdown_marker_file",
        "",
        Setting.Property.NodeScope
    );

    /**
     * The grace period of a short shutdown, which is the time before the platform kills the pod. Keep this equal to
     * {@code SharedBlobCacheWarmingService.SEARCH_RECOVERY_WARMING_GRACE_PERIOD_CAP_SETTING}, as the warming deadline uses the smaller of
     * the grace period and that cap.
     */
    public static final Setting<TimeValue> SHORT_GRACE_PERIOD_SETTING = Setting.timeSetting(
        "stateless.sigterm.short_grace_period",
        new TimeValue(14, TimeUnit.MINUTES),
        TimeValue.ZERO,
        Setting.Property.NodeScope
    );

    /**
     * How long to wait after the SIGTERM before checking whether the short shutdown marker file exists. The grace period is extended to
     * {@link #TIMEOUT_SETTING} when it does not exist by then.
     */
    public static final Setting<TimeValue> SHORT_SHUTDOWN_MARKER_CHECK_DELAY_SETTING = Setting.timeSetting(
        "stateless.sigterm.short_shutdown_marker_check_delay",
        new TimeValue(1, TimeUnit.MINUTES),
        TimeValue.ZERO,
        Setting.Property.NodeScope
    );

    @Override
    public Collection<?> createComponents(PluginServices services) {
        ClusterService clusterService = services.clusterService();

        this.terminationHandler = new SigtermTerminationHandler(
            services.client(),
            services.threadPool(),
            services.clusterService(),
            services.remoteTransportClient(),
            POLL_INTERVAL_SETTING.get(clusterService.getSettings()),
            TIMEOUT_SETTING.get(clusterService.getSettings()),
            services.nodeEnvironment().nodeId(),
            new SigtermShutdownMetrics(services.telemetryProvider().getMeterRegistry())
        );

        NodeSeenService nodeSeenService = new NodeSeenService(clusterService);
        SigtermShutdownCleanupService shutdownCleanupService = new SigtermShutdownCleanupService(clusterService, services.rerouteService());
        clusterService.addListener(shutdownCleanupService);
        return List.of(nodeSeenService, shutdownCleanupService);
    }

    @Override
    public List<RestHandler> getRestHandlers(
        RestHandlersServices restHandlersServices,
        Supplier<DiscoveryNodes> nodesInCluster,
        Predicate<NodeFeature> clusterSupportsFeature
    ) {
        // Nothing should be calling the Put or Delete Shutdown APIs from the REST layer, so don't register them.
        // Exposing Get Status internally will be useful for troubleshooting.
        return Collections.singletonList(new RestGetShutdownStatusAction());
    }

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(
            TIMEOUT_SETTING,
            POLL_INTERVAL_SETTING,
            SHORT_SHUTDOWN_MARKER_FILE_SETTING,
            SHORT_GRACE_PERIOD_SETTING,
            SHORT_SHUTDOWN_MARKER_CHECK_DELAY_SETTING
        );
    }

    TerminationHandler getTerminationHandler() {
        return this.terminationHandler;
    }
}
