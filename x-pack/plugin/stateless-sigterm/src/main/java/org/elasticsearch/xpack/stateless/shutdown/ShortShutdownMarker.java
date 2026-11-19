/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.shutdown;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;

import java.nio.file.Files;
import java.nio.file.Path;

/**
 * A marker file that tells this node that its shutdown is short.
 * <p>
 * The platform kills the pod after about 15 minutes in some cases, for example during cloud provider maintenance, so waiting for the long
 * grace period of {@link StatelessSigtermPlugin#TIMEOUT_SETTING} would be pointless. A SIGTERM does not tell the node which case it is in.
 * Instead, the control plane writes the marker file when the shutdown is short. Only the existence of the file is relevant, its content is
 * ignored. There is no fixed order between the SIGTERM and the creation of the file, so callers have to check for the file more than once.
 */
final class ShortShutdownMarker {

    private static final ShortShutdownMarker DISABLED = new ShortShutdownMarker(null);

    @Nullable
    private final Path path;

    private ShortShutdownMarker(@Nullable Path path) {
        this.path = path;
    }

    /**
     * @return a marker that is off and never exists
     */
    static ShortShutdownMarker disabled() {
        return DISABLED;
    }

    /**
     * Reads {@link StatelessSigtermPlugin#SHORT_SHUTDOWN_MARKER_FILE_SETTING}. A relative path is resolved against {@code configDir}, which
     * agrees with {@code basedir_if_relative: config} in the entitlement policy of this plugin.
     *
     * @return a disabled marker if the setting is empty
     */
    static ShortShutdownMarker fromSettings(Settings settings, Path configDir) {
        final String value = StatelessSigtermPlugin.SHORT_SHUTDOWN_MARKER_FILE_SETTING.get(settings);
        if (value.isEmpty()) {
            return disabled();
        }
        return new ShortShutdownMarker(configDir.resolve(value));
    }

    boolean enabled() {
        return path != null;
    }

    /**
     * @return whether the marker is enabled and its file exists right now
     */
    boolean exists() {
        return enabled() && Files.exists(path);
    }

    @Override
    public String toString() {
        return enabled() ? path.toString() : "";
    }
}
