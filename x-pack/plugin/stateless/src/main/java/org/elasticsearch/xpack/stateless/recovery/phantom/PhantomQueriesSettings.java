/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.recovery.phantom;

import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.unit.ByteSizeValue;

import java.util.List;

public class PhantomQueriesSettings {

    private static final String PREFIX = "stateless.search.phantom_queries.";

    public static final Setting<Boolean> ENABLED_SETTING = Setting.boolSetting(
        PREFIX + "enabled",
        false,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    public static final Setting<Integer> MAX_QUERIES_PER_SAMPLE_SETTING = Setting.intSetting(
        PREFIX + "max_queries_per_sample",
        100,
        0,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    public static final Setting<ByteSizeValue> MAX_BYTES_PER_SAMPLE_SETTING = Setting.byteSizeSetting(
        PREFIX + "max_bytes_per_sample",
        ByteSizeValue.ofMb(1),
        ByteSizeValue.ZERO,
        ByteSizeValue.ofBytes(Long.MAX_VALUE),
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    public static final Setting<ByteSizeValue> MAX_BYTES_PER_NODE_SETTING = Setting.byteSizeSetting(
        PREFIX + "max_bytes_per_node",
        ByteSizeValue.ofMb(16),
        ByteSizeValue.ZERO,
        ByteSizeValue.ofBytes(Long.MAX_VALUE),
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    private volatile boolean enabled;
    private volatile int maxQueriesPerSample;
    private volatile long maxBytesPerSample;
    private volatile long maxBytesPerNode;

    public PhantomQueriesSettings(ClusterSettings clusterSettings) {
        clusterSettings.initializeAndWatch(ENABLED_SETTING, value -> this.enabled = value);
        clusterSettings.initializeAndWatch(MAX_QUERIES_PER_SAMPLE_SETTING, value -> this.maxQueriesPerSample = value);
        clusterSettings.initializeAndWatch(MAX_BYTES_PER_SAMPLE_SETTING, value -> this.maxBytesPerSample = value.getBytes());
        clusterSettings.initializeAndWatch(MAX_BYTES_PER_NODE_SETTING, value -> this.maxBytesPerNode = value.getBytes());
    }

    public static List<Setting<?>> settings() {
        return List.of(ENABLED_SETTING, MAX_QUERIES_PER_SAMPLE_SETTING, MAX_BYTES_PER_SAMPLE_SETTING, MAX_BYTES_PER_NODE_SETTING);
    }

    public boolean enabled() {
        return enabled;
    }

    public int maxQueriesPerSample() {
        return maxQueriesPerSample;
    }

    public long maxBytesPerSample() {
        return maxBytesPerSample;
    }

    public long maxBytesPerNode() {
        return maxBytesPerNode;
    }
}
