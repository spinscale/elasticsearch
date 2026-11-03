/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.recovery.phantom;

import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.test.ESTestCase;

import java.util.Set;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

public class PhantomQueriesSettingsTests extends ESTestCase {

    public void testDefaults() {
        assertThat(PhantomQueriesSettings.ENABLED_SETTING.get(Settings.EMPTY), is(false));
        assertThat(PhantomQueriesSettings.MAX_QUERIES_PER_SAMPLE_SETTING.get(Settings.EMPTY), equalTo(100));
        assertThat(PhantomQueriesSettings.MAX_BYTES_PER_SAMPLE_SETTING.get(Settings.EMPTY), equalTo(ByteSizeValue.ofMb(1)));
        assertThat(PhantomQueriesSettings.MAX_BYTES_PER_NODE_SETTING.get(Settings.EMPTY), equalTo(ByteSizeValue.ofMb(16)));
    }

    public void testSettingKeys() {
        assertThat(PhantomQueriesSettings.ENABLED_SETTING.getKey(), equalTo("stateless.search.phantom_queries.enabled"));
        assertThat(
            PhantomQueriesSettings.MAX_QUERIES_PER_SAMPLE_SETTING.getKey(),
            equalTo("stateless.search.phantom_queries.max_queries_per_sample")
        );
        assertThat(
            PhantomQueriesSettings.MAX_BYTES_PER_SAMPLE_SETTING.getKey(),
            equalTo("stateless.search.phantom_queries.max_bytes_per_sample")
        );
        assertThat(
            PhantomQueriesSettings.MAX_BYTES_PER_NODE_SETTING.getKey(),
            equalTo("stateless.search.phantom_queries.max_bytes_per_node")
        );
    }

    public void testAllSettingsAreDynamicAndNodeScoped() {
        assertThat(PhantomQueriesSettings.settings(), hasSize(4));
        for (Setting<?> setting : PhantomQueriesSettings.settings()) {
            assertTrue(setting.getKey() + " must be dynamic", setting.isDynamic());
            assertTrue(setting.getKey() + " must be node scoped", setting.hasNodeScope());
        }
    }

    public void testMaxQueriesPerSampleBounds() {
        final var setting = PhantomQueriesSettings.MAX_QUERIES_PER_SAMPLE_SETTING;
        assertThat(setting.get(Settings.builder().put(setting.getKey(), 0).build()), equalTo(0));
        final int value = randomIntBetween(1, Integer.MAX_VALUE);
        assertThat(setting.get(Settings.builder().put(setting.getKey(), value).build()), equalTo(value));

        final var e = expectThrows(IllegalArgumentException.class, () -> setting.get(Settings.builder().put(setting.getKey(), -1).build()));
        assertThat(e.getMessage(), containsString(setting.getKey()));
        assertThat(e.getMessage(), containsString(">= 0"));
    }

    public void testByteLimitBounds() {
        for (var setting : Set.of(PhantomQueriesSettings.MAX_BYTES_PER_SAMPLE_SETTING, PhantomQueriesSettings.MAX_BYTES_PER_NODE_SETTING)) {
            assertThat(setting.get(Settings.builder().put(setting.getKey(), "0b").build()), equalTo(ByteSizeValue.ZERO));
            final long bytes = randomLongBetween(1, Long.MAX_VALUE);
            assertThat(
                setting.get(Settings.builder().put(setting.getKey(), ByteSizeValue.ofBytes(bytes).getStringRep()).build()).getBytes(),
                equalTo(bytes)
            );

            final var e = expectThrows(
                IllegalArgumentException.class,
                () -> setting.get(Settings.builder().put(setting.getKey(), "-1b").build())
            );
            assertThat(e.getMessage(), containsString(setting.getKey()));
        }
    }

    public void testCurrentValuesStartAsConfigured() {
        final var nodeSettings = Settings.builder()
            .put(PhantomQueriesSettings.ENABLED_SETTING.getKey(), true)
            .put(PhantomQueriesSettings.MAX_QUERIES_PER_SAMPLE_SETTING.getKey(), 7)
            .put(PhantomQueriesSettings.MAX_BYTES_PER_SAMPLE_SETTING.getKey(), "2kb")
            .put(PhantomQueriesSettings.MAX_BYTES_PER_NODE_SETTING.getKey(), "3mb")
            .build();
        final var settings = new PhantomQueriesSettings(clusterSettings(nodeSettings));

        assertThat(settings.enabled(), is(true));
        assertThat(settings.maxQueriesPerSample(), equalTo(7));
        assertThat(settings.maxBytesPerSample(), equalTo(ByteSizeValue.ofKb(2).getBytes()));
        assertThat(settings.maxBytesPerNode(), equalTo(ByteSizeValue.ofMb(3).getBytes()));
    }

    public void testCurrentValuesDefaultToDisabled() {
        final var settings = new PhantomQueriesSettings(clusterSettings(Settings.EMPTY));

        assertThat(settings.enabled(), is(false));
        assertThat(settings.maxQueriesPerSample(), equalTo(100));
        assertThat(settings.maxBytesPerSample(), equalTo(ByteSizeValue.ofMb(1).getBytes()));
        assertThat(settings.maxBytesPerNode(), equalTo(ByteSizeValue.ofMb(16).getBytes()));
    }

    public void testCurrentValuesFollowDynamicUpdates() {
        final var clusterSettings = clusterSettings(Settings.EMPTY);
        final var settings = new PhantomQueriesSettings(clusterSettings);
        assertThat(settings.enabled(), is(false));

        clusterSettings.applySettings(
            Settings.builder()
                .put(PhantomQueriesSettings.ENABLED_SETTING.getKey(), true)
                .put(PhantomQueriesSettings.MAX_QUERIES_PER_SAMPLE_SETTING.getKey(), 0)
                .put(PhantomQueriesSettings.MAX_BYTES_PER_SAMPLE_SETTING.getKey(), "10b")
                .put(PhantomQueriesSettings.MAX_BYTES_PER_NODE_SETTING.getKey(), "20b")
                .build()
        );
        assertThat(settings.enabled(), is(true));
        assertThat(settings.maxQueriesPerSample(), equalTo(0));
        assertThat(settings.maxBytesPerSample(), equalTo(10L));
        assertThat(settings.maxBytesPerNode(), equalTo(20L));

        clusterSettings.applySettings(Settings.EMPTY);
        assertThat(settings.enabled(), is(false));
        assertThat(settings.maxQueriesPerSample(), equalTo(100));
        assertThat(settings.maxBytesPerSample(), equalTo(ByteSizeValue.ofMb(1).getBytes()));
        assertThat(settings.maxBytesPerNode(), equalTo(ByteSizeValue.ofMb(16).getBytes()));
    }

    public void testSettingsListContainsEverySetting() {
        assertThat(
            PhantomQueriesSettings.settings(),
            hasItems(
                PhantomQueriesSettings.ENABLED_SETTING,
                PhantomQueriesSettings.MAX_QUERIES_PER_SAMPLE_SETTING,
                PhantomQueriesSettings.MAX_BYTES_PER_SAMPLE_SETTING,
                PhantomQueriesSettings.MAX_BYTES_PER_NODE_SETTING
            )
        );
    }

    private static ClusterSettings clusterSettings(Settings nodeSettings) {
        return new ClusterSettings(nodeSettings, Set.copyOf(PhantomQueriesSettings.settings()));
    }
}
