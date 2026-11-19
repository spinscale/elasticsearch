/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.shutdown;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.hamcrest.Matchers.equalTo;

public class ShortShutdownMarkerTests extends ESTestCase {

    public void testDisabledMarkerNeverExists() {
        var marker = ShortShutdownMarker.disabled();
        assertFalse(marker.enabled());
        assertFalse(marker.exists());
    }

    public void testEmptySettingDisablesMarker() throws IOException {
        var configDir = createTempDir();
        // a file in the config directory must not be mistaken for the marker when the setting is not set
        Files.createFile(configDir.resolve(randomAlphaOfLength(8)));

        var marker = ShortShutdownMarker.fromSettings(Settings.EMPTY, configDir);
        assertFalse(marker.enabled());
        assertFalse(marker.exists());

        marker = ShortShutdownMarker.fromSettings(markerSettings(""), configDir);
        assertFalse(marker.enabled());
        assertFalse(marker.exists());
    }

    public void testEnabledMarkerWithoutFile() {
        var configDir = createTempDir();
        var markerFile = createTempDir().resolve("short-shutdown");

        var marker = ShortShutdownMarker.fromSettings(markerSettings(markerFile.toString()), configDir);

        assertTrue(marker.enabled());
        assertFalse(marker.exists());
    }

    public void testEnabledMarkerWithFile() throws IOException {
        var configDir = createTempDir();
        var markerFile = createTempDir().resolve("short-shutdown");
        var marker = ShortShutdownMarker.fromSettings(markerSettings(markerFile.toString()), configDir);
        assertFalse(marker.exists());

        // the file content is ignored, an empty file is enough
        Files.createFile(markerFile);

        assertTrue(marker.enabled());
        assertTrue(marker.exists());
        assertThat(marker.toString(), equalTo(markerFile.toString()));

        // the marker checks the file system on each call, so removing the file is visible
        Files.delete(markerFile);
        assertFalse(marker.exists());
    }

    public void testRelativePathIsResolvedAgainstConfigDir() throws IOException {
        var configDir = createTempDir();
        var marker = ShortShutdownMarker.fromSettings(markerSettings("markers/short-shutdown"), configDir);
        assertTrue(marker.enabled());
        assertFalse(marker.exists());
        assertThat(marker.toString(), equalTo(configDir.resolve("markers/short-shutdown").toString()));

        Path markerFile = configDir.resolve("markers").resolve("short-shutdown");
        Files.createDirectories(markerFile.getParent());
        Files.createFile(markerFile);

        assertTrue(marker.exists());
    }

    private static Settings markerSettings(String value) {
        return Settings.builder().put(StatelessSigtermPlugin.SHORT_SHUTDOWN_MARKER_FILE_SETTING.getKey(), value).build();
    }
}
