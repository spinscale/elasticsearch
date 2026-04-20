/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.search;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.test.ESTestCase;

import java.util.List;

public class SearchResponseHeadersTests extends ESTestCase {

    public void testAddBytesReadHeaderSkipsZero() {
        assumeTrue("directory metrics flag must be enabled", Store.DIRECTORY_METRICS_FEATURE_FLAG.isEnabled());
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        SearchResponseHeaders.addBytesReadHeader(threadContext, 0L);
        SearchResponseHeaders.addBytesReadHeader(threadContext, -5L);
        assertNull(threadContext.getResponseHeaders().get(SearchResponseHeaders.BYTES_READ_HEADER));
    }

    public void testAddBytesReadHeaderWritesPositiveValue() {
        assumeTrue("directory metrics flag must be enabled", Store.DIRECTORY_METRICS_FEATURE_FLAG.isEnabled());
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        SearchResponseHeaders.addBytesReadHeader(threadContext, 4096L);
        List<String> values = threadContext.getResponseHeaders().get(SearchResponseHeaders.BYTES_READ_HEADER);
        assertNotNull(values);
        assertEquals(List.of("4096"), values);
    }
}
