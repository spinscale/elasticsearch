/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.fielddata;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.OrdinalMap;

/**
 * Specialization of {@link IndexFieldData} for data that is indexed with ordinals.
 */
public interface IndexOrdinalsFieldData extends IndexFieldData.Global<LeafOrdinalsFieldData> {

    /**
     * Load a global view of the ordinals for the given {@link IndexReader},
     * potentially from a cache.
     */
    @Override
    IndexOrdinalsFieldData loadGlobal(DirectoryReader indexReader);

    /**
     * Loads the global ordinals for the given {@link IndexReader} into the cache ahead of searches, for instance to warm a reader before
     * it is exposed. Unlike {@link #loadGlobal}, this is not reported as an access to the global ordinals through
     * {@link IndexFieldDataCache.Listener#onGlobalOrdinalsAccess}, as no search is using them.
     */
    default void warmGlobal(DirectoryReader indexReader) {
        loadGlobal(indexReader);
    }

    /**
     * Load a global view of the ordinals for the given {@link IndexReader}.
     */
    @Override
    IndexOrdinalsFieldData loadGlobalDirect(DirectoryReader indexReader) throws Exception;

    /**
     * Returns the underlying {@link OrdinalMap} for this fielddata
     * or null if global ordinals are not needed (constant value or single segment).
     */
    OrdinalMap getOrdinalMap();

    /**
     * Whether this field data is able to provide a mapping between global and segment ordinals,
     * by returning the underlying {@link OrdinalMap}. If this method returns false, then calling
     * {@link #getOrdinalMap} will result in an {@link UnsupportedOperationException}.
     */
    boolean supportsGlobalOrdinalsMapping();
}
