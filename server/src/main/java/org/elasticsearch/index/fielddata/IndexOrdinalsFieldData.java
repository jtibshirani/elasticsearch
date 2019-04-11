/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.fielddata;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.OrdinalMap;
import org.apache.lucene.util.LongValues;


/**
 * Specialization of {@link IndexFieldData} for data that is indexed with ordinals.
 */
public interface IndexOrdinalsFieldData extends IndexFieldData.Global<AtomicOrdinalsFieldData> {

    /**
     * Load a global view of the ordinals for the given {@link IndexReader},
     * potentially from a cache.
     */
    @Override
    IndexOrdinalsFieldData loadGlobal(DirectoryReader indexReader);

    /**
     * Load a global view of the ordinals for the given {@link IndexReader}.
     */
    @Override
    IndexOrdinalsFieldData localGlobalDirect(DirectoryReader indexReader) throws Exception;

    /**
     * Returns whether this field data implementation makes use of global ordinals.
     */
    boolean hasGlobalOrds();

    /**
     * Given a segment index, loads the mapping from segment to global ordinals. This
     * method will throw an exception if {@link #hasGlobalOrds()} returns false.
     */
    LongValues getGlobalOrds(int segmentIndex);

}
