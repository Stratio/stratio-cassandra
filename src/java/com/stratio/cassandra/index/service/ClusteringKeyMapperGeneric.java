/*
 * Copyright 2014, Stratio.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.stratio.cassandra.index.service;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.composites.Composite;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.FieldComparatorSource;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SortField;

import java.io.IOException;

/**
 * {@link ClusteringKeyMapper} that stores a binary representation of the clustering key.
 * <p/>
 * It uses custom {@link SortField}s and {@link Query}s, having worst performance than other implementations but being
 * applicable to any schema.
 *
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class ClusteringKeyMapperGeneric extends ClusteringKeyMapper {

    /**
     * Returns a new {@code ClusteringKeyMapper} according to the specified column family meta data.
     *
     * @param metadata The column family meta data.
     */
    private ClusteringKeyMapperGeneric(CFMetaData metadata) {
        super(metadata);
    }

    /**
     * Returns a new {@code ClusteringKeyMapper} according to the specified column family meta data.
     *
     * @param metadata The column family meta data.
     * @return A new {@code ClusteringKeyMapper} according to the specified column family meta data.
     */
    public static ClusteringKeyMapperGeneric instance(CFMetaData metadata) {
        return new ClusteringKeyMapperGeneric(metadata);
    }

    @Override
    public SortField[] sortFields() {
        return new SortField[]{new SortField(FIELD_NAME, new FieldComparatorSource() {
            @Override
            public FieldComparator<?> newComparator(String field,
                                                    int hits,
                                                    int sort,
                                                    boolean reversed) throws IOException {
                return new ClusteringKeySorter(ClusteringKeyMapperGeneric.this, hits, field);
            }
        })};
    }

    @Override
    public Query query(Composite start, Composite stop) {
        return new ClusteringKeyQuery(start, stop, this);
    }

}
