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
package com.stratio.cassandra.index.schema;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.SortField;

import java.util.HashSet;
import java.util.Set;

/**
 * Class for mapping between Cassandra's columns and Lucene documents.
 *
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public abstract class ColumnMapperSingle<BASE> extends ColumnMapper {

    /** The supported Cassandra types as clustering key. */
    private final AbstractType<?>[] supportedClusteringTypes;

    /**
     * Builds a new {@link com.stratio.cassandra.index.schema.ColumnMapperSingle} supporting the specified types for
     * indexing and clustering.
     *
     * @param supportedTypes           The supported Cassandra types for indexing.
     * @param supportedClusteringTypes The supported Cassandra types as clustering key.
     */
    ColumnMapperSingle(AbstractType<?>[] supportedTypes, AbstractType<?>[] supportedClusteringTypes) {
        super(supportedTypes);
        this.supportedClusteringTypes = supportedClusteringTypes;
    }

    /**
     * Returns {@code true} if the specified Cassandra type/marshaller can be used as clustering key, {@code false}.
     * otherwise.
     *
     * @param type A Cassandra type/marshaller.
     * @return {@code true} if the specified Cassandra type/marshaller can be used as clustering key, {@code false}.
     * otherwise.
     */
    public boolean supportsClustering(final AbstractType<?> type) {
        for (AbstractType<?> supportedClusteringType : supportedClusteringTypes) {
            if (type.getClass() == supportedClusteringType.getClass()) {
                return true;
            }
        }
        return false;
    }

    public Set<IndexableField> fields(Column column) {
        Field field = field(column.getFieldName(), column.getValue());
        Set<IndexableField> set = new HashSet<>();
        set.add(field);
        return set;
    }

    /**
     * Returns the Lucene {@link Field} resulting from the mapping of {@code value}, using {@code name} as field's
     * name.
     *
     * @param name  The name of the Lucene {@link Field}.
     * @param value The value of the Lucene {@link Field}.
     * @return The Lucene {@link Field} resulting from the mapping of {@code value}, using {@code name} as field's name.
     */
    public abstract Field field(String name, Object value);

    /**
     * Returns the Lucene type for this mapper.
     *
     * @return The Lucene type for this mapper.
     */
    public abstract Class<BASE> baseClass();

    /**
     * Returns the {@link com.stratio.cassandra.index.schema.Column} index value resulting from the mapping of the
     * specified object.
     *
     * @param field The field name.
     * @param value The object to be mapped.
     * @return The {@link com.stratio.cassandra.index.schema.Column} index value resulting from the mapping of the
     * specified object.
     */
    public abstract BASE indexValue(String field, Object value);

    /**
     * Returns the {@link com.stratio.cassandra.index.schema.Column} query value resulting from the mapping of the
     * specified object.
     *
     * @param field The field name.
     * @param value The object to be mapped.
     * @return The {@link com.stratio.cassandra.index.schema.Column} index value resulting from the mapping of the
     * specified object.
     */
    public abstract BASE queryValue(String field, Object value);

    /**
     * Returns the {@link org.apache.lucene.search.SortField} resulting from the mapping of the specified object.
     *
     * @param field   The field name.
     * @param reverse If the sort must be reversed.
     * @return The {@link org.apache.lucene.search.SortField} resulting from the mapping of the specified object.
     */
    public abstract SortField sortField(String field, boolean reverse);

}
