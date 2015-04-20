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
package com.stratio.cassandra.index.schema.mapping;

import com.stratio.cassandra.index.schema.Column;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.lucene.document.Field;
import org.apache.lucene.search.SortField;

import java.util.List;

/**
 * Class for mapping between Cassandra's columns and Lucene documents.
 *
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public abstract class ColumnMapperSingle<BASE> extends ColumnMapper {

    /** The supported Cassandra types as clustering key. */
    private final AbstractType<?>[] supportedClusteringTypes;

    /**
     * Builds a new {@link ColumnMapperSingle} supporting the specified types for indexing and clustering.
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
     * @return {@code true} if the specified Cassandra type/marshaller can be used as clustering key, {@code false}.
     * otherwise.
     */
    public boolean supportsClustering() {
        for (AbstractType<?> supportedClusteringType : supportedClusteringTypes) {
            if (type.getClass() == supportedClusteringType.getClass()) {
                return true;
            }
        }
        return false;
    }

    public final List<Field> fields(String name, Object value) {
        BASE indexValue = indexValue(name, value);
        return fieldsFromBase(name, indexValue);
    }

    /**
     * Returns the Lucene {@link Field} resulting from the mapping of {@code value}, using {@code name} as field's
     * name.
     *
     * @param name  The name of the Lucene {@link Field}.
     * @param value The value of the Lucene {@link Field}.
     * @return The Lucene {@link Field} resulting from the mapping of {@code value}, using {@code name} as field's name.
     */
    public abstract List<Field> fieldsFromBase(String name, BASE value);

    /**
     * Returns the Lucene type for this mapper.
     *
     * @return The Lucene type for this mapper.
     */
    public abstract Class<BASE> baseClass();

    /**
     * Returns the {@link Column} index value resulting from the mapping of the specified object.
     *
     * @param field The field name.
     * @param value The object to be mapped.
     * @return The {@link Column} index value resulting from the mapping of the specified object.
     */
    public final BASE indexValue(String field, Object value) {
        return toLucene(field, value, true);
    }

    /**
     * Returns the {@link Column} query value resulting from the mapping of the specified object.
     *
     * @param field The field name.
     * @param value The object to be mapped.
     * @return The {@link Column} index value resulting from the mapping of the specified object.
     */
    public final BASE queryValue(String field, Object value) {
        return toLucene(field, value, false);
    }

    /**
     * Returns the {@link Column} query value resulting from the mapping of the specified object.
     *
     * @param field         The field name.
     * @param value         The object to be mapped.
     * @param checkValidity {@code true} if value validity must be checked.
     * @return The {@link Column} index value resulting from the mapping of the specified object.
     */
    public abstract BASE toLucene(String field, Object value, boolean checkValidity);

    /**
     * Returns the {@link SortField} resulting from the mapping of the specified object.
     *
     * @param field   The field name.
     * @param reverse If the sort must be reversed.
     * @return The {@link SortField} resulting from the mapping of the specified object.
     */
    public abstract SortField sortField(String field, boolean reverse);

}
