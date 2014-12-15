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
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.search.SortField;
import org.codehaus.jackson.annotate.JsonSubTypes;
import org.codehaus.jackson.annotate.JsonTypeInfo;

/**
 * Class for mapping between Cassandra's columns and Lucene documents.
 *
 * @author Andres de la Pena <adelapena@stratio.com>
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = ColumnMapperBlob.class, name = "bytes"),
        @JsonSubTypes.Type(value = ColumnMapperBoolean.class, name = "boolean"),
        @JsonSubTypes.Type(value = ColumnMapperDate.class, name = "date"),
        @JsonSubTypes.Type(value = ColumnMapperDouble.class, name = "double"),
        @JsonSubTypes.Type(value = ColumnMapperFloat.class, name = "float"),
        @JsonSubTypes.Type(value = ColumnMapperInet.class, name = "inet"),
        @JsonSubTypes.Type(value = ColumnMapperInteger.class, name = "integer"),
        @JsonSubTypes.Type(value = ColumnMapperLong.class, name = "long"),
        @JsonSubTypes.Type(value = ColumnMapperString.class, name = "string"),
        @JsonSubTypes.Type(value = ColumnMapperText.class, name = "text"),
        @JsonSubTypes.Type(value = ColumnMapperUUID.class, name = "uuid"),
        @JsonSubTypes.Type(value = ColumnMapperBigDecimal.class, name = "bigdec"),
        @JsonSubTypes.Type(value = ColumnMapperBigInteger.class, name = "bigint"),})
public abstract class ColumnMapper<BASE>
{
    /** A no-action analyzer for not tokenized {@link ColumnMapper} implementations. */
    protected static final Analyzer EMPTY_ANALYZER = new KeywordAnalyzer();

    /** The store field in Lucene default option. */
    protected static final Store STORE = Store.NO;

    /** The supported Cassandra types for indexing. */
    private final AbstractType<?>[] supportedTypes;

    /** The supported Cassandra types as clustering key. */
    private final AbstractType<?>[] supportedClusteringTypes;

    /**
     * Builds a new {@link ColumnMapper} supporting the specified types for indexing and clustering.
     *
     * @param supportedTypes           The supported Cassandra types for indexing.
     * @param supportedClusteringTypes The supported Cassandra types as clustering key.
     */
    ColumnMapper(AbstractType<?>[] supportedTypes, AbstractType<?>[] supportedClusteringTypes)
    {
        this.supportedTypes = supportedTypes;
        this.supportedClusteringTypes = supportedClusteringTypes;
    }

    /**
     * Returns the used {@link Analyzer}.
     *
     * @return The used {@link Analyzer}.
     */
    public abstract Analyzer analyzer();

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
     * Returns the {@link Column} index value resulting from the mapping of the specified object.
     *
     * @param field The field name.
     * @param value The object to be mapped.
     * @return The {@link Column} index value resulting from the mapping of the specified object.
     */
    public abstract BASE indexValue(String field, Object value);

    /**
     * Returns the {@link Column} query value resulting from the mapping of the specified object.
     *
     * @param field The field name.
     * @param value The object to be mapped.
     * @return The {@link Column} index value resulting from the mapping of the specified object.
     */
    public abstract BASE queryValue(String field, Object value);

    /**
     * Returns the {@link SortField} resulting from the mapping of the specified object.
     *
     * @param field   The field name.
     * @param reverse If the sort must be reversed.
     * @return The {@link SortField} resulting from the mapping of the specified object.
     */
    public abstract SortField sortField(String field, boolean reverse);

    /**
     * Returns {@code true} if the specified Cassandra type/marshaller is supported, {@code false} otherwise.
     *
     * @param type A Cassandra type/marshaller.
     * @return {@code true} if the specified Cassandra type/marshaller is supported, {@code false} otherwise.
     */
    public boolean supports(final AbstractType<?> type)
    {
        AbstractType<?> checkedType = type;
        if (type.isCollection())
        {
            if (type instanceof MapType<?, ?>)
            {
                checkedType = ((MapType<?, ?>) type).values;
            }
            else if (type instanceof ListType<?>)
            {
                checkedType = ((ListType<?>) type).elements;
            }
            else if (type instanceof SetType)
            {
                checkedType = ((SetType<?>) type).elements;
            }
        }

        for (AbstractType<?> n : supportedTypes)
        {
            if (checkedType.getClass() == n.getClass())
            {
                return true;
            }
        }
        return false;
    }

    /**
     * Returns {@code true} if the specified Cassandra type/marshaller can be used as clustering key, {@code false}.
     * otherwise.
     *
     * @param type A Cassandra type/marshaller.
     * @return {@code true} if the specified Cassandra type/marshaller can be used as clustering key, {@code false}.
     * otherwise.
     */
    public boolean supportsClustering(final AbstractType<?> type)
    {
        for (AbstractType<?> supportedClusteringType : supportedClusteringTypes)
        {
            if (type.getClass() == supportedClusteringType.getClass())
            {
                return true;
            }
        }
        return false;
    }

}
