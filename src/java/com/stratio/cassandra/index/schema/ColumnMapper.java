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

import java.nio.ByteBuffer;

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

    protected static final Analyzer EMPTY_ANALYZER = new KeywordAnalyzer();
    protected static final Store STORE = Store.NO;

    private final AbstractType<?>[] supportedTypes;
    private final AbstractType<?>[] supportedClusteringTypes;

    ColumnMapper(AbstractType<?>[] supportedTypes, AbstractType<?>[] supportedClusteringTypes)
    {
        this.supportedTypes = supportedTypes;
        this.supportedClusteringTypes = supportedClusteringTypes;
    }

    public static Column column(String name, ByteBuffer value, AbstractType<?> type)
    {
        return new Column(name, value, type);
    }

    public static Column column(String name, String nameSufix, ByteBuffer value, AbstractType<?> type)
    {
        return new Column(name, nameSufix, value, type);
    }

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
     * Returns the cell value resulting from the mapping of the specified object.
     *
     * @param field The field name.
     * @param value The object to be mapped.
     * @return The cell value resulting from the mapping of the specified object.
     */
    public abstract BASE indexValue(String field, Object value);

    public abstract BASE queryValue(String field, Object value);

    public abstract SortField sortField(String field, boolean reverse);

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
