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

import com.stratio.cassandra.index.geospatial.GeoShapeMapper;
import com.stratio.cassandra.index.schema.analysis.AnalyzerFactory;
import com.stratio.cassandra.index.schema.analysis.PreBuiltAnalyzers;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.SortField;
import org.codehaus.jackson.annotate.JsonSubTypes;
import org.codehaus.jackson.annotate.JsonTypeInfo;

import java.util.Set;

/**
 * Class for mapping between Cassandra's columns and Lucene documents.
 *
 * @author Andres de la Pena <adelapena@stratio.com>
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({@JsonSubTypes.Type(value = ColumnMapperBlob.class, name = "bytes"),
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
               @JsonSubTypes.Type(value = ColumnMapperBigInteger.class, name = "bigint"),
               @JsonSubTypes.Type(value = GeoShapeMapper.class, name = "geo_shape"),})
public abstract class ColumnMapper {

    /** A no-action getAnalyzer for not tokenized {@link ColumnMapper} implementations. */
    protected static final String KEYWORD_ANALYZER = PreBuiltAnalyzers.KEYWORD.toString();

    /** The store field in Lucene default option. */
    protected static final Store STORE = Store.NO;

    /** The supported Cassandra types for indexing. */
    private final AbstractType<?>[] supportedTypes;

    /**
     * Builds a new {@link ColumnMapper} supporting the specified types for indexing.
     *
     * @param supportedTypes The supported Cassandra types for indexing.
     */
    protected ColumnMapper(AbstractType<?>[] supportedTypes) {
        this.supportedTypes = supportedTypes;
    }

    /**
     * Returns the name of the used {@link Analyzer}.
     *
     * @return The name of the used {@link Analyzer}.
     */
    public String analyzer() {
        return KEYWORD_ANALYZER;
    }

    /**
     * Returns the Lucene {@link Field}s resulting from the mapping of the specified {@link Column}.
     *
     * @param column The name of the {@link Column}.
     * @return The Lucene {@link Field}s resulting from the mapping of the specified {@link Column}.
     */
    public abstract Set<IndexableField> fields(Column column);

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
    public boolean supports(final AbstractType<?> type) {
        AbstractType<?> checkedType = type;
        if (type.isCollection()) {
            if (type instanceof MapType<?, ?>) {
                checkedType = ((MapType<?, ?>) type).getValuesType();
            } else if (type instanceof ListType<?>) {
                checkedType = ((ListType<?>) type).getElementsType();
            } else if (type instanceof SetType) {
                checkedType = ((SetType<?>) type).getElementsType();
            }
        }

        for (AbstractType<?> n : supportedTypes) {
            if (checkedType.getClass() == n.getClass()) {
                return true;
            }
        }
        return false;
    }

}
