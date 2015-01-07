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

import com.stratio.cassandra.index.AnalyzerFactory;
import com.stratio.cassandra.index.util.JsonSerializer;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.miscellaneous.PerFieldAnalyzerWrapper;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.util.Version;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Class for several columns mappings between Cassandra and Lucene.
 *
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class Schema {

    /** The default Lucene analyzer to be used if no other specified. */
    public static final Analyzer DEFAULT_ANALYZER = new StandardAnalyzer(Version.LUCENE_48);

    /** The Lucene {@link Analyzer}. */
    private final Analyzer defaultAnalyzer;

    /** The per field Lucene analyzer to be used. */
    private final PerFieldAnalyzerWrapper perFieldAnalyzer;

    /** The column mappers. */
    private Map<String, ColumnMapper> columnMappers;

    /**
     * Builds a new {@code ColumnsMapper} for the specified analyzer and cell mappers.
     *
     * @param analyzerClassName The name of the class of the analyzer to be used.
     * @param columnMappers     The {@link Column} mappers to be used.
     */
    @JsonCreator
    public Schema(@JsonProperty("default_analyzer") String analyzerClassName,
                  @JsonProperty("fields") Map<String, ColumnMapper> columnMappers) {
        // Copy lower cased mappers
        this.columnMappers = columnMappers;

        // Setup default analyzer
        if (analyzerClassName == null) {
            this.defaultAnalyzer = DEFAULT_ANALYZER;
        } else {
            this.defaultAnalyzer = AnalyzerFactory.getAnalyzer(analyzerClassName);
        }

        // Setup per field analyzer
        Map<String, Analyzer> analyzers = new HashMap<>();
        for (Entry<String, ColumnMapper> entry : columnMappers.entrySet()) {
            String name = entry.getKey();
            ColumnMapper mapper = entry.getValue();
            Analyzer fieldAnalyzer = mapper.analyzer();
            if (fieldAnalyzer != null) {
                analyzers.put(name, fieldAnalyzer);
            }
        }
        perFieldAnalyzer = new PerFieldAnalyzerWrapper(defaultAnalyzer, analyzers);
    }

    /**
     * Checks if this is consistent with the specified column family metadata.
     *
     * @param metadata A column family metadata.
     */
    public void validate(CFMetaData metadata) {
        for (Entry<String, ColumnMapper> entry : columnMappers.entrySet()) {

            String name = entry.getKey();
            ColumnMapper columnMapper = entry.getValue();
            ByteBuffer columnName = UTF8Type.instance.decompose(name);

            ColumnDefinition columnDefinition = metadata.getColumnDefinition(columnName);
            if (columnDefinition == null) {
                throw new RuntimeException("No column definition for mapper " + name);
            }

            if (columnDefinition.isStatic()) {
                throw new RuntimeException("Lucene indexes are not allowed on static columns as " + name);
            }

            AbstractType<?> type = columnDefinition.type;
            if (!columnMapper.supports(type)) {
                throw new RuntimeException("Not supported type for mapper " + name);
            }
        }
    }

    /**
     * Returns the used {@link PerFieldAnalyzerWrapper}.
     *
     * @return The used {@link PerFieldAnalyzerWrapper}.
     */
    public PerFieldAnalyzerWrapper analyzer() {
        return perFieldAnalyzer;
    }

    /**
     * Adds to the specified {@link Document} the Lucene fields representing the specified {@link Columns}.
     *
     * @param document The Lucene {@link Document} where the fields are going to be added.
     * @param columns  The {@link Columns} to be added.
     */
    public void addFields(Document document, Columns columns) {
        for (Column column : columns) {
            String name = column.getName();
            ColumnMapper columnMapper = getMapper(name);
            if (columnMapper != null) {
                for (IndexableField field : columnMapper.fields(column)) {
                    document.add(field);
                }
            }
        }
    }

    /**
     * Returns the {@link ColumnMapper} identified by the specified field name, or {@code null} if not found.
     *
     * @param field A field name.
     * @return The {@link ColumnMapper} identified by the specified field name, or {@code null} if not found.
     */
    public ColumnMapper getMapper(String field) {
        String[] components = field.split("\\.");
        for (int i = components.length - 1; i >= 0; i--) {
            StringBuilder sb = new StringBuilder();
            for (int j = 0; j <= i; j++) {
                sb.append(components[j]);
                if (j < i) sb.append('.');
            }
            ColumnMapper columnMapper = columnMappers.get(sb.toString());
            if (columnMapper != null) return columnMapper;
        }
        return null;
    }

    /**
     * Returns the {@link ColumnMapperSingle} identified by the specified field name, or {@code null} if not found.
     *
     * @param field A field name.
     * @return The {@link ColumnMapperSingle} identified by the specified field name, or {@code null} if not found.
     */
    public ColumnMapperSingle<?> getMapperSingle(String field) {
        ColumnMapper columnMapper = getMapper(field);
        if (columnMapper != null && columnMapper instanceof ColumnMapperSingle<?>) {
            return (ColumnMapperSingle<?>) columnMapper;
        } else {
            return null;
        }
    }

    /**
     * Returns the {@link Schema} contained in the specified JSON {@code String}.
     *
     * @param json A {@code String} containing the JSON representation of the {@link Schema} to be parsed.
     * @return The {@link Schema} contained in the specified JSON {@code String}.
     */
    public static Schema fromJson(String json) throws IOException {
        return JsonSerializer.fromString(json, Schema.class);
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return new ToStringBuilder(this).append("defaultAnalyzer", defaultAnalyzer)
                                        .append("perFieldAnalyzer", perFieldAnalyzer)
                                        .append("columnMappers", columnMappers)
                                        .toString();
    }
}
