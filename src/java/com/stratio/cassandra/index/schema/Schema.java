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

import com.google.common.base.Objects;
import com.stratio.cassandra.index.schema.analysis.Analysis;
import com.stratio.cassandra.index.schema.analysis.AnalyzerBuilder;
import com.stratio.cassandra.index.schema.mapping.ColumnMapper;
import com.stratio.cassandra.index.schema.mapping.ColumnMapperSingle;
import com.stratio.cassandra.index.schema.mapping.Mapping;
import com.stratio.cassandra.util.JsonSerializer;
import org.apache.cassandra.config.CFMetaData;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;

/**
 * Class for several columns mappings between Cassandra and Lucene.
 *
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class Schema implements Closeable {

    /** The analysis properties. */
    private final Analysis analysis;

    private final Mapping mapping;

    private final Analyzer defaultAnalyzer;

    private final Analyzer analyzer;

    /**
     * Builds a new {@code ColumnsMapper} for the specified getAnalyzer and cell mappers.
     *
     * @param columnMappers   The {@link Column} mappers to be used.
     * @param analyzers       The {@link AnalyzerBuilder}s to be used.
     * @param defaultAnalyzer The name of the class of the getAnalyzer to be used.
     */
    @JsonCreator
    public Schema(@JsonProperty("fields") Map<String, ColumnMapper> columnMappers,
                  @JsonProperty("analyzers") Map<String, AnalyzerBuilder> analyzers,
                  @JsonProperty("default_analyzer") String defaultAnalyzer) {

        this.mapping = new Mapping(columnMappers);
        this.analysis = new Analysis(analyzers);
        this.defaultAnalyzer = analysis.getDefaultAnalyzer(defaultAnalyzer);
        this.analyzer = mapping.getAnalyzer(this.defaultAnalyzer, analysis);
    }

    public Analyzer getDefaultAnalyzer() {
        return defaultAnalyzer;
    }

    public Analyzer getAnalyzer(String name) {
        return analysis.getAnalyzer(name);
    }

    /**
     * Returns the used {@link Analyzer} wrapper.
     *
     * @return The used {@link Analyzer} wrapper.
     */
    public Analyzer getAnalyzer() {
        return analyzer;
    }

    /**
     * Returns the {@link ColumnMapper} identified by the specified field name, or {@code null} if not found.
     *
     * @param field A field name.
     * @return The {@link ColumnMapper} identified by the specified field name, or {@code null} if not found.
     */
    public ColumnMapper getMapper(String field) {
        return mapping.getMapper(field);
    }

    /**
     * Returns the {@linkColumnMapperSingle} identified by the specified field name, or {@code null} if not found.
     *
     * @param field A field name.
     * @return The {@link ColumnMapperSingle} identified by the specified field name, or {@code null} if not found.
     */
    public ColumnMapperSingle<?> getMapperSingle(String field) {
        return mapping.getMapperSingle(field);
    }

    /**
     * Adds to the specified {@link Document} the Lucene fields representing the specified {@link
     * com.stratio.cassandra.index.schema.Columns}.
     *
     * @param document The Lucene {@link Document} where the fields are going to be added.
     * @param columns  The {@link Columns} to be added.
     */
    public void addFields(Document document, Columns columns) {
        mapping.addFields(document, columns);
    }

    /**
     * Checks if this is consistent with the specified column family metadata.
     *
     * @param metadata A column family metadata.
     */
    public void validate(CFMetaData metadata) {
        mapping.validate(metadata);
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
    public void close() {
        analyzer.close();
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                      .add("analysis", analysis)
                      .add("mapping", mapping)
                      .add("defaultAnalyzer", defaultAnalyzer)
                      .add("analyzer", analyzer)
                      .toString();
    }
}
