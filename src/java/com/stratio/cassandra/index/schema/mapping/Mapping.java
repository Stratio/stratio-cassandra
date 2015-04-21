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
import com.stratio.cassandra.index.schema.Columns;
import com.stratio.cassandra.index.schema.analysis.Analysis;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.miscellaneous.PerFieldAnalyzerWrapper;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexableField;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class Mapping {

    private final Map<String, ColumnMapper> columnMappers;

    public Mapping(Map<String, ColumnMapper> columnMappers) {
        this.columnMappers = columnMappers;
    }

    /**
     * Checks if this is consistent with the specified column family metadata.
     *
     * @param metadata A column family metadata.
     */
    public void validate(CFMetaData metadata) {
        for (Map.Entry<String, ColumnMapper> entry : columnMappers.entrySet()) {

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

            columnMapper.init(columnDefinition);
        }
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

    public Analyzer getAnalyzer(Analyzer defaultAnalyzer, Analysis analysis) {
        Map<String, Analyzer> perFieldAnalyzers = new HashMap<>();
        for (Map.Entry<String, ColumnMapper> entry : columnMappers.entrySet()) {
            String name = entry.getKey();
            ColumnMapper mapper = entry.getValue();
            String analyzerName = mapper.analyzer();
            Analyzer analyzer = analysis.getAnalyzer(analyzerName);
            perFieldAnalyzers.put(name, analyzer);
        }
        return new PerFieldAnalyzerWrapper(defaultAnalyzer, perFieldAnalyzers);
    }

}
