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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.Map.Entry;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.cql3.CQL3Row;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.composites.CellName;
import org.apache.cassandra.db.composites.CellNameType;
import org.apache.cassandra.db.composites.Composite;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.miscellaneous.PerFieldAnalyzerWrapper;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.util.Version;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

import com.stratio.cassandra.index.AnalyzerFactory;
import com.stratio.cassandra.index.util.ByteBufferUtils;
import com.stratio.cassandra.index.util.JsonSerializer;

/**
 * Class for several culumns mappings between Cassandra and Lucene.
 * 
 * @author Andres de la Pena <adelapena@stratio.com>
 * 
 */
public class Schema
{

    /** The default Lucene's analyzer to be used if no other specified. */
    public static final Analyzer DEFAULT_ANALYZER = new StandardAnalyzer(Version.LUCENE_48);

    /** The Lucene's {@link org.apache.lucene.analysis.Analyzer}. */
    private final Analyzer defaultAnalyzer;

    /** The per field Lucene's analyzer to be used. */
    private final PerFieldAnalyzerWrapper perFieldAnalyzer;

    /** The column mappers. */
    private Map<String, ColumnMapper<?>> columnMappers;

    /**
     * Builds a new {@code ColumnsMapper} for the specified analyzer and cell mappers.
     * 
     * @param analyzerClassName
     *            The name of the class of the analyzer to be used.
     * @param columnMappers
     *            The {@link Column} mappers to be used.
     */
    @JsonCreator
    public Schema(@JsonProperty("default_analyzer") String analyzerClassName,
                  @JsonProperty("fields") Map<String, ColumnMapper<?>> columnMappers)
    {

        // Copy lower cased mappers
        this.columnMappers = columnMappers;

        // Setup default analyzer
        if (analyzerClassName == null)
        {
            this.defaultAnalyzer = DEFAULT_ANALYZER;
        }
        else
        {
            this.defaultAnalyzer = AnalyzerFactory.getAnalyzer(analyzerClassName);
        }

        // Setup per field analyzer
        Map<String, Analyzer> analyzers = new HashMap<>();
        for (Entry<String, ColumnMapper<?>> entry : columnMappers.entrySet())
        {
            String name = entry.getKey();
            ColumnMapper<?> mapper = entry.getValue();
            Analyzer fieldAnalyzer = mapper.analyzer();
            if (fieldAnalyzer != null)
            {
                analyzers.put(name, fieldAnalyzer);
            }
        }
        perFieldAnalyzer = new PerFieldAnalyzerWrapper(defaultAnalyzer, analyzers);
    }

    /**
     * Checks if this is consistent with the specified column family metadata.
     * 
     * @param metadata
     *            A column family metadata.
     * @throws ConfigurationException
     *             If this is not consistent with the specified column family metadata.
     */
    public void validate(CFMetaData metadata) throws ConfigurationException
    {
        for (Entry<String, ColumnMapper<?>> entry : columnMappers.entrySet())
        {

            String name = entry.getKey();
            ColumnMapper<?> columnMapper = entry.getValue();
            ByteBuffer columnName = UTF8Type.instance.decompose(name);

            ColumnDefinition columnDefinition = metadata.getColumnDefinition(columnName);
            if (columnDefinition == null)
            {
                throw new RuntimeException("No column definition for mapper " + name);
            }

            AbstractType<?> type = columnDefinition.type;
            if (!columnMapper.supports(type))
            {
                throw new RuntimeException("Not supported type for mapper " + name);
            }
        }
    }

    /**
     * Returns all the {@link Column}s representing the CQL3 columns contained in the specified column family.
     * 
     * @param metadata
     *            The column family metadata
     * @param row
     *            A {@link Row}.
     * @return The cells contained in the specified columns.
     */
    public Columns cells(CFMetaData metadata, Row row)
    {
        Columns columns = new Columns(row);
        columns.addAll(partitionKeyCells(metadata, row.key));
        columns.addAll(clusteringKeyCells(metadata, row.cf));
        columns.addAll(regularCells(metadata, row.cf));
        return columns;
    }

    /**
     * Returns the {@link Column}s representing the CQL3 cells contained in the specified partition key.
     * 
     * @param metadata
     *            The indexed column family meta data.
     * @param partitionKey
     *            The partition key.
     * @return the {@link Column}s representing the CQL3 cells contained in the specified partition key.
     */
    private List<Column> partitionKeyCells(CFMetaData metadata, DecoratedKey partitionKey)
    {
        List<Column> columns = new LinkedList<>();
        AbstractType<?> rawKeyType = metadata.getKeyValidator();
        List<ColumnDefinition> columnDefinitions = metadata.partitionKeyColumns();
        for (ColumnDefinition columnDefinition : columnDefinitions)
        {
            String name = columnDefinition.name.toString();
            ByteBuffer[] components = ByteBufferUtils.split(partitionKey.getKey(), rawKeyType);
            int position = columnDefinition.position();
            ByteBuffer value = components[position];
            AbstractType<?> valueType = rawKeyType.getComponents().get(position);
            columns.add(ColumnMapper.cell(name, value, valueType));
        }
        return columns;
    }

    /**
     * Returns the clustering key {@link Column}s representing the CQL3 cells contained in the specified column family.
     * The clustering key, if exists, is contained in each {@link Column} of {@code columnFamily}.
     * 
     * @param metadata
     *            The indexed column family meta data.
     * @param columnFamily
     *            The column family.
     * @return The clustering key {@link Column}s representing the CQL3 columns contained in the specified column family.
     */
    private List<Column> clusteringKeyCells(CFMetaData metadata, ColumnFamily columnFamily)
    {
        int numClusteringColumns = metadata.clusteringColumns().size();
        List<Column> columns = new ArrayList<>(numClusteringColumns);

        for (Cell cell : columnFamily) {
            CellName cellName = cell.name();
            for (int i = 0; i < numClusteringColumns; i++) {
                ByteBuffer value = cellName.get(i);
                ColumnDefinition columnDefinition = metadata.clusteringColumns().get(i);
                String name = columnDefinition.name.toString();
                AbstractType<?> valueType = columnDefinition.type;
                columns.add(ColumnMapper.cell(name, value, valueType));
                System.out.println("ADDING CLUSTERING CELL "  + name);
            }
        }
        return columns;
    }

    /**
     * Returns the regular {@link Column}s representing the CQL3 columns contained in the specified column family.
     * 
     * @param metadata
     *            The indexed column family meta data.
     * @param columnFamily
     *            The column family.
     * @return The regular {@link Column}s representing the CQL3 columns contained in the specified column family.
     */
    @SuppressWarnings("rawtypes")
    private List<Column> regularCells(CFMetaData metadata, ColumnFamily columnFamily)
    {

        List<Column> columns = new LinkedList<>();

//        // Get row's cells iterator skipping clustering column
//        Iterator<Cell> cellIterator = columnFamily.iterator();
//        cellIterator.next();
//
//        // Stuff for grouping collection cells (sets, lists and maps)
//        String name = null;
//        CollectionType collectionType = null;
//
//        // int clusteringPosition = metadata.getCfDef().columns.size();
//        int clusteringPosition = metadata.allColumns().size();
//        CellNameType nameType = (CellNameType) metadata.comparator;
//
//        while (cellIterator.hasNext())
//        {
//
//            Cell cell = cellIterator.next();
//
//            ByteBuffer columnName = cell.name().toByteBuffer();
//            ByteBuffer columnValue = cell.value();
//
//            ByteBuffer[] columnNameComponents = nameType.split(columnName);
//            ByteBuffer columnSimpleName = columnNameComponents[clusteringPosition];
//
//            ColumnDefinition columnDefinition = metadata.getColumnDefinition(columnSimpleName);
//            final AbstractType<?> valueType = columnDefinition.getValidator();
//            int position = columnDefinition.position();
//
//            name = UTF8Type.instance.compose(columnDefinition.name);
//
//            if (valueType.isCollection())
//            {
//                collectionType = (CollectionType<?>) valueType;
//                switch (collectionType.kind)
//                {
//                case SET:
//                {
//                    AbstractType<?> type = collectionType.nameComparator();
//                    ByteBuffer value = ByteBufferUtils.split(column.name(), nameType)[position + 1];
//                    columns.add(ColumnMapper.cell(name, value, type));
//                    break;
//                }
//                case LIST:
//                {
//                    AbstractType<?> type = collectionType.valueComparator();
//                    ByteBuffer value = column.value();
//                    columns.add(ColumnMapper.cell(name, value, type));
//                    break;
//                }
//                case MAP:
//                {
//                    AbstractType<?> type = collectionType.valueComparator();
//                    AbstractType<?> keyType = collectionType.nameComparator();
//                    ByteBuffer keyValue = ByteBufferUtils.split(column.name(), nameType)[position + 1];
//                    ByteBuffer value = column.value();
//                    String nameSufix = keyType.compose(keyValue).toString();
//                    columns.add(ColumnMapper.cell(name, nameSufix, value, type));
//                    break;
//                }
//                }
//            }
//            else
//            {
//                columns.add(ColumnMapper.cell(name, columnValue, valueType));
//            }
//        }

        return columns;
    }

    /**
     * Returns the used {@link PerFieldAnalyzerWrapper}.
     * 
     * @return The used {@link PerFieldAnalyzerWrapper}.
     */
    public PerFieldAnalyzerWrapper analyzer()
    {
        return perFieldAnalyzer;
    }

    public void addFields(Document document, CFMetaData metadata, Row row)
    {
        Columns columns = cells(metadata, row);
        for (Column column : columns)
        {
            String name = column.getName();
            String fieldName = column.getFieldName();
            Object value = column.getValue();
            ColumnMapper<?> columnMapper = columnMappers.get(name);
            if (columnMapper != null)
            {
                Field field = columnMapper.field(fieldName, value);
                document.add(field);
            }
        }
    }

    public ColumnMapper<?> getMapper(String field)
    {
        ColumnMapper<?> columnMapper = columnMappers.get(field);
        if (columnMapper == null)
        {
            String[] components = field.split("\\.");
            if (components.length < 2)
            {
                throw new IllegalArgumentException("Not found mapper for field " + field);
            }
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < components.length - 1; i++)
            {
                sb.append(components[i]);
                if (i < components.length - 2)
                {
                    sb.append(".");
                }
            }
            return getMapper(sb.toString());
        }
        else
        {
            return columnMapper;
        }
    }

    /**
     * Returns the {@link Schema} contained in the specified JSON {@code String}.
     * 
     * @param json
     *            A {@code String} containing the JSON representation of the {@link Schema} to be parsed.
     * @return The {@link Schema} contained in the specified JSON {@code String}.
     */
    public static Schema fromJson(String json) throws IOException
    {
        return JsonSerializer.fromString(json, Schema.class);
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder();
        builder.append("Schema [defaultAnalyzer=");
        builder.append(defaultAnalyzer);
        builder.append(", perFieldAnalyzer=");
        builder.append(perFieldAnalyzer);
        builder.append(", columnMappers=");
        builder.append(columnMappers);
        builder.append("]");
        return builder.toString();
    }

}
