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
package com.stratio.cassandra.index;

import com.stratio.cassandra.index.schema.Column;
import com.stratio.cassandra.index.schema.ColumnMapper;
import com.stratio.cassandra.index.schema.Columns;
import com.stratio.cassandra.index.schema.Schema;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Row;
import org.apache.cassandra.db.composites.CellName;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;

import java.nio.ByteBuffer;

/**
 * Class for several {@link Row} mappings between Cassandra and Lucene.
 *
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class RowMapper
{

    protected final CFMetaData metadata;
    protected final ColumnDefinition columnDefinition;
    protected final Schema schema;

    protected final TokenMapper tokenMapper;
    protected final PartitionKeyMapper partitionKeyMapper;
    protected final RegularCellsMapper regularCellsMapper;

    /**
     * Builds a new {@link RowMapper} for the specified column family metadata, indexed column definition and {@link Schema}.
     *
     * @param metadata         The indexed column family metadata.
     * @param columnDefinition The indexed column definition.
     * @param schema           The mapping {@link Schema}.
     */
    RowMapper(CFMetaData metadata, ColumnDefinition columnDefinition, Schema schema)
    {
        this.metadata = metadata;
        this.columnDefinition = columnDefinition;
        this.schema = schema;
        this.tokenMapper = TokenMapper.instance(metadata);
        this.partitionKeyMapper = PartitionKeyMapper.instance(metadata);
        this.regularCellsMapper = RegularCellsMapper.instance(metadata);
    }

    /**
     * Returns a new {@link RowMapper} for the specified column family metadata, indexed column definition and {@link Schema}.
     *
     * @param metadata         The indexed column family metadata.
     * @param columnDefinition The indexed column definition.
     * @param schema           The mapping {@link Schema}.
     * @return A new {@link RowMapper} for the specified column family metadata, indexed column definition and {@link Schema}.
     */
    public static RowMapper build(CFMetaData metadata, ColumnDefinition columnDefinition, Schema schema)
    {
        int clusteringPosition = metadata.clusteringColumns().size();
        if (clusteringPosition > 0)
        {
            return new RowMapperWide(metadata, columnDefinition, schema);
        }
        else
        {
            return new RowMapper(metadata, columnDefinition, schema);
        }
    }

    /**
     * Returns the {@link Columns} representing the specified {@link Row}.
     *
     * @param row A {@link Row}.
     * @return The columns contained in the specified columns.
     */
    public Columns columns(Row row)
    {
        Columns columns = new Columns(row);
        columns.addAll(partitionKeyMapper.columns(row));
        columns.addAll(regularCellsMapper.columns(row));
        return columns;
    }

    /**
     * Returns the {@link Document} representing the specified {@link Row}.
     *
     * @param row A {@link Row}.
     * @return The {@link Document} representing the specified {@link Row}.
     */
    public Document document(Row row)
    {
        DecoratedKey partitionKey = row.key;

        Document document = new Document();

        tokenMapper.addFields(document, partitionKey);
        partitionKeyMapper.addFields(document, partitionKey);

        for (Column column : columns(row))
        {
            String name = column.getName();
            String fieldName = column.getFieldName();
            Object value = column.getValue();
            ColumnMapper<?> columnMapper = schema.getMapper(name);
            if (columnMapper != null)
            {
                Field field = columnMapper.field(fieldName, value);
                document.add(field);
            }
        }
        return document;
    }

    /**
     * Returns the decorated partition key representing the specified raw partition key.
     *
     * @param key A partition key.
     * @return The decorated partition key representing the specified raw partition key.
     */
    public DecoratedKey partitionKey(ByteBuffer key)
    {
        return partitionKeyMapper.decoratedKey(key);
    }

    /**
     * Returns the decorated partition key contained in the specified {@link Document}.
     *
     * @param document A {@link Document}.
     * @return The decorated partition key contained in the specified {@link Document}.
     */
    public DecoratedKey partitionKey(Document document)
    {
        return partitionKeyMapper.decoratedKey(document);
    }

    /**
     * Returns the Lucene {@link Query} to get the {@link Document}s containing the specified decorated partition key.
     *
     * @param partitionKey A decorated partition key.
     * @return The Lucene {@link Query} to get the {@link Document}s containing the specified decorated partition key.
     */
    public Query query(DecoratedKey partitionKey)
    {
        return partitionKeyMapper.query(partitionKey);
    }

    /**
     * Returns the Lucene {@link Term} to get the {@link Document}s containing the specified decorated partition key.
     *
     * @param partitionKey A decorated partition key.
     * @return The Lucene {@link Term} to get the {@link Document}s containing the specified decorated partition key.
     */
    public Term term(DecoratedKey partitionKey)
    {
        return partitionKeyMapper.term(partitionKey);
    }

    /**
     * Returns the Lucene {@link Sort} to get {@link Document}s in the same order that is used in Cassandra.
     *
     * @return The Lucene {@link Sort} to get {@link Document}s in the same order that is used in Cassandra.
     */
    public Sort sort()
    {
        return new Sort(tokenMapper.sortFields());
    }

    /**
     * Returns the Lucene {@link Filter} to get the {@link Document}s satisfying the specified {@link DataRange}.
     *
     * @param dataRange A {@link DataRange}.
     * @return The Lucene {@link Filter} to get the {@link Document}s satisfying the specified {@link DataRange}.
     */
    public Filter filter(DataRange dataRange)
    {
        return tokenMapper.filter(dataRange);
    }

    /**
     * Returns a {@link CellName} for the indexed column in the specified column family.
     *
     * @param columnFamily A column family.
     * @return A {@link CellName} for the indexed column in the specified column family.
     */
    public CellName makeCellName(ColumnFamily columnFamily)
    {
        return metadata.comparator.makeCellName(columnDefinition.name.bytes);
    }

    /**
     * Returns a {@link RowComparator} using the same order that is used in Cassandra.
     *
     * @return A {@link RowComparator} using the same order that is used in Cassandra.
     */
    public RowComparator naturalComparator()
    {
        return new RowComparatorNatural();
    }
}
