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

import com.stratio.cassandra.index.schema.Columns;
import com.stratio.cassandra.index.schema.Schema;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.composites.CellName;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * Class for several {@link org.apache.cassandra.db.Row} mappings between Cassandra and Lucene.
 *
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public abstract class RowMapper
{

    protected final CFMetaData metadata;
    protected final ColumnDefinition columnDefinition;
    protected final Schema schema;

    protected final TokenMapper tokenMapper;
    protected final PartitionKeyMapper partitionKeyMapper;
    protected final RegularCellsMapper regularCellsMapper;

    /**
     * Builds a new {@link com.stratio.cassandra.index.RowMapper} for the specified column family metadata, indexed column definition and {@link com.stratio.cassandra.index.schema.Schema}.
     *
     * @param metadata         The indexed column family metadata.
     * @param columnDefinition The indexed column definition.
     * @param schema           The mapping {@link com.stratio.cassandra.index.schema.Schema}.
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
     * Returns a new {@link com.stratio.cassandra.index.RowMapper} for the specified column family metadata, indexed column definition and {@link com.stratio.cassandra.index.schema.Schema}.
     *
     * @param metadata         The indexed column family metadata.
     * @param columnDefinition The indexed column definition.
     * @param schema           The mapping {@link com.stratio.cassandra.index.schema.Schema}.
     * @return A new {@link com.stratio.cassandra.index.RowMapper} for the specified column family metadata, indexed column definition and {@link com.stratio.cassandra.index.schema.Schema}.
     */
    public static RowMapper build(CFMetaData metadata, ColumnDefinition columnDefinition, Schema schema)
    {
        if (metadata.clusteringColumns().size() > 0)
        {
            return new RowMapperWide(metadata, columnDefinition, schema);
        }
        else
        {
            return new RowMapperSkinny(metadata, columnDefinition, schema);
        }
    }

    /**
     * Returns the {@link com.stratio.cassandra.index.schema.Columns} representing the specified {@link org.apache.cassandra.db.Row}.
     *
     * @param row A {@link org.apache.cassandra.db.Row}.
     * @return The columns contained in the specified columns.
     */
    public abstract Columns columns(Row row);

    /**
     * Returns the {@link org.apache.lucene.document.Document} representing the specified {@link org.apache.cassandra.db.Row}.
     *
     * @param row A {@link org.apache.cassandra.db.Row}.
     * @return The {@link org.apache.lucene.document.Document} representing the specified {@link org.apache.cassandra.db.Row}.
     */
    public abstract Document document(Row row);

    /**
     * Returns the decorated partition key representing the specified raw partition key.
     *
     * @param key A partition key.
     * @return The decorated partition key representing the specified raw partition key.
     */
    public final DecoratedKey partitionKey(ByteBuffer key)
    {
        return partitionKeyMapper.decoratedKey(key);
    }

    /**
     * Returns the decorated partition key contained in the specified {@link org.apache.lucene.document.Document}.
     *
     * @param document A {@link org.apache.lucene.document.Document}.
     * @return The decorated partition key contained in the specified {@link org.apache.lucene.document.Document}.
     */
    public final DecoratedKey partitionKey(Document document)
    {
        return partitionKeyMapper.decoratedKey(document);
    }

    /**
     * Returns the Lucene {@link org.apache.lucene.search.Query} to get the {@link org.apache.lucene.document.Document}s containing the specified decorated partition key.
     *
     * @param partitionKey A decorated partition key.
     * @return The Lucene {@link org.apache.lucene.search.Query} to get the {@link org.apache.lucene.document.Document}s containing the specified decorated partition key.
     */
    public final Query query(DecoratedKey partitionKey)
    {
        return partitionKeyMapper.query(partitionKey);
    }

    /**
     * Returns the Lucene {@link org.apache.lucene.index.Term} to get the {@link org.apache.lucene.document.Document}s containing the specified decorated partition key.
     *
     * @param partitionKey A decorated partition key.
     * @return The Lucene {@link org.apache.lucene.index.Term} to get the {@link org.apache.lucene.document.Document}s containing the specified decorated partition key.
     */
    public Term term(DecoratedKey partitionKey)
    {
        return partitionKeyMapper.term(partitionKey);
    }

    /**
     * Returns the Lucene {@link org.apache.lucene.search.Sort} to get {@link org.apache.lucene.document.Document}s in the same order that is used in Cassandra.
     *
     * @return The Lucene {@link org.apache.lucene.search.Sort} to get {@link org.apache.lucene.document.Document}s in the same order that is used in Cassandra.
     */
    public abstract Sort sort();

    /**
     * Returns the Lucene {@link org.apache.lucene.search.Filter} to get the {@link org.apache.lucene.document.Document}s satisfying the specified {@link org.apache.cassandra.db.DataRange}.
     *
     * @param dataRange A {@link org.apache.cassandra.db.DataRange}.
     * @return The Lucene {@link org.apache.lucene.search.Filter} to get the {@link org.apache.lucene.document.Document}s satisfying the specified {@link org.apache.cassandra.db.DataRange}.
     */
    public abstract Query query(DataRange dataRange);

    /**
     * Returns a {@link org.apache.cassandra.db.composites.CellName} for the indexed column in the specified column family.
     *
     * @param columnFamily A column family.
     * @return A {@link org.apache.cassandra.db.composites.CellName} for the indexed column in the specified column family.
     */
    public abstract CellName makeCellName(ColumnFamily columnFamily);

    /**
     * Returns a {@link com.stratio.cassandra.index.RowComparator} using the same order that is used in Cassandra.
     *
     * @return A {@link com.stratio.cassandra.index.RowComparator} using the same order that is used in Cassandra.
     */
    public abstract RowComparator naturalComparator();

    public abstract Comparator<ScoredDocument> scoredDocumentsComparator();

    public List<ScoredDocument> sort(List<ScoredDocument> scoredDocuments)
    {
        List<ScoredDocument> result = new ArrayList<>(scoredDocuments);
        Collections.sort(result, scoredDocumentsComparator());
        return result;
    }

    public Comparator<DecoratedKey> partitionKeyComparator() {
        return partitionKeyMapper.comparator();
    }


}
