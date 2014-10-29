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
import org.apache.cassandra.db.composites.Composite;
import org.apache.cassandra.db.filter.ColumnSlice;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.ChainedFilter;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;

import java.util.Set;

/**
 * {@link RowMapper} for wide rows.
 *
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class RowMapperWide extends RowMapper
{
    private final ClusteringKeyMapper clusteringKeyMapper;
    private final FullKeyMapper fullKeyMapper;

    /**
     * Builds a new {@link RowMapperWide} for the specified column family metadata, indexed column definition and {@link Schema}.
     *
     * @param metadata         The indexed column family metadata.
     * @param columnDefinition The indexed column definition.
     * @param schema           The mapping {@link Schema}.
     */
    RowMapperWide(CFMetaData metadata, ColumnDefinition columnDefinition, Schema schema)
    {
        super(metadata, columnDefinition, schema);
        this.clusteringKeyMapper = ClusteringKeyMapper.instance(metadata);
        this.fullKeyMapper = FullKeyMapper.instance(metadata);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Columns columns(Row row)
    {
        Columns columns = new Columns();
        columns.addAll(partitionKeyMapper.columns(row));
        columns.addAll(clusteringKeyMapper.columns(row));
        columns.addAll(regularCellsMapper.columns(row));
        return columns;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Document document(Row row)
    {
        DecoratedKey partitionKey = row.key;
        CellName clusteringKey = clusteringKeyMapper.cellName(row);

        Document document = new Document();
        tokenMapper.addFields(document, partitionKey);
        partitionKeyMapper.addFields(document, partitionKey);
        clusteringKeyMapper.addFields(document, clusteringKey);
        fullKeyMapper.addFields(document, partitionKey, clusteringKey);
        schema.addFields(document, columns(row));
        return document;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Filter filter(DataRange dataRange)
    {
        Filter tokenFilter = tokenMapper.filter(dataRange);
        Filter clusteringKeyFilter = clusteringKeyMapper.filter(dataRange);
        if (tokenFilter == null)
        {
            return clusteringKeyFilter == null ? null : clusteringKeyFilter;
        }
        else
        {
            if (clusteringKeyFilter == null)
            {
                return tokenFilter;
            }
            else
            {
                Filter[] filters = new Filter[]{tokenFilter, clusteringKeyFilter};
                return new ChainedFilter(filters, ChainedFilter.AND);
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Sort sort()
    {
        SortField[] partitionKeySort = tokenMapper.sort();
        SortField[] clusteringKeySort = clusteringKeyMapper.sortFields();
        return new Sort(ArrayUtils.addAll(partitionKeySort, clusteringKeySort));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CellName makeCellName(ColumnFamily columnFamily)
    {
        CellName clusteringKey = clusteringKey(columnFamily);
        return clusteringKeyMapper.makeCellName(clusteringKey, columnDefinition);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public RowComparator naturalComparator()
    {
        return new RowComparatorNatural(clusteringKeyMapper);
    }

    /**
     * Returns the clustering key contained in the specified {@link Document}.
     *
     * @param document A {@link Document}.
     * @return The clustering key contained in the specified {@link Document}.
     */
    public CellName clusteringKey(Document document)
    {
        return clusteringKeyMapper.clusteringKey(document);
    }

    /**
     * Returns the first clustering key contained in the specified {@link ColumnFamily}.
     *
     * @param columnFamily A {@link ColumnFamily}.
     * @return The first clustering key contained in the specified {@link ColumnFamily}.
     */
    public CellName clusteringKey(ColumnFamily columnFamily)
    {
        return clusteringKeyMapper.clusteringKey(columnFamily);
    }

    /**
     * Returns all the clustering keys contained in the specified {@link ColumnFamily}.
     *
     * @param columnFamily A {@link ColumnFamily}.
     * @return All the clustering keys contained in the specified {@link ColumnFamily}.
     */
    public Set<CellName> clusteringKeys(ColumnFamily columnFamily)
    {
        return clusteringKeyMapper.clusteringKeys(columnFamily);
    }

    /**
     * Returns the Lucene {@link Term} to get the {@link Document}s containing the specified decorated partition key and clustering key.
     *
     * @param partitionKey  A decorated partition key.
     * @param clusteringKey A clustering key.
     * @return The Lucene {@link Term} to get the {@link Document}s containing the specified decorated partition key and clustering key.
     */
    public Term term(DecoratedKey partitionKey, CellName clusteringKey)
    {
        return fullKeyMapper.term(partitionKey, clusteringKey);
    }

    /**
     * Returns the Lucene {@link Filter} to get the {@link Document}s satisfying the specified {@link RangeTombstone}.
     *
     * @param rangeTombstone A {@link RangeTombstone}.
     * @return The Lucene {@link Filter} to get the {@link Document}s satisfying the specified {@link RangeTombstone}.
     */
    public Filter filter(RangeTombstone rangeTombstone)
    {
        return clusteringKeyMapper.filter(rangeTombstone);
    }

    /**
     * Returns the {@link ColumnSlice} for selecting the logic CQL3 row identified by the specified clustering key.
     *
     * @param clusteringKey A clustering key.
     * @return The {@link ColumnSlice} for selecting the logic CQL3 row identified by the specified clustering key.
     */
    public ColumnSlice columnSlice(CellName clusteringKey)
    {
        Composite start = clusteringKeyMapper.start(clusteringKey);
        Composite end = clusteringKeyMapper.end(clusteringKey);
        return new ColumnSlice(start, end);
    }
}
