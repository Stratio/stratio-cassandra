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
import com.stratio.cassandra.index.util.ComparatorChain;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.composites.CellName;
import org.apache.cassandra.db.filter.ColumnSlice;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;

import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 * {@link RowMapper} for wide rows.
 *
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class RowMapperWide extends RowMapper
{
    private final ClusteringKeyMapper clusteringKeyMapper;
    private final FullKeyMapper fullKeyMapper;

    private final ComparatorChain<SearchResult> scoredDocumentComparator;
    private final SearchResultBuilder searchResultBuilder;

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
        this.fullKeyMapper = FullKeyMapper.instance(partitionKeyMapper, clusteringKeyMapper);

        final Comparator<DecoratedKey> partitionKeyComparator = partitionKeyMapper.comparator();
        final Comparator<CellName> clusteringKeyComparator = clusteringKeyMapper.comparator();
        scoredDocumentComparator = new ComparatorChain<>();
        scoredDocumentComparator.addComparator(new Comparator<SearchResult>()
        {
            @Override
            public int compare(SearchResult o1, SearchResult o2)
            {
                DecoratedKey pk1 = o1.getPartitionKey();
                DecoratedKey pk2 = o2.getPartitionKey();
                return partitionKeyComparator.compare(pk1, pk2);
            }
        });
        scoredDocumentComparator.addComparator(new Comparator<SearchResult>()
        {
            @Override
            public int compare(SearchResult o1, SearchResult o2)
            {
                CellName c1 = o1.getClusteringKey();
                CellName c2 = o2.getClusteringKey();
                return clusteringKeyComparator.compare(c1, c2);
            }
        });

        this.searchResultBuilder = new SearchResultBuilderWide(partitionKeyMapper, clusteringKeyMapper);
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
    public Sort sort()
    {
        SortField[] partitionKeySort = tokenMapper.sortFields();
        SortField[] clusteringKeySort = clusteringKeyMapper.sortFields();
        return new Sort(ArrayUtils.addAll(partitionKeySort, clusteringKeySort));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CellName makeCellName(ColumnFamily columnFamily)
    {
        CellName clusteringKey = clusteringKeyMapper.clusteringKey(columnFamily);
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
        return clusteringKeyMapper.cellName(document);
    }

    /**
     * Returns all the clustering keys contained in the specified {@link ColumnFamily}.
     *
     * @param columnFamily A {@link ColumnFamily}.
     * @return All the clustering keys contained in the specified {@link ColumnFamily}.
     */
    public List<CellName> clusteringKeys(ColumnFamily columnFamily)
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
     * Returns the Lucene {@link Query} to get the {@link Document}s satisfying the specified {@link RangeTombstone}.
     *
     * @param rangeTombstone A {@link RangeTombstone}.
     * @return The Lucene {@link Query} to get the {@link Document}s satisfying the specified {@link RangeTombstone}.
     */
    public Query query(RangeTombstone rangeTombstone)
    {
        return clusteringKeyMapper.query(rangeTombstone);
    }

    /**
     * Returns the array of {@link ColumnSlice}s for selecting the logic CQL3 row identified by the specified clustering keys.
     *
     * @param clusteringKeys A list of clustering keys.
     * @return The array of {@link ColumnSlice}s for selecting the logic CQL3 row identified by the specified clustering keys.
     */
    public ColumnSlice[] columnSlices(List<CellName> clusteringKeys)
    {
        return clusteringKeyMapper.columnSlices(clusteringKeys);
    }

    /**
     * Returns the logical CQL3 column families contained in the specified physical {@link ColumnFamily}.
     *
     * @param columnFamily A physical {@link ColumnFamily}.
     * @return The logical CQL3 column families contained in the specified physical {@link ColumnFamily}.
     */
    public Map<CellName, ColumnFamily> splitRows(ColumnFamily columnFamily)
    {
        return clusteringKeyMapper.splitRows(columnFamily);
    }

    @Override
    public Comparator<SearchResult> scoredDocumentsComparator()
    {
        return scoredDocumentComparator;
    }

    @Override
    public SearchResultBuilder searchResultBuilder()
    {
        return searchResultBuilder;
    }
}
