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

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.composites.CellName;
import org.apache.cassandra.db.composites.Composite;
import org.apache.cassandra.db.filter.ColumnSlice;
import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.db.filter.SliceQueryFilter;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.Token;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.ChainedFilter;
import org.apache.lucene.search.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * {@link RowService} that manages wide rows.
 *
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class RowServiceWide extends RowService
{

    private static final Set<String> FIELDS_TO_LOAD;

    static
    {
        FIELDS_TO_LOAD = new HashSet<>();
        FIELDS_TO_LOAD.add(PartitionKeyMapper.FIELD_NAME);
        FIELDS_TO_LOAD.add(ClusteringKeyMapper.FIELD_NAME);
    }

    private final ClusteringKeyMapper clusteringKeyMapper;
    private final FullKeyMapper fullKeyMapper;

    /**
     * Returns a new {@code RowServiceWide} for manage wide rows.
     *
     * @param baseCfs          The base column family store.
     * @param columnDefinition The indexed column definition.
     */
    public RowServiceWide(ColumnFamilyStore baseCfs, ColumnDefinition columnDefinition) throws IOException
    {
        super(baseCfs, columnDefinition);

        clusteringKeyMapper = ClusteringKeyMapper.instance(metadata);
        fullKeyMapper = FullKeyMapper.instance(metadata);

        luceneIndex.init(sort());
    }

    /**
     * {@inheritDoc}
     * <p/>
     * These fields are the partition and clustering keys.
     */
    @Override
    public Set<String> fieldsToLoad()
    {
        return FIELDS_TO_LOAD;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void indexInner(ByteBuffer key, ColumnFamily columnFamily, long timestamp) throws IOException
    {
        DeletionInfo deletionInfo = columnFamily.deletionInfo();
        DecoratedKey partitionKey = partitionKeyMapper.decoratedKey(key);

        if (columnFamily.iterator().hasNext())
        {
            for (CellName cellName : clusteringKeyMapper.cellNames(columnFamily))
            {
                // Load row
                Row row = row(partitionKey, cellName, timestamp);

                // Create document from row
                Document document = new Document();
                tokenMapper.addFields(document, partitionKey);
                partitionKeyMapper.addFields(document, partitionKey);
                clusteringKeyMapper.addFields(document, cellName);
                fullKeyMapper.addFields(document, partitionKey, cellName);
                schema.addFields(document, metadata, row);

                // Create document's identifying term for insert-update
                Term term = fullKeyMapper.term(partitionKey, cellName);

                // Insert-update on Lucene
                luceneIndex.upsert(term, document);
            }
        }
        else if (deletionInfo != null)
        {
            Iterator<RangeTombstone> iterator = deletionInfo.rangeIterator();
            if (iterator.hasNext())
            {
                while (iterator.hasNext())
                {
                    RangeTombstone rangeTombstone = iterator.next();
                    Filter filter = clusteringKeyMapper.filter(rangeTombstone);
                    Query partitionKeyQuery = partitionKeyMapper.query(partitionKey);
                    Query query = new FilteredQuery(partitionKeyQuery, filter);
                    luceneIndex.delete(query);
                }
            }
            else
            {
                Term term = partitionKeyMapper.term(partitionKey);
                luceneIndex.delete(term);
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteInner(DecoratedKey partitionKey) throws IOException
    {
        Term term = partitionKeyMapper.term(partitionKey);
        luceneIndex.delete(term);
    }

    /**
     * {@inheritDoc}
     * <p/>
     * The {@link Row} is a logical one.
     */
    @Override
    protected Row row(ScoredDocument scoredDocument, long timestamp) throws IOException
    {
        // Extract row from document
        Document document = scoredDocument.getDocument();
        DecoratedKey partitionKey = partitionKeyMapper.decoratedKey(document);
        CellName clusteringKey = clusteringKeyMapper.cellName(document);
        Row row = row(partitionKey, clusteringKey, timestamp);

        // Create score column from document score
        Float score = scoredDocument.getScore();
        ByteBuffer cellValue = UTF8Type.instance.decompose(score.toString());
        CellName cellName = clusteringKeyMapper.makeCellName(clusteringKey, columnDefinition);

        // Return new row with score column
        ColumnFamily decoratedCf = ArrayBackedSortedColumns.factory.create(baseCfs.metadata);
        decoratedCf.addColumn(cellName, cellValue, timestamp);
        decoratedCf.addAll(row.cf);
        return new Row(partitionKey, decoratedCf);
    }

    /**
     * Returns the CQL3 {@link Row} identified by the specified key pair, using the specified time stamp to ignore
     * deleted columns. The {@link Row} is retrieved from the storage engine, so it involves IO operations.
     *
     * @param partitionKey  The partition key.
     * @param clusteringKey The clustering key, maybe {@code null}.
     * @param timestamp     The time stamp to ignore deleted columns.
     * @return The CQL3 {@link Row} identified by the specified key pair.
     */
    private Row row(DecoratedKey partitionKey, CellName clusteringKey, long timestamp)
    {
        Composite start = clusteringKeyMapper.start(clusteringKey);
        Composite end = clusteringKeyMapper.end(clusteringKey);
        ColumnSlice slice = new ColumnSlice(start, end);
        SliceQueryFilter dataFilter = new SliceQueryFilter(slice, false, Integer.MAX_VALUE);
        QueryFilter queryFilter = new QueryFilter(partitionKey, baseCfs.name, dataFilter, timestamp);
        return row(queryFilter, timestamp);
    }

    /**
     * {@inheritDoc}
     * <p/>
     * The {@link Filter} is based on {@link Token} first, then clustering key order.
     */
    @Override
    protected Sort sort()
    {
        SortField[] partitionKeySort = tokenMapper.sortFields();
        SortField[] clusteringKeySort = clusteringKeyMapper.sortFields();
        return new Sort(ArrayUtils.addAll(partitionKeySort, clusteringKeySort));
    }

    /**
     * {@inheritDoc}
     * <p/>
     * The {@link Filter} is based on a {@link Token} first, then clustering key range.
     */
    @Override
    protected Filter filter(DataRange dataRange)
    {
        Filter tokenFilter = tokenMapper.filter(dataRange);
        Filter clusteringKeyFilter = clusteringKeyMapper.filter(dataRange);
        if (tokenFilter == null)
        {
            if (clusteringKeyFilter == null)
            {
                return null;
            }
            else
            {
                return clusteringKeyFilter;
            }
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

    @Override
    protected Float score(Row row)
    {
        ColumnFamily cf = row.cf;
        CellName clusteringKey = clusteringKeyMapper.cellNames(cf).iterator().next();
        CellName cellName = clusteringKeyMapper.makeCellName(clusteringKey, columnDefinition);
        Cell cell = cf.getColumn(cellName);
        String value = UTF8Type.instance.compose(cell.value());
        return Float.parseFloat(value);
    }

}
