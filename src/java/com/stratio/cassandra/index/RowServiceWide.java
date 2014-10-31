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
import org.apache.cassandra.db.filter.ColumnSlice;
import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.db.filter.SliceQueryFilter;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.FilteredQuery;
import org.apache.lucene.search.Query;

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

    private final RowMapperWide rowMapper;

    /**
     * Returns a new {@code RowServiceWide} for manage wide rows.
     *
     * @param baseCfs          The base column family store.
     * @param columnDefinition The indexed column definition.
     */
    public RowServiceWide(ColumnFamilyStore baseCfs, ColumnDefinition columnDefinition) throws IOException
    {
        super(baseCfs, columnDefinition);
        this.rowMapper = (RowMapperWide) super.rowMapper;
        luceneIndex.init(rowMapper.sort());
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
        DecoratedKey partitionKey = rowMapper.partitionKey(key);

        if (columnFamily.iterator().hasNext())
        {
            for (CellName cellName : rowMapper.clusteringKeys(columnFamily))
            {
                Row row = row(partitionKey, cellName, timestamp); // Read row
                Document document = rowMapper.document(row);
                Term term = rowMapper.term(partitionKey, cellName);
                luceneIndex.upsert(term, document); // Store document
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
                    Filter filter = rowMapper.filter(rangeTombstone);
                    Query partitionKeyQuery = rowMapper.query(partitionKey);
                    Query query = new FilteredQuery(partitionKeyQuery, filter);
                    luceneIndex.delete(query);
                }
            }
            else
            {
                Term term = rowMapper.term(partitionKey);
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
        Term term = rowMapper.term(partitionKey);
        luceneIndex.delete(term);
    }

    /**
     * {@inheritDoc}
     * <p/>
     * The {@link Row} is a logical one.
     */
    @Override
    protected Row row(ScoredDocument scoredDocument, long timestamp)
    {
        // Extract row from document
        Document document = scoredDocument.getDocument();
        DecoratedKey partitionKey = rowMapper.partitionKey(document);
        CellName clusteringKey = rowMapper.clusteringKey(document);
        Row row = row(partitionKey, clusteringKey, timestamp);

        if (row == null) {
            return null;
        }

        // Return decorated row
        Float score = scoredDocument.getScore();
        return decorate(row, timestamp, score);
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
        ColumnSlice slice = rowMapper.columnSlice(clusteringKey);

        ColumnSlice[] slices = baseCfs.metadata.hasStaticColumns()
                ? new ColumnSlice[]{baseCfs.metadata.comparator.staticPrefix().slice(), slice}
                : new ColumnSlice[]{slice};

        SliceQueryFilter dataFilter = new SliceQueryFilter(slices, false, Integer.MAX_VALUE, baseCfs.metadata.clusteringColumns().size());
        QueryFilter queryFilter = new QueryFilter(partitionKey, baseCfs.name, dataFilter, timestamp);

        return row(queryFilter, timestamp);
    }

}
