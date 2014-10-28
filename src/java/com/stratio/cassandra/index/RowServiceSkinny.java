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
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Row;
import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.Term;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Set;

/**
 * {@link RowService} that manages simple rows.
 *
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class RowServiceSkinny extends RowService
{

    /**
     * The Lucene's fields to be loaded
     */
    private static final Set<String> FIELDS_TO_LOAD;

    static
    {
        FIELDS_TO_LOAD = new HashSet<>();
        FIELDS_TO_LOAD.add(PartitionKeyMapper.FIELD_NAME);
    }

    /**
     * Returns a new {@code RowServiceSimple} for manage simple rows.
     *
     * @param baseCfs          The base column family store.
     * @param columnDefinition The indexed column definition.
     */
    public RowServiceSkinny(ColumnFamilyStore baseCfs, ColumnDefinition columnDefinition) throws IOException
    {
        super(baseCfs, columnDefinition);
        luceneIndex.init(rowMapper.sort());
    }

    /**
     * {@inheritDoc}
     * <p/>
     * These fields are just the partition key.
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
        DecoratedKey partitionKey = rowMapper.partitionKey(key);

        if (columnFamily.iterator().hasNext()) // Create or update row
        {
            Row row = row(partitionKey, timestamp); // Read row
            Document document = rowMapper.document(row);
            Term term = rowMapper.term(partitionKey);
            luceneIndex.upsert(term, document); // Store document
        }
        else if (columnFamily.deletionInfo() != null) // Delete full row
        {
            Term term = rowMapper.term(partitionKey);
            luceneIndex.delete(term);
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
     * The {@link Row} is a physical one.
     */
    @Override
    protected Row row(ScoredDocument scoredDocument, long timestamp)
    {

        // Extract row from document
        Document document = scoredDocument.getDocument();
        DecoratedKey partitionKey = rowMapper.partitionKey(document);
        Row row = row(partitionKey, timestamp);

        // Return decorated row
        Float score = scoredDocument.getScore();
        return decorate(row, timestamp, score);
    }

    /**
     * Returns the CQL3 {@link Row} identified by the specified key pair, using the specified time stamp to ignore
     * deleted columns. The {@link Row} is retrieved from the storage engine, so it involves IO operations.
     *
     * @param partitionKey The partition key.
     * @param timestamp    The time stamp to ignore deleted columns.
     * @return The CQL3 {@link Row} identified by the specified key pair.
     */
    private Row row(DecoratedKey partitionKey, long timestamp)
    {
        QueryFilter queryFilter = QueryFilter.getIdentityFilter(partitionKey, metadata.cfName, timestamp);
        return row(queryFilter, timestamp);
    }


}
