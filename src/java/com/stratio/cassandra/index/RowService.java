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

import com.stratio.cassandra.index.query.Search;
import com.stratio.cassandra.index.schema.Column;
import com.stratio.cassandra.index.schema.Columns;
import com.stratio.cassandra.index.schema.Schema;
import com.stratio.cassandra.index.util.Log;
import com.stratio.cassandra.index.util.TaskQueue;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.composites.CellNameType;
import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.lucene.document.Document;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

/**
 * Class for mapping rows between Cassandra and Lucene.
 *
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public abstract class RowService
{

    protected final ColumnFamilyStore baseCfs;
    protected final ColumnDefinition columnDefinition;
    protected final CFMetaData metadata;
    protected final CellNameType nameType;
    protected final ColumnIdentifier indexedColumnName;
    protected final Schema schema;
    protected final LuceneIndex luceneIndex;
    protected final FilterCache filterCache;

    /**
     * The max number of rows to be read per iteration
     */
    private static final int MAX_PAGE_SIZE = 100000;
    private static final int FILTERING_PAGE_SIZE = 1000;

    private TaskQueue indexQueue;

    /**
     * The partitioning token mapper
     */
    protected final TokenMapper tokenMapper;

    /**
     * The partitioning key mapper
     */
    protected final PartitionKeyMapper partitionKeyMapper;

    /**
     * Returns a new {@code RowService}.
     *
     * @param baseCfs          The base column family store.
     * @param columnDefinition The indexed column definition.
     */
    protected RowService(ColumnFamilyStore baseCfs, ColumnDefinition columnDefinition)
    {

        this.baseCfs = baseCfs;
        this.columnDefinition = columnDefinition;
        metadata = baseCfs.metadata;
        nameType = metadata.comparator;
        indexedColumnName = columnDefinition.name;

        RowIndexConfig config = new RowIndexConfig(metadata, columnDefinition.getIndexOptions());

        filterCache = config.getFilterCache();

        schema = config.getSchema();

        luceneIndex = new LuceneIndex(config.getPath(),
                                      config.getRefreshSeconds(),
                                      config.getRamBufferMB(),
                                      config.getMaxMergeMB(),
                                      config.getMaxCachedMB(),
                                      schema.analyzer());

        indexQueue = new TaskQueue(config.getIndexingThreads(), config.getIndexingQueuesSize());

        partitionKeyMapper = PartitionKeyMapper.instance();
        tokenMapper = TokenMapper.instance(baseCfs);
    }

    /**
     * Returns a new {@link RowService} for the specified {@link ColumnFamilyStore} and {@link ColumnDefinition}.
     *
     * @param baseCfs          The {@link ColumnFamilyStore} associated to the managed index.
     * @param columnDefinition The {@link ColumnDefinition} of the indexed column.
     * @return A new {@link RowService} for the specified {@link ColumnFamilyStore} and {@link ColumnDefinition}.
     */
    public static RowService build(ColumnFamilyStore baseCfs, ColumnDefinition columnDefinition) throws IOException
    {
        int clusteringPosition = baseCfs.metadata.clusteringColumns().size();
        if (clusteringPosition > 0)
        {
            return new RowServiceWide(baseCfs, columnDefinition);
        }
        else
        {
            return new RowServiceSimple(baseCfs, columnDefinition);
        }
    }

    /**
     * Returns the used {@link Schema}.
     *
     * @return The used {@link Schema}.
     */
    protected final Schema getSchema()
    {
        return schema;
    }

    /**
     * Returns the names of the document fields to be loaded when reading a Lucene index.
     *
     * @return The names of the document fields to be loaded.
     */
    protected abstract Set<String> fieldsToLoad();

    /**
     * Indexes the logical {@link Row} identified by the specified key and column family using the specified time stamp.
     * The must be read from the {@link ColumnFamilyStore} because it could exist previously having more columns than
     * the specified ones. The specified {@link ColumnFamily} is used for determine the cluster key. This operation is
     * performed asynchronously.
     *
     * @param key          A partition key.
     * @param columnFamily A {@link ColumnFamily} with a single common cluster key.
     * @param timestamp    The insertion time.
     */
    protected void index(final ByteBuffer key, final ColumnFamily columnFamily, final long timestamp)
    {
        indexQueue.submitAsynchronous(key, new Runnable()
        {
            @Override
            public void run()
            {
                try
                {
                    indexInner(key, columnFamily, timestamp);
                }
                catch (Exception e)
                {
                    Log.error(e, "Error while running indexing task");
                }
            }
        });
    }

    /**
     * Puts in the Lucene index the Cassandra's the row identified by the specified partition key and the clustering
     * keys contained in the specified {@link ColumnFamily}.
     *
     * @param key          The partition key.
     * @param columnFamily The column family containing the clustering keys.
     * @param timestamp    The operation time stamp.
     */
    protected abstract void indexInner(ByteBuffer key, ColumnFamily columnFamily, long timestamp) throws IOException;

    /**
     * Deletes the partition identified by the specified partition key. This operation is performed asynchronously.
     *
     * @param partitionKey The partition key identifying the partition to be deleted.
     */
    public void delete(final DecoratedKey partitionKey)
    {
        indexQueue.submitAsynchronous(partitionKey, new Runnable()
        {
            @Override
            public void run()
            {
                try
                {
                    deleteInner(partitionKey);
                }
                catch (Exception e)
                {
                    Log.error(e, "Error while running deletion task");
                }
            }
        });
    }

    /**
     * Deletes the partition identified by the specified partition key.
     *
     * @param partitionKey The partition key identifying the partition to be deleted.
     */
    protected abstract void deleteInner(DecoratedKey partitionKey) throws IOException;

    /**
     * Deletes all the {@link Document}s.
     */
    public final void truncate() throws IOException
    {
        luceneIndex.truncate();
    }

    /**
     * Closes and removes all the index files.
     */
    public final void delete() throws IOException
    {
        luceneIndex.drop();
    }

    /**
     * Commits the pending changes. This operation is performed asynchronously.
     */
    public final void commit()
    {
        indexQueue.submitSynchronous(new Runnable()
        {
            @Override
            public void run()
            {
                try
                {
                    luceneIndex.commit();
                }
                catch (Exception e)
                {
                    Log.error(e, "Error while running commit task");
                }
            }
        });
    }

    /**
     * Returns the stored and indexed {@link Row}s satisfying the specified restrictions.
     *
     * @param search      The {@link Search} to be performed.
     * @param expressions A list of filtering {@link IndexExpression}s to be satisfied.
     * @param dataRange   A {@link DataRange} to be satisfied.
     * @param limit       The max number of {@link Row}s to be returned.
     * @param timestamp   The operation time stamp.
     * @return The {@link Row}s satisfying the specified restrictions.
     */
    public final List<Row> search(Search search,
                                  List<IndexExpression> expressions,
                                  DataRange dataRange,
                                  final int limit,
                                  long timestamp) throws IOException
    {
        Log.debug("Searching with search %s ", search);

        // Setup search arguments
        Filter filter = cachedFilter(dataRange);
        Query query = search.filteredQuery(schema, filter);
        Sort sort = search.sort(schema);

        // Setup search pagination
        List<Row> rows = new LinkedList<>(); // The row list to be returned
        ScoredDocument lastDoc = null; // The last search result
        boolean pageFull;
        long searchTime = 0;
        long collectTime = 0;
        int numPages = 0;
        int numCollected = 0;

        // Paginate search collecting documents
        List<ScoredDocument> scoredDocuments;
        int pageSize = Math.min(limit, MAX_PAGE_SIZE);
        do
        {
            // Search rows identifiers in Lucene
            long searchStartTime = System.currentTimeMillis();
            scoredDocuments = luceneIndex.search(query, sort, lastDoc, pageSize, fieldsToLoad());
            searchTime += System.currentTimeMillis() - searchStartTime;
            lastDoc = scoredDocuments.isEmpty() ? null : scoredDocuments.get(scoredDocuments.size() -  1);

            // Collect rows from Cassandra
            long collectStartTime = System.currentTimeMillis();
            for (Row row : rows(scoredDocuments, timestamp))
            {
                numCollected++;
                if (row != null && accepted(row, expressions))
                {
                    rows.add(row);
                }

            }
            collectTime += System.currentTimeMillis() - collectStartTime;

            // Setup next iteration
            pageFull = scoredDocuments.size() == pageSize;
            pageSize = Math.max(FILTERING_PAGE_SIZE, rows.size() - limit);
            numPages++;

            // Iterate while there are still documents to read and we don't have enough rows
        } while (pageFull && rows.size() < limit);

        Log.debug("Lucene time: %d ms", searchTime);
        Log.debug("Cassandra time: %d ms", collectTime);
        Log.debug("Collected %d rows in %d pages", numCollected, numPages);
        Log.debug("Filtered %d rows in %d pages", rows.size(), numPages);

        return rows;
    }

    /**
     * Returns {@code true} if the specified {@link Row} satisfies the all the specified {@link IndexExpression}s,
     * {@code false} otherwise.
     *
     * @param row         A {@link Row}.
     * @param expressions A list of {@link IndexExpression}s to be satisfied by {@code row}.
     * @return {@code true} if the specified {@link Row} satisfies the all the specified {@link IndexExpression}s,
     * {@code false} otherwise.
     */
    private boolean accepted(Row row, List<IndexExpression> expressions)
    {
        if (!expressions.isEmpty())
        {
            Columns columns = schema.cells(metadata, row);
            for (IndexExpression expression : expressions)
            {
                if (!accepted(columns, expression))
                {
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * Returns {@code true} if the specified {@link com.stratio.cassandra.index.schema.Columns} satisfies the the specified {@link IndexExpression},
     * {@code false} otherwise.
     *
     * @param columns    A {@link com.stratio.cassandra.index.schema.Columns}
     * @param expression A {@link IndexExpression}s to be satisfied by {@code cells}.
     * @return {@code true} if the specified {@link com.stratio.cassandra.index.schema.Columns} satisfies the the specified {@link IndexExpression},
     * {@code false} otherwise.
     */
    private boolean accepted(Columns columns, IndexExpression expression)
    {

        ByteBuffer expectedValue = expression.value;

        ColumnDefinition def = metadata.getColumnDefinition(expression.column);
        String name = def.name.toString();

        Column column = columns.getCell(name);
        if (column == null)
        {
            return false;
        }

        ByteBuffer actualValue = column.getRawValue();
        if (actualValue == null)
        {
            return false;
        }

        AbstractType<?> validator = def.type;
        int comparison = validator.compare(actualValue, expectedValue);
        switch (expression.operator)
        {
            case EQ:
                return comparison == 0;
            case GTE:
                return comparison >= 0;
            case GT:
                return comparison > 0;
            case LTE:
                return comparison <= 0;
            case LT:
                return comparison < 0;
            default:
                throw new IllegalStateException();
        }
    }

    /**
     * Returns the {@link Row}s identified by the specified {@link Document}s, using the specified time stamp to ignore
     * deleted columns. The {@link Row}s are retrieved from the storage engine, so it involves IO operations.
     *
     * @param scoredDocuments A list of {@link ScoredDocument}s.
     * @param timestamp       The time stamp to ignore deleted columns.
     * @return The {@link Row}s identified by the specified {@link Document}s
     */
    protected abstract List<Row> rows(List<ScoredDocument> scoredDocuments, long timestamp) throws IOException;

    /**
     * Returns the CQL3 {@link Row} identified by the specified {@link QueryFilter}, using the specified time stamp to
     * ignore deleted columns. The {@link Row} is retrieved from the storage engine, so it involves IO operations.
     *
     * @param queryFilter A {@link QueryFilter}.
     * @param timestamp   The time stamp to ignore deleted columns.
     * @return The CQL3 {@link Row} identified by the specified {@link QueryFilter}
     */
    protected final Row row(QueryFilter queryFilter, long timestamp)
    {

        // Read the column family from the storage engine
        ColumnFamily columnFamily = baseCfs.getColumnFamily(queryFilter);

        // Remove deleted column families
        ColumnFamily cleanColumnFamily = ArrayBackedSortedColumns.factory.create(baseCfs.metadata);
        for (Cell cell : columnFamily)
        {
            if (cell.isLive(timestamp))
            {
                cleanColumnFamily.addColumn(cell);
            }
        }

        // Build and return the row
        DecoratedKey partitionKey = queryFilter.key;
        return new Row(partitionKey, cleanColumnFamily);
    }

    /**
     * Returns the Lucene's {@link Sort} to be used when querying.
     *
     * @return The Lucene's {@link Sort} to be used when querying.
     */
    protected abstract Sort sort();

    /**
     * Returns a Lucene's {@link Filter} representing the specified Cassandra's {@link DataRange}.
     *
     * @param dataRange The Cassandra's {@link DataRange} to be mapped.
     * @return A Lucene's {@link Filter} representing the specified Cassandra's {@link DataRange}.
     */
    protected abstract Filter filter(DataRange dataRange);

    /**
     * Returns a Lucene's {@link Filter} representing the specified Cassandra's {@link DataRange} using caching.
     *
     * @param dataRange The Cassandra's {@link DataRange} to be mapped.
     * @return A Lucene's {@link Filter} representing the specified Cassandra's {@link DataRange}.
     */
    protected final Filter cachedFilter(DataRange dataRange)
    {
        AbstractBounds<RowPosition> keyRange = dataRange.keyRange();
        if (filterCache == null)
        {
            Log.debug("Filter cache not present for range %s", keyRange);
            return filter(dataRange);
        }
        Filter filter = filterCache.get(dataRange);
        if (filter == null)
        {
            filter = filter(dataRange);
            if (filter != null)
            {
                Log.debug("Filter cache fails for range %s", keyRange);
                filterCache.put(dataRange, filter);
            }
            else
            {
                Log.debug("Filter cache unneeded for range %s", keyRange);
            }
        }
        else
        {
            Log.debug("Filter cache hits for range %s", keyRange);
        }
        return filter;
    }

    /**
     * Returns the {@link RowsComparator} to be used for ordering the {@link Row}s obtained from the specified
     * {@link Search}. This {@link Comparator} is useful for merging the partial results obtained from running the
     * specified {@link Search} against several indexes.
     *
     * @param search A {@link Search}.
     * @return The {@link RowsComparator} to be used for ordering the {@link Row}s obtained from the specified
     * {@link Search}.
     */
    public RowsComparator comparator(Search search)
    {
        if (search.usesSorting())
        // Sort with search itself
        {
            return new RowsComparatorSorting(metadata, schema, search.getSorting());
        }
        else if (search.usesRelevance())
        // Sort with row's score
        {
            return new RowsComparatorScoring(this);
        }
        else
        // No sorting is needed
        {
            return new RowsComparatorNatural(metadata);
        }
    }

    public RowsComparator naturalComparator()
    {
        return new RowsComparatorNatural(metadata);
    }

    /**
     * Returns the score of the specified {@link Row}.
     *
     * @param row A {@link Row}.
     * @return The score of the specified {@link Row}.
     */
    protected abstract Float score(Row row);

    /**
     * Optimizes the managed Lucene's index. It can be a very heavy operation.
     */
    public void optimize() throws IOException
    {
        luceneIndex.optimize();
    }

    public long getIndexSize() throws IOException
    {
        return luceneIndex.getNumDocs();
    }

}
