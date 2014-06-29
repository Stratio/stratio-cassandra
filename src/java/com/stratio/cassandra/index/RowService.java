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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.Column;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Row;
import org.apache.cassandra.db.TreeMapBackedSortedColumns;
import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.thrift.IndexExpression;
import org.apache.cassandra.utils.HeapAllocator;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.FilteredQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;

import com.stratio.cassandra.index.LuceneIndex.ScoredDocument;
import com.stratio.cassandra.index.query.Search;
import com.stratio.cassandra.index.query.Sorting;
import com.stratio.cassandra.index.schema.Cell;
import com.stratio.cassandra.index.schema.Cells;
import com.stratio.cassandra.index.schema.Schema;
import com.stratio.cassandra.index.util.Log;
import com.stratio.cassandra.index.util.TaskQueue;

/**
 * Class for mapping rows between Cassandra and Lucene.
 * 
 * @author Andres de la Pena <adelapena@stratio.com>
 * 
 */
public abstract class RowService
{

    protected final ColumnFamilyStore baseCfs;
    protected final CFMetaData metadata;
    protected final CompositeType nameType;
    protected final ColumnIdentifier indexedColumnName;
    protected final Schema schema;
    protected final LuceneIndex luceneIndex;
    protected final FilterCache filterCache;

    private static final int FILTERING_PAGE_SIZE = 1000;

    private TaskQueue indexQueue;

    /**
     * Returns a new {@code RowService}.
     * 
     * @param baseCfs
     *            The base column family store.
     * @param columnDefinition
     *            The indexed column definition.
     */
    protected RowService(ColumnFamilyStore baseCfs, ColumnDefinition columnDefinition)
    {

        this.baseCfs = baseCfs;
        metadata = baseCfs.metadata;
        nameType = (CompositeType) metadata.comparator;
        indexedColumnName = new ColumnIdentifier(columnDefinition.name, columnDefinition.getValidator());

        RowIndexConfig config = new RowIndexConfig(metadata,
                                                   columnDefinition.getIndexName(),
                                                   columnDefinition.getIndexOptions());

        filterCache = config.getFilterCache();

        schema = config.getSchema();

        luceneIndex = new LuceneIndex(config.getPath(),
                                      config.getRefreshSeconds(),
                                      config.getRamBufferMB(),
                                      config.getMaxMergeMB(),
                                      config.getMaxCachedMB(),
                                      schema.analyzer());

        indexQueue = new TaskQueue(config.getIndexingThreads(), config.getIndexingQueuesSize());
    }

    public static RowService build(ColumnFamilyStore baseCfs, ColumnDefinition columnDefinition)
    {
        int clusteringPosition = baseCfs.metadata.clusteringKeyColumns().size();
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
     * Returns the names of the document fields to be loaded when reading a Lucene's index.
     * 
     * @return The names of the document fields to be loaded.
     */
    protected abstract Set<String> fieldsToLoad();

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
     * @param key
     *            The partition key.
     * @param columnFamily
     *            The column family containing the clustering keys.
     * @param timestamp
     *            The operation time stamp.
     */
    protected abstract void indexInner(ByteBuffer key, ColumnFamily columnFamily, long timestamp);

    /**
     * Returns the {@link Document} represented by the specified {@link Row}. It's assumed that the {@link Row} is a
     * CQL3 one, so its {@link ColumnFamily} musts contain one and only one clustering key.
     * 
     * @param row
     *            A {@link Row}.
     * @return The Lucene {@link Document} representing the specified {@link Row}.
     */
    protected abstract Document document(Row row);

    /**
     * Deletes the partition identified by the specified partition key.
     * 
     * @param partitionKey
     *            The partition key identifying the partition to be deleted.
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
                    Log.error(e, "Error while running deleting task");
                }
            }
        });
    }

    /**
     * Deletes the partition identified by the specified partition key.
     * 
     * @param partitionKey
     *            The partition key identifying the partition to be deleted.
     */
    protected abstract void deleteInner(DecoratedKey partitionKey);

    /**
     * Deletes all the {@link Document}s.
     */
    public final void truncate()
    {
        luceneIndex.truncate();
    }

    /**
     * Closes and removes all the index files.
     * 
     * @return
     */
    public final void delete()
    {
        luceneIndex.drop();
    }

    /**
     * Commits the pending changes.
     */
    public final void commit()
    {
        indexQueue.submitSynchronous(new Runnable()
        {
            @Override
            public void run()
            {
                luceneIndex.commit();
            }
        });
    }

    /**
     * Returns the Cassandra rows satisfying {@code extendedFilter}. This rows are retrieved from the Cassandra storage
     * engine.
     * 
     * @param extendedFilter
     *            The filter to be satisfied.
     * @return The Cassandra rows satisfying {@code extendedFilter}.
     */
    public final List<Row> search(Search search,
                                  List<IndexExpression> filteredExpressions,
                                  DataRange dataRange,
                                  final int limit,
                                  long timestamp)
    {
        Log.debug("Searching with search %s ", search);

        // Setup search arguments
        Filter filter = cachedFilter(dataRange);
        Query query = query(search);
        Sort sort = search.sort(schema);

        // Setup search pagination
        List<Row> rows = new LinkedList<>(); // The row list to be returned
        ScoredDocument lastDoc = null; // The last search result
        long searchTime = 0;
        long collectTime = 0;

        // Paginate search collecting documents
        List<ScoredDocument> scoredDocuments;
        int pageSize = limit;
        boolean maybeMore;
        do
        {
            // Search rows identifiers in Lucene
            long searchStartTime = System.currentTimeMillis();
            scoredDocuments = luceneIndex.search(query, filter, sort, lastDoc, pageSize, fieldsToLoad());
            searchTime += System.currentTimeMillis() - searchStartTime;

            // Collect rows from Cassandra
            long collectStartTime = System.currentTimeMillis();
            for (ScoredDocument sd : scoredDocuments)
            {
                lastDoc = sd;
                float score = sd.scoreDoc.score;
                Document document = sd.document;
                Row row = row(document, timestamp);
                if (row != null && accepted(row, filteredExpressions))
                {

                    org.apache.cassandra.db.Column scoringCell = scoreCell(document, score);
                    ColumnFamily decoratedCf = TreeMapBackedSortedColumns.factory.create(baseCfs.metadata);
                    decoratedCf.addColumn(scoringCell);
                    decoratedCf.addAll(row.cf, HeapAllocator.instance);
                    row = new Row(row.key, decoratedCf);

                    rows.add(row);
                }
            }
            collectTime = System.currentTimeMillis() - collectStartTime;

            // Setup next iteration
            maybeMore = scoredDocuments.size() == pageSize;
            pageSize = Math.max(FILTERING_PAGE_SIZE, rows.size() - limit);

            // Iterate while there are still documents to read and we don't have enough rows
        } while (maybeMore && rows.size() < limit);

        Log.debug("Search time: " + searchTime + " ms");
        Log.debug("Collect time: " + collectTime + " ms");
        Log.debug("Query time: " + (System.currentTimeMillis() - timestamp) + " ms");

        return rows;
    }

    private Query query(Search search)
    {
        Query query = search.query(schema);
        Filter filter = search.filter(schema);

        if (query == null && filter == null)
        {
            return new MatchAllDocsQuery();
        }
        else if (query != null && filter == null)
        {
            return query;
        }
        else if (query == null && filter != null)
        {
            return new ConstantScoreQuery(filter);
        }
        else
        {
            return new FilteredQuery(query, filter);
        }
    }

    private boolean accepted(Row row, List<IndexExpression> expressions)
    {
        if (!expressions.isEmpty())
        {
            Cells cells = schema.cells(metadata, row);
            for (IndexExpression expression : expressions)
            {
                if (!accepted(cells, expression))
                {
                    return false;
                }
            }
        }
        return true;
    }

    private boolean accepted(Cells cells, IndexExpression expression)
    {

        ByteBuffer expectedValue = expression.value;

        ColumnDefinition def = metadata.getColumnDefinition(expression.column_name);
        String name = UTF8Type.instance.compose(def.name);

        Cell cell = cells.getCell(name);
        if (cell == null)
        {
            return false;
        }

        ByteBuffer actualValue = cell.getRawValue();
        if (actualValue == null)
        {
            return false;
        }

        AbstractType<?> validator = def.getValidator();
        int comparison = validator.compare(actualValue, expectedValue);
        switch (expression.op)
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
     * Returns the {@link Row} identified by the specified {@link Document}, using the specified time stamp to ignore
     * deleted columns. The {@link Row} is retrieved from the storage engine, so it involves IO operations.
     * 
     * @param document
     *            A {@link Document}
     * @param timestamp
     *            The time stamp to ignore deleted columns.
     * @return The {@link Row} identified by the specified {@link Document}
     */
    protected abstract Row row(Document document, long timestamp);

    /**
     * Returns the CQL3 {@link Row} identified by the specified {@link QueryFilter}, using the specified time stamp to
     * ignore deleted columns. The {@link Row} is retrieved from the storage engine, so it involves IO operations.
     * 
     * @param queryFilter
     *            A query filter
     * @param timestamp
     *            The time stamp to ignore deleted columns.
     * @return The CQL3 {@link Row} identified by the specified {@link QueryFilter}
     */
    protected final Row row(QueryFilter queryFilter, long timestamp)
    {

        // Read the column family from the storage engine
        ColumnFamily columnFamily = baseCfs.getColumnFamily(queryFilter);

        // Remove deleted column families
        ColumnFamily cleanColumnFamily = TreeMapBackedSortedColumns.factory.create(baseCfs.metadata);
        for (Column column : columnFamily)
        {
            if (!column.isMarkedForDelete(timestamp))
            {
                cleanColumnFamily.addColumn(column);
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
     * @param dataRange
     *            The Cassandra's {@link DataRange} to be mapped.
     * @return A Lucene's {@link Filter} representing the specified Cassandra's {@link DataRange}.
     */
    protected abstract Filter filter(DataRange dataRange);

    /**
     * Returns a Lucene's {@link Filter} representing the specified Cassandra's {@link DataRange} using caching.
     * 
     * @param dataRange
     *            The Cassandra's {@link DataRange} to be mapped.
     * @return A Lucene's {@link Filter} representing the specified Cassandra's {@link DataRange}.
     */
    protected final Filter cachedFilter(DataRange dataRange)
    {
        if (filterCache == null)
        {
            Log.debug(" -> Filter cache not present for range " + dataRange.keyRange());
            return filter(dataRange);
        }
        Filter filter = filterCache.get(dataRange);
        if (filter == null)
        {
            filter = filter(dataRange);
            if (filter != null)
            {
                Log.debug(" -> Filter cache fails for range " + dataRange.keyRange());
                filterCache.put(dataRange, filter);
            }
            else
            {
                Log.debug(" -> Filter cache unneeded for range " + dataRange.keyRange());
            }
        }
        else
        {
            Log.debug(" -> Filter cache hits for range " + dataRange.keyRange());
        }
        return filter;
    }

    /**
     * Returns a Lucene's {@link Term} to be used as the unique identifier of a row.
     * 
     * @param row
     *            A {@link Row}.
     * @return A Lucene's {@link Term} to be used as the unique identifier of a row.
     */
    protected abstract Term identifyingTerm(Row row);

    protected abstract ByteBuffer getUniqueId(Document document);

    protected abstract ByteBuffer getUniqueId(Row row);

    /**
     * Return the {@code limit} {@link Row}s of those in the specified {@link Row}s selected according to the specified
     * {@link Search}.
     * 
     * @param search
     *            A {@link Search}.
     * @param rows
     *            A list of {@link Row}s.
     * @param count
     *            The number of {@link Row}s to be returned.
     * @return The {@code limit} {@link Row}s of those in the specified {@link Row}s selected according to the specified
     *         {@link Search}.
     */
    public List<Row> combine(Search search, List<Row> rows, int count)
    {
        Log.debug("Combining %d partial results", rows.size());
        long startTime = System.currentTimeMillis();

        // Skip trivia
        if (rows.isEmpty())
        {
            return rows;
        }

        // If it is not a relevance search, simply trunk results
        if (!search.usesRelevanceOrSorting())
        {
            return rows.size() > count ? rows.subList(0, count) : rows;
        }

        List<Row> result;
        if (search.usesSorting())
        {
            result = combineWithSort(search, rows, count);
        }
        else
        {
            result = combineWithScore(search, rows, count);
        }

        long time = System.currentTimeMillis() - startTime;
        Log.debug("Combined %d partial results to %d rows in %d ms", rows.size(), result.size(), time);

        return result;
    }

    private List<Row> combineWithScore(Search search, List<Row> rows, int count)
    {
        Collections.sort(rows, new Comparator<Row>()
        {
            public int compare(Row r1, Row r2)
            {
                Float score1 = score(r1);
                Float score2 = score(r2);
                return score2.compareTo(score1);
            }
        });
        return rows.size() > count ? rows.subList(0, count) : rows;
    }

    private List<Row> combineWithSort(Search search, List<Row> rows, int count)
    {

        Sorting sorting = search.getSorting();
        Comparator<Cells> comparator = sorting.comparator();
        List<Cells> allCells = new ArrayList<>(rows.size());
        for (Row row : rows)
        {
            Cells cells = schema.cells(metadata, row);
            allCells.add(cells);
        }
        Collections.sort(allCells, comparator);
        List<Row> result = new ArrayList<Row>(rows.size());
        for (Cells cells : allCells)
        {
            result.add(cells.getRow());
        }
        return result.size() > count ? result.subList(0, count) : result;
    }

    protected abstract Column scoreCell(Document document, Float score);

    protected abstract Float score(Row row);

    public void compact()
    {
        luceneIndex.optimize();
    }

}
