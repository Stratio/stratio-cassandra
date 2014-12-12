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
import com.stratio.cassandra.index.schema.Schema;
import com.stratio.cassandra.index.util.Log;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.IndexExpression;
import org.apache.cassandra.db.Row;
import org.apache.cassandra.db.filter.ExtendedFilter;
import org.apache.cassandra.db.index.SecondaryIndexManager;
import org.apache.cassandra.db.index.SecondaryIndexSearcher;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.*;

import static org.apache.cassandra.cql3.Operator.EQ;

/**
 * A {@link SecondaryIndexSearcher} for {@link RowIndex}.
 *
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class RowIndexSearcher extends SecondaryIndexSearcher
{

    protected static final Logger logger = LoggerFactory.getLogger(SecondaryIndexSearcher.class);

    private final RowIndex index;
    private final RowService rowService;
    private final Schema schema;
    private final ByteBuffer indexedColumnName;

    /**
     * Returns a new {@code RowIndexSearcher}.
     *
     * @param indexManager A 2i manger.
     * @param index        A {@link RowIndex}.
     * @param columns      A set of columns.
     * @param rowService   A {@link RowService}.
     */
    public RowIndexSearcher(SecondaryIndexManager indexManager,
                            RowIndex index,
                            Set<ByteBuffer> columns,
                            RowService rowService)
    {
        super(indexManager, columns);
        this.index = index;
        this.rowService = rowService;
        schema = rowService.getSchema();
        indexedColumnName = index.getColumnDefinition().name.bytes;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Row> search(ExtendedFilter extendedFilter)
    {
        long timestamp = extendedFilter.timestamp;
        int limit = extendedFilter.currentLimit();
        DataRange dataRange = extendedFilter.dataRange;
        List<IndexExpression> clause = extendedFilter.getClause();
        List<IndexExpression> filteredExpressions = filteredExpressions(clause);
        Search search = search(clause);
        return rowService.search(search, filteredExpressions, dataRange, limit, timestamp);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean canHandleIndexClause(List<IndexExpression> clause)
    {
        for (IndexExpression expression : clause)
        {
            ByteBuffer columnName = expression.column;
            boolean sameName = indexedColumnName.equals(columnName);
            if (expression.operator.equals(EQ) && sameName)
            {
                return true;
            }
        }
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public IndexExpression highestSelectivityPredicate(List<IndexExpression> clause)
    {
        for (IndexExpression expression : clause)
        {
            ByteBuffer columnName = expression.column;
            boolean sameName = indexedColumnName.equals(columnName);
            if (expression.operator.equals(EQ) && sameName)
            {
                return expression;
            }
        }
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void validate(IndexExpression indexExpression) throws InvalidRequestException
    {
        try
        {
            String json = UTF8Type.instance.compose(indexExpression.value);
            Search.fromJson(json).validate(schema);
        }
        catch (Exception e)
        {
            throw new InvalidRequestException(e.getMessage());
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean requiresFullScan(List<IndexExpression> clause)
    {
        Search search = search(clause);
        return search.usesRelevanceOrSorting();
    }

    /**
     * Returns the {@link Search} contained in the specified list of {@link IndexExpression}s.
     *
     * @param clause A list of {@link IndexExpression}s.
     * @return The {@link Search} contained in the specified list of {@link IndexExpression}s.
     */
    private Search search(List<IndexExpression> clause)
    {
        IndexExpression indexedExpression = indexedExpression(clause);
        String json = UTF8Type.instance.compose(indexedExpression.value);
        return Search.fromJson(json);
    }

    /**
     * Returns the {@link IndexExpression} relative to this index.
     *
     * @param clause A list of {@link IndexExpression}s.
     * @return The {@link IndexExpression} relative to this index.
     */
    private IndexExpression indexedExpression(List<IndexExpression> clause)
    {
        for (IndexExpression indexExpression : clause)
        {
            ByteBuffer columnName = indexExpression.column;
            if (indexedColumnName.equals(columnName))
            {
                return indexExpression;
            }
        }
        return null;
    }

    /**
     * Returns the {@link IndexExpression} not relative to this index.
     *
     * @param clause A list of {@link IndexExpression}s.
     * @return The {@link IndexExpression} not relative to this index.
     */
    private List<IndexExpression> filteredExpressions(List<IndexExpression> clause)
    {
        List<IndexExpression> filteredExpressions = new ArrayList<>(clause.size());
        for (IndexExpression ie : clause)
        {
            ByteBuffer columnName = ie.column;
            if (!indexedColumnName.equals(columnName))
            {
                filteredExpressions.add(ie);
            }
        }
        return filteredExpressions;
    }

    @Override
    public List<Row> sort(List<IndexExpression> clause, List<Row> rows)
    {
        int startSize = rows.size();
        long startTime = System.currentTimeMillis();

        // Remove duplicates
        TreeSet<Row> set = new TreeSet<>(rowService.comparator());
        set.addAll(rows);
        List<Row> result = new ArrayList<>(set);

        // Sort
        Search search = search(clause);
        Comparator<Row> comparator = rowService.comparator(search);
        Collections.sort(result, comparator);

        String comparatorName = comparator.getClass().getSimpleName();
        int endSize = result.size();
        long endTime = System.currentTimeMillis() - startTime;

//        result = rowService.group(result);

        Log.debug("Sorted %d rows to %d with comparator %s in %d ms\n", startSize, endSize, comparatorName, endTime);

        return result;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString()
    {
        return String.format("RowIndexSearcher [index=%s, keyspace=%s, table=%s, column=%s]",
                             index.getIndexName(),
                             index.getKeyspaceName(),
                             index.getTableName(),
                             index.getColumnName());
    }

}