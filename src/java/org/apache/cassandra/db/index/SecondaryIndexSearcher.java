/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.db.index;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.cassandra.db.AbstractRangeCommand;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Row;
import org.apache.cassandra.db.filter.ExtendedFilter;
import org.apache.cassandra.thrift.IndexExpression;
import org.apache.cassandra.thrift.IndexOperator;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.FBUtilities;

public abstract class SecondaryIndexSearcher
{
    protected final SecondaryIndexManager indexManager;
    protected final Set<ByteBuffer> columns;
    protected final ColumnFamilyStore baseCfs;

    public SecondaryIndexSearcher(SecondaryIndexManager indexManager, Set<ByteBuffer> columns)
    {
        this.indexManager = indexManager;
        this.columns = columns;
        this.baseCfs = indexManager.baseCfs;
    }

    public abstract List<Row> search(ExtendedFilter filter);

    /**
     * @return true this index is able to handle given clauses.
     */
    public boolean isIndexing(List<IndexExpression> clause)
    {
        return highestSelectivityPredicate(clause) != null;
    }

    protected IndexExpression highestSelectivityPredicate(List<IndexExpression> clause)
    {
        IndexExpression best = null;
        int bestMeanCount = Integer.MAX_VALUE;
        Map<SecondaryIndex, Integer> candidates = new HashMap<>();

        for (IndexExpression expression : clause)
        {
            // skip columns belonging to a different index type
            if (!columns.contains(expression.column_name))
                continue;

            SecondaryIndex index = indexManager.getIndexForColumn(expression.column_name);
            if (index == null || index.getIndexCfs() == null || expression.op != IndexOperator.EQ)
                continue;
            int columns = index.getIndexCfs().getMeanColumns();
            candidates.put(index, columns);
            if (columns < bestMeanCount)
            {
                best = expression;
                bestMeanCount = columns;
            }
        }

        if (best == null)
            Tracing.trace("No applicable indexes found");
        else
            Tracing.trace("Candidate index mean cardinalities are {}. Scanning with {}.",
                          FBUtilities.toString(candidates), indexManager.getIndexForColumn(best.column_name)
                                  .getIndexName());

        return best;
    }

    /**
     * Validates the specified {@link IndexExpression}. It will throw a {@link RuntimeException} if the provided
     * clause is not valid for the index implementation.
     * 
     * @param indexExpression
     *            An {@link IndexExpression} to be validated
     */
    public void validate(IndexExpression indexExpression)
    {
    }

    /**
     * Returns {@code true} if the specified {@link AbstractRangeCommand} requires a full scan of all the nodes,
     * {@code false} otherwise.
     * 
     * @param clause
     *            An {@link IndexExpression}.
     * @return {@code true} if the {@code command} requires a full scan, {@code false} otherwise.
     */
    public boolean requiresFullScan(List<IndexExpression> clause)
    {
        return false;
    }

    /**
     * Combines the partial results of several local index queries.
     *
     * @param clause
     *            An {@link IndexExpression}.
     * @param rows
     *            The partial results to be combined.
     * @return The combination of the partial results.
     */
    public List<Row> sort(List<IndexExpression> clause, List<Row> rows)
    {
        return rows;
    }
}
