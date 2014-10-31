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

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.dht.*;
import org.apache.cassandra.service.StorageService;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.SortField;

import java.util.Collection;

/**
 * Class for several row partitioning {@link Token} mappings between Cassandra and Lucene.
 *
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public abstract class TokenMapper
{

    /**
     * The lazily created singleton instance.
     */
    private static TokenMapper instance;

    protected final CFMetaData metadata;

    /**
     * Returns a new {@link TokenMapper} instance for the current partitioner using the specified
     * column family metadata.
     *
     * @param metadata The column family metadata.
     * @return A new {@link TokenMapper} instance for the current partitioner.
     */
    public static TokenMapper instance(CFMetaData metadata)
    {
        if (instance == null)
        {
            IPartitioner<?> partitioner = DatabaseDescriptor.getPartitioner();
            if (partitioner instanceof Murmur3Partitioner)
            {
                instance = new TokenMapperMurmur(metadata);
            }
            else
            {
                instance = new TokenMapperGeneric(metadata);
            }
        }
        return instance;
    }

    public TokenMapper(CFMetaData metadata)
    {
        this.metadata = metadata;
    }

    /**
     * Adds to the specified {@link Document} the {@link Field}s associated to the token of the specified row key.
     *
     * @param document     A {@link Document}.
     * @param partitionKey The raw partition key to be added.
     */
    public abstract void addFields(Document document, DecoratedKey partitionKey);

    @SuppressWarnings({"rawtypes", "unchecked"})
    private boolean needsFilter(DataRange dataRange)
    {

        // Get token bounds
        AbstractBounds<Token> bounds = dataRange.keyRange().toTokenBounds();
        Token left = bounds.left;
        Token right = bounds.right;

        // Check if it's a full cluster range
        Token minimum = DatabaseDescriptor.getPartitioner().getMinimumToken();
        if (left.compareTo(minimum) == 0 && right.compareTo(minimum) == 0)
        {
            return false;
        }

        // Check if it's a full node range
        String ksName = metadata.ksName;
        Collection<Range<Token>> localRanges = StorageService.instance.getLocalRanges(ksName);
        if (localRanges.size() == 1)
        { // No virtual nodes
            Range<Token> nodeRange = localRanges.iterator().next();
            if (left.compareTo(nodeRange.left) == 0 && right.compareTo(nodeRange.right) == 0)
            {
                return false;
            }
        }

        return true;
    }

    /**
     * Returns a Lucene's {@link Filter} for filtering documents/rows according to the row token range specified in
     * {@code dataRange}.
     *
     * @param dataRange The data range containing the row token range to be filtered.
     * @return A Lucene's {@link Filter} for filtering documents/rows according to the row token range specified in
     * {@code dataRage}.
     */
    public Filter filter(DataRange dataRange)
    {
        return needsFilter(dataRange) ? newFilter(dataRange) : null;
    }

    /**
     * Returns a Lucene's {@link Filter} for filtering documents/rows according to the row token range specified in
     * {@code dataRange}.
     *
     * @param dataRange The data range containing the row token range to be filtered.
     * @return A Lucene's {@link Filter} for filtering documents/rows according to the row token range specified in
     * {@code dataRage}.
     */
    public abstract Filter newFilter(DataRange dataRange);

    /**
     * Returns a Lucene's {@link SortField} array for sorting documents/rows according to the current partitioner.
     *
     * @return A Lucene's {@link SortField} array for sorting documents/rows according to the current partitioner.
     */
    public abstract SortField[] sort();

}
