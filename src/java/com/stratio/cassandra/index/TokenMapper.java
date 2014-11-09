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
import org.apache.cassandra.db.RowPosition;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Token;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SortField;

import java.nio.ByteBuffer;

/**
 * Class for several row partitioning {@link Token} mappings between Cassandra and Lucene.
 *
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public abstract class TokenMapper
{

    protected final CFMetaData metadata;
    protected final AbstractType<?> keyType;

    /**
     * Returns a new {@link TokenMapper} instance for the current partitioner using the specified
     * column family metadata.
     *
     * @param metadata The column family metadata.
     * @return A new {@link TokenMapper} instance for the current partitioner.
     */
    public static TokenMapper instance(CFMetaData metadata)
    {
        IPartitioner<?> partitioner = DatabaseDescriptor.getPartitioner();
        if (partitioner instanceof Murmur3Partitioner)
        {
            return new TokenMapperMurmur(metadata);
        }
        else
        {
            return new TokenMapperGeneric(metadata);
        }
    }

    public TokenMapper(CFMetaData metadata)
    {
        this.metadata = metadata;
        this.keyType = metadata.getKeyValidator();
    }

    /**
     * Adds to the specified {@link Document} the {@link Field}s associated to the token of the specified row key.
     *
     * @param document     A {@link Document}.
     * @param partitionKey The raw partition key to be added.
     */
    public abstract void addFields(Document document, DecoratedKey partitionKey);

    /**
     * Returns a Lucene's {@link Query} for filtering documents/rows according to the row token range specified in
     * {@link RowRange}.
     *
     * @param rowRange The key range containing the row token range to be filtered.
     * @return A Lucene's {@link Query} for filtering documents/rows according to the row token range specified in
     * {@link RowRange}.
     */
    public abstract Query query(RowRange rowRange);

    /**
     * Returns a Lucene's {@link SortField} array for sorting documents/rows according to the current partitioner.
     *
     * @return A Lucene's {@link SortField} array for sorting documents/rows according to the current partitioner.
     */
    public abstract SortField[] sortFields();

    /**
     * Returns {@code true} if the specified lower row position kind must be included in the filtered range, {@code false} otherwise.
     *
     * @param rowPosition A {@link RowPosition} kind.
     * @return {@code true} if the specified lower row position kind must be included in the filtered range, {@code false} otherwise.
     */
    public boolean includeLower(RowPosition rowPosition)
    {
        switch (rowPosition.kind())
        {
            case MAX_BOUND:
                return false;
            case MIN_BOUND:
                return true;
            case ROW_KEY:
                return true;
            default:
                throw new IllegalArgumentException();
        }
    }

    /**
     * Returns {@code true} if the specified upper row position kind must be included in the filtered range, {@code false} otherwise.
     *
     * @param rowPosition A {@link RowPosition} kind.
     * @return {@code true} if the specified upper row position kind must be included in the filtered range, {@code false} otherwise.
     */
    protected boolean includeUpper(RowPosition rowPosition)
    {
        switch (rowPosition.kind())
        {
            case MAX_BOUND:
                return true;
            case MIN_BOUND:
                return false;
            case ROW_KEY:
                return true;
            default:
                throw new IllegalArgumentException();
        }
    }

}
