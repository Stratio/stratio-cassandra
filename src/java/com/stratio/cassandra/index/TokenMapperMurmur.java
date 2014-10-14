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

import com.stratio.cassandra.index.util.Log;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.RowPosition;
import org.apache.cassandra.db.RowPosition.Kind;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.LongField;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.NumericRangeFilter;
import org.apache.lucene.search.SortField;

/**
 * {@link PartitionKeyMapper} to be used when {@link Murmur3Partitioner} is used. It indexes the token long value as a
 * Lucene's long field.
 *
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class TokenMapperMurmur extends TokenMapper
{

    private static final String FIELD_NAME = "_token_murmur";

    public TokenMapperMurmur(ColumnFamilyStore baseCfs)
    {
        super(baseCfs);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void addFields(Document document, DecoratedKey partitionKey)
    {
        Long value = (Long) partitionKey.getToken().token;
        Field tokenField = new LongField(FIELD_NAME, value, Store.NO);
        document.add(tokenField);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Filter newFilter(DataRange dataRange)
    {
        RowPosition startPosition = dataRange.startKey();
        RowPosition stopPosition = dataRange.stopKey();
        Long start = (Long) startPosition.getToken().token;
        Long stop = (Long) stopPosition.getToken().token;
        if (startPosition.isMinimum())
        {
            start = null;
        }
        if (stopPosition.isMinimum())
        {
            stop = null;
        }
        boolean includeLower = startPosition.kind() == Kind.MIN_BOUND;
        boolean includeUpper = stopPosition.kind() == Kind.MAX_BOUND;
        Log.debug("Querying %s %d %d %s", includeLower ? "[" : "{", start, stop, includeUpper ? "]" : "}");
        return NumericRangeFilter.newLongRange(FIELD_NAME, start, stop, includeLower, includeUpper);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public SortField[] sortFields()
    {
        return new SortField[]{new SortField(FIELD_NAME, SortField.Type.LONG)};
    }

}
