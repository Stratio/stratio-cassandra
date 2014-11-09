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

import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.RowPosition;
import org.apache.cassandra.db.composites.CellName;
import org.apache.cassandra.db.composites.CellNameType;
import org.apache.cassandra.db.composites.Composite;
import org.apache.cassandra.db.filter.IDiskAtomFilter;
import org.apache.cassandra.db.filter.SliceQueryFilter;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * Class representing a range of {@link org.apache.cassandra.db.Row}s to be filtered.
 *
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class RowRange
{
    private final AbstractBounds<RowPosition> keyBounds;
    private final AbstractBounds<Token> tokenBounds;
    private final Token startToken;
    private final Token stopToken;
    private DecoratedKey startKey = null;
    private DecoratedKey stopKey = null;
    private Composite startName = null;
    private Composite stopName = null;
    CellNameType type;

    public RowRange(DataRange dataRange, CellNameType type)
    {
        this.type = type;
        keyBounds = dataRange.keyRange();
        tokenBounds = dataRange.keyRange().toTokenBounds();
        startToken = dataRange.startKey().getToken();
        stopToken = dataRange.stopKey().getToken();
        if (dataRange.startKey() instanceof DecoratedKey)
        {
            startKey = (DecoratedKey) dataRange.startKey();

        }
        if (dataRange.stopKey() instanceof DecoratedKey)
        {
            stopKey = (DecoratedKey) dataRange.stopKey();

        }
        if (dataRange instanceof DataRange.Paging)
        {
            DataRange.Paging paging = (DataRange.Paging) dataRange;
            startName = paging.columnStart;
            stopName = paging.columnFinish;
        }
        else
        {
            IDiskAtomFilter columnFilter = dataRange.columnFilter(ByteBufferUtil.EMPTY_BYTE_BUFFER);
            SliceQueryFilter sqf = (SliceQueryFilter) columnFilter;
            startName = sqf.start();
            stopName = sqf.finish();
        }
    }

    public AbstractBounds<Token> getTokenBounds()
    {
        return tokenBounds;
    }

    public RowPosition startKey()
    {
        return keyBounds.left;
    }

    public RowPosition stopKey()
    {
        return keyBounds.right;
    }

    public boolean includeStartKey()
    {
        switch (keyBounds.left.kind())
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

    protected boolean includeStopKey()
    {
        switch (keyBounds.right.kind())
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

    public boolean accepts(SearchResult searchResult)
    {
        DecoratedKey partitionKey = searchResult.getPartitionKey();
        CellName clusteringKey = searchResult.getClusteringKey();
        return accepts(partitionKey, clusteringKey);
    }

    public boolean accepts(DecoratedKey partitionKey, CellName clusteringKey)
    {
        if (!keyBounds.contains(partitionKey))
        {
            return false;
        }
        if (startKey != null)
        {
            if (ByteBufferUtil.compareUnsigned(startKey.getKey(), partitionKey.getKey()) == 0)
            {
                if (startName != null && !startName.isEmpty() && type.compare(startName, clusteringKey) >= 0)
                {
                    return false;
                }
            }
        }
        else if (startToken != null)
        {
            Token token = partitionKey.getToken();
            if (includeStartKey() ? startToken.compareTo(token) >= 0 : startToken.compareTo(token) > 0)
            {
                if (startName != null && !startName.isEmpty() && type.compare(startName, clusteringKey) >= 0)
                {
                    return false;
                }
            }
        }
        if (stopKey != null)
        {
            if (ByteBufferUtil.compareUnsigned(stopKey.getKey(), partitionKey.getKey()) == 0)
            {
                if (stopName != null && !stopName.isEmpty() && type.compare(stopName, clusteringKey) <= 0)
                {
                    return false;
                }
            }
        }
        else if (startToken != null)
        {
            Token token = partitionKey.getToken();
            if (includeStopKey() ? stopToken.compareTo(token) <= 0 : stopToken.compareTo(token) < 0)
            {
                if (stopName != null && !stopName.isEmpty() && type.compare(stopName, clusteringKey) <= 0)
                {
                    return false;
                }
            }
        }
        return true;
    }

//    public String toString(Composite composite) {
//        return ByteBufferUtils.toString(composite.toByteBuffer(), type.asAbstractType());
//    }
}
