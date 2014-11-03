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

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.composites.CellName;
import org.apache.cassandra.dht.Token;
import org.apache.commons.lang3.builder.ToStringBuilder;

import java.nio.ByteBuffer;

/**
 * Class representing the identifying key of a CQL3 logic row.
 *
 * It's composed by a partition key and a clustering key.
 *
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class FullKey
{
    private final DecoratedKey decoratedKey;
    private final CellName clusteringKey;

    /**
     * Returns a new {@link FullKey} composed by the specified partition key and clustering key.
     * @param decoratedKey A partition key.
     * @param clusteringKey A clustering key.
     */
    public FullKey(DecoratedKey decoratedKey, CellName clusteringKey)
    {
        this.decoratedKey = decoratedKey;
        this.clusteringKey = clusteringKey;
    }

    /**
     * Returns the partition key as a {@link DecoratedKey}.
     * @return The partition key as a {@link DecoratedKey}.
     */
    public DecoratedKey getDecoratedKey()
    {
        return decoratedKey;
    }

    /**
     * Returns the partition key as a {@link ByteBuffer}.
     * @return The partition key as a {@link ByteBuffer}.
     */
    public ByteBuffer getKey() {
        return decoratedKey.getKey();
    }

    /**
     * Returns the partition key as a {@link Token}.
     * @return The partition key as a {@link Token}.
     */
    public Token<?> getToken() {
        return decoratedKey.getToken();
    }

    /**
     * Returns the clustering key as a {@link CellName}.
     * @return The clustering key as a {@link CellName}.
     */
    public CellName getClusteringKey()
    {
        return clusteringKey;
    }

    @Override
    public String toString()
    {
        return new ToStringBuilder(this)
                .append("decoratedKey", decoratedKey)
                .append("clusteringKey", clusteringKey)
                .toString();
    }
}
