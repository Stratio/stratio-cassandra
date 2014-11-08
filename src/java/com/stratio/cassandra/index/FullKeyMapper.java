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

import com.stratio.cassandra.index.util.ByteBufferUtils;
import com.stratio.cassandra.index.util.ComparatorChain;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.composites.CellName;
import org.apache.cassandra.db.composites.CellNameType;
import org.apache.cassandra.db.composites.Composite;
import org.apache.cassandra.db.filter.SliceQueryFilter;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.dht.Token;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Comparator;

/**
 * Class for several row full key mappings between Cassandra and Lucene. The full key includes both the partitioning and
 * the clustering keys.
 *
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class FullKeyMapper
{

    /**
     * The Lucene's field name.
     */
    public static final String FIELD_NAME = "_full_key";

    private final PartitionKeyMapper partitionKeyMapper;
    private  final  ClusteringKeyMapper clusteringKeyMapper;

    /**
     * The partition key type.
     */
    public AbstractType<?> partitionKeyType;

    /**
     * The clustering key type.
     */
    public CellNameType clusteringKeyType;

    /**
     * The type of the full row key, which is composed by the partition and clustering key types.
     */
    public CompositeType type;

    /**
     * Builds a new {@link FullKeyMapper} using the specified column family metadata.
     *
     * @param partitionKeyMapper A {@link PartitionKeyMapper}.
     * @param clusteringKeyMapper A {@link ClusteringKeyMapper}.
     */
    private FullKeyMapper(PartitionKeyMapper partitionKeyMapper, ClusteringKeyMapper clusteringKeyMapper)
    {
        this.partitionKeyMapper = partitionKeyMapper;
        this.clusteringKeyMapper = clusteringKeyMapper;
        this.partitionKeyType = partitionKeyMapper.getType();
        this.clusteringKeyType = clusteringKeyMapper.getType();
        type = CompositeType.getInstance(partitionKeyType, clusteringKeyType.asAbstractType());
    }

    /**
     * Returns a new {@link FullKeyMapper} using the specified column family metadata.
     *
     * @param partitionKeyMapper A {@link PartitionKeyMapper}.
     * @param clusteringKeyMapper A {@link ClusteringKeyMapper}.
     * @return A new {@link FullKeyMapper} using the specified column family metadata.
     */
    public static FullKeyMapper instance(PartitionKeyMapper partitionKeyMapper, ClusteringKeyMapper clusteringKeyMapper)
    {
        return new FullKeyMapper(partitionKeyMapper, clusteringKeyMapper);
    }

    /**
     * Adds to the specified Lucene's {@link Document} the full row key formed by the specified partition key and the
     * clustering key.
     *
     * @param document     A Lucene's {@link Document}.
     * @param partitionKey A partition key.
     * @param clusteringKey     A clustering key.
     */
    public void addFields(Document document, DecoratedKey partitionKey, CellName clusteringKey)
    {
        String string = string(partitionKey, clusteringKey);
        Field field = new StringField(FIELD_NAME, string, Store.NO);
        document.add(field);
    }

    /**
     * Returns the Lucene's {@link Term} representing the full row key formed by the specified partition key and the
     * clustering key.
     *
     * @param partitionKey A partition key.
     * @param clusteringKey     A clustering key.
     * @return The Lucene's {@link Term} representing the full row key formed by the specified key pair.
     */
    public Term term(DecoratedKey partitionKey, CellName clusteringKey)
    {
        String string = string(partitionKey, clusteringKey);
        return new Term(FIELD_NAME, string);
    }

    public Term term(DecoratedKey partitionKey, Composite clusteringKey)
    {
        String string = string(partitionKey, clusteringKey);
        return new Term(FIELD_NAME, string);
    }

    private String string(DecoratedKey partitionKey, Composite cellName) {
        ByteBuffer bb = type.builder().add(partitionKey.getKey()).add(cellName.toByteBuffer()).build();
        return ByteBufferUtils.toString(bb);
    }

}
