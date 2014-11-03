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
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.RangeTombstone;
import org.apache.cassandra.db.composites.CellName;
import org.apache.cassandra.db.composites.CellNameType;
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
    private final ClusteringKeyMapper clusteringKeyMapper;

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
     * Returns a new {@link FullKeyMapper} using the specified column family metadata.
     *
     * @param metadata The column family metadata to be used.
     */
    private FullKeyMapper(CFMetaData metadata, PartitionKeyMapper partitionKeyMapper, ClusteringKeyMapper clusteringKeyMapper)
    {
        this.partitionKeyMapper = partitionKeyMapper;
        this.clusteringKeyMapper = clusteringKeyMapper;
        this.partitionKeyType = metadata.getKeyValidator();
        this.clusteringKeyType = metadata.comparator;
        type = CompositeType.getInstance(partitionKeyType, clusteringKeyType.asAbstractType());
    }

    /**
     * Returns a new {@link FullKeyMapper} using the specified column family metadata.
     *
     * @param metadata The column family metadata to be used.
     * @return A new {@link FullKeyMapper} using the specified column family metadata.
     */
    public static FullKeyMapper instance(CFMetaData metadata, PartitionKeyMapper partitionKeyMapper, ClusteringKeyMapper clusteringKeyMapper)
    {
        return metadata.clusteringColumns().size() > 0 ? new FullKeyMapper(metadata, partitionKeyMapper, clusteringKeyMapper) : null;
    }

    public AbstractType<?> getPartitionKeyType()
    {
        return partitionKeyType;
    }

    public CellNameType getClusteringKeyType()
    {
        return clusteringKeyType;
    }

    /**
     * Returns the type of the full row key, which is a {@link CompositeType} composed by the partition key and the
     * clustering key.
     *
     * @return The type of the full row key
     */
    public CompositeType getType()
    {
        return type;
    }

    /**
     * Returns the {@link ByteBuffer} representation of the full row key formed by the specified partition key and the
     * clustering key.
     *
     * @param partitionKey A partition key.
     * @param cellName     A clustering key.
     * @return The {@link ByteBuffer} representation of the full row key formed by the specified key pair.
     */
    public ByteBuffer byteBuffer(DecoratedKey partitionKey, CellName cellName)
    {
        return type.builder().add(partitionKey.getKey()).add(cellName.toByteBuffer()).build();
    }

    public DecoratedKey decoratedKey(BytesRef bytesRef)
    {
        String string = bytesRef.utf8ToString();
        ByteBuffer bb = ByteBufferUtils.fromString(string);
        return decoratedKey(bb);
    }

    public DecoratedKey decoratedKey(ByteBuffer byteBuffer)
    {
        ByteBuffer[] components = type.split(byteBuffer);
        return partitionKeyMapper.decoratedKey(components[0]);
    }

    public CellName cellName(Document document)
    {
        String string = document.get(FIELD_NAME);
        ByteBuffer bb = ByteBufferUtils.fromString(string);
        return cellName(bb);
    }

    public CellName cellName(BytesRef bytesRef)
    {
        String string = bytesRef.utf8ToString();
        ByteBuffer bb = ByteBufferUtils.fromString(string);
        return cellName(bb);
    }

    public CellName cellName(ByteBuffer byteBuffer)
    {
        ByteBuffer[] components = type.split(byteBuffer);
        ByteBuffer bb = components[1];
        return clusteringKeyType.cellFromByteBuffer(bb);
    }

    /**
     * Adds to the specified Lucene's {@link Document} the full row key formed by the specified partition key and the
     * clustering key.
     *
     * @param document     A Lucene's {@link Document}.
     * @param partitionKey A partition key.
     * @param cellName     A clustering key.
     */
    public void addFields(Document document, DecoratedKey partitionKey, CellName cellName)
    {
        ByteBuffer fullKey = byteBuffer(partitionKey, cellName);
        Field field = new StringField(FIELD_NAME, ByteBufferUtils.toString(fullKey), Store.YES);
        document.add(field);
    }

    /**
     * Returns the Lucene's {@link Term} representing the full row key formed by the specified partition key and the
     * clustering key.
     *
     * @param partitionKey A partition key.
     * @param cellName     A clustering key.
     * @return The Lucene's {@link Term} representing the full row key formed by the specified key pair.
     */
    public Term term(DecoratedKey partitionKey, CellName cellName)
    {
        ByteBuffer fullKey = type.builder().add(partitionKey.getKey()).add(cellName.toByteBuffer()).build();
        return new Term(FIELD_NAME, ByteBufferUtils.toString(fullKey));
    }

    public Filter filter(DataRange dataRange)
    {
        Filter filter = new FullKeyMapperDataRangeFilter(this, dataRange);
        return new CachingWrapperFilter(filter);
    }

    public Filter filter(RangeTombstone rangeTombstone)
    {
        Filter filter = new FullKeyMapperRangeTombstoneFilter(this, rangeTombstone);
        return new CachingWrapperFilter(filter);
    }

    public SortField[] sortFields()
    {
        return new SortField[]{
                new SortField(FIELD_NAME, new FieldComparatorSource()
                {
                    @Override
                    public FieldComparator<?>
                    newComparator(String field, int hits, int sort, boolean reversed) throws IOException
                    {
                        return new FullKeyMapperSorter(FullKeyMapper.this, hits, field);
                    }
                })};
    }

    public int compare(BytesRef bytesRef1, BytesRef bytesRef2)
    {
        Token token1 = decoratedKey(bytesRef1).getToken();
        Token token2 = decoratedKey(bytesRef2).getToken();
        int comp = token1.compareTo(token2);
        if (comp == 0)
        {
            CellName bb1 = cellName(bytesRef1);
            CellName bb2 = cellName(bytesRef2);
            comp = clusteringKeyType.compare(bb1, bb2);
        }
        return comp;
    }

}
