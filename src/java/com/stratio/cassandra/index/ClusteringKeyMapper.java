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

import com.stratio.cassandra.index.schema.ColumnMapper;
import com.stratio.cassandra.index.schema.Columns;
import com.stratio.cassandra.index.util.ByteBufferUtils;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.composites.CBuilder;
import org.apache.cassandra.db.composites.CellName;
import org.apache.cassandra.db.composites.CellNameType;
import org.apache.cassandra.db.composites.Composite;
import org.apache.cassandra.db.filter.ColumnSlice;
import org.apache.cassandra.db.filter.SliceQueryFilter;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.StringField;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.FieldComparatorSource;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SortField;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

/**
 * Class for several clustering key mappings between Cassandra and Lucene. This class only be used
 * in column families with wide rows.
 *
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class ClusteringKeyMapper
{
    /** The Lucene's field name. */
    public static final String FIELD_NAME = "_clustering_key";

    /** The column family meta data. */
    private final CFMetaData metadata;

    /** The type of the clustering key, which is the type of the column names. */
    private final CellNameType type;

    /** The number of clustering columns. */
    private final int numClusteringColumns;

    /**
     * Returns a new {@code ClusteringKeyMapper} according to the specified column family meta data.
     *
     * @param metadata The column family meta data.
     */
    private ClusteringKeyMapper(CFMetaData metadata)
    {
        this.metadata = metadata;
        this.type = metadata.comparator;
        this.numClusteringColumns = metadata.clusteringColumns().size();
    }

    /**
     * Returns a new {@code ClusteringKeyMapper} according to the specified column family meta data.
     *
     * @param metadata The column family meta data.
     * @return A new {@code ClusteringKeyMapper} according to the specified column family meta data.
     */
    public static ClusteringKeyMapper instance(CFMetaData metadata)
    {
        return metadata.clusteringColumns().size() > 0 ? new ClusteringKeyMapper(metadata) : null;
    }

    /**
     * Returns the clustering key validation type. It's always a {@link CompositeType} in CQL3
     * tables.
     *
     * @return The clustering key validation type.
     */
    public CellNameType getType()
    {
        return type;
    }

    public CellName clusteringKey(Row row)
    {
        return clusteringKey(row.cf);
    }

    public CellName clusteringKey(ColumnFamily columnFamily)
    {
        for (Cell cell : columnFamily)
        {
            CellName cellName = cell.name();
            if (isClusteringKey(cellName))
            {
                return cellName;
            }
        }
        return null;
    }

    /**
     * Returns the common clustering keys of the specified column family.
     *
     * @param columnFamily A storage engine {@link org.apache.cassandra.db.ColumnFamily}.
     * @return The common clustering keys of the specified column family.
     */
    public List<CellName> clusteringKeys(ColumnFamily columnFamily)
    {
        List<CellName> clusteringKeys = new ArrayList<>();
        CellName lastClusteringKey = null;
        for (Cell cell : columnFamily)
        {
            CellName cellName = cell.name();
            if (!isStatic(cellName))
            {
                CellName clusteringKey = clusteringKey(cellName);
                if (lastClusteringKey == null || !lastClusteringKey.isSameCQL3RowAs(type, clusteringKey))
                {
                    lastClusteringKey = clusteringKey;
                    clusteringKeys.add(clusteringKey);
                }
            }
        }
        return sort(clusteringKeys);
    }

    public ByteBuffer[] byteBuffers(CellName cellName)
    {
        ByteBuffer[] components = new ByteBuffer[numClusteringColumns + 1];
        for (int i = 0; i < numClusteringColumns; i++)
        {
            components[i] = cellName.get(i);
        }
        components[numClusteringColumns] = ByteBufferUtil.EMPTY_BYTE_BUFFER;
        return components;
    }

    public CellName clusteringKey(Composite composite)
    {
        ByteBuffer[] bbs = ByteBufferUtils.split(composite.toByteBuffer(), type.asAbstractType());
        return type.makeCellName(bbs);
    }

    private CellName clusteringKey(CellName cellName)
    {
        return type.makeCellName(byteBuffers(cellName));
    }

    private boolean isStatic(CellName cellName)
    {
        for (int i = 0; i < numClusteringColumns; i++)
        {
            if (ByteBufferUtils.isEmpty(cellName.get(i))) // Ignore static columns
            {
                return true;
            }
        }
        return false;
    }

    /**
     * Returns {@code true} if the specified {@link CellName} is a valid clustering key, {@code false} otherwise.
     *
     * @param cellName A {@link CellName}.
     * @return {@code true} if the specified {@link CellName} is a valid clustering key, {@code false} otherwise.
     */
    private boolean isClusteringKey(CellName cellName)
    {
        for (int i = 0; i < numClusteringColumns; i++)
        {
            if (ByteBufferUtils.isEmpty(cellName.get(i))) // Ignore static columns
            {
                return false;
            }
        }
        return ByteBufferUtils.isEmpty(cellName.get(numClusteringColumns));
    }

    /**
     * Returns the storage engine column name for the specified column identifier using the
     * specified clustering key.
     *
     * @param cellName         The clustering key.
     * @param columnDefinition The column definition.
     * @return A storage engine column name.
     */
    public CellName makeCellName(CellName cellName, ColumnDefinition columnDefinition)
    {
        return type.create(start(cellName), columnDefinition);
    }

    /**
     * Returns the clustering key contained in the specified {@link Document}.
     *
     * @param document A {@link Document}.
     * @return The clustering key contained in the specified {@link Document}.
     */
    public CellName cellName(Document document)
    {
        String string = document.get(FIELD_NAME);
        ByteBuffer bb = ByteBufferUtils.fromString(string);
        return type.cellFromByteBuffer(bb);
    }

    /**
     * Returns the first clustering key contained in the specified row.
     *
     * @param row A {@link Row}.
     * @return The first clustering key contained in the specified row.
     */
    public CellName cellName(Row row)
    {
        return clusteringKey(row.cf);
    }

    /**
     * Returns the raw clustering key contained in the specified Lucene's field value.
     *
     * @param bytesRef The {@link BytesRef} containing the raw clustering key to be get.
     * @return The raw clustering key contained in the specified Lucene's field value.
     */
    public CellName cellName(BytesRef bytesRef)
    {
        String string = bytesRef.utf8ToString();
        ByteBuffer bb = ByteBufferUtils.fromString(string);
        return type.cellFromByteBuffer(bb);
    }

    public CellName cellName(ByteBuffer byteBuffer)
    {
        return type.cellFromByteBuffer(byteBuffer);
    }

    /**
     * Returns the first possible cell name of those having the same clustering key that the
     * specified cell name.
     *
     * @param cellName A storage engine cell name.
     * @return The first column name of for {@code clusteringKey}.
     */
    public Composite start(CellName cellName)
    {
        CBuilder builder = type.builder();
        for (int i = 0; i < cellName.clusteringSize(); i++)
        {
            ByteBuffer component = cellName.get(i);
            builder.add(component);
        }
        return builder.build();
    }

    /**
     * Returns the last possible cell name of those having the same clustering key that the
     * specified cell name.
     *
     * @param cellName A storage engine cell name.
     * @return The first column name of for {@code clusteringKey}.
     */
    public Composite end(CellName cellName)
    {
        return start(cellName).withEOC(Composite.EOC.END);
    }

    public Columns columns(Row row)
    {
        ColumnFamily columnFamily = row.cf;
        Columns columns = new Columns();
        if (numClusteringColumns > 0)
        {
            CellName cellName = clusteringKey(columnFamily);
            if (cellName != null)
            {
                for (int i = 0; i < numClusteringColumns; i++)
                {
                    ByteBuffer value = cellName.get(i);
                    ColumnDefinition columnDefinition = metadata.clusteringColumns().get(i);
                    String name = columnDefinition.name.toString();
                    AbstractType<?> valueType = columnDefinition.type;
                    columns.add(ColumnMapper.column(name, value, valueType));
                }
            }
        }
        return columns;
    }

    public Map<CellName, ColumnFamily> splitRows(ColumnFamily columnFamily)
    {
        Map<CellName, ColumnFamily> columnFamilies = new HashMap<>();
        ColumnFamily rowColumnFamily = null;
        CellName clusteringKey = null;
        for (Cell cell : columnFamily)
        {
            CellName cellName = cell.name();
            if (isClusteringKey(cellName))
            {
                if (rowColumnFamily != null)
                {
                    columnFamilies.put(clusteringKey, rowColumnFamily);
                }
                clusteringKey = cellName;
                rowColumnFamily = ArrayBackedSortedColumns.factory.create(metadata);
            }
            rowColumnFamily.addColumn(cell);
        }
        if (rowColumnFamily != null)
        {
            columnFamilies.put(clusteringKey, rowColumnFamily);
        }
        return columnFamilies;
    }

    public ColumnSlice[] columnSlices(List<CellName> clusteringKeys)
    {
        List<CellName> sortedClusteringKeys = sort(clusteringKeys);
        ColumnSlice[] columnSlices = new ColumnSlice[clusteringKeys.size()];
        int i = 0;
        for (CellName clusteringKey : sortedClusteringKeys)
        {
            Composite start = start(clusteringKey);
            Composite end = end(clusteringKey);
            ColumnSlice columnSlice = new ColumnSlice(start, end);
            columnSlices[i++] = columnSlice;
        }
        return columnSlices;
    }

    public List<CellName> sort(List<CellName> clusteringKeys)
    {
        List<CellName> result = new ArrayList<>(clusteringKeys);
        Collections.sort(result, new Comparator<CellName>()
        {
            @Override
            public int compare(CellName o1, CellName o2)
            {
                return type.compare(o1, o2);
            }
        });
        return result;
    }

    /**
     * Adds the to the specified {@link Document} the {@link Field}s representing the clustering key
     * of the specified storage engine {@link Cell} name.
     *
     * @param document      A {@link Document}.
     * @param clusteringKey A clustering key.
     */
    public void addFields(Document document, CellName clusteringKey)
    {
        String serializedKey = ByteBufferUtils.toString(clusteringKey.toByteBuffer());
        Field field = new StringField(FIELD_NAME, serializedKey, Store.YES);
        document.add(field);
    }

    /**
     * Returns a Lucene's {@link SortField} array for sorting documents/rows according to the column
     * family name.
     *
     * @return A Lucene's {@link SortField} array for sorting documents/rows according to the column
     * family name.
     */
    public SortField[] sortFields()
    {
        return new SortField[]{
                new SortField(FIELD_NAME, new FieldComparatorSource()
                {
                    @Override
                    public FieldComparator<?>
                    newComparator(String field, int hits, int sort, boolean reversed) throws IOException
                    {
                        return new ClusteringKeyMapperSorter(ClusteringKeyMapper.this, hits, field);
                    }
                })};
    }

    public String toString(CellName cellName)
    {
        return ByteBufferUtils.toString(cellName.toByteBuffer(), type.asAbstractType());
    }

    public Query query(SliceQueryFilter sliceQueryFilter)
    {
        return new ClusteringKeyColumnSliceQuery(FIELD_NAME, sliceQueryFilter, this);
    }

    public Query query(RangeTombstone rangeTombstone)
    {
        return new ClusteringKeyRangeTombstoneQuery(FIELD_NAME, rangeTombstone, this);
    }

    public Comparator<CellName> comparator()
    {
        return new Comparator<CellName>()
        {
            @Override
            public int compare(CellName o1, CellName o2)
            {
                return type.compare(o1, o2);
            }
        };
    }

}
