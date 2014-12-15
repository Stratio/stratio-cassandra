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

import com.stratio.cassandra.index.schema.Column;
import com.stratio.cassandra.index.schema.Columns;
import com.stratio.cassandra.index.schema.Schema;
import com.stratio.cassandra.index.util.ByteBufferUtils;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.ArrayBackedSortedColumns;
import org.apache.cassandra.db.Cell;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.Row;
import org.apache.cassandra.db.composites.CBuilder;
import org.apache.cassandra.db.composites.CellName;
import org.apache.cassandra.db.composites.CellNameType;
import org.apache.cassandra.db.composites.Composite;
import org.apache.cassandra.db.filter.ColumnSlice;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SortField;
import org.apache.lucene.util.BytesRef;

import java.nio.ByteBuffer;
import java.util.*;

/**
 * Class for several clustering key mappings between Cassandra and Lucene. This class only be used in column families
 * with wide rows.
 *
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public abstract class ClusteringKeyMapper
{
    /** The Lucene field name */
    public static final String FIELD_NAME = "_clustering_key";

    /** The column family meta data */
    protected final CFMetaData metadata;

    /** The type of the clustering key, which is the type of the column names */
    protected final CellNameType cellNameType;

    /** The clustering key type as composite */
    protected final CompositeType compositeType;

    /**
     * Returns a new {@code ClusteringKeyMapper} according to the specified column family meta data.
     *
     * @param metadata The column family meta data.
     */
    protected ClusteringKeyMapper(CFMetaData metadata)
    {
        this.metadata = metadata;
        this.cellNameType = metadata.comparator;
        this.compositeType = (CompositeType) cellNameType.asAbstractType();
    }

    /**
     * Returns a new {@code ClusteringKeyMapper} according to the specified column family meta data.
     *
     * @param metadata The column family meta data.
     * @return A new {@code ClusteringKeyMapper} according to the specified column family meta data.
     */
    public static ClusteringKeyMapper instance(CFMetaData metadata, Schema schema)
    {
        ClusteringKeyMapperColumns mapper = ClusteringKeyMapperColumns.instance(metadata, schema);
        return mapper == null ? ClusteringKeyMapperGeneric.instance(metadata) : mapper;
    }

    /**
     * Returns the clustering key validation type. It's always a {@link CompositeType} in CQL3 tables.
     *
     * @return The clustering key validation type.
     */
    public final CellNameType getType()
    {
        return cellNameType;
    }

    public final void addFields(Document document, CellName cellName)
    {
        String serializedKey = ByteBufferUtils.toString(cellName.toByteBuffer());
        Field field = new StringField(FIELD_NAME, serializedKey, Field.Store.YES);
        document.add(field);
    }

    public final CellName clusteringKey(ColumnFamily columnFamily)
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
    public final List<CellName> clusteringKeys(ColumnFamily columnFamily)
    {
        List<CellName> clusteringKeys = new ArrayList<>();
        CellName lastClusteringKey = null;
        for (Cell cell : columnFamily)
        {
            CellName cellName = cell.name();
            if (!isStatic(cellName))
            {
                CellName clusteringKey = extractClusteringKey(cellName);
                if (lastClusteringKey == null || !lastClusteringKey.isSameCQL3RowAs(cellNameType, clusteringKey))
                {
                    lastClusteringKey = clusteringKey;
                    clusteringKeys.add(clusteringKey);
                }
            }
        }
        return sort(clusteringKeys);
    }

    protected final CellName extractClusteringKey(CellName cellName)
    {
        int numClusteringColumns = metadata.clusteringColumns().size();
        ByteBuffer[] components = new ByteBuffer[numClusteringColumns + 1];
        for (int i = 0; i < numClusteringColumns; i++)
        {
            components[i] = cellName.get(i);
        }
        components[numClusteringColumns] = ByteBufferUtil.EMPTY_BYTE_BUFFER;
        return cellNameType.makeCellName(components);
    }

    protected final boolean isStatic(CellName cellName)
    {
        int numClusteringColumns = metadata.clusteringColumns().size();
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
    protected final boolean isClusteringKey(CellName cellName)
    {
        int numClusteringColumns = metadata.clusteringColumns().size();
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
     * Returns the storage engine column name for the specified column identifier using the specified clustering key.
     *
     * @param cellName         The clustering key.
     * @param columnDefinition The column definition.
     * @return A storage engine column name.
     */
    public final CellName makeCellName(CellName cellName, ColumnDefinition columnDefinition)
    {
        return cellNameType.create(start(cellName), columnDefinition);
    }

    /**
     * Returns the first clustering key contained in the specified row.
     *
     * @param row A {@link Row}.
     * @return The first clustering key contained in the specified row.
     */
    public final CellName clusteringKey(Row row)
    {
        return clusteringKey(row.cf);
    }

    public final CellName clusteringKey(Document document)
    {
        String string = document.get(FIELD_NAME);
        ByteBuffer bb = ByteBufferUtils.fromString(string);
        return cellNameType.cellFromByteBuffer(bb);
    }

    /**
     * Returns the raw clustering key contained in the specified Lucene field value.
     *
     * @param bytesRef The {@link BytesRef} containing the raw clustering key to be get.
     * @return The raw clustering key contained in the specified Lucene field value.
     */
    public final CellName clusteringKey(BytesRef bytesRef)
    {
        String string = bytesRef.utf8ToString();
        ByteBuffer bb = ByteBufferUtils.fromString(string);
        return cellNameType.cellFromByteBuffer(bb);
    }

    /**
     * Returns the first possible cell name of those having the same clustering key that the specified cell name.
     *
     * @param cellName A storage engine cell name.
     * @return The first column name of for {@code clusteringKey}.
     */
    public final Composite start(CellName cellName)
    {
        CBuilder builder = cellNameType.builder();
        for (int i = 0; i < cellName.clusteringSize(); i++)
        {
            ByteBuffer component = cellName.get(i);
            builder.add(component);
        }
        return builder.build();
    }

    /**
     * Returns the last possible cell name of those having the same clustering key that the specified cell name.
     *
     * @param cellName A storage engine cell name.
     * @return The first column name of for {@code clusteringKey}.
     */
    public final Composite end(CellName cellName)
    {
        return start(cellName).withEOC(Composite.EOC.END);
    }

    public final Columns columns(Row row)
    {
        ColumnFamily columnFamily = row.cf;
        int numClusteringColumns = metadata.clusteringColumns().size();
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
                    columns.add(new Column(name, value, valueType));
                }
            }
        }
        return columns;
    }

    public final Map<CellName, ColumnFamily> splitRows(ColumnFamily columnFamily)
    {
        Map<CellName, ColumnFamily> columnFamilies = new LinkedHashMap<>();
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

    public final ColumnSlice[] columnSlices(List<CellName> clusteringKeys)
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

    public final List<CellName> sort(List<CellName> clusteringKeys)
    {
        List<CellName> result = new ArrayList<>(clusteringKeys);
        Collections.sort(result, new Comparator<CellName>()
        {
            @Override
            public int compare(CellName o1, CellName o2)
            {
                return cellNameType.compare(o1, o2);
            }
        });
        return result;
    }

    /**
     * Returns a Lucene {@link SortField} array for sorting documents/rows according to the column family name.
     *
     * @return A Lucene {@link SortField} array for sorting documents/rows according to the column family name.
     */
    public abstract SortField[] sortFields();

    public abstract Query query(Composite start, Composite stop);

    public final String toString(Composite cellName)
    {
        return ByteBufferUtils.toString(cellName.toByteBuffer(), cellNameType.asAbstractType());
    }

}
