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
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.Cell;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.RangeTombstone;
import org.apache.cassandra.db.composites.CBuilder;
import org.apache.cassandra.db.composites.CellName;
import org.apache.cassandra.db.composites.CellNameType;
import org.apache.cassandra.db.composites.Composite;
import org.apache.cassandra.db.filter.IDiskAtomFilter;
import org.apache.cassandra.db.filter.SliceQueryFilter;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.StringField;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.FieldComparatorSource;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.SortField;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Set;

/**
 * Class for several clustering key mappings between Cassandra and Lucene. This class only be used
 * in column families with wide rows.
 *
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class ClusteringKeyMapper
{

    /**
     * The Lucene's field name.
     */
    public static final String FIELD_NAME = "_clustering_key";

    /**
     * The type of the clustering key, which is the type of the column names.
     */
    private final CellNameType type;

    /**
     * Returns a new {@code ClusteringKeyMapper} according to the specified column family meta data.
     *
     * @param metadata The column family meta data.
     */
    private ClusteringKeyMapper(CFMetaData metadata)
    {
        type = metadata.comparator;
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

    /**
     * Returns the common clustering keys of the specified column family.
     *
     * @param columnFamily A storage engine {@link org.apache.cassandra.db.ColumnFamily}.
     * @return The common clustering keys of the specified column family.
     */
    public Set<CellName> cellNames(ColumnFamily columnFamily)
    {
        Set<CellName> cellNames = new HashSet<>();
        CellName lastCellName = null;
        for (Cell cell : columnFamily)
        {
            CellName cellName = cell.name();
            if (lastCellName == null || !cellName.isSameCQL3RowAs(type, lastCellName))
            {
                cellNames.add(cellName);
                lastCellName = cellName;
            }
        }
        return cellNames;
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

    public void addFields(Document document, CellName cellName)
    {
        String serializedKey = ByteBufferUtils.toString(cellName.toByteBuffer());
        Field field = new StringField(FIELD_NAME, serializedKey, Store.YES);
        document.add(field);
    }

    /**
     * Returns the clustering key contained in the specified Lucene's {@link Document}.
     *
     * @param document A {@link Document}.
     * @return The clustering key contained in the specified Lucene's {@link Document}.
     */
    public CellName cellName(Document document) throws IOException
    {
        String string = document.get(FIELD_NAME);
        ByteBuffer bb = ByteBufferUtils.fromString(string);
        return type.cellFromByteBuffer(bb);
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

    private boolean needsFilter(DataRange dataRange)
    {
        if (dataRange.columnFilter(ByteBufferUtil.EMPTY_BYTE_BUFFER) != null)
        {
            IDiskAtomFilter filter = dataRange.columnFilter(ByteBufferUtil.EMPTY_BYTE_BUFFER);
            if (filter != null)
            {
                SliceQueryFilter sqf = (SliceQueryFilter) dataRange.columnFilter(ByteBufferUtil.EMPTY_BYTE_BUFFER);
                if (sqf.start().toByteBuffer().remaining() > 0 || sqf.finish().toByteBuffer().remaining() > 0)
                {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Returns a Lucene's {@link Filter} for filtering documents/rows according to the column name
     * range specified in {@code dataRange}.
     *
     * @param dataRange The data range containing the column name range to be filtered.
     * @return A Lucene's {@link Filter} for filtering documents/rows according to the column name
     * range specified in {@code dataRage}.
     */
    public Filter filter(DataRange dataRange)
    {
        if (needsFilter(dataRange))
        {
            return new ClusteringKeyMapperDataRangeFilter(this, dataRange);
        }
        else
        {
            return null;
        }
    }

    /**
     * Returns a Lucene's {@link Filter} for filtering documents/rows according to the column
     * tombstone range specified in {@code rangeTombstone}.
     *
     * @param rangeTombstone The data range containing the column tombstone range to be filtered.
     * @return A Lucene's {@link Filter} for filtering documents/rows according to the column
     * tombstone range specified in {@code rangeTombstone}.
     */
    public Filter filter(RangeTombstone rangeTombstone)
    {
        return new ClusteringKeyMapperRangeTombstoneFilter(this, rangeTombstone);
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

    /**
     * Returns the first possible cell name of those having the same clustering key that the
     * specified cell name.
     *
     * @param cellName A storage engine cell name.
     * @return The first column name of for {@code cellName}.
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
     * @return The first column name of for {@code cellName}.
     */
    public Composite end(CellName cellName)
    {
        return start(cellName).withEOC(Composite.EOC.END);
    }

}
