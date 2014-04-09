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
package org.apache.cassandra.db.index.stratio;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.Column;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.RangeTombstone;
import org.apache.cassandra.db.index.stratio.util.ByteBufferUtils;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.Fields;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.FieldComparatorSource;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.SortField;
import org.apache.lucene.util.BytesRef;

/**
 * Class for several clustering key mappings between Cassandra and Lucene. This class only be used
 * in column families with wide rows.
 * 
 * @author adelapena
 * 
 */
public class ClusteringKeyMapper {

	/** The Lucene's field name. */
	public static final String FIELD_NAME = "_clustering_key";

	private final CompositeType type;
	private final int clusteringPosition;

	/**
	 * Returns a new {@code ClusteringKeyMapper} according to the specified column family meta data.
	 * 
	 * @param metadata
	 *            The column family meta data.
	 */
	private ClusteringKeyMapper(CFMetaData metadata) {
		type = (CompositeType) metadata.comparator;
		clusteringPosition = metadata.getCfDef().columns.size();
	}

	/**
	 * Returns a new {@code ClusteringKeyMapper} according to the specified column family meta data.
	 * 
	 * @param metadata
	 *            The column family meta data.
	 * @return A new {@code ClusteringKeyMapper} according to the specified column family meta data.
	 */
	public static ClusteringKeyMapper instance(CFMetaData metadata) {
		return metadata.clusteringKeyColumns().size() > 0 ? new ClusteringKeyMapper(metadata) : null;
	}

	/**
	 * Returns the clustering key validation type.
	 * 
	 * @return The clustering key validation type.
	 */
	public CompositeType getType() {
		return type;
	}

	/**
	 * Returns the first possible column name of those having the same clustering key that the
	 * specified column name.
	 * 
	 * @param columnName
	 *            A storage engine column name.
	 * @return The first column name of for {@code columnName}.
	 */
	public ByteBuffer start(final ByteBuffer columnName) {
		CompositeType.Builder builder = type.builder();
		ByteBuffer[] components = ByteBufferUtils.split(columnName, type);
		for (int i = 0; i < clusteringPosition; i++) {
			ByteBuffer component = components[i];
			builder.add(component);
		}
		ByteBuffer bb = builder.build();
		return bb;
	}

	/**
	 * Returns the last possible column name of those having the same clustering key that the
	 * specified column name.
	 * 
	 * @param columnName
	 *            A storage engine column name.
	 * @return The first column name of for {@code columnName}.
	 */
	public ByteBuffer stop(ByteBuffer columnName) {
		CompositeType.Builder builder = type.builder();
		ByteBuffer[] components = ByteBufferUtils.split(columnName, type);
		for (int i = 0; i < clusteringPosition; i++) {
			ByteBuffer component = components[i];
			builder.add(component);
		}
		ByteBuffer bb = builder.buildAsEndOfRange();
		return bb;
	}

	/**
	 * Returns the common clustering keys of the specified column family
	 * 
	 * @param columnFamily
	 * @return
	 */
	public Set<ByteBuffer> byteBuffers(ColumnFamily columnFamily) {
		Set<ByteBuffer> keys = new HashSet<>();
		Iterator<Column> iterator = columnFamily.iterator();
		while (iterator.hasNext()) {
			Column column = iterator.next();
			ByteBuffer columnName = column.name();
			ByteBuffer clusteringKey = start(columnName);
			keys.add(clusteringKey);
		}
		return keys;
	}

	/**
	 * Returns the storage engine column name for the specified column identifier using the
	 * specified clustering key.
	 * 
	 * @param columnIdentifier
	 *            The column logic name.
	 * @param clusteringKey
	 *            The clustering key.
	 * @return A storage engine column name.
	 */
	public ByteBuffer name(ColumnIdentifier columnIdentifier, ByteBuffer clusteringKey) {
		CompositeType.Builder builder = type.builder();
		ByteBuffer[] components = ByteBufferUtils.split(clusteringKey, type);
		for (int i = 0; i < clusteringPosition; i++) {
			ByteBuffer component = components[i];
			builder.add(component);
		}
		builder.add(columnIdentifier.key);
		return builder.build();
	}

	/**
	 * Adds the to the specified {@link Document} the {@link Fields} representing the clustering key
	 * of the specified storage engine {@link Column}.
	 * 
	 * @param document
	 *            A {@link Document}.
	 * @param column
	 *            A {@link Column}.
	 */
	public void addFields(Document document, ByteBuffer name) {
		Field field = new StringField(FIELD_NAME, ByteBufferUtils.toString(name), Store.YES);
		document.add(field);
	}

	/**
	 * Returns the clustering key contained in the specified Lucene's {@link Document}.
	 * 
	 * @param document
	 *            A {@link Document}.
	 * @return The clustering key contained in the specified Lucene's {@link Document}.
	 */
	public ByteBuffer byteBuffer(Document document) {
		String string = document.get(FIELD_NAME);
		return ByteBufferUtils.fromString(string);
	}

	/**
	 * Returns the raw clustering key contained in the specified Lucene's field value.
	 * 
	 * @param bytesRef
	 *            The {@link BytesRef} containing the raw clustering key to be get.
	 * @return The raw clustering key contained in the specified Lucene's field value.
	 */
	public ByteBuffer byteBuffer(BytesRef bytesRef) {
		String string = bytesRef.utf8ToString();
		return ByteBufferUtils.fromString(string);
	}

	/**
	 * Returns a Lucene's {@link Filter} for filtering documents/rows according to the column name
	 * range specified in {@code dataRange}.
	 * 
	 * @param dataRange
	 *            The data range containing the column name range to be filtered.
	 * @return A Lucene's {@link Filter} for filtering documents/rows according to the column name
	 *         range specified in {@code dataRage}.
	 */
	public Filter filter(DataRange dataRange) {
		return new ClusteringKeyMapperDataRangeFilter(this, dataRange);
	}

	public Filter filter(RangeTombstone rangeTombstone) {
		return new ClusteringKeyMapperRangeTombstoneFilter(this, rangeTombstone);
	}

	/**
	 * Returns a Lucene's {@link SortField} array for sorting documents/rows according to the column
	 * family name.
	 * 
	 * @return A Lucene's {@link SortField} array for sorting documents/rows according to the column
	 *         family name.
	 */
	public SortField[] sortFields() {
		return new SortField[] { new SortField(FIELD_NAME, new FieldComparatorSource() {
			@Override
			public FieldComparator<?> newComparator(String field, int hits, int sort, boolean reversed) throws IOException {
				return new ClusteringKeyMapperSorter(ClusteringKeyMapper.this, hits, field);
			}
		}) };
	}

}
