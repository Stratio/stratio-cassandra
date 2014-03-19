package org.apache.cassandra.db.index.stratio;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.Column;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.index.stratio.util.ByteBufferUtils;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.StringField;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.FieldComparatorSource;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.SortField;
import org.apache.lucene.util.BytesRef;

/**
 * Class for several clustering key mappings between Cassandra and Lucene.
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

	public ByteBuffer start(ByteBuffer key) {
		CompositeType.Builder builder = type.builder();
		ByteBuffer[] components = ByteBufferUtils.split(key, type);
		for (int i = 0; i < clusteringPosition; i++) {
			ByteBuffer component = components[i];
			builder.add(component);
		}
		ByteBuffer bb = builder.build();
		return bb;
	}

	public ByteBuffer stop(ByteBuffer key) {
		CompositeType.Builder builder = type.builder();
		ByteBuffer[] components = ByteBufferUtils.split(key, type);
		for (int i = 0; i < clusteringPosition; i++) {
			ByteBuffer component = components[i];
			builder.add(component);
		}
		ByteBuffer bb = builder.buildAsEndOfRange();
		return bb;
	}

	public ByteBuffer name(Document document, ColumnIdentifier columnIdentifier) {
		ByteBuffer key = byteBuffer(document);
		CompositeType.Builder builder = type.builder();
		ByteBuffer[] components = ByteBufferUtils.split(key, type);
		for (int i = 0; i < clusteringPosition; i++) {
			ByteBuffer component = components[i];
			builder.add(component);
		}
		builder.add(columnIdentifier.key);
		return builder.build();
	}

	public void addFields(Document document, ColumnFamily columnFamily) {
		Column column = columnFamily.iterator().next();
		ByteBuffer name = column.name();
		Field field = new StringField(FIELD_NAME, ByteBufferUtils.toString(name), Store.YES);
		document.add(field);
	}

	/**
	 * Returns the clustering key contained in the specified Lucene's {@link Document}.
	 * 
	 * @param document
	 *            the {@link Document}.
	 * @return the clustering key contained in the specified Lucene's {@link Document}.
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
		return new ClusteringKeyMapperFilter(this, dataRange);
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
			public	FieldComparator<?>
			        newComparator(String field, int hits, int sort, boolean reversed) throws IOException {
				return new ClusteringKeyMapperSorter(ClusteringKeyMapper.this, hits, field);
			}
		}) };
	}

}
