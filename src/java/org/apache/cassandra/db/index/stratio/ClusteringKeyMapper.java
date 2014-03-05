package org.apache.cassandra.db.index.stratio;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedList;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.index.stratio.util.ByteBufferUtils;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.ColumnToCollectionType;
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
	public static final String FIELD_NAME = "clustering_key";

	private final CompositeType columnNameType;
	private final CompositeType type;

	/**
	 * Builds a new {@code ClusteringKeyMapper} according to the specified column family meta data.
	 * 
	 * @param metadata
	 *            The column family meta data.
	 */
	public ClusteringKeyMapper(CFMetaData metadata) {
		columnNameType = (CompositeType) metadata.comparator;
		LinkedList<AbstractType<?>> keyComponents = new LinkedList<>(columnNameType.getComponents());
		keyComponents.removeLast();
		boolean hasCollections = false;
		for (AbstractType<?> type : ByteBufferUtils.split(columnNameType)) {
			if (type instanceof ColumnToCollectionType)
				hasCollections = true;
		}
		if (hasCollections) {
			keyComponents.removeLast();
		}
		type = CompositeType.getInstance(keyComponents);
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
		CompositeType.Builder builder = columnNameType.builder();
		for (ByteBuffer b : ByteBufferUtils.split(key, type)) {
			builder.add(b);
		}
		ByteBuffer bb = builder.build();
		return bb;
	}

	public ByteBuffer stop(ByteBuffer key) {
		CompositeType.Builder builder = columnNameType.builder();
		for (ByteBuffer b : ByteBufferUtils.split(key, type)) {
			builder.add(b);
		}
		ByteBuffer bb = builder.buildAsEndOfRange();
		return bb;
	}

	public ByteBuffer columnName(ByteBuffer key, ColumnIdentifier name) {
		CompositeType.Builder builder = columnNameType.builder();
		for (ByteBuffer b : ByteBufferUtils.split(key, type)) {
			builder.add(b);
		}
		builder.add(name.key);
		ByteBuffer bb = builder.build();
		return bb;
	}

	public void field(Document document, ColumnFamily columnFamily) {
		org.apache.cassandra.db.Column cell = columnFamily.iterator().next();
		ByteBuffer originalValue = cell.name();
		ByteBuffer[] originalValues = columnNameType.split(originalValue);
		CompositeType.Builder builder = type.builder();
		for (int i = 0; i < type.getComponents().size(); i++) {
			ByteBuffer valueComponent = originalValues[i];
			builder.add(valueComponent);
		}
		ByteBuffer key = builder.build();
		Field field = new StringField(FIELD_NAME, ByteBufferUtils.toString(key), Store.YES);
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
	public Filter[] filters(DataRange dataRange) {
		return new Filter[] { new ClusteringKeyMapperFilter(this, dataRange) };
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
				return new ClusteringKeyMapperComparator(ClusteringKeyMapper.this, hits, field);
			}
		}) };
	}

}
