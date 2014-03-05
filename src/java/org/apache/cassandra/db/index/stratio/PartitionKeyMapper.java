package org.apache.cassandra.db.index.stratio;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.index.stratio.util.ByteBufferUtils;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.FieldComparatorSource;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;

/**
 * Class for several partition key mappings between Cassandra and Lucene.
 * 
 * @author adelapena
 * 
 */
public class PartitionKeyMapper {

	/** The Lucene's field name. */
	protected static final String FIELD_NAME = "_partition_key";

	/** The active active partition key. */
	private final IPartitioner<?> partitioner;

	/**
	 * Builds a new {@code PartitionKeyMapper} according to the specified column family meta data.
	 */
	protected PartitionKeyMapper() {
		partitioner = DatabaseDescriptor.getPartitioner();
	}

	public static PartitionKeyMapper build() {
		IPartitioner<?> partitioner = DatabaseDescriptor.getPartitioner();
		if (partitioner instanceof Murmur3Partitioner) {
			return new PartitionKeyMapperMurmur();
		} else {
			return new PartitionKeyMapper();
		}
	}

	/**
	 * Adds to the specified {@link Documents} the {@link Field}s associated to the specified raw
	 * partition key.
	 * 
	 * @param document
	 * @param partitionKey
	 *            The raw partition key to be converted.
	 */
	public void document(Document document, DecoratedKey partitionKey) {
		String serializedKey = ByteBufferUtils.toString(partitionKey.key);
		Field field = new StringField(FIELD_NAME, serializedKey, Store.YES);
		document.add(field);
	}

	/**
	 * Returns the specified raw partition key as a not indexed, stored Lucene's {@link Term}.
	 * 
	 * @param partitionKey
	 *            The raw partition key to be converted.
	 * @return The specified raw partition key as a not indexed, stored Lucene's {@link Term}.
	 */
	public Term term(DecoratedKey partitionKey) {
		String serializedKey = ByteBufferUtils.toString(partitionKey.key);
		return new Term(FIELD_NAME, serializedKey);
	}
	
	public Query query(DecoratedKey partitionKey) {
		return new TermQuery(term(partitionKey));
	}

	/**
	 * Returns the {@link DocoratedKey} contained in the specified Lucene's {@link Document}.
	 * 
	 * @param document
	 *            the {@link Document} containing the partition key to be get.
	 * @return The {@link DocoratedKey} contained in the specified Lucene's {@link Document}.
	 */
	public DecoratedKey decoratedKey(Document document) {
		String string = document.get(FIELD_NAME);
		ByteBuffer partitionKey = ByteBufferUtils.fromString(string);
		return decoratedKey(partitionKey);
	}

	/**
	 * Returns the partition key contained in the specified Lucene's {@link Document}.
	 * 
	 * @param document
	 *            the {@link Document} containing the partition key to be get.
	 * @return The partition key contained in the specified Lucene's {@link Document}.
	 */
	public ByteBuffer partitionKey(Document document) {
		String string = document.get(FIELD_NAME);
		return ByteBufferUtils.fromString(string);
	}

	/**
	 * Returns the {@link DocoratedKey} contained in the specified Lucene's field value.
	 * 
	 * @param bytesRef
	 *            The {@link BytesRef} containing the partition key to be get.
	 * @return The {@link DocoratedKey} contained in the specified Lucene's field value.
	 */
	public DecoratedKey decoratedKey(BytesRef bytesRef) {
		String string = bytesRef.utf8ToString();
		ByteBuffer partitionKey = ByteBufferUtils.fromString(string);
		return decoratedKey(partitionKey);
	}

	/**
	 * Returns the specified raw partition key as a a {@link DecoratedKey}.
	 * 
	 * @param partitionKey
	 *            The raw partition key to be converted.
	 * @return The specified raw partition key as a a {@link DecoratedKey}.
	 */
	public DecoratedKey decoratedKey(ByteBuffer partitionKey) {
		return partitioner.decorateKey(partitionKey);
	}

	/**
	 * Returns the Lucene's {@link Filter}s for filtering documents/rows according to the token
	 * range specified in {@code dataRange}.
	 * 
	 * @param dataRange
	 *            The data range containing the token range to be filtered.
	 * @return The Lucene's {@link Filter}s for filtering documents/rows according to the token
	 *         range specified in {@code dataRage}.
	 */
	public Filter[] filters(DataRange dataRange) {
		Filter filter = new PartitionKeyMapperFilter(this, dataRange);
		return new Filter[] { filter };
	}

	/**
	 * Returns the Lucene's {@link SortField} array for sorting documents/rows according to the
	 * current Cassandra's partitioner.
	 * 
	 * @return The Lucene's {@link SortField} array for sorting documents/rows according to the
	 *         current Cassandra's partitioner.
	 */
	public SortField[] sortFields() {
		return new SortField[] { new SortField(FIELD_NAME, new FieldComparatorSource() {
			@Override
			public	FieldComparator<?>
			        newComparator(String field, int hits, int sort, boolean reversed) throws IOException {
				return new PartitionKeyMapperComparator(PartitionKeyMapper.this, hits, field);
			}
		}) };
	}

}
