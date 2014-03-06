package org.apache.cassandra.db.index.stratio;

import java.nio.ByteBuffer;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.index.stratio.util.ByteBufferUtils;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;

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
	private final CompositeType nameType;

	/**
	 * Builds a new {@code PartitionKeyMapper} according to the specified column family meta data.
	 */
	private PartitionKeyMapper(CFMetaData metadata) {
		partitioner = DatabaseDescriptor.getPartitioner();
		nameType = (CompositeType) metadata.comparator;
	}

	public static PartitionKeyMapper instance(CFMetaData metadata) {
		return new PartitionKeyMapper(metadata);
	}

	/**
	 * Adds to the specified {@link Document} the {@link Field}s associated to the specified raw
	 * partition key.
	 * 
	 * @param document
	 * @param partitionKey
	 *            The raw partition key to be converted.
	 */
	public void addFields(Document document, DecoratedKey partitionKey) {
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
	 * Returns the specified raw partition key as a a {@link DecoratedKey}.
	 * 
	 * @param partitionKey
	 *            The raw partition key to be converted.
	 * @return The specified raw partition key as a a {@link DecoratedKey}.
	 */
	public DecoratedKey decoratedKey(ByteBuffer partitionKey) {
		return partitioner.decorateKey(partitionKey);
	}

	public ByteBuffer name(Document document, ColumnIdentifier columnIdentifier) {
		return nameType.builder().add(columnIdentifier.key).build();
	}

}
