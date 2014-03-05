package org.apache.cassandra.db.index.stratio;

import java.nio.ByteBuffer;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.Column;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.index.stratio.util.ByteBufferUtils;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.Term;

public class FullKeyMapper {

	/** The Lucene's field name. */
	public static final String FIELD_NAME = "_full_key";

	public AbstractType<?> partitionKeyType;
	public AbstractType<?> clusteringKeyType;
	public CompositeType type;

	public FullKeyMapper(CFMetaData metadata) {
		this.partitionKeyType = metadata.getKeyValidator();
		this.clusteringKeyType = metadata.comparator;
		type = CompositeType.getInstance(partitionKeyType, clusteringKeyType);
	}

	public AbstractType<?> getPartitionKeyType() {
		return partitionKeyType;
	}

	public AbstractType<?> getClusteringKeyType() {
		return clusteringKeyType;
	}

	public CompositeType getType() {
		return type;
	}

	public void field(Document document, DecoratedKey partitionKey, ColumnFamily columnFamily) {
		Column column = columnFamily.iterator().next();
		ByteBuffer fullKey = type.builder().add(partitionKey.key).add(column.name()).build();
		Field field = new StringField(FIELD_NAME, ByteBufferUtils.toString(fullKey), Store.NO);
		document.add(field);
	}

	public Term term(DecoratedKey partitionKey, ByteBuffer clusteringKey) {
		ByteBuffer fullKey = type.builder().add(partitionKey.key).add(clusteringKey).build();
		return new Term(FIELD_NAME, ByteBufferUtils.toString(fullKey));
	}

}
