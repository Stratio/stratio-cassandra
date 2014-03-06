package org.apache.cassandra.db.index.stratio;

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
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.SortField;
import org.apache.lucene.util.BytesRef;

/**
 * {@link PartitionKeyMapper} to be used when {@link Murmur3Partitioner} is used. It indexes the
 * token long value as a Lucene's long field.
 * 
 * @author adelapena
 * 
 */
public class TokenMapperGeneric extends TokenMapper {

	private static final String FIELD_NAME = "_token_generic";

	private final IPartitioner<?> partitioner = DatabaseDescriptor.getPartitioner();

	@Override
	public void document(Document document, DecoratedKey partitionKey) {
		String serializedKey = ByteBufferUtils.toString(partitionKey.key);
		Field field = new StringField(FIELD_NAME, serializedKey, Store.YES);
		document.add(field);
	}

	@Override
	public Filter[] filters(DataRange dataRange) {
		Filter filter = new TokenMapperGenericFilter(this, dataRange);
		return new Filter[] { filter };
	}

	@Override
	public SortField[] sortFields() {
		return new SortField[] { new SortField(FIELD_NAME, SortField.Type.LONG) };
	}

	public ByteBuffer byteBuffer(BytesRef bytesRef) {
		String string = bytesRef.utf8ToString();
		return ByteBufferUtils.fromString(string);
	}

	public DecoratedKey decoratedKey(BytesRef bytesRef) {
		ByteBuffer partitionKey = byteBuffer(bytesRef);
		return partitioner.decorateKey(partitionKey);
	}

}
