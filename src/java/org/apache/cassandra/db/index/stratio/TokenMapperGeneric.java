package org.apache.cassandra.db.index.stratio;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.index.stratio.util.ByteBufferUtils;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.dht.Token.TokenFactory;
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
 * {@link TokenMapper} to be used when any {@link IPartitioner} when there is not a more specific
 * implementation. It indexes the token raw binary value as a Lucene's string field.
 * 
 * @author adelapena
 * 
 */
public class TokenMapperGeneric extends TokenMapper {

	public static final String FIELD_NAME = "_token_generic";

	private final TokenFactory<?> factory;

	public TokenMapperGeneric() {
		factory = DatabaseDescriptor.getPartitioner().getTokenFactory();
	}

	@Override
	@SuppressWarnings("unchecked")
	public void addFields(Document document, DecoratedKey partitionKey) {
		ByteBuffer bb = factory.toByteArray(partitionKey.token);
		String serialized = ByteBufferUtils.toString(bb);
		Field field = new StringField(FIELD_NAME, serialized, Store.YES);
		document.add(field);
	}

	@Override
	public Filter[] filters(DataRange dataRange) {
		Filter filter = new TokenMapperGenericFilter(this, dataRange);
		return new Filter[] { filter };
	}

	@Override
	public SortField[] sortFields() {
		return new SortField[] { new SortField(FIELD_NAME, new FieldComparatorSource() {
			@Override
			public	FieldComparator<?>
			        newComparator(String field, int hits, int sort, boolean reversed) throws IOException {
				return new TokenMapperGenericSorter(TokenMapperGeneric.this, hits, field);
			}
		}) };
	}

	public Token<?> token(BytesRef bytesRef) {
		String string = bytesRef.utf8ToString();
		ByteBuffer bb = ByteBufferUtils.fromString(string);
		return factory.fromByteArray(bb);
	}

}
