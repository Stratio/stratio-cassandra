package org.apache.cassandra.db.index.stratio;

import java.nio.ByteBuffer;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.RowPosition;
import org.apache.cassandra.db.RowPosition.Kind;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.LongField;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.NumericRangeFilter;
import org.apache.lucene.search.SortField;

/**
 * {@link PartitionKeyMapper} to be used when {@link Murmur3Partitioner} is used. It indexes the
 * token long value as a Lucene's long field.
 * 
 * @author adelapena
 * 
 */
public class PartitionKeyMapperMurmur extends PartitionKeyMapper {

	public static final String TOKEN_NAME = "token";

	private final Murmur3Partitioner partitioner;

	/**
	 * Returns a new {@code PartitionKeyMapperMurmur}.
	 */
	public PartitionKeyMapperMurmur() {
		partitioner = (Murmur3Partitioner) DatabaseDescriptor.getPartitioner();
	}

	@Override
	public void document(Document document, ByteBuffer partitionKey) {

		super.document(document, partitionKey);

		Long value = partitioner.getToken(partitionKey).token;
		Field tokenField = new LongField(TOKEN_NAME, value, Store.NO);
		document.add(tokenField);
	}

	@Override
	public Filter[] filters(DataRange dataRange) {
		RowPosition startPosition = dataRange.startKey();
		RowPosition stopPosition = dataRange.stopKey();
		Long start = (Long) startPosition.getToken().token;
		Long stop = (Long) stopPosition.getToken().token;
		if (startPosition.isMinimum())
			start = null;
		if (stopPosition.isMinimum())
			stop = null;
		boolean includeLower = startPosition.kind() == Kind.MIN_BOUND;
		boolean includeUpper = stopPosition.kind() == Kind.MAX_BOUND;
		Filter filter = NumericRangeFilter.newLongRange(TOKEN_NAME, start, stop, includeLower, includeUpper);
		return new Filter[] { filter };
	}

	@Override
	public SortField[] sortFields() {
		return new SortField[] { new SortField(TOKEN_NAME, SortField.Type.LONG) };
	}

}
