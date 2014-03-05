package org.apache.cassandra.db.index.stratio;

import java.io.IOException;

import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.RowPosition;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Filter;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.OpenBitSet;

/**
 * {@link Filter} that filters documents which partition key field satisfies a certain
 * {@link DataRange}.
 * 
 * @author adelapena
 * 
 */
public class PartitionKeyMapperFilter extends Filter {

	private final PartitionKeyMapper partitionKeyMapper;
	private final AbstractBounds<RowPosition> keyRange;

	/**
	 * Returns a new {@code PartitionKeyMapperFilter} for the specified data range using the
	 * specified partition key mapper.
	 * 
	 * @param partitionKeyMapper
	 *            The partition key mapper.
	 * @param dataRange
	 *            The partition data range to be filtered.
	 */
	public PartitionKeyMapperFilter(PartitionKeyMapper partitionKeyMapper, DataRange dataRange) {
		this.partitionKeyMapper = partitionKeyMapper;
		this.keyRange = dataRange.keyRange();
	}

	@Override
	public DocIdSet getDocIdSet(AtomicReaderContext context, final Bits acceptDocs) throws IOException {

		AtomicReader atomicReader = context.reader();
		Bits liveDocs = atomicReader.getLiveDocs();

		OpenBitSet bitSet = new OpenBitSet(atomicReader.maxDoc());

		Terms terms = atomicReader.terms(PartitionKeyMapper.FIELD_NAME);
		if (terms == null) {
			return null;
		}

		final TermsEnum termsEnum = terms.iterator(null);

		BytesRef bytesRef = termsEnum.next();
		while (bytesRef != null) {
			DocsEnum docsEnum = termsEnum.docs(liveDocs, null);
			DecoratedKey decoratedKey = partitionKeyMapper.decoratedKey(bytesRef);
			if (keyRange.contains(decoratedKey)) {
				Integer docID = docsEnum.nextDoc();
				while (docID != DocIdSetIterator.NO_MORE_DOCS) {
					bitSet.set(docID);
					docID = docsEnum.nextDoc();
				}
			}
			bytesRef = termsEnum.next();
		}
		return bitSet;
	}

}
