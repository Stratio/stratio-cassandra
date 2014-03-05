package org.apache.cassandra.db.index.stratio;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.filter.ColumnSlice;
import org.apache.cassandra.db.filter.SliceQueryFilter;
import org.apache.cassandra.db.index.stratio.util.ByteBufferUtils;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.utils.ByteBufferUtil;
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
 * {@link Filter} that filters documents which clustering key field satisfies a certain
 * {@link DataRange}. This means that the clustering key value must be contained in the slice query
 * filter specified in the data range.
 * 
 * @author adelapena
 * 
 */
public class ClusteringKeyMapperFilter extends Filter {

	/** The {@link ClusteringKeyMapper} to be used. */
	private final ClusteringKeyMapper clusteringKeyMapper;

	/** The filtering column slice. */
	private final SliceQueryFilter sliceQueryFilter;

	/**
	 * Build a new {@code ClusteringKeyFilter} for the {@code dataRange} using
	 * {@code clusteringKeyMapper}.
	 * 
	 * @param clusteringKeyMapper
	 *            The {@link ClusteringKeyMapper} to be used.
	 * @param dataRange
	 *            The filtering data range.
	 */
	public ClusteringKeyMapperFilter(ClusteringKeyMapper clusteringKeyMapper, DataRange dataRange) {
		this.clusteringKeyMapper = clusteringKeyMapper;
		this.sliceQueryFilter = (SliceQueryFilter) dataRange.columnFilter(ByteBufferUtil.EMPTY_BYTE_BUFFER);
	}

	@Override
	public DocIdSet getDocIdSet(AtomicReaderContext context, final Bits acceptDocs) throws IOException {
		AtomicReader atomicReader = context.reader();
		Bits liveDocs = atomicReader.getLiveDocs();

		OpenBitSet bitSet = new OpenBitSet(atomicReader.maxDoc());

		Terms terms = atomicReader.terms(ClusteringKeyMapper.FIELD_NAME);
		if (terms == null) {
			return null;
		}

		final TermsEnum termsEnum = terms.iterator(null);

		DocsEnum docsEnum = null;
		BytesRef bytesRef = termsEnum.next();

		while (bytesRef != null) {
			ByteBuffer value = clusteringKeyMapper.byteBuffer(bytesRef);
			boolean accepted = true;

			for (ColumnSlice columnSlice : sliceQueryFilter.slices) {
				accepted &= isInSlice(value, columnSlice);
			}

			docsEnum = termsEnum.docs(liveDocs, docsEnum);
			if (accepted) {
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

	/**
	 * Returns {@code true} if the specified clustering key is inside the specified column slice,
	 * {@code false} otherwise.
	 * 
	 * @param key
	 *            The clustering key to be checked.
	 * @param columnSlice
	 *            The column slice to be satisfied.
	 * @return {@code true} if the specified clustering key is inside the specified column slice,
	 *         {@code false} otherwise.
	 */
	private boolean isInSlice(ByteBuffer key, ColumnSlice columnSlice) {
		AbstractType<?> type = clusteringKeyMapper.getType();
		boolean accepted = true;
		ByteBuffer start = columnSlice.start;
		if (start != null && !ByteBufferUtils.isEmpty(start)) {
			accepted &= type.compare(start, key) <= 0;
		}
		ByteBuffer finish = columnSlice.finish;
		if (finish != null && !ByteBufferUtils.isEmpty(finish)) {
			accepted &= type.compare(finish, key) >= 0;
		}
		return accepted;
	}

}
