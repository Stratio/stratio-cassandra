package org.apache.cassandra.db.index.stratio;

import java.io.IOException;

import org.apache.cassandra.dht.Token;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.search.FieldCache;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;

/**
 * 
 * {@link FieldComparator} that compares token field sorting by its Cassandra's partitioner.
 * 
 * @author adelapena
 * 
 */
public class TokenMapperGenericSorter extends FieldComparator<BytesRef> {

	private static final byte[] MISSING_BYTES = new byte[0];

	/** The PartitionKeyComparator to be used. */
	private final TokenMapperGeneric tokenMapperGeneric;

	private BytesRef[] values;
	private BinaryDocValues docTerms;
	private Bits docsWithField;
	private final String field;
	private BytesRef bottom;
	private final BytesRef tempBR = new BytesRef();

	/**
	 * Returns a new {@code TokenMapperGenericSorter}
	 * 
	 * @param tokenMapperGeneric
	 *            The {@code TokenMapperGenericSorter} to be used.
	 * @param numHits
	 *            The number of hits.
	 * @param field
	 *            The field name.
	 */
	public TokenMapperGenericSorter(TokenMapperGeneric tokenMapperGeneric, int numHits, String field) {
		this.tokenMapperGeneric = tokenMapperGeneric;
		values = new BytesRef[numHits];
		this.field = field;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int compare(int slot1, int slot2) {
		final BytesRef val1 = values[slot1];
		final BytesRef val2 = values[slot2];
		if (val1 == null) {
			if (val2 == null) {
				return 0;
			}
			return -1;
		} else if (val2 == null) {
			return 1;
		}
		return compare(val1, val2);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int compareBottom(int doc) {
		docTerms.get(doc, tempBR);
		if (tempBR.length == 0 && docsWithField.get(doc) == false) {
			tempBR.bytes = MISSING_BYTES;
		}
		if (bottom.bytes == MISSING_BYTES) {
			if (tempBR.bytes == MISSING_BYTES) {
				return 0;
			}
			return -1;
		} else if (tempBR.bytes == MISSING_BYTES) {
			return 1;
		}
		return compare(bottom, tempBR);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void copy(int slot, int doc) {
		if (values[slot] == null) {
			values[slot] = new BytesRef();
		}
		docTerms.get(doc, values[slot]);
		if (values[slot].length == 0 && docsWithField.get(doc) == false) {
			values[slot].bytes = MISSING_BYTES;
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public FieldComparator<BytesRef> setNextReader(AtomicReaderContext context) throws IOException {
		docTerms = FieldCache.DEFAULT.getTerms(context.reader(), field, true);
		docsWithField = FieldCache.DEFAULT.getDocsWithField(context.reader(), field);
		return this;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void setBottom(final int bottom) {
		this.bottom = values[bottom];
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public BytesRef value(int slot) {
		return values[slot];
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int compareValues(BytesRef val1, BytesRef val2) {
		if (val1 == null) {
			if (val2 == null) {
				return 0;
			}
			return -1;
		} else if (val2 == null) {
			return 1;
		}
		return compare(val1, val2);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int compareDocToValue(int doc, BytesRef value) {
		docTerms.get(doc, tempBR);
		if (tempBR.length == 0 && docsWithField.get(doc) == false) {
			tempBR.bytes = MISSING_BYTES;
		}
		return compare(tempBR, value);
	}

	/**
	 * Compares its two field value arguments for order. Returns a negative integer, zero, or a
	 * positive integer as the first argument is less than, equal to, or greater than the second.
	 * 
	 * @param fieldValue1
	 *            The first field value to be compared.
	 * @param fieldValue2
	 *            The second field value to be compared.
	 * @return A negative integer, zero, or a positive integer as the first argument is less than,
	 *         equal to, or greater than the second.
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	private int compare(BytesRef fieldValue1, BytesRef fieldValue2) {
		Token t1 = tokenMapperGeneric.token(fieldValue1);
		Token t2 = tokenMapperGeneric.token(fieldValue2);
		return t1.compareTo(t2);
	}
}