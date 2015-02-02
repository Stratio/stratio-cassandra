/*
 * Copyright 2014, Stratio.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.stratio.cassandra.index;

import org.apache.cassandra.dht.Token;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.search.FieldCache;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;

/**
 * {@link FieldComparator} that compares {@link Token} field sorting by its Cassandra's partitioner.
 *
 * @author Andres de la Pena <adelapena@stratio.com>
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
    private BytesRef topValue;

    /**
     * Returns a new {@code TokenMapperGenericSorter}
     *
     * @param tokenMapperGeneric The {@code TokenMapperGenericSorter} to be used.
     * @param numHits            The number of hits.
     * @param field              The field name.
     */
    public TokenMapperGenericSorter(TokenMapperGeneric tokenMapperGeneric, int numHits, String field) {
        this.tokenMapperGeneric = tokenMapperGeneric;
        values = new BytesRef[numHits];
        this.field = field;
    }

    /** {@inheritDoc} */
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

    /** {@inheritDoc} */
    @Override
    public int compareBottom(int doc) {
        docTerms.get(doc, tempBR);
        if (tempBR.length == 0 && !docsWithField.get(doc)) {
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

    /** {@inheritDoc} */
    @Override
    public void copy(int slot, int doc) {
        if (values[slot] == null) {
            values[slot] = new BytesRef();
        }
        docTerms.get(doc, values[slot]);
        if (values[slot].length == 0 && !docsWithField.get(doc)) {
            values[slot].bytes = MISSING_BYTES;
        }
    }

    /** {@inheritDoc} */
    @Override
    public FieldComparator<BytesRef> setNextReader(AtomicReaderContext context) throws IOException {
        docTerms = FieldCache.DEFAULT.getTerms(context.reader(), field, true);
        docsWithField = FieldCache.DEFAULT.getDocsWithField(context.reader(), field);
        return this;
    }

    /** {@inheritDoc} */
    @Override
    public void setBottom(final int bottom) {
        this.bottom = values[bottom];
    }

    /** {@inheritDoc} */
    @Override
    public BytesRef value(int slot) {
        return values[slot];
    }

    /** {@inheritDoc} */
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

    /** {@inheritDoc} */
    @Override
    public int compareTop(int doc) {
        docTerms.get(doc, tempBR);
        if (tempBR.length == 0 && !docsWithField.get(doc)) {
            tempBR.bytes = MISSING_BYTES;
        }
        return compare(tempBR, topValue);
    }

    /** {@inheritDoc} */
    @Override
    public void setTopValue(BytesRef value) {
        topValue = value;
    }

    /**
     * Compares its two field value arguments for order. Returns a negative integer, zero, or a positive integer as the
     * first argument is less than, equal to, or greater than the second.
     *
     * @param value1 The first field value to be compared.
     * @param value2 The second field value to be compared.
     * @return A negative integer, zero, or a positive integer as the first argument is less than, equal to, or greater
     * than the second.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private int compare(BytesRef value1, BytesRef value2) {
        Token t1 = tokenMapperGeneric.token(value1);
        Token t2 = tokenMapperGeneric.token(value2);
        return t1.compareTo(t2);
    }
}