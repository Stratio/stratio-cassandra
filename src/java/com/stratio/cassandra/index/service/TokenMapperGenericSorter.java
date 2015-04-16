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
package com.stratio.cassandra.index.service;

import org.apache.cassandra.dht.Token;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.util.BytesRef;

/**
 * {@link FieldComparator} that compares {@link Token} field sorting by its Cassandra's partitioner.
 *
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class TokenMapperGenericSorter extends FieldComparator.TermOrdValComparator {

    /** The PartitionKeyComparator to be used. */
    private final TokenMapperGeneric tokenMapperGeneric;

    /**
     * Returns a new {@code TokenMapperGenericSorter}
     *
     * @param tokenMapperGeneric The {@code TokenMapperGenericSorter} to be used.
     * @param numHits            The number of hits.
     * @param field              The field name.
     */
    public TokenMapperGenericSorter(TokenMapperGeneric tokenMapperGeneric, int numHits, String field) {
        super(numHits, field, false);
        this.tokenMapperGeneric = tokenMapperGeneric;
    }

    /**
     * Compares its two field value arguments for order. Returns a negative integer, zero, or a positive integer as the
     * first argument is less than, equal to, or greater than the second.
     *
     * @param val1 The first field value to be compared.
     * @param val2 The second field value to be compared.
     * @return A negative integer, zero, or a positive integer as the first argument is less than, equal to, or greater
     * than the second.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
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
        Token t1 = tokenMapperGeneric.token(val1);
        Token t2 = tokenMapperGeneric.token(val2);
        return t1.compareTo(t2);
    }
}