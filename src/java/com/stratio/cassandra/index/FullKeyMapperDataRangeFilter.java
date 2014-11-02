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

import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.composites.CellName;
import org.apache.cassandra.db.composites.CellNameType;
import org.apache.cassandra.db.composites.Composite;
import org.apache.cassandra.db.filter.ColumnSlice;
import org.apache.cassandra.db.filter.SliceQueryFilter;
import org.apache.lucene.index.*;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Filter;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.OpenBitSet;

import java.io.IOException;

/**
 * {@link org.apache.lucene.search.Filter} that filters documents which clustering key field satisfies a certain {@link org.apache.cassandra.db.DataRange}. This means
 * that the clustering key value must be contained in the slice query clusteringKeyFilter specified in the data range.
 *
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class FullKeyMapperDataRangeFilter extends Filter
{

    /**
     * The {@link com.stratio.cassandra.index.FullKeyMapper} to be used.
     */
    private final FullKeyMapper fullKeyMapper;

    private final DataRange dataRange;

    /**
     * Returns a new {@code ClusteringKeyFilter} for {@code dataRange} using {@code clusteringKeyMapper}.
     *
     * @param fullKeyMapper The {@link com.stratio.cassandra.index.FullKeyMapper} to be used.
     * @param dataRange     The filtering data range.
     */
    public FullKeyMapperDataRangeFilter(FullKeyMapper fullKeyMapper, DataRange dataRange)
    {
        this.dataRange = dataRange;
        this.fullKeyMapper = fullKeyMapper;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DocIdSet getDocIdSet(AtomicReaderContext context, final Bits acceptDocs) throws IOException
    {
        AtomicReader atomicReader = context.reader();
        Bits liveDocs = atomicReader.getLiveDocs();

        OpenBitSet bitSet = new OpenBitSet(atomicReader.maxDoc());

        Terms terms = atomicReader.terms(FullKeyMapper.FIELD_NAME);
        if (terms == null)
        {
            return null;
        }

        final TermsEnum termsEnum = terms.iterator(null);

        DocsEnum docsEnum = null;
        BytesRef bytesRef = termsEnum.next();

        while (bytesRef != null)
        {
            DecoratedKey decoratedKey = fullKeyMapper.decoratedKey(bytesRef);

            boolean accepted = dataRange.keyRange().contains(decoratedKey);

            SliceQueryFilter sliceQueryFilter = (SliceQueryFilter) dataRange.columnFilter(decoratedKey.getKey());
            CellName value = fullKeyMapper.cellName(bytesRef);
            for (ColumnSlice columnSlice : sliceQueryFilter.slices)
            {
                accepted &= isInSlice(value, columnSlice);
            }

            docsEnum = termsEnum.docs(liveDocs, docsEnum);
            if (accepted)
            {
                Integer docID = docsEnum.nextDoc();
                while (docID != DocIdSetIterator.NO_MORE_DOCS)
                {
                    bitSet.set(docID);
                    docID = docsEnum.nextDoc();
                }
            }
            bytesRef = termsEnum.next();
        }
        return bitSet;
    }

    /**
     * Returns {@code true} if the specified clustering key is inside the specified column slice, {@code false}
     * otherwise.
     *
     * @param key         The clustering key to be checked.
     * @param columnSlice The column slice to be satisfied.
     * @return {@code true} if the specified clustering key is inside the specified column slice, {@code false}
     * otherwise.
     */
    private boolean isInSlice(Composite key, ColumnSlice columnSlice)
    {
        CellNameType type = fullKeyMapper.clusteringKeyType;
        boolean accepted = true;
        Composite start = columnSlice.start;
        if (!start.isEmpty())
        {
            accepted = type.compare(start, key) <= 0;
        }
        Composite finish = columnSlice.finish;
        if (!finish.isEmpty())
        {
            accepted &= type.compare(finish, key) >= 0;
        }
        return accepted;
    }

}
