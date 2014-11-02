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

import org.apache.cassandra.db.RangeTombstone;
import org.apache.cassandra.db.composites.CellName;
import org.apache.cassandra.db.composites.CellNameType;
import org.apache.cassandra.db.composites.Composite;
import org.apache.lucene.index.*;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Filter;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.OpenBitSet;

import java.io.IOException;

/**
 * {@link org.apache.lucene.search.Filter} that filters documents which clustering key field satisfies a certain {@link org.apache.cassandra.db.RangeTombstone}. This
 * means that the clustering key value must be contained in the slice query clusteringKeyFilter specified in the tombstone range.
 *
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class FullKeyMapperRangeTombstoneFilter extends Filter
{
    private final FullKeyMapper fullKeyMapper;

    /**
     * The minimum accepted column name.
     */
    private final Composite min;

    /**
     * The maximum accepted column name.
     */
    private final Composite max;

    /**
     * The type of the column names to be filtered.
     */
    private final CellNameType columnNameType;

    /**
     * Returns a new {@code ClusteringKeyMapperRangeTombstoneFilter} for {@code rangeTombstone} using
     * {@code fullKeyMapper}.
     *
     * @param fullKeyMapper The {@link com.stratio.cassandra.index.FullKeyMapper} to be used.
     * @param rangeTombstone      The filtering tombstone range.
     */
    public FullKeyMapperRangeTombstoneFilter(FullKeyMapper fullKeyMapper,
                                             RangeTombstone rangeTombstone)
    {
        super();
        this.fullKeyMapper = fullKeyMapper;
        this.min = rangeTombstone.min;
        this.max = rangeTombstone.max;
        this.columnNameType = fullKeyMapper.getClusteringKeyType();
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
            CellName value = fullKeyMapper.cellName(bytesRef);
            boolean accepted = true;
            if (min != null && !min.isEmpty())
            {
                accepted = columnNameType.compare(min, value) <= 0;
            }
            if (max != null && !max.isEmpty())
            {
                accepted &= columnNameType.compare(max, value) >= 0;
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

}
