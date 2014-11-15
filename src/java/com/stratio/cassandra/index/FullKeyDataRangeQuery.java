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
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.lucene.index.FilteredTermsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;

/**
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class FullKeyDataRangeQuery extends MultiTermQuery
{
    private final DataRange dataRange;
    private final FullKeyMapper fullKeyMapper;
    private final CellNameType cellNameType;

    public FullKeyDataRangeQuery(String field, DataRange dataRange, FullKeyMapper fullKeyMapper)
    {
        super(field);
        this.dataRange = dataRange;
        this.fullKeyMapper = fullKeyMapper;
        this.cellNameType = fullKeyMapper.clusteringKeyType;
    }

    @Override
    protected TermsEnum getTermsEnum(Terms terms, AttributeSource atts) throws IOException
    {
        TermsEnum tenum = terms.iterator(null);
        return new FullKeyDataRangeFilteredTermsEnum(tenum);
    }

    @Override
    public String toString(String field)
    {
        return new ToStringBuilder(this)
                .append("field", field)
                .append("dataRange", dataRange)
                .append("fullKeyMapper", fullKeyMapper)
                .toString();
    }

    private class FullKeyDataRangeFilteredTermsEnum extends FilteredTermsEnum
    {

        public FullKeyDataRangeFilteredTermsEnum(TermsEnum tenum)
        {
            super(tenum);
            setInitialSeekTerm(new BytesRef());
        }

        @Override
        protected AcceptStatus accept(BytesRef term)
        {

            DecoratedKey decoratedKey = fullKeyMapper.partitionKey(term);
            boolean accepted = dataRange.keyRange().contains(decoratedKey);

            SliceQueryFilter sliceQueryFilter = (SliceQueryFilter) dataRange.columnFilter(decoratedKey.getKey());
            CellName value = fullKeyMapper.clusteringKey(term);
            for (ColumnSlice columnSlice : sliceQueryFilter.slices)
            {
                accepted &= isInSlice(value, columnSlice);
            }
            return accepted ? AcceptStatus.YES : AcceptStatus.NO;
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
            Composite start = columnSlice.start;
            if (!start.isEmpty() && cellNameType.compare(start, key) > 0)
            {
                return false;
            }
            Composite finish = columnSlice.finish;
            if (!finish.isEmpty() && cellNameType.compare(finish, key) < 0)
            {
                return false;
            }
            return true;
        }
    }
}
