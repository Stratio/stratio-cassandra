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

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.RangeTombstone;
import org.apache.cassandra.db.composites.CellName;
import org.apache.cassandra.db.composites.CellNameType;
import org.apache.cassandra.db.composites.Composite;
import org.apache.cassandra.db.marshal.AbstractType;
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
public class FullKeyRangeTombstoneQuery extends MultiTermQuery
{
    private final DecoratedKey partitionKey;
    private final FullKeyMapper fullKeyMapper;
    private final Composite min;
    private final Composite max;
    private final AbstractType<?> keyType;
    private final CellNameType cellNameType;


    public FullKeyRangeTombstoneQuery(String field, DecoratedKey partitionKey, RangeTombstone rangeTombstone, FullKeyMapper fullKeyMapper) {
        super(field);
        this.partitionKey = partitionKey;
        this.fullKeyMapper = fullKeyMapper;
        this.min = rangeTombstone.min;
        this.max = rangeTombstone.max;
        keyType = fullKeyMapper.getPartitionKeyType();
        this.cellNameType = fullKeyMapper.getClusteringKeyType();
    }

    @Override
    protected TermsEnum getTermsEnum(Terms terms, AttributeSource atts) throws IOException {
        TermsEnum tenum = terms.iterator(null);
        return new FullKeyRangeTombstoneFilteredTermsEnum(tenum);
    }

    @Override
    public String toString(String field)
    {
        return new ToStringBuilder(this)
                .append("field", field)
                .append("partitionKey", partitionKey)
                .append("fullKeyMapper", fullKeyMapper)
                .toString();
    }

    private class FullKeyRangeTombstoneFilteredTermsEnum extends FilteredTermsEnum
    {

        public FullKeyRangeTombstoneFilteredTermsEnum(TermsEnum tenum)
        {
            super(tenum);
            setInitialSeekTerm(new BytesRef());
        }

        @Override
        protected AcceptStatus accept(BytesRef term)
        {
            DecoratedKey partitionKey = fullKeyMapper.partitionKey(term);
            if (keyType.compare(partitionKey.getKey(), FullKeyRangeTombstoneQuery.this.partitionKey.getKey()) != 0)
            {
                return AcceptStatus.NO;
            }

            CellName clusteringKey = fullKeyMapper.clusteringKey(term);
            if (min != null && !min.isEmpty() && cellNameType.compare(min, clusteringKey) > 0)
            {
                return AcceptStatus.NO;
            }
            if (max != null && !max.isEmpty() && cellNameType.compare(max, clusteringKey) < 0)
            {
                return AcceptStatus.NO;
            }

            return AcceptStatus.YES;
        }
    }
}
