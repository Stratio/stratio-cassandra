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
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Token;
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
public class TokenDataRangeQuery extends MultiTermQuery
{
    private final TokenMapperGeneric tokenMapper;
    private final AbstractBounds<Token> tokenBounds;

    public TokenDataRangeQuery(String field, DataRange dataRange, TokenMapperGeneric tokenMapper) {
        super(field);
        this.tokenMapper = tokenMapper;
        this.tokenBounds = dataRange.keyRange().toTokenBounds();
    }

    @Override
    protected TermsEnum getTermsEnum(Terms terms, AttributeSource atts) throws IOException {
        TermsEnum tenum = terms.iterator(null);
        return new TokenDataRangeFilteredTermsEnum(tenum);
    }

    @Override
    public String toString(String field)
    {
        return new ToStringBuilder(this)
                .append("tokenBounds", tokenBounds)
                .append("tokenMapper", tokenMapper)
                .toString();
    }

    private class TokenDataRangeFilteredTermsEnum extends FilteredTermsEnum
    {

        public TokenDataRangeFilteredTermsEnum(TermsEnum tenum)
        {
            super(tenum);
            setInitialSeekTerm(tokenMapper.bytesRef(tokenBounds.left));
        }

        @Override
        protected AcceptStatus accept(BytesRef term)
        {

            Token token = tokenMapper.token(term);

            if (tokenBounds.contains(token)) {
                if (token.compareTo(tokenBounds.right) > 0) {
                    return AcceptStatus.END;
                } else {
                    return AcceptStatus.YES;
                }
            } else {
                return AcceptStatus.NO;
            }
        }
    }
}
