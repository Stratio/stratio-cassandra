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
public class TokenRangeQuery extends MultiTermQuery
{
    private final TokenMapperGeneric tokenMapper;
    private final Token lower;
    private final Token upper;
    private final boolean includeLower;
    private final boolean includeUpper;

    public TokenRangeQuery(Token lower, Token upper, boolean includeLower, boolean includeUpper, TokenMapperGeneric tokenMapper) {
        super(TokenMapperGeneric.FIELD_NAME);
        this.tokenMapper = tokenMapper;
        this.lower = lower;
        this.upper = upper;
        this.includeLower = includeLower;
        this.includeUpper = includeUpper;
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
                .append("field", field)
                .append("lower", lower)
                .append("upper", upper)
                .append("includeStart", includeLower)
                .append("includeStop", includeUpper)
                .toString();
    }

    private class TokenDataRangeFilteredTermsEnum extends FilteredTermsEnum
    {

        public TokenDataRangeFilteredTermsEnum(TermsEnum tenum)
        {
            super(tenum);
            setInitialSeekTerm(new BytesRef());
        }

        @Override
        @SuppressWarnings("unchecked")
        protected AcceptStatus accept(BytesRef term)
        {
            Token token = tokenMapper.token(term);
            if (includeLower ? token.compareTo(lower) < 0 : token.compareTo(lower) <= 0) {
                return AcceptStatus.NO;
            } else if (includeUpper ? token.compareTo(upper) > 0 : token.compareTo(upper) >= 0) {
                return AcceptStatus.NO;
            } else {
                return AcceptStatus.YES;
            }
        }
    }
}
