package com.stratio.cassandra.index;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.util.AttributeSource;

import java.io.IOException;

/**
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class TokenDataRangeQuery extends MultiTermQuery
{
    private RowRange rowRange;
    private TokenMapperGeneric tokenMapper;


    public TokenDataRangeQuery(String field, RowRange rowRange, TokenMapperGeneric tokenMapper) {
        super(field);
        this.rowRange = rowRange;
        this.tokenMapper = tokenMapper;
    }

    @Override
    protected TermsEnum getTermsEnum(Terms terms, AttributeSource atts) throws IOException {
        TermsEnum tenum = terms.iterator(null);
        return new TokenDataRangeFilteredTermsEnum(tenum, rowRange, tokenMapper);
    }

    @Override
    public String toString(String field)
    {
        return new ToStringBuilder(this)
                .append("rowRange", rowRange)
                .append("tokenMapper", tokenMapper)
                .toString();
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        TokenDataRangeQuery that = (TokenDataRangeQuery) o;

        if (rowRange != null ? !rowRange.equals(that.rowRange) : that.rowRange != null) return false;
        if (tokenMapper != null ? !tokenMapper.equals(that.tokenMapper) : that.tokenMapper != null) return false;

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = super.hashCode();
        result = 31 * result + (rowRange != null ? rowRange.hashCode() : 0);
        result = 31 * result + (tokenMapper != null ? tokenMapper.hashCode() : 0);
        return result;
    }
}
