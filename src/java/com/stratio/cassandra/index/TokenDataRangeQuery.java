package com.stratio.cassandra.index;

import org.apache.cassandra.db.DataRange;
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
    private DataRange dataRange;
    private TokenMapperGeneric tokenMapper;


    public TokenDataRangeQuery(String field, DataRange dataRange, TokenMapperGeneric tokenMapper) {
        super(field);
        this.dataRange = dataRange;
        this.tokenMapper = tokenMapper;
    }

    @Override
    protected TermsEnum getTermsEnum(Terms terms, AttributeSource atts) throws IOException {
//        if (lowerTerm != null && upperTerm != null && lowerTerm.compareTo(upperTerm) > 0) {
//            return TermsEnum.EMPTY;
//        }

        TermsEnum tenum = terms.iterator(null);

//        if ((lowerTerm == null || (includeLower && lowerTerm.length == 0)) && upperTerm == null) {
//            return tenum;
//        }
        return new TokenDataRangeFilteredTermsEnum(tenum, dataRange, tokenMapper);
    }

    @Override
    public String toString(String field)
    {
        return new ToStringBuilder(this)
                .append("dataRange", dataRange)
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

        if (dataRange != null ? !dataRange.equals(that.dataRange) : that.dataRange != null) return false;
        if (tokenMapper != null ? !tokenMapper.equals(that.tokenMapper) : that.tokenMapper != null) return false;

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = super.hashCode();
        result = 31 * result + (dataRange != null ? dataRange.hashCode() : 0);
        result = 31 * result + (tokenMapper != null ? tokenMapper.hashCode() : 0);
        return result;
    }
}
