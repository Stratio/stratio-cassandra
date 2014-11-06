package com.stratio.cassandra.index;

import java.io.IOException;

import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.filter.ColumnSlice;
import org.apache.cassandra.db.filter.SliceQueryFilter;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.ToStringUtils;

/**
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class ClusteringKeyColumnSliceQuery extends MultiTermQuery
{

    private SliceQueryFilter sliceQueryFilter;
    private ClusteringKeyMapper clusteringKeyMapper;


    public ClusteringKeyColumnSliceQuery(String field, SliceQueryFilter sliceQueryFilter, ClusteringKeyMapper clusteringKeyMapper) {
        super(field);
        this.sliceQueryFilter = sliceQueryFilter;
        this.clusteringKeyMapper = clusteringKeyMapper;
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
        return new ClusteringKeyColumnSliceTermsEnum(tenum, sliceQueryFilter, clusteringKeyMapper);
    }

    @Override
    public String toString(String field)
    {
        return new ToStringBuilder(this)
                .append("sliceQueryFilter", sliceQueryFilter)
                .append("clusteringKeyMapper", clusteringKeyMapper)
                .toString();
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        ClusteringKeyColumnSliceQuery that = (ClusteringKeyColumnSliceQuery) o;

        if (sliceQueryFilter != null ? !sliceQueryFilter.equals(that.sliceQueryFilter) : that.sliceQueryFilter != null) return false;
        if (clusteringKeyMapper != null ? !clusteringKeyMapper.equals(that.clusteringKeyMapper) : that.clusteringKeyMapper != null) return false;

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = super.hashCode();
        result = 31 * result + (sliceQueryFilter != null ? sliceQueryFilter.hashCode() : 0);
        result = 31 * result + (clusteringKeyMapper != null ? clusteringKeyMapper.hashCode() : 0);
        return result;
    }
}

