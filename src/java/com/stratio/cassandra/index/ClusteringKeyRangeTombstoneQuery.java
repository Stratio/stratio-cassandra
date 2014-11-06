package com.stratio.cassandra.index;

import org.apache.cassandra.db.RangeTombstone;
import org.apache.cassandra.db.filter.SliceQueryFilter;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.util.AttributeSource;

import java.io.IOException;

/**
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class ClusteringKeyRangeTombstoneQuery extends MultiTermQuery
{

    private RangeTombstone rangeTombstone;
    private ClusteringKeyMapper clusteringKeyMapper;


    public ClusteringKeyRangeTombstoneQuery(String field, RangeTombstone rangeTombstone, ClusteringKeyMapper clusteringKeyMapper) {
        super(field);
        this.rangeTombstone = rangeTombstone;
        this.clusteringKeyMapper = clusteringKeyMapper;
    }

    @Override
    protected TermsEnum getTermsEnum(Terms terms, AttributeSource atts) throws IOException {
        TermsEnum tenum = terms.iterator(null);
        return new ClusteringKeyRangeTombstoneTermsEnum(tenum, rangeTombstone, clusteringKeyMapper);
    }

    @Override
    public String toString(String field)
    {
        return new ToStringBuilder(this)
                .append("rangeTombstone", rangeTombstone)
                .append("clusteringKeyMapper", clusteringKeyMapper)
                .toString();
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        ClusteringKeyRangeTombstoneQuery that = (ClusteringKeyRangeTombstoneQuery) o;

        if (rangeTombstone != null ? !rangeTombstone.equals(that.rangeTombstone) : that.rangeTombstone != null) return false;
        if (clusteringKeyMapper != null ? !clusteringKeyMapper.equals(that.clusteringKeyMapper) : that.clusteringKeyMapper != null) return false;

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = super.hashCode();
        result = 31 * result + (rangeTombstone != null ? rangeTombstone.hashCode() : 0);
        result = 31 * result + (clusteringKeyMapper != null ? clusteringKeyMapper.hashCode() : 0);
        return result;
    }
}

