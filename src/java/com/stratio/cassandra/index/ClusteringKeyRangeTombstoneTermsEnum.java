package com.stratio.cassandra.index;

import org.apache.cassandra.db.RangeTombstone;
import org.apache.cassandra.db.composites.CellName;
import org.apache.cassandra.db.composites.CellNameType;
import org.apache.cassandra.db.composites.Composite;
import org.apache.cassandra.db.filter.SliceQueryFilter;
import org.apache.lucene.index.FilteredTermsEnum;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;

/**
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class ClusteringKeyRangeTombstoneTermsEnum extends FilteredTermsEnum
{

    private final ClusteringKeyMapper clusteringKeyMapper;
    private final CellNameType type;
    private final Composite min;
    private final Composite max;

    public ClusteringKeyRangeTombstoneTermsEnum(TermsEnum tenum, RangeTombstone rangeTombstone, ClusteringKeyMapper clusteringKeyMapper)
    {
        super(tenum);

        setInitialSeekTerm(new BytesRef());

        this.clusteringKeyMapper = clusteringKeyMapper;
        this.type = clusteringKeyMapper.getType();
        this.min = rangeTombstone.min;
        this.max = rangeTombstone.max;
    }

    @Override
    protected AcceptStatus accept(BytesRef term)
    {
        CellName clusteringKey = clusteringKeyMapper.cellName(term);

        if (min != null && !min.isEmpty() && type.compare(min, clusteringKey) > 0)
        {
            return AcceptStatus.NO;
        }
        if (max != null && !max.isEmpty() && type.compare(max, clusteringKey) < 0)
        {
            return AcceptStatus.NO;
        }
        return AcceptStatus.YES;
    }
}
