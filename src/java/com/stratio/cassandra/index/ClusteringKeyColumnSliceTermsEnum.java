package com.stratio.cassandra.index;

import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.RowPosition;
import org.apache.cassandra.db.composites.CellName;
import org.apache.cassandra.db.composites.CellNameType;
import org.apache.cassandra.db.composites.Composite;
import org.apache.cassandra.db.filter.ColumnSlice;
import org.apache.cassandra.db.filter.SliceQueryFilter;
import org.apache.cassandra.dht.Token;
import org.apache.lucene.index.FilteredTermsEnum;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;

/**
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class ClusteringKeyColumnSliceTermsEnum extends FilteredTermsEnum
{

    final private BytesRef lowerBytesRef;
    final private SliceQueryFilter sliceQueryFilter;
    final private ClusteringKeyMapper clusteringKeyMapper;

    public ClusteringKeyColumnSliceTermsEnum(TermsEnum tenum, SliceQueryFilter sliceQueryFilter, ClusteringKeyMapper clusteringKeyMapper)
    {
        super(tenum);

        // do a little bit of normalization...
        // open ended range queries should always be inclusive.
//        if (lowerTerm == null) {
        this.lowerBytesRef = new BytesRef();
//            this.includeLower = true;
//        } else {
//            this.lowerBytesRef = lowerTerm;
//            this.includeLower = includeLower;
//        }

//        if (upperTerm == null) {
//            this.includeUpper = true;
//            upperBytesRef = null;
//        } else {
//            this.includeUpper = includeUpper;
//            upperBytesRef = upperTerm;
//        }

        setInitialSeekTerm(lowerBytesRef);

        this.sliceQueryFilter = sliceQueryFilter;
        this.clusteringKeyMapper = clusteringKeyMapper;
    }

    @Override
    protected AcceptStatus accept(BytesRef term)
    {

        CellName clusteringKey = clusteringKeyMapper.cellName(term);

        CellNameType type = clusteringKeyMapper.getType();

        Composite start = sliceQueryFilter.start();
        boolean accepted = false;
        if (!start.isEmpty())
        {
            if (type.compare(start, clusteringKey)  <= 0) {
                return AcceptStatus.NO;
            }
        }
        Composite finish = sliceQueryFilter.finish();
        if (!finish.isEmpty())
        {
            int finishComp = type.compare(finish, clusteringKey);
            if (type.compare(finish, clusteringKey) >= 0) {
                return AcceptStatus.END;
            }
        }
        return AcceptStatus.YES;
    }
}
