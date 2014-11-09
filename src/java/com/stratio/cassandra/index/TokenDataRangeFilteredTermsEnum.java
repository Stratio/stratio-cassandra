package com.stratio.cassandra.index;

import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.RowPosition;
import org.apache.cassandra.db.composites.CellName;
import org.apache.cassandra.db.composites.CellNameType;
import org.apache.cassandra.db.composites.Composite;
import org.apache.cassandra.db.filter.ColumnSlice;
import org.apache.cassandra.db.filter.SliceQueryFilter;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Token;
import org.apache.lucene.index.FilteredTermsEnum;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;


/**
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class TokenDataRangeFilteredTermsEnum extends FilteredTermsEnum
{

    final private BytesRef lowerBytesRef;
    final private AbstractBounds<Token> tokenBounds;
    final private TokenMapperGeneric tokenMapper;

    public TokenDataRangeFilteredTermsEnum(TermsEnum tenum, RowRange rowRange, TokenMapperGeneric tokenMapper)
    {
        super(tenum);

        this.tokenBounds = rowRange.getTokenBounds();
        this.tokenMapper = tokenMapper;

        this.lowerBytesRef = tokenMapper.bytesRef(tokenBounds.left);
        setInitialSeekTerm(lowerBytesRef);
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
