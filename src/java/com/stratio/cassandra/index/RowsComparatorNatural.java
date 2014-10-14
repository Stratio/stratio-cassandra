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

import com.stratio.cassandra.index.util.ComparatorChain;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.Cell;
import org.apache.cassandra.db.Row;
import org.apache.cassandra.db.composites.CellName;
import org.apache.cassandra.db.composites.CellNameType;
import org.apache.cassandra.dht.Token;

import java.util.Comparator;
import java.util.Iterator;

/**
 * A {@link Comparator} for comparing {@link Row}s according to its Cassandra's natural order.
 *
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class RowsComparatorNatural implements RowsComparator
{

    private final CellNameType nameType;

    private final ComparatorChain<Row> comparatorChain;

    public RowsComparatorNatural(CFMetaData metadata)
    {
        super();
        nameType = metadata.comparator;
        comparatorChain = new ComparatorChain<>();
        comparatorChain.addComparator(new Comparator<Row>()
        {
            @Override
            @SuppressWarnings({"unchecked", "rawtypes"})
            public int compare(Row row1, Row row2)
            {
                Token t1 = row1.key.getToken();
                Token t2 = row2.key.getToken();
                return t1.compareTo(t2);
            }
        });
        comparatorChain.addComparator(new Comparator<Row>()
        {
            @Override
            public int compare(Row row1, Row row2)
            {
                Iterator<Cell> i1 = row1.cf.iterator();
                Iterator<Cell> i2 = row2.cf.iterator();
                CellName name1 = i1.hasNext() ? i1.next().name() : null;
                CellName name2 = i2.hasNext() ? i2.next().name() : null;
                return nameType.compare(name1, name2);
            }
        });
    }

    @Override
    public int compare(Row row1, Row row2)
    {
        return comparatorChain.compare(row1, row2);
    }

}
