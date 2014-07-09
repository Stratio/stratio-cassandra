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

import java.util.Comparator;

import org.apache.cassandra.db.Row;
import org.apache.cassandra.dht.IPartitioner;

/**
 * A {@link Comparator} for comparing {@link Row}s according to its {@link IPartitioner} order.
 * 
 * @author Andres de la Pena <adelapena@stratio.com>
 * 
 */
public class RowsComparatorScoring implements RowsComparator
{

    private final RowService rowService;

    public RowsComparatorScoring(RowService rowService)
    {
        this.rowService = rowService;
    }

    @Override
    public int compare(Row row1, Row row2)
    {
        Float score1 = rowService.score(row1);
        Float score2 = rowService.score(row2);
        return score2.compareTo(score1);
    }

}
