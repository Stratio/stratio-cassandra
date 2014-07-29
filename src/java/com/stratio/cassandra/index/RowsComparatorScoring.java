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
import java.util.HashMap;
import java.util.Map;

import org.apache.cassandra.db.DecoratedKey;
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
    private final Map<DecoratedKey, Float> scoresCache;

    public RowsComparatorScoring(RowService rowService)
    {
        this.rowService = rowService;
        scoresCache = new HashMap<>();
    }

    @Override
    public int compare(Row row1, Row row2)
    {
        Float score1 = scoresCache.get(row1.key);
        if (score1 == null) {
            score1 = rowService.score(row1);
            scoresCache.put(row1.key, score1);
        }
        Float score2 = scoresCache.get(row2.key);
        if (score2 == null) {
            score2 = rowService.score(row2);
            scoresCache.put(row2.key, score2);
        }
        return score2.compareTo(score1);
    }

}
