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
package com.stratio.cassandra.index.schema;

import java.util.Comparator;
import java.util.List;

import com.stratio.cassandra.index.query.SortingField;
import com.stratio.cassandra.index.util.ComparatorChain;

/**
 * A {@link Cells} {@link Comparator} that uses a list of {@link SortingField}s.
 * 
 * @author Andres de la Pena <adelapena@stratio.com>
 * 
 */
public class CellsComparator implements Comparator<Cells>
{
    private final ComparatorChain<Cells> comparatorChain;

    /**
     * Returns a new {@link CellsComparator} for the specified {@link SortingField}s.
     * 
     * @param sortingFields
     *            A list of {@link SortingField}s to be used in the comparison.
     */
    public CellsComparator(List<SortingField> sortingFields)
    {
        comparatorChain = new ComparatorChain<>();
        for (SortingField sortingField : sortingFields)
        {
            Comparator<Cells> comparator = sortingField.comparator();
            comparatorChain.addComparator(comparator);
        }
    }

    @Override
    public int compare(Cells o1, Cells o2)
    {
        return comparatorChain.compare(o1, o2);
    }
}
