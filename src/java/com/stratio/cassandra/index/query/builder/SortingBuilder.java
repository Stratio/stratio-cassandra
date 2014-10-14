/*
 * Copyright 2014, Stratio.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.stratio.cassandra.index.query.builder;

import com.stratio.cassandra.index.query.Sorting;
import com.stratio.cassandra.index.query.SortingField;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * {@link Builder} for building a new {@link Sorting}.
 *
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class SortingBuilder implements Builder<Sorting>
{

    private final List<SortingField> sortingFields;

    /**
     * Creates a new {@link SortingBuilder} for the specified {@link com.stratio.cassandra.index.query.builder.SortingFieldBuilder}.
     *
     * @param sortingFieldBuilders The {@link SortingFieldBuilder}s.
     */
    public SortingBuilder(List<SortingFieldBuilder> sortingFieldBuilders)
    {
        this.sortingFields = new ArrayList<>(sortingFieldBuilders.size());
        for (SortingFieldBuilder sortingFieldBuilder : sortingFieldBuilders)
        {
            sortingFields.add(sortingFieldBuilder.build());
        }
    }

    /**
     * Creates a new {@link SortingBuilder} for the specified {@link com.stratio.cassandra.index.query.builder.SortingFieldBuilder}.
     *
     * @param sortingFieldBuilders The {@link SortingFieldBuilder}s.
     */
    public SortingBuilder(SortingFieldBuilder... sortingFieldBuilders)
    {
        this(Arrays.asList(sortingFieldBuilders));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Sorting build()
    {
        return new Sorting(sortingFields);
    }
}
