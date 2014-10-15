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

import com.stratio.cassandra.index.query.SortingField;

/**
 * {@link Builder} for building a new {@link SortingField}.
 *
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class SortingFieldBuilder implements Builder<SortingField>
{
    private final String field;
    private final boolean reverse;

    /**
     * Creates a new {@link SortingFieldBuilder} for the specified field and reverse option.
     *
     * @param field   the name of the field to be sorted.
     * @param reverse if the sorting is reverse.
     */
    public SortingFieldBuilder(String field, boolean reverse)
    {
        this.field = field;
        this.reverse = reverse;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public SortingField build()
    {
        return new SortingField(field, reverse);
    }
}
