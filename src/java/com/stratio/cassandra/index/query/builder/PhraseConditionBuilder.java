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
package com.stratio.cassandra.index.query.builder;

import com.stratio.cassandra.index.query.PhraseCondition;

import java.util.Arrays;
import java.util.List;

/**
 * {@link ConditionBuilder} for building a new {@link com.stratio.cassandra.index.query.PhraseCondition}.
 *
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class PhraseConditionBuilder extends ConditionBuilder<PhraseCondition, PhraseConditionBuilder>
{

    private final String field;
    private final List<String> values;
    private Integer slop;

    /**
     * Returns a new {@link PhraseConditionBuilder} with the specified field name and values to be mathced.
     *
     * @param field  the name of the field.
     * @param values the values to be matched.
     */
    protected PhraseConditionBuilder(String field, List<String> values)
    {
        this.field = field;
        this.values = values;
    }

    /**
     * Returns a new {@link PhraseConditionBuilder} with the specified field name and values to be mathced.
     *
     * @param field  the name of the field.
     * @param values the values to be matched.
     */
    public PhraseConditionBuilder(String field, String... values)
    {
        this.field = field;
        this.values = Arrays.asList(values);
    }

    /**
     * Returns this builder with the specified slop.
     *
     * @param slop the slop to be set.
     * @return this builder with the specified slop.
     */
    public PhraseConditionBuilder slop(Integer slop)
    {
        this.slop = slop;
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PhraseCondition build()
    {
        return new PhraseCondition(boost, field, values, slop);
    }
}
