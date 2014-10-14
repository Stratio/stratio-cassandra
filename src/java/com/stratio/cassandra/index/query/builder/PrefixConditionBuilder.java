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

import com.stratio.cassandra.index.query.PrefixCondition;

/**
 * {@link ConditionBuilder} for building a new {@link com.stratio.cassandra.index.query.PrefixCondition}.
 *
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class PrefixConditionBuilder extends ConditionBuilder<PrefixCondition, PrefixConditionBuilder>
{

    private final String field;
    private final String value;

    /**
     * Creates a new {@link PrefixConditionBuilder}.
     *
     * @param field the name of the field to be matched.
     * @param value the value of the field to be matched.
     */
    protected PrefixConditionBuilder(String field, String value)
    {
        this.field = field;
        this.value = value;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PrefixCondition build()
    {
        return new PrefixCondition(boost, field, value);
    }
}