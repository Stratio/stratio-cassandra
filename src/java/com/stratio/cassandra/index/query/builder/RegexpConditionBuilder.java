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

import com.stratio.cassandra.index.query.RegexpCondition;

/**
 * {@link ConditionBuilder} for building a new {@link com.stratio.cassandra.index.query.RegexpCondition}.
 *
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class RegexpConditionBuilder extends ConditionBuilder<RegexpCondition, RegexpConditionBuilder>
{

    private final String field;
    private final String value;

    /**
     * Creates a new {@link RegexpConditionBuilder} for the specified field and value.
     *
     * @param field the name of the field to be matched.
     * @param value the value of the field to be matched.
     */
    protected RegexpConditionBuilder(String field, String value)
    {

        this.field = field;
        this.value = value;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public RegexpCondition build()
    {
        return new RegexpCondition(boost, field, value);
    }
}
