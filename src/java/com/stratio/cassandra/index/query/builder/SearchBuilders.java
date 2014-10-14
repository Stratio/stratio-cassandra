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

import java.util.List;

/**
 * Factory for {@link com.stratio.cassandra.index.query.builder.SearchBuilder} and {@link com.stratio.cassandra.index.query.builder.ConditionBuilder}s.
 *
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class SearchBuilders
{

    /**
     * Returns a new {@link SearchBuilder}.
     *
     * @return a new {@link SearchBuilder}.
     */
    public static SearchBuilder search()
    {
        return new SearchBuilder();
    }

    /**
     * Returns a new {@link SearchBuilder} using the specified {@link ConditionBuilder} as query.
     *
     * @return a new {@link SearchBuilder} using the specified {@link ConditionBuilder} as query.
     */
    public static SearchBuilder query(ConditionBuilder<?, ?> queryConditionBuilder)
    {
        return search().query(queryConditionBuilder);
    }

    /**
     * Returns a new {@link SearchBuilder} using the specified {@link ConditionBuilder} as filter.
     *
     * @return a new {@link SearchBuilder} using the specified {@link ConditionBuilder} as filter.
     */
    public static SearchBuilder filter(ConditionBuilder<?, ?> filterConditionBuilder)
    {
        return search().filter(filterConditionBuilder);
    }

    /**
     * Returns a new {@link SearchBuilder} using the specified {@link SortingBuilder} as sorting.
     *
     * @return a new {@link SearchBuilder} using the specified {@link SortingBuilder} as sorting.
     */
    public static SearchBuilder sorting(SortingBuilder sortingBuilder)
    {
        return search().sorting(sortingBuilder);
    }

    /**
     * Returns a new {@link BooleanConditionBuilder}.
     */
    public static BooleanConditionBuilder bool()
    {
        return new BooleanConditionBuilder();
    }

    /**
     * Returns a new {@link FuzzyConditionBuilder} for the specified field and value.
     *
     * @param field the name of the field to be matched.
     * @param value the value of the field to be matched.
     */
    public static FuzzyConditionBuilder fuzzy(String field, String value)
    {
        return new FuzzyConditionBuilder(field, value);
    }

    /**
     * Returns a new {@link LuceneConditionBuilder} with the specified query.
     *
     * @param query the Lucene syntax query.
     */
    public static LuceneConditionBuilder lucene(String query)
    {
        return new LuceneConditionBuilder(query);
    }

    /**
     * Returns a new {@link MatchConditionBuilder} for the specified field and value.
     *
     * @param field the name of the field to be matched.
     * @param value the value of the field to be matched.
     */
    public static MatchConditionBuilder match(String field, Object value)
    {
        return new MatchConditionBuilder(field, value);
    }

    /**
     * Returns a new {@link PhraseConditionBuilder} for the specified field and values.
     *
     * @param field  the name of the field to be matched.
     * @param values the values of the field to be matched.
     */
    public static PhraseConditionBuilder phrase(String field, String... values)
    {
        return new PhraseConditionBuilder(field, values);
    }

    /**
     * Returns a new {@link PhraseConditionBuilder} for the specified field and values.
     *
     * @param field  the name of the field to be matched.
     * @param values the values of the field to be matched.
     */
    public static PhraseConditionBuilder phrase(String field, List<String> values)
    {
        return new PhraseConditionBuilder(field, values);
    }

    /**
     * Returns a new {@link PrefixConditionBuilder} for the specified field and value.
     *
     * @param field the name of the field to be matched.
     * @param value the value of the field to be matched.
     */
    public static PrefixConditionBuilder prefix(String field, String value)
    {
        return new PrefixConditionBuilder(field, value);
    }

    /**
     * Returns a new {@link RangeConditionBuilder} for the specified field.
     *
     * @param field the name of the field to be matched.
     */
    public static RangeConditionBuilder range(String field)
    {
        return new RangeConditionBuilder(field);
    }

    /**
     * Returns a new {@link RegexpConditionBuilder} for the specified field and value.
     *
     * @param field the name of the field to be matched.
     * @param value the value of the field to be matched.
     */
    public static RegexpConditionBuilder regexp(String field, String value)
    {
        return new RegexpConditionBuilder(field, value);
    }

    /**
     * Returns a new {@link WildcardConditionBuilder} for the specified field and value.
     *
     * @param field the name of the field to be matched.
     * @param value the value of the field to be matched.
     */
    public static WildcardConditionBuilder wildcard(String field, String value)
    {
        return new WildcardConditionBuilder(field, value);
    }
}
