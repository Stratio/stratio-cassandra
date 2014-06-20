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
package com.stratio.cassandra.index.query;

import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.FilteredQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

import com.stratio.cassandra.index.schema.Schema;
import com.stratio.cassandra.index.util.JsonSerializer;
import com.stratio.cassandra.index.util.Log;

/**
 * 
 * Class representing an Lucene's index search. It is formed by an optional querying {@link Condition} and an optional
 * filtering {@link Condition}. It can be translated to a Lucene's {@link Query} using a {@link Schema}.
 * 
 * @author Andres de la Pena <adelapena@stratio.com>
 * 
 */
public class Search
{

    /** The querying condition */
    private final Condition queryCondition;

    /** The filtering condition */
    private final Condition filterCondition;

    private final Sorting sorting;

    /**
     * Returns a new {@link Search} composed by the specified querying and filtering conditions.
     * 
     * @param queryCondition
     *            The {@link Condition} for querying, maybe {@code null} meaning no querying.
     * @param filterCondition
     *            The {@link Condition} for filtering, maybe {@code null} meaning no filtering.
     */
    @JsonCreator
    public Search(@JsonProperty("query") Condition queryCondition,
                  @JsonProperty("filter") Condition filterCondition,
                  @JsonProperty("sort") Sorting sorting)
    {
        this.queryCondition = queryCondition;
        this.filterCondition = filterCondition;
        this.sorting = sorting;
    }

    /**
     * Returns {@code true} if the results must be ordered by relevance. If {@code false}, then the results are sorted
     * by the natural Cassandra's order. Results must be ordered by relevance if the querying condition is not {code
     * null}.
     * 
     * Relevance is used when the query condition is set, and it is not used when only the filter condition is set.
     * 
     * @return {@code true} if the results must be ordered by relevance. If {@code false}, then the results must be
     *         sorted by the natural Cassandra's order.
     */
    public boolean usesSorting()
    {
        return queryCondition != null || sorting != null;
    }

    /**
     * Returns the {@link Condition} for querying. Maybe {@code null} meaning no querying.
     * 
     * @return The {@link Condition} for querying. Maybe {@code null} meaning no querying.
     */
    public Condition queryCondition()
    {
        return queryCondition;
    }

    /**
     * Returns the {@link Condition} for filtering. Maybe {@code null} meaning no filtering.
     * 
     * @return The {@link Condition} for filtering. Maybe {@code null} meaning no filtering.
     */
    public Condition filterCondition()
    {
        return filterCondition;
    }
    
    public Filter filter(Schema schema) {
        return filterCondition == null ? null : filterCondition.filter(schema);
    }

    /**
     * Returns the Lucene's {@link Query} representation of this search. This {@link Query} include both the querying
     * and filtering {@link Condition}s. If none of them is set, then a {@link MatchAllDocsQuery} is returned.
     * 
     * @param schema
     *            The {@link Schema} to be used.
     * @return The Lucene's {@link Query} representation of this search.
     */
    public Query query(Schema schema)
    {
        return queryCondition == null ? null : queryCondition.query(schema);
    }

    public Sort sort(Schema schema)
    {
        return sorting == null ? null : sorting.sort(schema);
    }

    /**
     * Returns a new {@link Search} from the specified JSON {@code String}.
     * 
     * @param json
     *            A JSON {@code String} representing a {@link Search}.
     * @return The {@link Search} represented by the specified JSON {@code String}.
     */
    public static Search fromJson(String json)
    {
        try
        {
            Search search = JsonSerializer.fromString(json, Search.class);
            return search;
        }
        catch (Exception e)
        {
            String message = "Unparseable JSON index expression: " + e.getMessage();
            Log.error(e, message);
            throw new IllegalArgumentException(message, e);
        }
    }

    /**
     * Validates this {@link Search} against the specified {@link Schema}.
     * 
     * @param schema
     *            A {@link Schema}.
     */
    public void validate(Schema schema)
    {
        if (queryCondition != null)
        {
            queryCondition.query(schema);
        }
        if (filterCondition != null)
        {
            filterCondition.filter(schema);
        }
        if (sorting != null)
        {
            sorting.sort(schema);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder();
        builder.append("Search [query=");
        builder.append(queryCondition);
        builder.append(", filter=");
        builder.append(filterCondition);
        builder.append("]");
        return builder.toString();
    }

}
