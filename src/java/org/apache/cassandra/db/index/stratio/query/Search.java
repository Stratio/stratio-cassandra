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
package org.apache.cassandra.db.index.stratio.query;

import org.apache.cassandra.db.index.stratio.schema.Schema;
import org.apache.cassandra.db.index.stratio.util.JsonSerializer;
import org.apache.cassandra.db.index.stratio.util.Log;
import org.apache.lucene.queries.ChainedFilter;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.FilteredQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 * 
 * Class representing an Lucene's index search. It is formed by an optional querying
 * {@link Condition} and an optional filtering {@link Condition}. It can be translated to a Lucene's
 * {@link Query} using a {@link Schema}.
 * 
 * @author adelapena
 * 
 */
public class Search {

	/** The querying condition */
	private final Condition queryCondition;

	/** The filtering condition */
	private final Condition filterCondition;

	/**
	 * Returns a new {@link Search} composed by the specified querying and filtering conditions.
	 * 
	 * @param queryCondition
	 *            The query, maybe {@code null} meaning {@link MatchAllDocsQuery}.
	 * @param filterCondition
	 *            The filter, maybe {@code null} meaning no filtering.
	 */
	@JsonCreator
	public Search(@JsonProperty("query") Condition queryCondition, @JsonProperty("filter") Condition filterCondition) {
		this.queryCondition = queryCondition;
		this.filterCondition = filterCondition;
	}

	/**
	 * Returns {@code true} if the results must be ordered by relevance. If {@code false}, then the
	 * results are sorted by the natural Cassandra's order.
	 * 
	 * Relevance is used when the query condition is set, and it is not used when only the filter
	 * condition is set.
	 * 
	 * @return {@code true} if the results must be ordered by relevance. If {@code false}, then the
	 *         results are sorted by the natural Cassandra's order.
	 */
	public boolean usesRelevance() {
		return queryCondition != null;
	}

	/**
	 * Returns the Lucene's {@link Query} representation of this search. This {@link Query} include
	 * both the querying and filtering {@link Condition}s. If none of them is set, then a
	 * {@link MatchAllDocsQuery} is returned.
	 * 
	 * @param schema
	 *            The {@link Schema} to be used.
	 * @param extraFilter
	 *            An extra {@link Filter} to be added to the search.
	 * @return The Lucene's {@link Query} representation of this search.
	 */
	public Query query(Schema schema, Filter extraFilter) {
		Query query = queryCondition == null ? new MatchAllDocsQuery() : queryCondition.query(schema);
		if (filterCondition == null && extraFilter == null) {
			return query;
		} else if (filterCondition == null && extraFilter != null) {
			return new FilteredQuery(query, extraFilter);
		} else if (filterCondition != null && extraFilter == null) {
			return new FilteredQuery(query, filterCondition.filter(schema));
		} else {
			Filter[] filters = new Filter[] { extraFilter, filterCondition.filter(schema) };
			Filter filter = new ChainedFilter(filters, ChainedFilter.AND);
			return new FilteredQuery(query, filter);
		}
	}

	/**
	 * Returns a new {@link Search} from the specified JSON {@code String}.
	 * 
	 * @param json
	 *            A JSON {@code String} representing a {@link Search}.
	 * @return The {@link Search} represented by the specified JSON {@code String}.
	 */
	public static Search fromJson(String json) {
		try {
			Search search = JsonSerializer.fromString(json, Search.class);
			Log.debug("Parsed %s", search);
			return search;
		} catch (Exception e) {
			String message = "Unparseable JSON index expression: " + e.getMessage();
			Log.error(e, message);
			throw new IllegalArgumentException(message);
		}
	}

	/**
	 * Validates this {@link Search} against the specified {@link Schema}.
	 * 
	 * @param schema
	 *            A {@link Schema}.
	 */
	public void validate(Schema schema) {
		if (queryCondition != null) {
			queryCondition.query(schema);
		}
		if (filterCondition != null) {
			filterCondition.filter(schema);
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("Search [query=");
		builder.append(queryCondition);
		builder.append(", filter=");
		builder.append(filterCondition);
		builder.append("]");
		return builder.toString();
	}

}
