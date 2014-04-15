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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.cassandra.db.AbstractRangeCommand;
import org.apache.cassandra.db.index.stratio.schema.Schema;
import org.apache.cassandra.db.index.stratio.util.JsonSerializer;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.thrift.IndexExpression;
import org.apache.lucene.search.Filter;
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
	 * @param query
	 *            The query, maybe {@code null} meaning {@link MatchAllDocsQuery}.
	 * @param filter
	 *            The filter, maybe {@code null} meaning no filtering.
	 */
	@JsonCreator
	public Search(@JsonProperty("query") Condition query, @JsonProperty("filter") Condition filter) {
		this.queryCondition = query;
		this.filterCondition = filter;
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
	public boolean relevance() {
		return queryCondition != null;
	}

	/**
	 * Returns the Lucene's {@link Query} representation of this search. This {@link Query} include
	 * both the querying and filtering {@link Condition}s. If none of them is set, then a
	 * {@link MatchAllDocsQuery} is returned.
	 * 
	 * @param schema
	 *            The {@link Schema} to be used.
	 * @return The Lucene's {@link Query} representation of this search.
	 */
	public Query query(Schema schema) {
		return queryCondition == null ? new MatchAllDocsQuery() : queryCondition.query(schema);
	}

	public Filter filter(Schema schema) {
		return filterCondition == null ? null : filterCondition.filter(schema);
	}

	public static Search fromClause(List<IndexExpression> clause, ByteBuffer indexedColumnName) {
		IndexExpression indexExpression = null;
		for (IndexExpression ie : clause) {
			ByteBuffer columnName = ie.column_name;
			if (columnName.equals(indexedColumnName)) {
				indexExpression = ie;
			}
		}
		if (indexExpression == null) {
			return null;
		}
		ByteBuffer columnValue = indexExpression.value;
		String json = UTF8Type.instance.compose(columnValue);
		try {
			Search search = JsonSerializer.fromString(json, Search.class);
			return search;
		} catch (IOException e) {
			throw new IllegalArgumentException("Unparseable JSON index expression");
		} catch (Exception e) {
			throw new IllegalArgumentException(e.getMessage());
		}
	}

	public void validate(Schema schema) {
		query(schema);
		filter(schema);
	}

	public static final Search fromCommand(AbstractRangeCommand command, ByteBuffer indexedColumnName) {
		List<IndexExpression> clause = command.rowFilter;
		return fromClause(clause, indexedColumnName);
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
