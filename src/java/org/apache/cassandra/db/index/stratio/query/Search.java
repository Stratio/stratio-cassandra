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

import java.nio.ByteBuffer;
import java.util.List;

import org.apache.cassandra.db.AbstractRangeCommand;
import org.apache.cassandra.db.index.stratio.schema.CellsMapper;
import org.apache.cassandra.db.index.stratio.util.JsonSerializer;
import org.apache.cassandra.db.index.stratio.util.Log;
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
 * {@link Query} using a {@link CellsMapper}.
 * 
 * @author adelapena
 * 
 */
public class Search {

	/** The querying condition */
	private final Condition query;

	/** The filtering condition */
	private final Condition filter;

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
		this.query = queryCondition;
		this.filter = filterCondition;
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
		return query != null;
	}

	/**
	 * Returns the Lucene's {@link Query} representation of this search. This {@link Query} include
	 * both the querying and filtering {@link Condition}s. If none of them is set, then a
	 * {@link MatchAllDocsQuery} is returned.
	 * 
	 * @param cellsMapper
	 *            The {@link CellsMapper} to be used.
	 * @return The Lucene's {@link Query} representation of this search.
	 */
	public Query query(CellsMapper cellsMapper) {
		return query == null ? new MatchAllDocsQuery() : query.query(cellsMapper);
	}

	public Filter filter(CellsMapper cellsMapper) {
		return filter == null ? null : filter.filter(cellsMapper);
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
		} catch (Exception e) {
			Log.error(e, "Error while parsing index expression clause");
			throw new IllegalArgumentException(e);
		}
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
		builder.append(query);
		builder.append(", filter=");
		builder.append(filter);
		builder.append("]");
		return builder.toString();
	}

}
