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

import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.index.stratio.schema.CellsMapper;
import org.apache.cassandra.db.index.stratio.util.JsonSerializer;
import org.apache.cassandra.db.index.stratio.util.Log;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.thrift.IndexExpression;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.FilteredQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 * 
 * @author adelapena
 * 
 */
public class Search {

	public static final boolean DEFAULT_RELEVANCE = false;

	private final boolean relevance;

	private final Condition queryCondition;

	private final Condition filterCondition;

	private DataRange dataRange;

	@JsonCreator
	public Search(@JsonProperty("relevance") boolean relevance,
	              @JsonProperty("query") Condition queryCondition,
	              @JsonProperty("filter") Condition filterCondition) {
		this.relevance = relevance;
		this.queryCondition = queryCondition;
		this.filterCondition = filterCondition;
	}

	/**
	 * Returns {@code true} if the results must be ordered by relevance. If {@code false}, then the
	 * results are sorted by the natural Cassandra's order.
	 * 
	 * @return {@code true} if the results must be ordered by relevance. If {@code false}, then the
	 *         results are sorted by the natural Cassandra's order.
	 */
	public boolean relevance() {
		return relevance;
	}

	public DataRange dataRange() {
		return dataRange;
	}

	/**
	 * Returns the Lucene's {@link Query} representation of this search.
	 * 
	 * @param cellsMapper
	 *            The {@link CellsMapper} to be used.
	 * @return The Lucene's {@link Query} representation of this search.
	 */
	public Query query(CellsMapper cellsMapper) {
		analyze(cellsMapper.analyzer());
		Query query = (queryCondition == null) ? new MatchAllDocsQuery() : queryCondition.query(cellsMapper);
		Filter filter = (filterCondition == null) ? null : filterCondition.filter(cellsMapper);
		if (filter != null) {
			return new FilteredQuery(query, filter);
		} else {
			return query;
		}
	}

	/**
	 * Applies the specified {@link Analyzer} to the required arguments.
	 * 
	 * @param analyzer
	 *            An {@link Analyzer}.
	 */
	private void analyze(Analyzer analyzer) {
		if (queryCondition != null) {
			queryCondition.analyze(analyzer);
		}
		if (filterCondition != null) {
			filterCondition.analyze(analyzer);
		}
	}

	public static Search fromClause(List<IndexExpression> clause, ByteBuffer name) {
		IndexExpression indexExpression = null;
		for (IndexExpression ie : clause) {
			ByteBuffer columnName = ie.column_name;
			if (columnName.equals(name)) {
				indexExpression = ie;
			}
		}
		if (indexExpression == null) {
			return null;
		}
		ByteBuffer columnValue = indexExpression.value;
		String querySentence = UTF8Type.instance.compose(columnValue);
		try {
			Search search = Search.fromJSON(querySentence);
			return search;
		} catch (Exception e) {
			Log.error(e, e.getMessage());
			return null;
		}
	}

	/**
	 * Returns the {@link Search} represented by the specified JSON.
	 * 
	 * @param json
	 *            the JSON to be parsed.
	 * @return the {@link Search} represented by the specified JSON.
	 */
	public static Search fromJSON(String json) throws IOException {
		return JsonSerializer.fromString(json, Search.class);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("Search [queryCondition=");
		builder.append(queryCondition);
		builder.append(", filterCondition=");
		builder.append(filterCondition);
		builder.append("]");
		return builder.toString();
	}

}
