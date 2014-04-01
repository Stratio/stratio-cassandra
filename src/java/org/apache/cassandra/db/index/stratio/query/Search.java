package org.apache.cassandra.db.index.stratio.query;

import java.io.IOException;

import org.apache.cassandra.db.index.stratio.schema.CellsMapper;
import org.apache.cassandra.db.index.stratio.util.JsonSerializer;
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

	@JsonProperty("relevance")
	private final boolean relevance;

	@JsonProperty("query")
	private final Condition queryCondition;

	@JsonProperty("filter")
	private final Condition filterCondition;

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

	/**
	 * Returns the Lucene's {@link Query} representation of this search.
	 * 
	 * @param cellsMapper
	 *            The {@link CellsMapper} to be used.
	 * @return The Lucene's {@link Query} representation of this search.
	 */
	public Query query(CellsMapper cellsMapper) {
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
	public void analyze(Analyzer analyzer) {
		if (queryCondition != null) {
			queryCondition.analyze(analyzer);
		}
		if (filterCondition != null) {
			filterCondition.analyze(analyzer);
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
