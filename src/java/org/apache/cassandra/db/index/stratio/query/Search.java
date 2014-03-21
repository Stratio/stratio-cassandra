package org.apache.cassandra.db.index.stratio.query;

import java.io.IOException;

import org.apache.cassandra.db.index.stratio.schema.CellsMapper;
import org.apache.cassandra.db.index.stratio.util.JsonSerializer;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.FilteredQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryWrapperFilter;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 * 
 * @author adelapena
 * 
 */
public class Search {

	@JsonProperty("query")
	private final AbstractQuery query;

	@JsonProperty("filter")
	private final AbstractQuery filter;

	@JsonCreator
	public Search(@JsonProperty("query") AbstractQuery query, @JsonProperty("filter") AbstractQuery filter) {
		this.query = query;
		this.filter = filter;
	}

	public Query toLucene(CellsMapper cellsMapper) {
		if (query != null && filter != null) {
			Query query = this.query.toLucene(cellsMapper);
			Filter filter = new QueryWrapperFilter(this.filter.toLucene(cellsMapper));
			return new FilteredQuery(query, filter);
		} else if (query == null) {
			Filter filter = new QueryWrapperFilter(this.filter.toLucene(cellsMapper));
			return new FilteredQuery(new MatchAllDocsQuery(), filter, FilteredQuery.LEAP_FROG_FILTER_FIRST_STRATEGY);
		} else if (filter == null) {
			return this.query.toLucene(cellsMapper);
		} else {
			throw new IllegalArgumentException("Query and/or filter required");
		}
	}

	public AbstractQuery getQuery() {
		return query;
	}

	public AbstractQuery getFilter() {
		return filter;
	}

	/**
	 * Applies the specified {@link Analyzer} to the required arguments.
	 * 
	 * @param analyzer
	 *            An {@link Analyzer}.
	 */
	public void analyze(Analyzer analyzer) {
		if (query != null) {
			query.analyze(analyzer);
		}
		if (filter != null) {
			filter.analyze(analyzer);
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

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("Search [query=");
		builder.append(query);
		builder.append("]");
		return builder.toString();
	}

}
