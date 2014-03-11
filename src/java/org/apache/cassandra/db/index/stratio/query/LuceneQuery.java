package org.apache.cassandra.db.index.stratio.query;

import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.annotate.JsonTypeName;

@JsonTypeName("lucene")
public class LuceneQuery extends AbstractQuery {

	@JsonProperty("query")
	private final String query;

	/**
	 * @param query
	 */
	@JsonCreator
	public LuceneQuery(@JsonProperty("query") String query) {
		this.query = query;
	}

	public String getQuery() {
		return query;
	}

	@Override
	public String toString() {
		return "LuceneQuery [query=" + query + "]";
	}

}
