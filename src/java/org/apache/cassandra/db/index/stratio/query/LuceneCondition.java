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
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.Version;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.annotate.JsonTypeName;

/**
 * A {@link Condition} implementation that matches documents satisfying a Lucene Query Syntax.
 * 
 * @author Andres de la Pena <adelapen@stratio.com>
 */
@JsonTypeName("lucene")
public class LuceneCondition extends Condition {

	public static final String DEFAULT_FIELD = "lucene";

	/** The field name */
	private final String defaultField;

	/** The field value */
	private final String query;

	/**
	 * Constructor using the field name and the value to be matched.
	 * 
	 * @param boost
	 *            The boost for this query clause. Documents matching this clause will (in addition
	 *            to the normal weightings) have their score multiplied by {@code boost}. If
	 *            {@code null}, then {@link DEFAULT_BOOST} is used as default.
	 * @param defaultField
	 *            the default field name.
	 * @param query
	 *            the Lucene Query Syntax query.
	 */
	@JsonCreator
	public LuceneCondition(@JsonProperty("boost") Float boost,
	                       @JsonProperty("default_field") String defaultField,
	                       @JsonProperty("query") String query) {
		super(boost);

		this.query = query;
		this.defaultField = defaultField == null ? DEFAULT_FIELD : defaultField;
	}

	/**
	 * Returns the default field name.
	 * 
	 * @return the default field name.
	 */
	public String getDefaultField() {
		return defaultField;
	}

	/**
	 * Returns the Lucene Query Syntax query.
	 * 
	 * @return the Lucene Query Syntax query.
	 */
	public String getQuery() {
		return query;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Query query(Schema schema) {

		if (query == null) {
			throw new IllegalArgumentException("Query statement required");
		}
		
		try {
			Analyzer analyzer = schema.analyzer();
			QueryParser queryParser = new QueryParser(Version.LUCENE_46, defaultField, analyzer);
			queryParser.setAllowLeadingWildcard(true);
			queryParser.setLowercaseExpandedTerms(false);
			Query luceneQuery = queryParser.parse(query);
			luceneQuery.setBoost(boost);
			return luceneQuery;
		} catch (ParseException e) {
			throw new RuntimeException("Error while parsing lucene syntax query", e);
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append(getClass().getSimpleName());
		builder.append(" [boost=");
		builder.append(boost);
		builder.append(", defaultField=");
		builder.append(defaultField);
		builder.append(", query=");
		builder.append(query);
		builder.append("]");
		return builder.toString();
	}

}