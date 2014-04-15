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

import org.apache.cassandra.db.index.stratio.schema.CellMapper;
import org.apache.cassandra.db.index.stratio.schema.Schema;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.automaton.LevenshteinAutomata;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.annotate.JsonTypeName;

/**
 * A {@link Condition} that implements the fuzzy search query. The similarity measurement is based
 * on the Damerau-Levenshtein (optimal string alignment) algorithm, though you can explicitly choose
 * classic Levenshtein by passing {@code false} to the {@code transpositions} parameter.
 * 
 * @author adelapena
 * 
 */
@JsonTypeName("fuzzy")
public class FuzzyCondition extends Condition {

	public final static int DEFAULT_MAX_EDITS = LevenshteinAutomata.MAXIMUM_SUPPORTED_DISTANCE;
	public final static int DEFAULT_PREFIX_LENGTH = 0;
	public final static int DEFAULT_MAX_EXPANSIONS = 50;
	public final static boolean DEFAULT_TRANSPOSITIONS = true;

	/** The field name */
	@JsonProperty("field")
	private final String field;

	/** The field value */
	@JsonProperty("value")
	private Object value;

	@JsonProperty("max_edits")
	private final int maxEdits;

	@JsonProperty("prefix_length")
	private final int prefixLength;

	@JsonProperty("max_expansions")
	private final int maxExpansions;

	@JsonProperty("transpositions")
	private final boolean transpositions;

	/**
	 * Returns a new {@link FuzzyCondition}.
	 * 
	 * @param boost
	 *            The boost for this query clause. Documents matching this clause will (in addition
	 *            to the normal weightings) have their score multiplied by {@code boost}. If
	 *            {@code null}, then {@link DEFAULT_BOOST} is used as default.
	 * @param field
	 *            The field name.
	 * @param value
	 *            The field fuzzy value.
	 * @param maxEdits
	 *            Must be >= 0 and <= {@link LevenshteinAutomata#MAXIMUM_SUPPORTED_DISTANCE}.
	 * @param prefixLength
	 *            Length of common (non-fuzzy) prefix
	 * @param maxExpansions
	 *            The maximum number of terms to match. If this number is greater than
	 *            {@link BooleanQuery#getMaxClauseCount} when the query is rewritten, then the
	 *            maxClauseCount will be used instead.
	 * @param transpositions
	 *            True if transpositions should be treated as a primitive edit operation. If this is
	 *            false, comparisons will implement the classic Levenshtein algorithm.
	 */
	@JsonCreator
	public FuzzyCondition(@JsonProperty("boost") Float boost,
	                      @JsonProperty("field") String field,
	                      @JsonProperty("value") Object value,
	                      @JsonProperty("max_edits") Integer maxEdits,
	                      @JsonProperty("prefix_length") Integer prefixLength,
	                      @JsonProperty("max_expansions") Integer maxExpansions,
	                      @JsonProperty("transpositions") Boolean transpositions) {
		super(boost);

		if (field == null || field.trim().isEmpty()) {
			throw new IllegalArgumentException("Field name required");
		}

		this.field = field;
		this.value = value;
		this.maxEdits = maxEdits == null ? DEFAULT_MAX_EDITS : maxEdits;
		this.prefixLength = prefixLength == null ? DEFAULT_PREFIX_LENGTH : prefixLength;
		this.maxExpansions = maxExpansions == null ? DEFAULT_MAX_EXPANSIONS : maxExpansions;
		this.transpositions = transpositions == null ? DEFAULT_TRANSPOSITIONS : transpositions;
	}

	/**
	 * Returns the field name.
	 * 
	 * @return the field name.
	 */
	public String getField() {
		return field;
	}

	/**
	 * Returns the field value.
	 * 
	 * @return the field value.
	 */
	public Object getValue() {
		return value;
	}

	/**
	 * Returns the Damerau-Levenshtein max distance.
	 * 
	 * @return The Damerau-Levenshtein max distance.
	 */
	public int getMaxEdits() {
		return maxEdits;
	}

	/**
	 * Returns the length of common (non-fuzzy) prefix.
	 * 
	 * @return The length of common (non-fuzzy) prefix.
	 */
	public int getPrefixLength() {
		return prefixLength;
	}

	/**
	 * Returns the maximum number of terms to match.
	 * 
	 * @return The maximum number of terms to match.
	 */
	public int getMaxExpansions() {
		return maxExpansions;
	}

	/**
	 * Returns if transpositions should be treated as a primitive edit operation.
	 * 
	 * @return If transpositions should be treated as a primitive edit operation.
	 */
	public boolean getTranspositions() {
		return transpositions;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Query query(Schema schema) {
		CellMapper<?> cellMapper = schema.getMapper(field);
		Class<?> clazz = cellMapper.baseClass();
		if (clazz == String.class) {
			String value = (String) cellMapper.queryValue(this.value);
			value = analyze(field, value, schema.analyzer());
			Term term = new Term(field, value);
			Query query = new FuzzyQuery(term, maxEdits, prefixLength, maxExpansions, transpositions);
			query.setBoost(boost);
			return query;
		} else {
			String message = String.format("Fuzzy queries are not supported by %s mapper", clazz.getSimpleName());
			throw new UnsupportedOperationException(message);
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
		builder.append(", field=");
		builder.append(field);
		builder.append(", value=");
		builder.append(value);
		builder.append(", maxEdits=");
		builder.append(maxEdits);
		builder.append(", prefixLength=");
		builder.append(prefixLength);
		builder.append(", maxExpansions=");
		builder.append(maxExpansions);
		builder.append(", transpositions=");
		builder.append(transpositions);
		builder.append("]");
		return builder.toString();
	}

}