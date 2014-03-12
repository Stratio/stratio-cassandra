package org.apache.cassandra.db.index.stratio.query;

import org.apache.lucene.util.automaton.LevenshteinAutomata;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.annotate.JsonTypeName;

/**
 * 
 * @author adelapena
 * 
 */
@JsonTypeName("fuzzy")
public class FuzzyQuery extends AbstractQuery {

	public final static int DEFAULT_MAX_EDITS = LevenshteinAutomata.MAXIMUM_SUPPORTED_DISTANCE;
	public final static int DEFAULT_PREFIX_LENGTH = 0;
	public final static int DEFAULT_MAX_EXPANSIONS = 50;
	public final static boolean DEFAULT_TRANSPOSITIONS = true;

	/** The field name */
	@JsonProperty("field")
	private final String field;

	/** The field value */
	@JsonProperty("value")
	private final Object value;

	@JsonProperty("max_edits")
	private final int maxEdits;

	@JsonProperty("prefix_length")
	private final int prefixLength;

	@JsonProperty("max_expansions")
	private final int maxExpansions;

	@JsonProperty("transpositions")
	private final boolean transpositions;

	@JsonCreator
	public FuzzyQuery(@JsonProperty("boost") Float boost,
	                  @JsonProperty("field") String field,
	                  @JsonProperty("value") Object value,
	                  @JsonProperty("max_edits") Integer maxEdits,
	                  @JsonProperty("prefix_length") Integer prefixLength,
	                  @JsonProperty("max_expansions") Integer maxExpansions,
	                  @JsonProperty("transpositions") Boolean transpositions) {
		super(boost);
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

	public int getMaxEdits() {
		return maxEdits;
	}

	public int getPrefixLength() {
		return prefixLength;
	}

	public int getMaxExpansions() {
		return maxExpansions;
	}

	public boolean getTranspositions() {
		return transpositions;
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("FuzzyQuery [boost=");
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
