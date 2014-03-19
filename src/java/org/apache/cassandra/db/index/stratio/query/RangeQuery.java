package org.apache.cassandra.db.index.stratio.query;

import org.apache.lucene.analysis.Analyzer;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.annotate.JsonTypeName;

/**
 * A {@link AbstractQuery} implementation that matches a field within an range of values.
 * 
 * @version 0.1
 * @author adelapena
 */
@JsonTypeName("range")
public class RangeQuery extends AbstractQuery {

	/** The field name. */
	@JsonProperty("field")
	private final String field;

	/** The lower field value included in the range. */
	@JsonProperty("lower")
	private Object lower;

	/** The upper field value included in the range. */
	@JsonProperty("upper")
	private Object upper;

	/** If the lower value is included in the range. */
	@JsonProperty("include_lower")
	private final boolean includeLower;

	/** If the upper value is included in the range. */
	@JsonProperty("include_upper")
	private final boolean includeUpper;

	/**
	 * Constructs a query selecting all fields greater/equal than {@code lowerValue} but less/equal
	 * than {@code upperValue}.
	 * 
	 * If an endpoint is null, it is said to be "open". Either or both endpoints may be open. Open
	 * endpoints may not be exclusive (you can't select all but the first or last term without
	 * explicitly specifying the term to exclude.)
	 * 
	 * @param boost
	 *            The boost for this query clause. Documents matching this clause will (in addition
	 *            to the normal weightings) have their score multiplied by {@code boost}.
	 * @param field
	 *            the field name.
	 * @param lowerValue
	 *            the field value at the lower end of the range.
	 * @param upperValue
	 *            the field value at the upper end of the range.
	 * @param includeLower
	 *            if {@code true}, the {@code lowerValue} is included in the range.
	 * @param includeUpper
	 *            if {@code true}, the {@code upperValue} is included in the range.
	 */
	@JsonCreator
	public RangeQuery(@JsonProperty("boost") Float boost,
	                  @JsonProperty("field") String field,
	                  @JsonProperty("lower") Object lowerValue,
	                  @JsonProperty("upper") Object upperValue,
	                  @JsonProperty("include_lower") boolean includeLower,
	                  @JsonProperty("include_upper") boolean includeUpper) {
		super(boost);
		this.field = field;
		this.lower = lowerValue;
		this.upper = upperValue;
		this.includeLower = includeLower;
		this.includeUpper = includeUpper;
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
	 * Returns the field value at the lower end of the range.
	 * 
	 * @return the field value at the lower end of the range.
	 */
	public Object getLowerValue() {
		return lower;
	}

	/**
	 * Returns the field value at the upper end of the range.
	 * 
	 * @return the field value at the upper end of the range.
	 */
	public Object getUpperValue() {
		return upper;
	}

	/**
	 * Returns {@code true} if the {@code lowerValue} is included in the range, {@code false}
	 * otherwise.
	 * 
	 * @return {@code true} if the {@code lowerValue} is included in the range, {@code false}
	 *         otherwise.
	 */
	public boolean getIncludeLower() {
		return includeLower;
	}

	/**
	 * Returns {@code true} if the {@code includeUpper} is included in the range, {@code false}
	 * otherwise.
	 * 
	 * @return {@code true} if the {@code includeUpper} is included in the range, {@code false}
	 *         otherwise.
	 */
	public boolean getIncludeUpper() {
		return includeUpper;
	}

	@Override
	public void analyze(Analyzer analyzer) {
		this.lower = analyze(field, lower, analyzer);
		this.upper = analyze(field, upper, analyzer);
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("RangeQuery [boost=");
		builder.append(boost);
		builder.append(", field=");
		builder.append(field);
		builder.append(", lowerValue=");
		builder.append(lower);
		builder.append(", upperValue=");
		builder.append(upper);
		builder.append(", includeLower=");
		builder.append(includeLower);
		builder.append(", includeUpper=");
		builder.append(includeUpper);
		builder.append("]");
		return builder.toString();
	}

}