package org.apache.cassandra.db.index.stratio.query;

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
	private final Object lowerValue;

	/** The upper field value included in the range. */
	@JsonProperty("upper")
	private final Object upperValue;

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
	public RangeQuery(@JsonProperty("field") String field,
	                  @JsonProperty("lower") Object lowerValue,
	                  @JsonProperty("upper") Object upperValue,
	                  @JsonProperty("include_lower") boolean includeLower,
	                  @JsonProperty("include_upper") boolean includeUpper) {

		this.field = field;
		this.lowerValue = lowerValue;
		this.upperValue = upperValue;
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
		return lowerValue;
	}

	/**
	 * Returns the field value at the upper end of the range.
	 * 
	 * @return the field value at the upper end of the range.
	 */
	public Object getUpperValue() {
		return upperValue;
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

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("RangeQuery [field=");
		builder.append(field);
		builder.append(", lowerValue=");
		builder.append(lowerValue);
		builder.append(", upperValue=");
		builder.append(upperValue);
		builder.append(", includeLower=");
		builder.append(includeLower);
		builder.append(", includeUpper=");
		builder.append(includeUpper);
		builder.append("]");
		return builder.toString();
	}

}
