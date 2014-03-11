package org.apache.cassandra.db.index.stratio.query;

import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.annotate.JsonTypeName;

/**
 * A {@link AbstractQuery} implementation that matches documents containing a value for a field.
 * 
 * @version 0.1
 * @author adelapena
 */
@JsonTypeName("match")
public class MatchQuery extends AbstractQuery {

	/** The field name */
	@JsonProperty("field")
	private final String field;

	/** The field value */
	@JsonProperty("value")
	private final Object value;

	/**
	 * Constructor using the field name and the value to be matched.
	 * 
	 * @param field
	 *            the field name.
	 * @param value
	 *            the field value.
	 */
	@JsonCreator
	public MatchQuery(@JsonProperty("field") String field, @JsonProperty("value") Object value) {

		this.field = field;
		this.value = value;
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

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("MatchQuery [field=");
		builder.append(field);
		builder.append(", value=");
		builder.append(value);
		builder.append("]");
		return builder.toString();
	}

}
