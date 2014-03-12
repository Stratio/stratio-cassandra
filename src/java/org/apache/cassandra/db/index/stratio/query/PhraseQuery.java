package org.apache.cassandra.db.index.stratio.query;

import java.util.List;

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
public class PhraseQuery extends AbstractQuery {

	public static final int DEFAULT_SLOP = 0;

	/** The field name */
	@JsonProperty("field")
	private final String field;

	/** The field values */
	@JsonProperty("values")
	private final List<Object> values;

	/** The slop */
	@JsonProperty("slop")
	private final int slop;

	/**
	 * Constructor using the field name and the value to be matched.
	 * 
	 * @param boost
	 *            The boost for this query clause. Documents matching this clause will (in addition
	 *            to the normal weightings) have their score multiplied by {@code boost}.
	 * @param field
	 *            the field name.
	 * @param values
	 *            the field values.
	 * @param slop
	 *            the slop.
	 */
	@JsonCreator
	public PhraseQuery(@JsonProperty("boost") Float boost,
	                   @JsonProperty("field") String field,
	                   @JsonProperty("values") List<Object> values,
	                   @JsonProperty("slop") Integer slop) {
		super(boost);
		this.field = field;
		this.values = values;
		this.slop = slop == null ? DEFAULT_SLOP : slop;
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
	 * Returns the field values.
	 * 
	 * @return the field values.
	 */
	public List<Object> getValues() {
		return values;
	}

	/**
	 * Returns the slop.
	 * 
	 * @return the slop.
	 */
	public int getSlop() {
		return slop;
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("PhraseQuery [boost=");
		builder.append(boost);
		builder.append(", field=");
		builder.append(field);
		builder.append(", values=");
		builder.append(values);
		builder.append(", slop=");
		builder.append(slop);
		builder.append("]");
		return builder.toString();
	}

}
