package org.apache.cassandra.db.index.stratio.schema;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntField;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 * A {@link CellMapper} to map an integer field.
 * 
 * @author adelapena
 */
public class CellMapperInteger extends CellMapper<Integer> {

	private Float DEFAULT_BOOST = 1.0f;

	@JsonProperty("boost")
	private final Float boost;

	@JsonCreator
	public CellMapperInteger(@JsonProperty("boost") Float boost) {
		super();
		this.boost = boost == null ? DEFAULT_BOOST : boost;
	}

	@Override
	public Analyzer analyzer() {
		return EMPTY_ANALYZER;
	}

	@Override
	public Integer indexValue(Object value) {
		if (value == null) {
			return null;
		} else if (value instanceof Number) {
			return ((Number) value).intValue();
		} else if (value instanceof String) {
			return Double.valueOf(value.toString()).intValue();
		} else {
			throw new IllegalArgumentException(String.format("Value '%s' cannot be cast to Integer", value));
		}
	}

	@Override
	public Integer queryValue(Object value) {
		return indexValue(value);
	}

	@Override
	public Field field(String name, Object value) {
		Integer number = indexValue(value);
		Field field = new IntField(name, number, STORE);
		field.setBoost(boost);
		return field;
	}

	@Override
	public Class<Integer> getBaseClass() {
		return Integer.class;
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append(getClass().getSimpleName());
		builder.append(" []");
		return builder.toString();
	}

}
