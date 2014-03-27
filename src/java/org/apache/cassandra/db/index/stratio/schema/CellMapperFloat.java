package org.apache.cassandra.db.index.stratio.schema;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FloatField;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 * A {@link CellMapper} to map a float field.
 * 
 * @author adelapena
 */
public class CellMapperFloat extends CellMapper<Float> {

	private Float DEFAULT_BOOST = 1.0f;

	@JsonProperty("boost")
	private final Float boost;

	@JsonCreator
	public CellMapperFloat(@JsonProperty("boost") Float boost) {
		super();
		this.boost = boost == null ? DEFAULT_BOOST : boost;
	}

	@Override
	public Analyzer analyzer() {
		return EMPTY_ANALYZER;
	}

	@Override
	public Float indexValue(Object value) {
		if (value == null) {
			return null;
		} else if (value instanceof Number) {
			return ((Number) value).floatValue();
		} else if (value instanceof String) {
			return Double.valueOf(value.toString()).floatValue();
		} else {
			throw new IllegalArgumentException(String.format("Value '%s' cannot be cast to Float", value));
		}
	}

	@Override
	public Float queryValue(Object value) {
		return indexValue(value);
	}

	@Override
	public Field field(String name, Object value) {
		Float number = indexValue(value);
		Field field = new FloatField(name, number, STORE);
		field.setBoost(boost);
		return field;
	}

	@Override
	public Class<Float> baseClass() {
		return Float.class;
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append(getClass().getSimpleName());
		builder.append(" []");
		return builder.toString();
	}

}
