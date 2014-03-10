package org.apache.cassandra.db.index.stratio;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.DoubleField;
import org.apache.lucene.document.Field;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.Query;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 * A {@link CellMapper} to map a double field.
 * 
 * @author adelapena
 */
public class CellMapperDouble extends CellMapper<Double> {

	private Float DEFAULT_BOOST = 1.0f;

	@JsonProperty("boost")
	private final Float boost;

	@JsonCreator
	public CellMapperDouble(@JsonProperty("boost") Float boost) {
		super();
		this.boost = boost == null ? DEFAULT_BOOST : boost;
	}

	@Override
	public Analyzer analyzer() {
		return EMPTY_ANALYZER;
	}

	@Override
    public Field field(String name, Object value) {
		Double number = parseColumnValue(value);
		Field field = new DoubleField(name, number, STORE);
		field.setBoost(boost);
		return field;
	}

	@Override
    public Query range(String name, String start, String end, boolean startInclusive, boolean endInclusive) {
		return NumericRangeQuery.newDoubleRange(name,
		                                        parseQueryValue(start),
		                                        parseQueryValue(end),
		                                        startInclusive,
		                                        endInclusive);
	}
	
	@Override
    public Query match(String name, String value) {
		return NumericRangeQuery.newDoubleRange(name, parseQueryValue(value), parseQueryValue(value), true, true);
	}

	@Override
	protected Double parseColumnValue(Object value) {
		if (value == null) {
			return null;
		} else if (value instanceof Number) {
			return ((Number) value).doubleValue();
		} else if (value instanceof String) {
			return Double.valueOf(value.toString());
		} else {
			throw new MappingException("Value '%s' cannot be cast to Double", value);
		}
	}

	@Override
	protected Double parseQueryValue(String value) {
		if (value == null) {
			return null;
		} else {
			return Double.valueOf(value);
		}
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append(getClass().getSimpleName());
		builder.append(" []");
		return builder.toString();
	}

}
