package org.apache.cassandra.db.index.stratio;

import org.apache.cassandra.db.index.stratio.query.MatchQuery;
import org.apache.cassandra.db.index.stratio.query.RangeQuery;
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
	public Query query(MatchQuery matchQuery) {
		String name = matchQuery.getField();
		Double value = parseColumnValue(matchQuery.getValue());
		return NumericRangeQuery.newDoubleRange(name, value, value, true, true);
	}

	@Override
	public Query query(RangeQuery rangeQuery) {
		String name = rangeQuery.getField();
		Double lowerValue = parseColumnValue(rangeQuery.getLowerValue());
		Double upperValue = parseColumnValue(rangeQuery.getUpperValue());
		boolean includeLower = rangeQuery.getIncludeLower();
		boolean includeUpper = rangeQuery.getIncludeUpper();
		return NumericRangeQuery.newDoubleRange(name, lowerValue, upperValue, includeLower, includeUpper);
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
