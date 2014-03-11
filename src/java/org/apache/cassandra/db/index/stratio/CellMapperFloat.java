package org.apache.cassandra.db.index.stratio;

import org.apache.cassandra.db.index.stratio.query.MatchQuery;
import org.apache.cassandra.db.index.stratio.query.RangeQuery;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FloatField;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.Query;
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
	public Field field(String name, Object value) {
		Float number = parseColumnValue(value);
		Field field = new FloatField(name, number, STORE);
		field.setBoost(boost);
		return field;
	}

	@Override
	public Query query(MatchQuery matchQuery) {
		String name = matchQuery.getField();
		Float value = parseColumnValue(matchQuery.getValue());
		return NumericRangeQuery.newFloatRange(name, value, value, true, true);
	}

	@Override
	public Query query(RangeQuery rangeQuery) {
		String name = rangeQuery.getField();
		Float lowerValue = parseColumnValue(rangeQuery.getLowerValue());
		Float upperValue = parseColumnValue(rangeQuery.getUpperValue());
		boolean includeLower = rangeQuery.getIncludeLower();
		boolean includeUpper = rangeQuery.getIncludeUpper();
		return NumericRangeQuery.newFloatRange(name, lowerValue, upperValue, includeLower, includeUpper);
	}

	@Override
	protected Float parseColumnValue(Object value) {
		if (value == null) {
			return null;
		} else if (value instanceof Number) {
			return ((Number) value).floatValue();
		} else if (value instanceof String) {
			return Float.valueOf(value.toString());
		} else {
			throw new MappingException("Value '%s' cannot be cast to Float", value);
		}
	}

	@Override
	protected Float parseQueryValue(String value) {
		if (value == null) {
			return null;
		} else {
			return Float.valueOf(value);
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
