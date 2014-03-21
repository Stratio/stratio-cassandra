package org.apache.cassandra.db.index.stratio.schema;

import org.apache.cassandra.db.index.stratio.query.AbstractQuery;
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
	public Query toLucene(AbstractQuery query) {
		if (query instanceof MatchQuery) {
			return query((MatchQuery) query);
		} else if (query instanceof RangeQuery) {
			return query((RangeQuery) query);
		} else {
			String message = String.format("Unsupported query %s for mapper %s", query, this);
			throw new UnsupportedOperationException(message);
		}
	}

	private Query query(MatchQuery matchQuery) {
		String name = matchQuery.getField();
		Float value = queryValue(matchQuery.getValue());
		Query query = NumericRangeQuery.newFloatRange(name, value, value, true, true);
		query.setBoost(matchQuery.getBoost());
		return query;
	}

	private Query query(RangeQuery rangeQuery) {
		String name = rangeQuery.getField();
		Float lowerValue = queryValue(rangeQuery.getLowerValue());
		Float upperValue = queryValue(rangeQuery.getUpperValue());
		boolean includeLower = rangeQuery.getIncludeLower();
		boolean includeUpper = rangeQuery.getIncludeUpper();
		Query query = NumericRangeQuery.newFloatRange(name, lowerValue, upperValue, includeLower, includeUpper);
		query.setBoost(rangeQuery.getBoost());
		return query;
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append(getClass().getSimpleName());
		builder.append(" []");
		return builder.toString();
	}

}
