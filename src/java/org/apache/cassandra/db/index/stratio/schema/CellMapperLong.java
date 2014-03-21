package org.apache.cassandra.db.index.stratio.schema;

import org.apache.cassandra.db.index.stratio.query.AbstractQuery;
import org.apache.cassandra.db.index.stratio.query.MatchQuery;
import org.apache.cassandra.db.index.stratio.query.RangeQuery;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongField;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.Query;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 * A {@link CellMapper} to map a long field.
 * 
 * @author adelapena
 */
public class CellMapperLong extends CellMapper<Long> {

	private Float DEFAULT_BOOST = 1.0f;

	@JsonProperty("boost")
	private final Float boost;

	@JsonCreator
	public CellMapperLong(@JsonProperty("boost") Float boost) {
		super();
		this.boost = boost == null ? DEFAULT_BOOST : boost;
	}

	@Override
	public Analyzer analyzer() {
		return EMPTY_ANALYZER;
	}

	@Override
	public Long indexValue(Object value) {
		if (value == null) {
			return null;
		} else if (value instanceof Number) {
			return ((Number) value).longValue();
		} else if (value instanceof String) {
			return Double.valueOf(value.toString()).longValue();
		} else {
			throw new IllegalArgumentException(String.format("Value '%s' cannot be cast to Long", value));
		}
	}

	@Override
	public Long queryValue(Object value) {
		return indexValue(value);
	}

	@Override
	public Field field(String name, Object value) {
		Long number = indexValue(value);
		Field field = new LongField(name, number, STORE);
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
		Long value = queryValue(matchQuery.getValue());
		Query query = NumericRangeQuery.newLongRange(name, value, value, true, true);
		query.setBoost(matchQuery.getBoost());
		return query;
	}

	private Query query(RangeQuery rangeQuery) {
		String name = rangeQuery.getField();
		Long lowerValue = queryValue(rangeQuery.getLowerValue());
		Long upperValue = queryValue(rangeQuery.getUpperValue());
		boolean includeLower = rangeQuery.getIncludeLower();
		boolean includeUpper = rangeQuery.getIncludeUpper();
		Query query = NumericRangeQuery.newLongRange(name, lowerValue, upperValue, includeLower, includeUpper);
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
