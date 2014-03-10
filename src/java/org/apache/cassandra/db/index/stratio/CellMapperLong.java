package org.apache.cassandra.db.index.stratio;

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
    public Field field(String name, Object value) {
		Long number = parseColumnValue(value);
		Field field = new LongField(name, number, STORE);
		field.setBoost(boost);
		return field;
	}

	@Override
    public Query range(String name, String start, String end, boolean startInclusive, boolean endInclusive) {
		return NumericRangeQuery.newLongRange(name,
		                                      parseQueryValue(start),
		                                      parseQueryValue(end),
		                                      startInclusive,
		                                      endInclusive);
	}
	
	@Override
    public Query match(String name, String value) {
		return NumericRangeQuery.newLongRange(name, parseQueryValue(value), parseQueryValue(value), true, true);
	}

	@Override
	protected Long parseColumnValue(Object value) {
		if (value == null) {
			return null;
		} else if (value instanceof Number) {
			return ((Number) value).longValue();
		} else if (value instanceof String) {
			return Long.valueOf(value.toString());
		} else {
			throw new MappingException("Value '%s' cannot be cast to Long", value);
		}
	}

	@Override
	protected Long parseQueryValue(String value) {
		if (value == null) {
			return null;
		} else {
			return Long.valueOf(value);
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
