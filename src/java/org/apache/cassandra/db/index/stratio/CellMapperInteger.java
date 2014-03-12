package org.apache.cassandra.db.index.stratio;

import org.apache.cassandra.db.index.stratio.query.FuzzyQuery;
import org.apache.cassandra.db.index.stratio.query.MatchQuery;
import org.apache.cassandra.db.index.stratio.query.PhraseQuery;
import org.apache.cassandra.db.index.stratio.query.RangeQuery;
import org.apache.cassandra.db.index.stratio.query.WildcardQuery;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntField;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.Query;
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
	public Field field(String name, Object value) {
		Integer number = value(value);
		Field field = new IntField(name, number, STORE);
		field.setBoost(boost);
		return field;
	}

	@Override
	public Query query(MatchQuery matchQuery) {
		String name = matchQuery.getField();
		Integer value = value(matchQuery.getValue());
		return NumericRangeQuery.newIntRange(name, value, value, true, true);
	}

	@Override
	public Query query(WildcardQuery wildcardQuery) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Query query(PhraseQuery phraseQuery) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Query query(FuzzyQuery fuzzyQuery) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Query query(RangeQuery rangeQuery) {
		String name = rangeQuery.getField();
		Integer lowerValue = value(rangeQuery.getLowerValue());
		Integer upperValue = value(rangeQuery.getUpperValue());
		boolean includeLower = rangeQuery.getIncludeLower();
		boolean includeUpper = rangeQuery.getIncludeUpper();
		Query query = NumericRangeQuery.newIntRange(name, lowerValue, upperValue, includeLower, includeUpper);
		query.setBoost(rangeQuery.getBoost());
		return query;
	}

	@Override
	protected Integer value(Object value) {
		if (value == null) {
			return null;
		} else if (value instanceof Number) {
			return ((Number) value).intValue();
		} else if (value instanceof String) {
			return Integer.valueOf(value.toString());
		} else {
			throw new MappingException("Value '%s' cannot be cast to Integer", value);
		}
	}

	@Override
	public Query query(String name, String start, String end, boolean startInclusive, boolean endInclusive) {
		return NumericRangeQuery.newIntRange(name,
		                                     value(start),
		                                     value(end),
		                                     startInclusive,
		                                     endInclusive);
	}

	@Override
	public Query query(String name, String value) {
		return NumericRangeQuery.newIntRange(name, value(value), value(value), true, true);
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append(getClass().getSimpleName());
		builder.append(" []");
		return builder.toString();
	}

}
