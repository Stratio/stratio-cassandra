package org.apache.cassandra.db.index.stratio;

import org.apache.cassandra.db.index.stratio.query.FuzzyQuery;
import org.apache.cassandra.db.index.stratio.query.MatchQuery;
import org.apache.cassandra.db.index.stratio.query.PhraseQuery;
import org.apache.cassandra.db.index.stratio.query.PrefixQuery;
import org.apache.cassandra.db.index.stratio.query.RangeQuery;
import org.apache.cassandra.db.index.stratio.query.WildcardQuery;
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
		Long number = value(value);
		Field field = new LongField(name, number, STORE);
		field.setBoost(boost);
		return field;
	}

	@Override
	protected Long value(Object value) {
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
	public Query query(MatchQuery matchQuery) {
		String name = matchQuery.getField();
		Long value = value(matchQuery.getValue());
		return NumericRangeQuery.newLongRange(name, value, value, true, true);
	}

	@Override
	public Query query(PrefixQuery prefixQuery) {
		throw new UnsupportedOperationException();
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
		Long lowerValue = value(rangeQuery.getLowerValue());
		Long upperValue = value(rangeQuery.getUpperValue());
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
