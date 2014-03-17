package org.apache.cassandra.db.index.stratio.schema;

import org.apache.cassandra.db.index.stratio.MappingException;
import org.apache.cassandra.db.index.stratio.query.FuzzyQuery;
import org.apache.cassandra.db.index.stratio.query.MatchQuery;
import org.apache.cassandra.db.index.stratio.query.PhraseQuery;
import org.apache.cassandra.db.index.stratio.query.PrefixQuery;
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
	protected Query query(MatchQuery matchQuery) {
		String name = matchQuery.getField();
		Integer value = value(matchQuery.getValue());
		Query query = NumericRangeQuery.newIntRange(name, value, value, true, true);
		query.setBoost(matchQuery.getBoost());
		return query;
	}

	@Override
	protected Query query(PrefixQuery prefixQuery) {
		throw new MappingException();
	}

	@Override
	protected Query query(WildcardQuery wildcardQuery) {
		throw new MappingException();
	}

	@Override
	protected Query query(PhraseQuery phraseQuery) {
		throw new MappingException();
	}

	@Override
	protected Query query(FuzzyQuery fuzzyQuery) {
		throw new MappingException();
	}

	@Override
	protected Query query(RangeQuery rangeQuery) {
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
	public Integer value(Object value) {
		if (value == null) {
			return null;
		} else if (value instanceof Number) {
			return ((Number) value).intValue();
		} else if (value instanceof String) {
			return Double.valueOf(value.toString()).intValue();
		} else {
			throw new MappingException("Value '%s' cannot be cast to Integer", value);
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
