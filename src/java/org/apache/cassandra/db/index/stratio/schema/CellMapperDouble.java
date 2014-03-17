package org.apache.cassandra.db.index.stratio.schema;

import org.apache.cassandra.db.index.stratio.MappingException;
import org.apache.cassandra.db.index.stratio.query.FuzzyQuery;
import org.apache.cassandra.db.index.stratio.query.MatchQuery;
import org.apache.cassandra.db.index.stratio.query.PhraseQuery;
import org.apache.cassandra.db.index.stratio.query.PrefixQuery;
import org.apache.cassandra.db.index.stratio.query.RangeQuery;
import org.apache.cassandra.db.index.stratio.query.WildcardQuery;
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
		Double number = value(value);
		Field field = new DoubleField(name, number, STORE);
		field.setBoost(boost);
		return field;
	}

	@Override
	public Double value(Object value) {
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
	protected Query query(MatchQuery matchQuery) {
		String name = matchQuery.getField();
		Double value = value(matchQuery.getValue());
		Query query = NumericRangeQuery.newDoubleRange(name, value, value, true, true);
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
		Double lowerValue = value(rangeQuery.getLowerValue());
		Double upperValue = value(rangeQuery.getUpperValue());
		boolean includeLower = rangeQuery.getIncludeLower();
		boolean includeUpper = rangeQuery.getIncludeUpper();
		Query query = NumericRangeQuery.newDoubleRange(name, lowerValue, upperValue, includeLower, includeUpper);
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
