package org.apache.cassandra.db.index.stratio;

import org.apache.cassandra.db.index.stratio.query.FuzzyQuery;
import org.apache.cassandra.db.index.stratio.query.MatchQuery;
import org.apache.cassandra.db.index.stratio.query.PhraseQuery;
import org.apache.cassandra.db.index.stratio.query.RangeQuery;
import org.apache.cassandra.db.index.stratio.query.WildcardQuery;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermRangeQuery;
import org.codehaus.jackson.annotate.JsonCreator;

/**
 * A {@link CellMapper} to map a boolean field.
 * 
 * @author adelapena
 */
public class CellMapperBoolean extends CellMapper<String> {

	private static final String TRUE = "true";
	private static final String FALSE = "false";

	@JsonCreator
	public CellMapperBoolean() {
		super();
	}

	@Override
	public Analyzer analyzer() {
		return EMPTY_ANALYZER;
	}

	@Override
	public Field field(String name, Object value) {
		return new StringField(name, value(value), STORE);
	}

	@Override
	public Query query(MatchQuery matchQuery) {
		String name = matchQuery.getField();
		String value = value(matchQuery.getValue());
		Term term = new Term(name, value);
		return new TermQuery(term);
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
		String lowerValue = value(rangeQuery.getLowerValue());
		String upperValue = value(rangeQuery.getUpperValue());
		boolean includeLower = rangeQuery.getIncludeLower();
		boolean includeUpper = rangeQuery.getIncludeUpper();
		Query query = TermRangeQuery.newStringRange(name, lowerValue, upperValue, includeLower, includeUpper);
		query.setBoost(rangeQuery.getBoost());
		return query;
	}

	@Override
	protected String value(Object value) {
		if (value == null) {
			return null;
		} else if (value instanceof Boolean) {
			return (Boolean) value ? TRUE : FALSE;
		} else if (value instanceof String) {
			String s = (String) value;
			if (s.equalsIgnoreCase(TRUE)) {
				return TRUE;
			} else if (s.equalsIgnoreCase(FALSE)) {
				return FALSE;
			}
		}
		throw new MappingException("Value '%s' cannot be cast to Boolean", value);
	}

	@Override
	public Query query(String name, String value) {
		return new TermQuery(new Term(name, value(value)));
	}

	@Override
	public Query query(String name, String start, String end, boolean startInclusive, boolean endInclusive) {
		return TermRangeQuery.newStringRange(name, value(start), value(end), startInclusive, endInclusive);
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append(getClass().getSimpleName());
		builder.append(" []");
		return builder.toString();
	}

}
