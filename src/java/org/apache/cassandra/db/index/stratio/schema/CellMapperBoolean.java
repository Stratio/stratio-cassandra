package org.apache.cassandra.db.index.stratio.schema;

import org.apache.cassandra.db.index.stratio.query.AbstractQuery;
import org.apache.cassandra.db.index.stratio.query.BooleanQuery;
import org.apache.cassandra.db.index.stratio.query.FuzzyQuery;
import org.apache.cassandra.db.index.stratio.query.MatchQuery;
import org.apache.cassandra.db.index.stratio.query.PrefixQuery;
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
	public String indexValue(Object value) {
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
		throw new IllegalArgumentException();
	}

	@Override
	public String queryValue(Object value) {
		if (value == null) {
			return null;
		} else if (value instanceof Boolean) {
			return (Boolean) value ? TRUE : FALSE;
		} else {
			return value.toString();
		}
	}

	@Override
	public Field field(String name, Object value) {
		return new StringField(name, indexValue(value), STORE);
	}

	@Override
	public Query query(AbstractQuery query) {
		if (query instanceof MatchQuery) {
			return query((MatchQuery) query);
		} else if (query instanceof PrefixQuery) {
			return query((PrefixQuery) query);
		} else if (query instanceof WildcardQuery) {
			return query((WildcardQuery) query);
		} else if (query instanceof FuzzyQuery) {
			return query((FuzzyQuery) query);
		} else if (query instanceof RangeQuery) {
			return query((RangeQuery) query);
		} else if (query instanceof BooleanQuery) {
			return query((BooleanQuery) query);
		} else {
			String message = String.format("Unsupported query %s for mapper %s", query, this);
			throw new UnsupportedOperationException(message);
		}
	}

	private Query query(MatchQuery matchQuery) {
		String name = matchQuery.getField();
		String value = queryValue(matchQuery.getValue());
		Term term = new Term(name, value);
		Query query = new TermQuery(term);
		query.setBoost(matchQuery.getBoost());
		return query;
	}

	private Query query(RangeQuery rangeQuery) {
		String name = rangeQuery.getField();
		String lowerValue = queryValue(rangeQuery.getLowerValue());
		String upperValue = queryValue(rangeQuery.getUpperValue());
		boolean includeLower = rangeQuery.getIncludeLower();
		boolean includeUpper = rangeQuery.getIncludeUpper();
		Query query = TermRangeQuery.newStringRange(name, lowerValue, upperValue, includeLower, includeUpper);
		query.setBoost(rangeQuery.getBoost());
		return query;
	}

	private Query query(PrefixQuery prefixQuery) {
		String name = prefixQuery.getField();
		String value = queryValue(prefixQuery.getValue());
		Term term = new Term(name, value);
		Query query = new org.apache.lucene.search.PrefixQuery(term);
		query.setBoost(prefixQuery.getBoost());
		return query;
	}

	private Query query(WildcardQuery wildcardQuery) {
		String name = wildcardQuery.getField();
		String value = queryValue(wildcardQuery.getValue());
		Term term = new Term(name, value);
		Query query = new org.apache.lucene.search.WildcardQuery(term);
		query.setBoost(wildcardQuery.getBoost());
		return query;
	}

	private Query query(FuzzyQuery fuzzyQuery) {
		String name = fuzzyQuery.getField();
		String value = queryValue(fuzzyQuery.getValue());
		Term term = new Term(name, value);
		int maxEdits = fuzzyQuery.getMaxEdits();
		int prefixLength = fuzzyQuery.getPrefixLength();
		int maxExpansions = fuzzyQuery.getMaxExpansions();
		boolean transpositions = fuzzyQuery.getTranspositions();
		Query query = new org.apache.lucene.search.FuzzyQuery(term,
		                                                      maxEdits,
		                                                      prefixLength,
		                                                      maxExpansions,
		                                                      transpositions);
		query.setBoost(fuzzyQuery.getBoost());
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
