package org.apache.cassandra.db.index.stratio.schema;

import org.apache.cassandra.db.index.stratio.query.FuzzyQuery;
import org.apache.cassandra.db.index.stratio.query.MatchQuery;
import org.apache.cassandra.db.index.stratio.query.PhraseQuery;
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
 * A {@link CellMapper} to map a string, not tokenized field.
 * 
 * @author adelapena
 */
public class CellMapperString extends CellMapper<String> {

	@JsonCreator
	public CellMapperString() {
		super();
	}

	@Override
	public Field field(String name, Object value) {
		String string = value(value);
		return new StringField(name, string, STORE);
	}

	@Override
	public Analyzer analyzer() {
		return EMPTY_ANALYZER;
	}

	@Override
	public String value(Object value) {
		if (value == null) {
			return null;
		} else {
			return value.toString();
		}
	}

	@Override
	protected Query query(MatchQuery matchQuery) {
		String name = matchQuery.getField();
		String value = value(matchQuery.getValue());
		Term term = new Term(name, value);
		Query query = new TermQuery(term);
		query.setBoost(matchQuery.getBoost());
		return query;
	}

	@Override
	protected Query query(PrefixQuery prefixQuery) {
		String name = prefixQuery.getField();
		String value = value(prefixQuery.getValue());
		Term term = new Term(name, value);
		org.apache.lucene.search.PrefixQuery query = new org.apache.lucene.search.PrefixQuery(term);
		query.setBoost(prefixQuery.getBoost());
		return query;
	}

	@Override
	protected Query query(WildcardQuery wildcardQuery) {
		String name = wildcardQuery.getField();
		String value = value(wildcardQuery.getValue());
		Term term = new Term(name, value);
		org.apache.lucene.search.WildcardQuery query = new org.apache.lucene.search.WildcardQuery(term);
		query.setBoost(wildcardQuery.getBoost());
		return query;
	}

	@Override
	protected Query query(PhraseQuery phraseQuery) {
		org.apache.lucene.search.PhraseQuery query = new org.apache.lucene.search.PhraseQuery();
		String name = phraseQuery.getField();
		for (Object o : phraseQuery.getValues()) {
			String value = value(o);
			Term term = new Term(name, value);
			query.add(term);
		}
		query.setSlop(phraseQuery.getSlop());
		query.setBoost(phraseQuery.getBoost());
		return query;
	}

	@Override
	protected Query query(FuzzyQuery fuzzyQuery) {
		String name = fuzzyQuery.getField();
		String value = value(fuzzyQuery.getValue());
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
	protected Query query(RangeQuery rangeQuery) {
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
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append(getClass().getSimpleName());
		builder.append(" []");
		return builder.toString();
	}

}
