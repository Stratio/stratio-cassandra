package org.apache.cassandra.db.index.stratio.schema;

import java.util.UUID;

import org.apache.cassandra.db.index.stratio.MappingException;
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
 * A {@link CellMapper} to map a UUID field.
 * 
 * @author adelapena
 */
public class CellMapperUUID extends CellMapper<String> {

	@JsonCreator
	public CellMapperUUID() {
		super();
	}

	@Override
	public Analyzer analyzer() {
		return EMPTY_ANALYZER;
	}

	@Override
	public Field field(String name, Object value) {
		String uuid = value(value);
		return new StringField(name, uuid, STORE);
	}

	@Override
	public String value(Object value) {
		if (value == null) {
			return null;
		} else if (value instanceof UUID) {
			return value.toString();
		} else if (value instanceof String) {
			return UUID.fromString((String) value).toString();
		} else {
			throw new MappingException("Value '%s' cannot be cast to UUID", value);
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
		Query query = new org.apache.lucene.search.PrefixQuery(term);
		query.setBoost(prefixQuery.getBoost());
		return query;
	}

	@Override
	protected Query query(WildcardQuery wildcardQuery) {
		String name = wildcardQuery.getField();
		String value = value(wildcardQuery.getValue());
		Term term = new Term(name, value);
		Query query = new org.apache.lucene.search.WildcardQuery(term);
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
