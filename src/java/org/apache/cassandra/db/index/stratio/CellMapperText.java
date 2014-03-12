package org.apache.cassandra.db.index.stratio;

import org.apache.cassandra.db.index.stratio.query.FuzzyQuery;
import org.apache.cassandra.db.index.stratio.query.MatchQuery;
import org.apache.cassandra.db.index.stratio.query.PhraseQuery;
import org.apache.cassandra.db.index.stratio.query.RangeQuery;
import org.apache.cassandra.db.index.stratio.query.WildcardQuery;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.util.Version;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 * A {@link CellMapper} to map a string, tokenized field.
 * 
 * @author adelapena
 */
public class CellMapperText extends CellMapper<String> {

	public static final Analyzer DEFAULT_ANALYZER = new StandardAnalyzer(Version.LUCENE_46);

	/** The Lucene's {@link corg.apache.lucene.analysis.Analyzer} class name. */
	@JsonProperty("analyzer")
	private final String analyzerClassName;

	/** The Lucene's {@link corg.apache.lucene.analysis.Analyzer}. */
	@JsonIgnore
	private final Analyzer analyzer;

	@JsonCreator
	public CellMapperText(@JsonProperty("analyzer") String analyzerClassName) {
		if (analyzerClassName == null) {
			this.analyzer = DEFAULT_ANALYZER;
			this.analyzerClassName = DEFAULT_ANALYZER.getClass().getName();
		} else {
			this.analyzer = AnalyzerFactory.getAnalyzer(analyzerClassName);
			this.analyzerClassName = analyzerClassName;
		}
	}

	@Override
	public Analyzer analyzer() {
		return analyzer;
	}

	@Override
	public Field field(String name, Object value) {
		String text = value(value);
		return new TextField(name, text, STORE);
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
		String name = wildcardQuery.getField();
		String value = value(wildcardQuery.getValue());
		Term term = new Term(name, value);
		return new org.apache.lucene.search.WildcardQuery(term);
	}

	@Override
	public Query query(PhraseQuery phraseQuery) {
		org.apache.lucene.search.PhraseQuery query = new org.apache.lucene.search.PhraseQuery();
		String name = phraseQuery.getField();
		for (Object value : phraseQuery.getValues()) {
			Term term = new Term(name, value(value));
			query.add(term);
		}
		query.setSlop(phraseQuery.getSlop());
		query.setBoost(phraseQuery.getBoost());
		return query;
	}

	@Override
	public Query query(FuzzyQuery fuzzyQuery) {
		String name = fuzzyQuery.getField();
		String value = value(fuzzyQuery.getValue());
		Term term = new Term(name, value);
		int maxEdits = fuzzyQuery.getMaxEdits();
		int prefixLength = fuzzyQuery.getPrefixLength();
		int maxExpansions = fuzzyQuery.getMaxExpansions();
		boolean transpositions = fuzzyQuery.getTranspositions();
		return new org.apache.lucene.search.FuzzyQuery(term, maxEdits, prefixLength, maxExpansions, transpositions);
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
		} else {
			return value.toString();
		}
	}

	@Override
	public Query query(String name, String start, String end, boolean startInclusive, boolean endInclusive) {
		return TermRangeQuery.newStringRange(name, value(start), value(end), startInclusive, endInclusive);
	}

	@Override
	public Query query(String name, String value) {
		return new TermQuery(new Term(name, value(value)));
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append(getClass().getSimpleName());
		builder.append(" [analyzer=");
		builder.append(analyzer);
		builder.append("]");
		return builder.toString();
	}

}
