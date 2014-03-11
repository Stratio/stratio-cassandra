package org.apache.cassandra.db.index.stratio;

import org.apache.cassandra.db.index.stratio.query.MatchQuery;
import org.apache.cassandra.db.index.stratio.query.RangeQuery;
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
		String text = parseColumnValue(value);
		return new TextField(name, text, STORE);
	}

	@Override
	public Query query(MatchQuery matchQuery) {
		String name = matchQuery.getField();
		String value = parseColumnValue(matchQuery.getValue());
		Term term = new Term(name, value);
		return new TermQuery(term);
	}

	@Override
	public Query query(RangeQuery rangeQuery) {
		String name = rangeQuery.getField();
		String lowerValue = parseColumnValue(rangeQuery.getLowerValue());
		String upperValue = parseColumnValue(rangeQuery.getUpperValue());
		boolean includeLower = rangeQuery.getIncludeLower();
		boolean includeUpper = rangeQuery.getIncludeUpper();
		return TermRangeQuery.newStringRange(name, lowerValue, upperValue, includeLower, includeUpper);
	}

	@Override
	protected String parseColumnValue(Object value) {
		if (value == null) {
			return null;
		} else {
			return value.toString();
		}
	}

	@Override
	protected String parseQueryValue(String value) {
		if (value == null) {
			return null;
		} else {
			return value;
		}
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
