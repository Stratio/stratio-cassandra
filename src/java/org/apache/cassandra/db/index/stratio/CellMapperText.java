package org.apache.cassandra.db.index.stratio;

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
    public Query range(String name, String start, String end, boolean startInclusive, boolean endInclusive) {
		return TermRangeQuery.newStringRange(name,
		                                     parseQueryValue(start),
		                                     parseQueryValue(end),
		                                     startInclusive,
		                                     endInclusive);
	}
	
	@Override
    public Query match(String name, String value) {
		return new TermQuery(new Term(name, parseQueryValue(value)));
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
		builder.append("ColumnMapperText [analyzer=");
		builder.append(analyzer);
		builder.append("]");
		return builder.toString();
	}

}
