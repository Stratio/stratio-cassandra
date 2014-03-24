package org.apache.cassandra.db.index.stratio.schema;

import org.apache.cassandra.db.index.stratio.AnalyzerFactory;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 * A {@link CellMapper} to map a string, tokenized field.
 * 
 * @author adelapena
 */
public class CellMapperText extends CellMapper<String> {

	/** The Lucene's {@link corg.apache.lucene.analysis.Analyzer} class name. */
	@JsonProperty("analyzer")
	private String analyzerClassName;

	/** The Lucene's {@link corg.apache.lucene.analysis.Analyzer}. */
	@JsonIgnore
	private Analyzer analyzer;

	@JsonCreator
	public CellMapperText(@JsonProperty("analyzer") String analyzerClassName) {
		if (analyzerClassName != null) {
			this.analyzer = AnalyzerFactory.getAnalyzer(analyzerClassName);
			this.analyzerClassName = analyzerClassName;
		} else {
			this.analyzer = null;
			this.analyzerClassName = null;
		}
	}

	@Override
	public Analyzer analyzer() {
		return analyzer;
	}

	@Override
	public String indexValue(Object value) {
		if (value == null) {
			return null;
		} else {
			return value.toString();
		}
	}

	@Override
	public String queryValue(Object value) {
		return indexValue(value);
	}

	@Override
	public Field field(String name, Object value) {
		String text = indexValue(value);
		return new TextField(name, text, STORE);
	}

	@Override
	public Class<String> getBaseClass() {
		return String.class;
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
