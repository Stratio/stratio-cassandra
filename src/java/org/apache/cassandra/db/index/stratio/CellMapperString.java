package org.apache.cassandra.db.index.stratio;

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
		String string = parseColumnValue(value);
		return new StringField(name, string, STORE);
	}

	@Override
	public Analyzer analyzer() {
		return EMPTY_ANALYZER;
	}

	@Override
    public Query range(String name, String start, String end, boolean startInclusive, boolean endInclusive) {
		return TermRangeQuery.newStringRange(name, parseQueryValue(start), parseQueryValue(end), startInclusive, endInclusive);
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
		builder.append("ColumnMapperString []");
		return builder.toString();
	}

}
