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
		return new StringField(name, parseColumnValue(value), STORE);
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
		} else if (value instanceof Boolean) {
			return (Boolean) value ? TRUE : FALSE;
		} else {
			throw new MappingException("Value '%s' cannot be cast to Boolean", value);
		}
	}

	@Override
	protected String parseQueryValue(String value) {
		if (value == null) {
			return null;
		} else if (value.equalsIgnoreCase(TRUE)) {
			return TRUE;
		} else if (value.equalsIgnoreCase(FALSE)) {
			return FALSE;
		} else {
			throw new MappingException("Value '%s' cannot be cast to Boolean", value);
		}
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append(getClass().getSimpleName());
		builder.append(" []");
		return builder.toString();
	}

}
