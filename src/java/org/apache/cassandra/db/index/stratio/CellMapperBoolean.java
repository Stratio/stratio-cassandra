package org.apache.cassandra.db.index.stratio;

import org.apache.cassandra.db.index.stratio.query.MatchQuery;
import org.apache.cassandra.db.index.stratio.query.RangeQuery;
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
