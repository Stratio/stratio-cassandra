package org.apache.cassandra.db.index.stratio;

import java.util.UUID;

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
		String uuid = parseColumnValue(value);
		return new StringField(name, uuid, STORE);
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
		} else if (value instanceof UUID) {
			return value.toString();
		} else if (value instanceof String) {
			return UUID.fromString((String) value).toString();
		} else {
			throw new MappingException("Value '%s' cannot be cast to UUID", value);
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
		builder.append(" []");
		return builder.toString();
	}

}
