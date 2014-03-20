package org.apache.cassandra.db.index.stratio.schema;

import java.math.BigInteger;

import org.apache.cassandra.db.index.stratio.query.AbstractQuery;
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
import org.codehaus.jackson.annotate.JsonProperty;

/**
 * A {@link CellMapper} to map a string, not tokenized field.
 * 
 * @author adelapena
 */
public class CellMapperVarint extends CellMapper<String> {

	@JsonProperty("digits")
	private final int digits;

	@JsonCreator
	public CellMapperVarint(@JsonProperty("digits") int digits) {
		super();
		this.digits = digits;
	}

	@Override
	public Analyzer analyzer() {
		return EMPTY_ANALYZER;
	}

	@Override
	public String indexValue(Object value) {
		if (value == null) {
			return null;
		} else {
			BigInteger bi = new BigInteger(value.toString());
			String format = String.format("%%0%dd", digits);
			String result = String.format(format, bi);
			return result;
		}
	}

	@Override
	public String queryValue(Object value) {
		return indexValue(value);
	}

	@Override
	public Field field(String name, Object value) {
		String string = indexValue(value);
		return new StringField(name, string, STORE);
	}

	@Override
	public Query query(AbstractQuery query) {
		if (query instanceof MatchQuery) {
			return query((MatchQuery) query);
		} else if (query instanceof RangeQuery) {
			return query((RangeQuery) query);
		} else {
			String message = String.format("Unsupported query %s for mapper %s", query, this);
			throw new UnsupportedOperationException(message);
		}
	}

	private Query query(MatchQuery matchQuery) {
		String name = matchQuery.getField();
		String value = queryValue(matchQuery.getValue());
		Term term = new Term(name, value);
		Query query = new TermQuery(term);
		query.setBoost(matchQuery.getBoost());
		return query;
	}

	private Query query(RangeQuery rangeQuery) {
		String name = rangeQuery.getField();
		String lowerValue = queryValue(rangeQuery.getLowerValue());
		String upperValue = queryValue(rangeQuery.getUpperValue());
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
