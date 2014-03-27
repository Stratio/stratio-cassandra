package org.apache.cassandra.db.index.stratio;

import org.apache.cassandra.db.index.stratio.query.Condition;
import org.apache.cassandra.db.index.stratio.query.MatchCondition;
import org.apache.cassandra.db.index.stratio.query.PrefixCondition;
import org.apache.cassandra.db.index.stratio.query.RangeCondition;
import org.apache.cassandra.db.index.stratio.query.WildcardCondition;
import org.apache.cassandra.db.index.stratio.schema.CellsMapper;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.Version;

/**
 * A {@link QueryParser} for Cassandra rows.
 * 
 * @author adelapena
 * 
 */
public class RowQueryParser extends QueryParser {

	private final CellsMapper mapper;

	public RowQueryParser(Version matchVersion, String f, CellsMapper mapper) {
		super(matchVersion, f, mapper.analyzer());
		this.mapper = mapper;
	}

	@Override
	public Query getRangeQuery(String field, String lower, String upper, boolean includeLower, boolean includeUpper) throws ParseException {
		RangeCondition condition = new RangeCondition(Condition.DEFAULT_BOOST,
		                                          field,
		                                          lower,
		                                          upper,
		                                          includeLower,
		                                          includeUpper);
		return condition.query(mapper);
	}

	@Override
	protected Query newTermQuery(Term term) {
		String field = term.field();
		String text = term.text();
		MatchCondition condition = new MatchCondition(Condition.DEFAULT_BOOST, field, text);
		return condition.query(mapper);
	}

	@Override
	protected PhraseQuery newPhraseQuery() {
		return super.newPhraseQuery();
	}

	@Override
	protected Query newRegexpQuery(Term regexp) {
		String field = regexp.field();
		String text = regexp.text();
		WildcardCondition condition = new WildcardCondition(Condition.DEFAULT_BOOST, field, text);
		return condition.query(mapper);
	}

	@Override
	protected Query newPrefixQuery(Term regexp) {
		String field = regexp.field();
		String text = regexp.text();
		PrefixCondition query = new PrefixCondition(Condition.DEFAULT_BOOST, field, text);
		return query.query(mapper);
	}

	@Override
	protected Query newWildcardQuery(Term t) {
		String field = t.field();
		String text = t.text();
		WildcardCondition condition = new WildcardCondition(Condition.DEFAULT_BOOST, field, text);
		return condition.query(mapper);
	}

}
