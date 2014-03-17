package org.apache.cassandra.db.index.stratio;

import org.apache.cassandra.db.index.stratio.query.AbstractQuery;
import org.apache.cassandra.db.index.stratio.query.MatchQuery;
import org.apache.cassandra.db.index.stratio.query.PrefixQuery;
import org.apache.cassandra.db.index.stratio.query.RangeQuery;
import org.apache.cassandra.db.index.stratio.query.WildcardQuery;
import org.apache.lucene.analysis.Analyzer;
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

	public RowQueryParser(Version matchVersion, String f, Analyzer a, CellsMapper mapper) {
		super(matchVersion, f, a);
		this.mapper = mapper;
	}

	@Override
	public Query getRangeQuery(String field, String lower, String upper, boolean includeLower, boolean includeUpper) throws ParseException {
		System.out.println(" -> NEW RANGE ");
		RangeQuery query = new RangeQuery(AbstractQuery.DEFAULT_BOOST, field, lower, upper, includeLower, includeUpper);
		return mapper.query(query);
	}

	@Override
	protected Query newTermQuery(Term term) {
		System.out.println(" -> NEW TERM ");
		String field = term.field();
		String text = term.text();
		MatchQuery query = new MatchQuery(AbstractQuery.DEFAULT_BOOST, field, text);
		return mapper.query(query);
	}

	@Override
	protected PhraseQuery newPhraseQuery() {
		System.out.println(" -> NEW PHRASE ");
		return super.newPhraseQuery();
	}

	@Override
	protected Query newRegexpQuery(Term regexp) {
		System.out.println(" -> NEW REGEXP " + regexp);
		String field = regexp.field();
		String text = regexp.text();
		WildcardQuery query = new WildcardQuery(AbstractQuery.DEFAULT_BOOST, field, text);
		return mapper.query(query);
	}

	@Override
	protected Query newPrefixQuery(Term regexp) {
		System.out.println(" -> NEW PREFIX " + regexp);
		String field = regexp.field();
		String text = regexp.text();
		PrefixQuery query = new PrefixQuery(AbstractQuery.DEFAULT_BOOST, field, text);
		return mapper.query(query);
	}

	@Override
	public Query createPhraseQuery(String field, String queryText, int phraseSlop) {
		System.out.println(" -> CREATE PHRASE " + field + " " + queryText);
		return super.createPhraseQuery(field, queryText, phraseSlop);
	}

}
