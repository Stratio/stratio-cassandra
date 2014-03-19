package org.apache.cassandra.db.index.stratio;

import org.apache.cassandra.db.index.stratio.query.AbstractQuery;
import org.apache.cassandra.db.index.stratio.query.MatchQuery;
import org.apache.cassandra.db.index.stratio.query.PrefixQuery;
import org.apache.cassandra.db.index.stratio.query.RangeQuery;
import org.apache.cassandra.db.index.stratio.query.WildcardQuery;
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
		RangeQuery query = new RangeQuery(AbstractQuery.DEFAULT_BOOST, field, lower, upper, includeLower, includeUpper);
		return query.toLucene(mapper);
	}

	@Override
	protected Query newTermQuery(Term term) {
		String field = term.field();
		String text = term.text();
		MatchQuery query = new MatchQuery(AbstractQuery.DEFAULT_BOOST, field, text);
		return query.toLucene(mapper);
	}

	@Override
	protected PhraseQuery newPhraseQuery() {
		return super.newPhraseQuery();
	}

	@Override
	protected Query newRegexpQuery(Term regexp) {
		String field = regexp.field();
		String text = regexp.text();
		WildcardQuery query = new WildcardQuery(AbstractQuery.DEFAULT_BOOST, field, text);
		return query.toLucene(mapper);
	}

	@Override
	protected Query newPrefixQuery(Term regexp) {
		String field = regexp.field();
		String text = regexp.text();
		PrefixQuery query = new PrefixQuery(AbstractQuery.DEFAULT_BOOST, field, text);
		return query.toLucene(mapper);
	}

}
