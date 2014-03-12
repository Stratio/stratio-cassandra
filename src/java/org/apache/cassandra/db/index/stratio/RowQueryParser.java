package org.apache.cassandra.db.index.stratio;

import java.util.Map;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.Version;

/**
 * A {@link QueryParser} for Cassandra rows.
 * 
 * @author adelapena
 * 
 */
public class RowQueryParser extends QueryParser {

	private final Map<String, CellMapper<?>> mappers;

	public RowQueryParser(Version matchVersion, String f, Analyzer a, Map<String, CellMapper<?>> fields) {
		super(matchVersion, f, a);
		this.mappers = fields;
	}
	
	@Override
	public	Query
	        getRangeQuery(String field, String upper, String lower, boolean startInclusive, boolean endInclusive) throws ParseException {
		CellMapper<?> columnMapper = mappers.get(field);
		if (columnMapper != null) {
			Query query = columnMapper.query(field, upper, lower, startInclusive, endInclusive);
			if (query != null) {
				return query;
			}
		}
		return super.getRangeQuery(field, upper, lower, startInclusive, endInclusive);
	}

	@Override
	protected Query newTermQuery(Term term) {
		String name = term.field();
		String value = term.text();
		CellMapper<?> columnMapper = mappers.get(name);
		if (columnMapper != null) {
			Query query = columnMapper.query(name, value);
			if (query != null) {
				return query;
			}
		}
		return new TermQuery(term);
	}

	@Override
	protected PhraseQuery newPhraseQuery() {
		System.out.println(" -> NEW PHRASE ");
		return super.newPhraseQuery();
	}

	@Override
	protected Query newRegexpQuery(Term regexp) {
		System.out.println(" -> NEW REGEXP " + regexp);
		return super.newRegexpQuery(regexp);
	}

	@Override
	protected Query newPrefixQuery(Term regexp) {
		System.out.println(" -> NEW PREFIX " + regexp);
		return super.newPrefixQuery(regexp);
	}

	@Override
	protected Query getPrefixQuery(String field, String termStr) throws ParseException {
		System.out.println(" -> GET PREFIX " + field + " " + termStr);
		return super.getPrefixQuery(field, termStr);
	}

	@Override
	protected Query getRegexpQuery(String field, String termStr) throws ParseException {
		System.out.println(" -> GET REGEXP " + field + " " + termStr);
		return super.getRegexpQuery(field, termStr);
	}

	@Override
	public Query createPhraseQuery(String field, String queryText, int phraseSlop) {
		System.out.println(" -> CREATE PHRASE " + field + " " + queryText);
		return super.createPhraseQuery(field, queryText, phraseSlop);
	}

}
