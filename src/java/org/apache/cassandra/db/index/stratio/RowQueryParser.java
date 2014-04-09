/*
* Copyright 2014, Stratio.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package org.apache.cassandra.db.index.stratio;

import org.apache.cassandra.db.index.stratio.query.Condition;
import org.apache.cassandra.db.index.stratio.query.MatchCondition;
import org.apache.cassandra.db.index.stratio.query.PrefixCondition;
import org.apache.cassandra.db.index.stratio.query.RangeCondition;
import org.apache.cassandra.db.index.stratio.query.RegexpCondition;
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
		RegexpCondition condition = new RegexpCondition(Condition.DEFAULT_BOOST, field, text);
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
