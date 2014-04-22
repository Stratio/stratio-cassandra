package org.apache.cassandra.db.index.stratio.query;

import java.util.HashMap;
import java.util.Map;

import org.apache.cassandra.db.index.stratio.schema.CellMapper;
import org.apache.cassandra.db.index.stratio.schema.CellMapperBoolean;
import org.apache.cassandra.db.index.stratio.schema.Schema;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.search.Query;
import org.junit.Assert;
import org.junit.Test;

public class FuzzyConditionTest {

	@Test
	public void testFuzzyQuery() {

		Map<String, CellMapper<?>> map = new HashMap<>();
		map.put("name", new CellMapperBoolean());
		Schema mappers = new Schema(EnglishAnalyzer.class.getName(), map);

		FuzzyCondition fuzzyCondition = new FuzzyCondition(0.5f, "name", "tr", 1, 2, 49, true);
		Query query = fuzzyCondition.query(mappers);

		Assert.assertNotNull(query);
		Assert.assertEquals(org.apache.lucene.search.FuzzyQuery.class, query.getClass());
		org.apache.lucene.search.FuzzyQuery luceneQuery = (org.apache.lucene.search.FuzzyQuery) query;
		Assert.assertEquals("name", luceneQuery.getField());
		Assert.assertEquals("tr", luceneQuery.getTerm().text());
		Assert.assertEquals(1, luceneQuery.getMaxEdits());
		Assert.assertEquals(2, luceneQuery.getPrefixLength());
		Assert.assertEquals(0.5f, query.getBoost(), 0);
	}

}
