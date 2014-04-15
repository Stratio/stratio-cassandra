package org.apache.cassandra.db.index.stratio.query;

import java.util.HashMap;
import java.util.Map;

import org.apache.cassandra.db.index.stratio.schema.CellMapper;
import org.apache.cassandra.db.index.stratio.schema.CellMapperBoolean;
import org.apache.cassandra.db.index.stratio.schema.CellMapperDouble;
import org.apache.cassandra.db.index.stratio.schema.CellMapperFloat;
import org.apache.cassandra.db.index.stratio.schema.CellMapperInteger;
import org.apache.cassandra.db.index.stratio.schema.CellMapperLong;
import org.apache.cassandra.db.index.stratio.schema.Schema;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.search.Query;
import org.junit.Assert;
import org.junit.Test;

public class FuzzyQueryTest {

	@Test
	public void testFuzzyQuery() {

		Map<String, CellMapper<?>> map = new HashMap<>();
		map.put("name", new CellMapperBoolean());
		Schema mappers = new Schema(EnglishAnalyzer.class.getName(), map);

		FuzzyCondition fuzzyCondition = new FuzzyCondition(mappers, 0.5f, "name", "tr", 1, 2, 49, true);
		Query query = fuzzyCondition.query();

		Assert.assertNotNull(query);
		Assert.assertEquals(org.apache.lucene.search.FuzzyQuery.class, query.getClass());
		org.apache.lucene.search.FuzzyQuery luceneQuery = (org.apache.lucene.search.FuzzyQuery) query;
		Assert.assertEquals("name", luceneQuery.getField());
		Assert.assertEquals("tr", luceneQuery.getTerm().text());
		Assert.assertEquals(1, luceneQuery.getMaxEdits());
		Assert.assertEquals(2, luceneQuery.getPrefixLength());
		Assert.assertEquals(0.5f, query.getBoost(), 0);
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testInteger() {

		Map<String, CellMapper<?>> map = new HashMap<>();
		map.put("name", new CellMapperInteger(1f));
		Schema mappers = new Schema(EnglishAnalyzer.class.getName(), map);

		FuzzyCondition fuzzyCondition = new FuzzyCondition(mappers, 0.5f, "name", 42, 1, 2, 49, true);
		fuzzyCondition.query();
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testLong() {

		Map<String, CellMapper<?>> map = new HashMap<>();
		map.put("name", new CellMapperLong(1f));
		Schema mappers = new Schema(EnglishAnalyzer.class.getName(), map);

		FuzzyCondition fuzzyCondition = new FuzzyCondition(mappers, 0.5f, "name", 42L, 1, 2, 49, true);
		fuzzyCondition.query();
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testFloat() {

		Map<String, CellMapper<?>> map = new HashMap<>();
		map.put("name", new CellMapperFloat(1f));
		Schema mappers = new Schema(EnglishAnalyzer.class.getName(), map);

		FuzzyCondition fuzzyCondition = new FuzzyCondition(mappers, 0.5f, "name", 42F, 1, 2, 49, true);
		fuzzyCondition.query();
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testDouble() {

		Map<String, CellMapper<?>> map = new HashMap<>();
		map.put("name", new CellMapperDouble(1f));
		Schema mappers = new Schema(EnglishAnalyzer.class.getName(), map);

		FuzzyCondition fuzzyCondition = new FuzzyCondition(mappers, 0.5f, "name", 42D, 1, 2, 49, true);
		fuzzyCondition.query();
	}

}
