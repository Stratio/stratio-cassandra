package org.apache.cassandra.db.index.stratio.query;

import java.util.HashMap;
import java.util.Map;

import org.apache.cassandra.db.index.stratio.schema.CellMapper;
import org.apache.cassandra.db.index.stratio.schema.CellMapperDouble;
import org.apache.cassandra.db.index.stratio.schema.CellMapperFloat;
import org.apache.cassandra.db.index.stratio.schema.CellMapperInet;
import org.apache.cassandra.db.index.stratio.schema.CellMapperInteger;
import org.apache.cassandra.db.index.stratio.schema.CellMapperLong;
import org.apache.cassandra.db.index.stratio.schema.CellMapperString;
import org.apache.cassandra.db.index.stratio.schema.CellsMapper;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.search.Query;
import org.junit.Assert;
import org.junit.Test;

public class WildcardQueryTest {

	@Test
	public void testString() {

		Map<String, CellMapper<?>> map = new HashMap<>();
		map.put("name", new CellMapperString());
		CellsMapper mappers = new CellsMapper(EnglishAnalyzer.class.getName(), map);

		WildcardCondition wildcardCondition = new WildcardCondition(0.5f, "name", "tr*");
		Query query = wildcardCondition.query(mappers);

		Assert.assertNotNull(query);
		Assert.assertEquals(org.apache.lucene.search.WildcardQuery.class, query.getClass());
		org.apache.lucene.search.WildcardQuery luceneQuery = (org.apache.lucene.search.WildcardQuery) query;
		Assert.assertEquals("name", luceneQuery.getField());
		Assert.assertEquals("tr*", luceneQuery.getTerm().text());
		Assert.assertEquals(0.5f, query.getBoost(), 0);
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testInteger() {

		Map<String, CellMapper<?>> map = new HashMap<>();
		map.put("name", new CellMapperInteger(1f));
		CellsMapper mappers = new CellsMapper(EnglishAnalyzer.class.getName(), map);

		WildcardCondition wildcardCondition = new WildcardCondition(0.5f, "name", "22*");
		wildcardCondition.query(mappers);
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testLong() {

		Map<String, CellMapper<?>> map = new HashMap<>();
		map.put("name", new CellMapperLong(1f));
		CellsMapper mappers = new CellsMapper(EnglishAnalyzer.class.getName(), map);

		WildcardCondition wildcardCondition = new WildcardCondition(0.5f, "name", 22L);
		wildcardCondition.query(mappers);
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testFloat() {

		Map<String, CellMapper<?>> map = new HashMap<>();
		map.put("name", new CellMapperFloat(1f));
		CellsMapper mappers = new CellsMapper(EnglishAnalyzer.class.getName(), map);

		WildcardCondition wildcardCondition = new WildcardCondition(0.5f, "name", 22F);
		wildcardCondition.query(mappers);
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testDouble() {

		Map<String, CellMapper<?>> map = new HashMap<>();
		map.put("name", new CellMapperDouble(1f));
		CellsMapper mappers = new CellsMapper(EnglishAnalyzer.class.getName(), map);

		WildcardCondition wildcardCondition = new WildcardCondition(0.5f, "name", 22D);
		wildcardCondition.query(mappers);
	}

	@Test
	public void testInetV4() {

		Map<String, CellMapper<?>> map = new HashMap<>();
		map.put("name", new CellMapperInet());
		CellsMapper mappers = new CellsMapper(EnglishAnalyzer.class.getName(), map);

		WildcardCondition wildcardCondition = new WildcardCondition(0.5f, "name", "192.168.*");
		Query query = wildcardCondition.query(mappers);

		Assert.assertNotNull(query);
		Assert.assertEquals(org.apache.lucene.search.WildcardQuery.class, query.getClass());
		org.apache.lucene.search.WildcardQuery luceneQuery = (org.apache.lucene.search.WildcardQuery) query;
		Assert.assertEquals("name", luceneQuery.getField());
		Assert.assertEquals("192.168.*", luceneQuery.getTerm().text());
		Assert.assertEquals(0.5f, query.getBoost(), 0);
	}

	@Test
	public void testInetV6() {

		Map<String, CellMapper<?>> map = new HashMap<>();
		map.put("name", new CellMapperInet());
		CellsMapper mappers = new CellsMapper(EnglishAnalyzer.class.getName(), map);

		WildcardCondition wildcardCondition = new WildcardCondition(0.5f, "name", "2001:db8:2de:0:0:0:0:e*");
		Query query = wildcardCondition.query(mappers);

		Assert.assertNotNull(query);
		Assert.assertEquals(org.apache.lucene.search.WildcardQuery.class, query.getClass());
		org.apache.lucene.search.WildcardQuery luceneQuery = (org.apache.lucene.search.WildcardQuery) query;
		Assert.assertEquals("name", luceneQuery.getField());
		Assert.assertEquals("2001:db8:2de:0:0:0:0:e*", luceneQuery.getTerm().text());
		Assert.assertEquals(0.5f, query.getBoost(), 0);
	}

}
