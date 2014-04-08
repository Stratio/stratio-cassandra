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
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.junit.Assert;
import org.junit.Test;

public class PrefixQueryTest {

	@Test
	public void testString() {

		Map<String, CellMapper<?>> map = new HashMap<>();
		map.put("name", new CellMapperString());
		CellsMapper mappers = new CellsMapper(EnglishAnalyzer.class.getName(), map);

		PrefixCondition prefixCondition = new PrefixCondition(0.5f, "name", "tr");
		Query query = prefixCondition.query(mappers);

		Assert.assertNotNull(query);
		Assert.assertEquals(PrefixQuery.class, query.getClass());
		PrefixQuery luceneQuery = (PrefixQuery) query;
		Assert.assertEquals("name", luceneQuery.getField());
		Assert.assertEquals("tr", luceneQuery.getPrefix().text());
		Assert.assertEquals(0.5f, query.getBoost(), 0);
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testInteger() {

		Map<String, CellMapper<?>> map = new HashMap<>();
		map.put("name", new CellMapperInteger(1f));
		CellsMapper mappers = new CellsMapper(EnglishAnalyzer.class.getName(), map);

		PrefixCondition prefixCondition = new PrefixCondition(0.5f, "name", "2*");
		prefixCondition.query(mappers);
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testLong() {

		Map<String, CellMapper<?>> map = new HashMap<>();
		map.put("name", new CellMapperLong(1f));
		CellsMapper mappers = new CellsMapper(EnglishAnalyzer.class.getName(), map);

		PrefixCondition prefixCondition = new PrefixCondition(0.5f, "name", 22L);
		prefixCondition.query(mappers);
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testFloat() {

		Map<String, CellMapper<?>> map = new HashMap<>();
		map.put("name", new CellMapperFloat(1f));
		CellsMapper mappers = new CellsMapper(EnglishAnalyzer.class.getName(), map);

		PrefixCondition prefixCondition = new PrefixCondition(0.5f, "name", 22F);
		prefixCondition.query(mappers);
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testDouble() {

		Map<String, CellMapper<?>> map = new HashMap<>();
		map.put("name", new CellMapperDouble(1f));
		CellsMapper mappers = new CellsMapper(EnglishAnalyzer.class.getName(), map);

		PrefixCondition prefixCondition = new PrefixCondition(0.5f, "name", 22D);
		prefixCondition.query(mappers);
	}

	@Test
	public void testInetV4() {

		Map<String, CellMapper<?>> map = new HashMap<>();
		map.put("name", new CellMapperInet());
		CellsMapper mappers = new CellsMapper(EnglishAnalyzer.class.getName(), map);

		PrefixCondition wildcardCondition = new PrefixCondition(0.5f, "name", "192.168.");
		Query query = wildcardCondition.query(mappers);

		Assert.assertNotNull(query);
		Assert.assertEquals(PrefixQuery.class, query.getClass());
		PrefixQuery luceneQuery = (PrefixQuery) query;
		Assert.assertEquals("name", luceneQuery.getField());
		Assert.assertEquals("192.168.", luceneQuery.getPrefix().text());
		Assert.assertEquals(0.5f, query.getBoost(), 0);
	}

	@Test
	public void testInetV6() {

		Map<String, CellMapper<?>> map = new HashMap<>();
		map.put("name", new CellMapperInet());
		CellsMapper mappers = new CellsMapper(EnglishAnalyzer.class.getName(), map);

		PrefixCondition wildcardCondition = new PrefixCondition(0.5f, "name", "2001:db8:2de:0:0:0:0:e");
		Query query = wildcardCondition.query(mappers);

		Assert.assertNotNull(query);
		Assert.assertEquals(PrefixQuery.class, query.getClass());
		PrefixQuery luceneQuery = (PrefixQuery) query;
		Assert.assertEquals("name", luceneQuery.getField());
		Assert.assertEquals("2001:db8:2de:0:0:0:0:e", luceneQuery.getPrefix().text());
		Assert.assertEquals(0.5f, query.getBoost(), 0);
	}

}
