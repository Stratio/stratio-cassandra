package org.apache.cassandra.db.index.stratio.query;

import java.util.HashMap;
import java.util.Map;

import org.apache.cassandra.db.index.stratio.schema.CellMapper;
import org.apache.cassandra.db.index.stratio.schema.CellMapperBoolean;
import org.apache.cassandra.db.index.stratio.schema.CellMapperDouble;
import org.apache.cassandra.db.index.stratio.schema.CellMapperFloat;
import org.apache.cassandra.db.index.stratio.schema.CellMapperInteger;
import org.apache.cassandra.db.index.stratio.schema.CellMapperLong;
import org.apache.cassandra.db.index.stratio.schema.CellsMapper;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermRangeQuery;
import org.junit.Assert;
import org.junit.Test;

public class RangeQueryTest {

	@Test
	public void testStringClose() {

		Map<String, CellMapper<?>> map = new HashMap<>();
		map.put("name", new CellMapperBoolean());
		CellsMapper mappers = new CellsMapper(EnglishAnalyzer.class.getName(), map);

		RangeCondition rangeCondition = new RangeCondition(0.5f, "name", "alpha", "beta", true, true);
		Query query = rangeCondition.query(mappers);

		Assert.assertNotNull(query);
		Assert.assertEquals(TermRangeQuery.class, query.getClass());
		Assert.assertEquals("name", ((TermRangeQuery) query).getField());
		Assert.assertEquals("alpha", ((TermRangeQuery) query).getLowerTerm().utf8ToString());
		Assert.assertEquals("beta", ((TermRangeQuery) query).getUpperTerm().utf8ToString());
		Assert.assertEquals(true, ((TermRangeQuery) query).includesLower());
		Assert.assertEquals(true, ((TermRangeQuery) query).includesUpper());
		Assert.assertEquals(0.5f, query.getBoost(), 0);
	}

	@Test
	public void testStringOpen() {

		Map<String, CellMapper<?>> map = new HashMap<>();
		map.put("name", new CellMapperBoolean());
		CellsMapper mappers = new CellsMapper(EnglishAnalyzer.class.getName(), map);

		RangeCondition rangeCondition = new RangeCondition(0.5f, "name", "alpha", null, true, false);
		Query query = rangeCondition.query(mappers);

		Assert.assertNotNull(query);
		Assert.assertEquals(TermRangeQuery.class, query.getClass());
		Assert.assertEquals("name", ((TermRangeQuery) query).getField());
		Assert.assertEquals("alpha", ((TermRangeQuery) query).getLowerTerm().utf8ToString());
		Assert.assertEquals(null, ((TermRangeQuery) query).getUpperTerm());
		Assert.assertNull(((TermRangeQuery) query).getUpperTerm());
		Assert.assertEquals(true, ((TermRangeQuery) query).includesLower());
		Assert.assertEquals(false, ((TermRangeQuery) query).includesUpper());
		Assert.assertEquals(0.5f, query.getBoost(), 0);
	}

	@Test
	public void testIntegerClose() {

		Map<String, CellMapper<?>> map = new HashMap<>();
		map.put("name", new CellMapperInteger(1f));
		CellsMapper mappers = new CellsMapper(EnglishAnalyzer.class.getName(), map);

		RangeCondition rangeCondition = new RangeCondition(0.5f, "name", 42, 43, false, false);
		Query query = rangeCondition.query(mappers);

		Assert.assertNotNull(query);
		Assert.assertEquals(NumericRangeQuery.class, query.getClass());
		Assert.assertEquals("name", ((NumericRangeQuery<?>) query).getField());
		Assert.assertEquals(42, ((NumericRangeQuery<?>) query).getMin());
		Assert.assertEquals(43, ((NumericRangeQuery<?>) query).getMax());
		Assert.assertEquals(false, ((NumericRangeQuery<?>) query).includesMin());
		Assert.assertEquals(false, ((NumericRangeQuery<?>) query).includesMax());
		Assert.assertEquals(0.5f, query.getBoost(), 0);
	}

	@Test
	public void testIntegerOpen() {

		Map<String, CellMapper<?>> map = new HashMap<>();
		map.put("name", new CellMapperInteger(1f));
		CellsMapper mappers = new CellsMapper(EnglishAnalyzer.class.getName(), map);

		RangeCondition rangeCondition = new RangeCondition(0.5f, "name", 42, null, true, false);
		Query query = rangeCondition.query(mappers);

		Assert.assertNotNull(query);
		Assert.assertEquals(NumericRangeQuery.class, query.getClass());
		Assert.assertEquals("name", ((NumericRangeQuery<?>) query).getField());
		Assert.assertEquals(42, ((NumericRangeQuery<?>) query).getMin());
		Assert.assertEquals(null, ((NumericRangeQuery<?>) query).getMax());
		Assert.assertEquals(true, ((NumericRangeQuery<?>) query).includesMin());
		Assert.assertEquals(false, ((NumericRangeQuery<?>) query).includesMax());
		Assert.assertEquals(0.5f, query.getBoost(), 0);
	}

	@Test
	public void testLongClose() {

		Map<String, CellMapper<?>> map = new HashMap<>();
		map.put("name", new CellMapperLong(1f));
		CellsMapper mappers = new CellsMapper(EnglishAnalyzer.class.getName(), map);

		RangeCondition rangeCondition = new RangeCondition(0.5f, "name", 42L, 43, false, false);
		Query query = rangeCondition.query(mappers);

		Assert.assertNotNull(query);
		Assert.assertEquals(NumericRangeQuery.class, query.getClass());
		Assert.assertEquals("name", ((NumericRangeQuery<?>) query).getField());
		Assert.assertEquals(42L, ((NumericRangeQuery<?>) query).getMin());
		Assert.assertEquals(43L, ((NumericRangeQuery<?>) query).getMax());
		Assert.assertEquals(false, ((NumericRangeQuery<?>) query).includesMin());
		Assert.assertEquals(false, ((NumericRangeQuery<?>) query).includesMax());
		Assert.assertEquals(0.5f, query.getBoost(), 0);
	}

	@Test
	public void testLongOpen() {

		Map<String, CellMapper<?>> map = new HashMap<>();
		map.put("name", new CellMapperLong(1f));
		CellsMapper mappers = new CellsMapper(EnglishAnalyzer.class.getName(), map);

		RangeCondition rangeCondition = new RangeCondition(0.5f, "name", 42f, null, true, false);
		Query query = rangeCondition.query(mappers);

		Assert.assertNotNull(query);
		Assert.assertEquals(NumericRangeQuery.class, query.getClass());
		Assert.assertEquals("name", ((NumericRangeQuery<?>) query).getField());
		Assert.assertEquals(42L, ((NumericRangeQuery<?>) query).getMin());
		Assert.assertEquals(null, ((NumericRangeQuery<?>) query).getMax());
		Assert.assertEquals(true, ((NumericRangeQuery<?>) query).includesMin());
		Assert.assertEquals(false, ((NumericRangeQuery<?>) query).includesMax());
		Assert.assertEquals(0.5f, query.getBoost(), 0);
	}

	@Test
	public void testFloatClose() {

		Map<String, CellMapper<?>> map = new HashMap<>();
		map.put("name", new CellMapperFloat(1f));
		CellsMapper mappers = new CellsMapper(EnglishAnalyzer.class.getName(), map);

		RangeCondition rangeCondition = new RangeCondition(0.5f, "name", 42.42D, 43.42F, false, false);
		Query query = rangeCondition.query(mappers);

		Assert.assertNotNull(query);
		Assert.assertEquals(NumericRangeQuery.class, query.getClass());
		Assert.assertEquals("name", ((NumericRangeQuery<?>) query).getField());
		Assert.assertEquals(42.42F, ((NumericRangeQuery<?>) query).getMin());
		Assert.assertEquals(43.42f, ((NumericRangeQuery<?>) query).getMax());
		Assert.assertEquals(false, ((NumericRangeQuery<?>) query).includesMin());
		Assert.assertEquals(false, ((NumericRangeQuery<?>) query).includesMax());
		Assert.assertEquals(0.5f, query.getBoost(), 0);
	}

	@Test
	public void testFloatOpen() {

		Map<String, CellMapper<?>> map = new HashMap<>();
		map.put("name", new CellMapperFloat(1f));
		CellsMapper mappers = new CellsMapper(EnglishAnalyzer.class.getName(), map);

		RangeCondition rangeCondition = new RangeCondition(0.5f, "name", 42.42f, null, true, false);
		Query query = rangeCondition.query(mappers);

		Assert.assertNotNull(query);
		Assert.assertEquals(NumericRangeQuery.class, query.getClass());
		Assert.assertEquals("name", ((NumericRangeQuery<?>) query).getField());
		Assert.assertEquals(42.42f, ((NumericRangeQuery<?>) query).getMin());
		Assert.assertEquals(null, ((NumericRangeQuery<?>) query).getMax());
		Assert.assertEquals(true, ((NumericRangeQuery<?>) query).includesMin());
		Assert.assertEquals(false, ((NumericRangeQuery<?>) query).includesMax());
		Assert.assertEquals(0.5f, query.getBoost(), 0);
	}

	@Test
	public void testDoubleClose() {

		Map<String, CellMapper<?>> map = new HashMap<>();
		map.put("name", new CellMapperDouble(1f));
		CellsMapper mappers = new CellsMapper(EnglishAnalyzer.class.getName(), map);

		RangeCondition rangeCondition = new RangeCondition(0.5f, "name", 42.42D, 43.42D, false, false);
		Query query = rangeCondition.query(mappers);

		Assert.assertNotNull(query);
		Assert.assertEquals(NumericRangeQuery.class, query.getClass());
		Assert.assertEquals("name", ((NumericRangeQuery<?>) query).getField());
		Assert.assertEquals(42.42D, ((NumericRangeQuery<?>) query).getMin());
		Assert.assertEquals(43.42D, ((NumericRangeQuery<?>) query).getMax());
		Assert.assertEquals(false, ((NumericRangeQuery<?>) query).includesMin());
		Assert.assertEquals(false, ((NumericRangeQuery<?>) query).includesMax());
		Assert.assertEquals(0.5f, query.getBoost(), 0);
	}

	@Test
	public void testDoubleOpen() {

		Map<String, CellMapper<?>> map = new HashMap<>();
		map.put("name", new CellMapperDouble(1f));
		CellsMapper mappers = new CellsMapper(EnglishAnalyzer.class.getName(), map);

		RangeCondition rangeCondition = new RangeCondition(0.5f, "name", 42.42D, null, true, false);
		Query query = rangeCondition.query(mappers);

		Assert.assertNotNull(query);
		Assert.assertEquals(NumericRangeQuery.class, query.getClass());
		Assert.assertEquals("name", ((NumericRangeQuery<?>) query).getField());
		Assert.assertEquals(42.42D, ((NumericRangeQuery<?>) query).getMin());
		Assert.assertEquals(null, ((NumericRangeQuery<?>) query).getMax());
		Assert.assertEquals(true, ((NumericRangeQuery<?>) query).includesMin());
		Assert.assertEquals(false, ((NumericRangeQuery<?>) query).includesMax());
		Assert.assertEquals(0.5f, query.getBoost(), 0);
	}

}
