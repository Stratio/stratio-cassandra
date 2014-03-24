package org.apache.cassandra.db.index.stratio.query;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.db.index.stratio.schema.CellMapper;
import org.apache.cassandra.db.index.stratio.schema.CellMapperBoolean;
import org.apache.cassandra.db.index.stratio.schema.CellMapperDouble;
import org.apache.cassandra.db.index.stratio.schema.CellMapperFloat;
import org.apache.cassandra.db.index.stratio.schema.CellMapperInteger;
import org.apache.cassandra.db.index.stratio.schema.CellMapperLong;
import org.apache.cassandra.db.index.stratio.schema.CellsMapper;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.search.Query;
import org.junit.Assert;
import org.junit.Test;

public class PhraseQueryTest {

	@Test
	public void testPhraseQuery() {

		Map<String, CellMapper<?>> map = new HashMap<>();
		map.put("name", new CellMapperBoolean());
		CellsMapper mappers = new CellsMapper(EnglishAnalyzer.class.getName(), map);

		List<Object> values = new ArrayList<>();
		values.add("false");
		values.add(true);

		PhraseCondition phraseCondition = new PhraseCondition(0.5f, "name", values, 2);
		Query query = phraseCondition.query(mappers);
		Assert.assertNotNull(query);
		Assert.assertEquals(org.apache.lucene.search.PhraseQuery.class, query.getClass());
		org.apache.lucene.search.PhraseQuery luceneQuery = (org.apache.lucene.search.PhraseQuery) query;
		Assert.assertEquals(values.size(), luceneQuery.getTerms().length);
		Assert.assertEquals(2, luceneQuery.getSlop());
		Assert.assertEquals(0.5f, query.getBoost(), 0);
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testInteger() {

		Map<String, CellMapper<?>> map = new HashMap<>();
		map.put("name", new CellMapperInteger(1f));
		CellsMapper mappers = new CellsMapper(EnglishAnalyzer.class.getName(), map);

		List<Object> values = new ArrayList<>();
		values.add(42);

		PhraseCondition phraseCondition = new PhraseCondition(0.5f, "name", values, 2);
		phraseCondition.query(mappers);
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testLong() {

		Map<String, CellMapper<?>> map = new HashMap<>();
		map.put("name", new CellMapperLong(1f));
		CellsMapper mappers = new CellsMapper(EnglishAnalyzer.class.getName(), map);

		List<Object> values = new ArrayList<>();
		values.add(42L);

		PhraseCondition phraseCondition = new PhraseCondition(0.5f, "name", values, 2);
		phraseCondition.query(mappers);
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testFloat() {

		Map<String, CellMapper<?>> map = new HashMap<>();
		map.put("name", new CellMapperFloat(1f));
		CellsMapper mappers = new CellsMapper(EnglishAnalyzer.class.getName(), map);

		List<Object> values = new ArrayList<>();
		values.add(42F);

		PhraseCondition phraseCondition = new PhraseCondition(0.5f, "name", values, 2);
		phraseCondition.query(mappers);
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testDouble() {

		Map<String, CellMapper<?>> map = new HashMap<>();
		map.put("name", new CellMapperDouble(1f));
		CellsMapper mappers = new CellsMapper(EnglishAnalyzer.class.getName(), map);

		List<Object> values = new ArrayList<>();
		values.add(42D);

		PhraseCondition phraseCondition = new PhraseCondition(0.5f, "name", values, 2);
		phraseCondition.query(mappers);
	}

}
