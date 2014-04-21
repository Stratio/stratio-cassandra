package org.apache.cassandra.db.index.stratio.query;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.db.index.stratio.schema.CellMapper;
import org.apache.cassandra.db.index.stratio.schema.CellMapperBoolean;
import org.apache.cassandra.db.index.stratio.schema.Schema;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.search.Query;
import org.junit.Assert;
import org.junit.Test;

public class PhraseConditionTest {

	@Test
	public void testPhraseQuery() {

		Map<String, CellMapper<?>> map = new HashMap<>();
		map.put("name", new CellMapperBoolean());
		Schema mappers = new Schema(EnglishAnalyzer.class.getName(), map);

		List<String> values = new ArrayList<>();
		values.add("hola");
		values.add("adios");

		PhraseCondition phraseCondition = new PhraseCondition(0.5f, "name", values, 2);
		Query query = phraseCondition.query(mappers);
		Assert.assertNotNull(query);
		Assert.assertEquals(org.apache.lucene.search.PhraseQuery.class, query.getClass());
		org.apache.lucene.search.PhraseQuery luceneQuery = (org.apache.lucene.search.PhraseQuery) query;
		Assert.assertEquals(values.size(), luceneQuery.getTerms().length);
		Assert.assertEquals(2, luceneQuery.getSlop());
		Assert.assertEquals(0.5f, query.getBoost(), 0);
	}

}
