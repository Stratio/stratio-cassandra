package org.apache.cassandra.db.index.stratio.schema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.cassandra.db.index.stratio.query.FuzzyQuery;
import org.apache.cassandra.db.index.stratio.query.MatchQuery;
import org.apache.cassandra.db.index.stratio.query.PhraseQuery;
import org.apache.cassandra.db.index.stratio.query.PrefixQuery;
import org.apache.cassandra.db.index.stratio.query.RangeQuery;
import org.apache.cassandra.db.index.stratio.query.WildcardQuery;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Field;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermRangeQuery;
import org.junit.Assert;
import org.junit.Test;

public class CellMapperUUIDTest {

	@Test()
	public void testValueNull() {
		CellMapperUUID mapper = new CellMapperUUID();
		String parsed = mapper.indexValue(null);
		Assert.assertNull(parsed);
	}

	@Test
	public void testValueUUID() {
		CellMapperUUID mapper = new CellMapperUUID();
		String parsed = mapper.indexValue(UUID.fromString("550e8400-e29b-41d4-a716-446655440000"));
		Assert.assertEquals("550e8400-e29b-41d4-a716-446655440000", parsed);
	}

	@Test
	public void testValueString() {
		CellMapperUUID mapper = new CellMapperUUID();
		String parsed = mapper.indexValue("550e8400-e29b-41d4-a716-446655440000");
		Assert.assertEquals("550e8400-e29b-41d4-a716-446655440000", parsed);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testValueStringInvalid() {
		CellMapperUUID mapper = new CellMapperUUID();
		mapper.indexValue("550e840");
	}

	@Test(expected = IllegalArgumentException.class)
	public void testValueInteger() {
		CellMapperUUID mapper = new CellMapperUUID();
		String parsed = mapper.indexValue(3);
		Assert.assertEquals("3", parsed);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testValueLong() {
		CellMapperUUID mapper = new CellMapperUUID();
		String parsed = mapper.indexValue(3l);
		Assert.assertEquals("3", parsed);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testValueFloat() {
		CellMapperUUID mapper = new CellMapperUUID();
		String parsed = mapper.indexValue(3.6f);
		Assert.assertEquals("3.6", parsed);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testValueDouble() {
		CellMapperUUID mapper = new CellMapperUUID();
		String parsed = mapper.indexValue(3d);
		Assert.assertEquals("3.0", parsed);
	}

	@Test
	public void testField() {
		CellMapperUUID mapper = new CellMapperUUID();
		UUID uuid = UUID.fromString("550e8400-e29b-41d4-a716-446655440000");
		Field field = mapper.field("name", uuid);
		Assert.assertNotNull(field);
		Assert.assertEquals("name", field.name());
		Assert.assertEquals(uuid.toString(), field.stringValue());
		Assert.assertFalse(field.fieldType().stored());
	}

	@Test
	public void testMatchQuery() {
		CellMapperUUID mapper = new CellMapperUUID();
		UUID uuid = UUID.fromString("550e8400-e29b-41d4-a716-446655440000");
		MatchQuery matchQuery = new MatchQuery(0.5f, "name", uuid);
		Query query = mapper.toLucene(matchQuery);
		Assert.assertNotNull(query);
		Assert.assertEquals(TermQuery.class, query.getClass());
		Assert.assertEquals(uuid.toString(), ((TermQuery) query).getTerm().bytes().utf8ToString());
		Assert.assertEquals(0.5f, query.getBoost(), 0);
	}

	@Test
	public void testRangeQueryClose() {
		CellMapperUUID mapper = new CellMapperUUID();
		RangeQuery rangeQuery = new RangeQuery(0.5f,
		                                       "name",
		                                       "46ab3550-ae7c-11e3-a5e2-0800200c9a66",
		                                       UUID.fromString("5007c0f0-ae7c-11e3-a5e2-0800200c9a66"),
		                                       true,
		                                       false);
		Query query = mapper.toLucene(rangeQuery);
		Assert.assertNotNull(query);
		Assert.assertEquals(TermRangeQuery.class, query.getClass());
		TermRangeQuery termRangeQuery = (TermRangeQuery) query;
		Assert.assertEquals("46ab3550-ae7c-11e3-a5e2-0800200c9a66", termRangeQuery.getLowerTerm().utf8ToString());
		Assert.assertEquals("5007c0f0-ae7c-11e3-a5e2-0800200c9a66", termRangeQuery.getUpperTerm().utf8ToString());
		Assert.assertEquals(true, termRangeQuery.includesLower());
		Assert.assertEquals(false, termRangeQuery.includesUpper());
		Assert.assertEquals(0.5f, query.getBoost(), 0);
	}

	@Test
	public void testRangeQueryOpen() {
		CellMapperUUID mapper = new CellMapperUUID();
		RangeQuery rangeQuery = new RangeQuery(0.5f, "name", null, "46ab3550-ae7c-11e3-a5e2-0800200c9a66", true, false);
		Query query = mapper.toLucene(rangeQuery);
		Assert.assertNotNull(query);
		Assert.assertEquals(TermRangeQuery.class, query.getClass());
		TermRangeQuery termRangeQuery = (TermRangeQuery) query;
		Assert.assertNull(termRangeQuery.getLowerTerm());
		Assert.assertEquals("46ab3550-ae7c-11e3-a5e2-0800200c9a66", termRangeQuery.getUpperTerm().utf8ToString());
		Assert.assertEquals(true, termRangeQuery.includesLower());
		Assert.assertEquals(false, termRangeQuery.includesUpper());
		Assert.assertEquals(0.5f, query.getBoost(), 0);
	}

	@Test
	public void testPrefixQuery() {
		CellMapperUUID mapper = new CellMapperUUID();
		PrefixQuery prefixQuery = new PrefixQuery(0.5f, "name", "46ab");
		Query query = mapper.toLucene(prefixQuery);
		Assert.assertNotNull(query);
		Assert.assertEquals(org.apache.lucene.search.PrefixQuery.class, query.getClass());
		org.apache.lucene.search.PrefixQuery luceneQuery = (org.apache.lucene.search.PrefixQuery) query;
		Assert.assertEquals("name", luceneQuery.getField());
		Assert.assertEquals("46ab", luceneQuery.getPrefix().text());
		Assert.assertEquals(0.5f, query.getBoost(), 0);
	}

	@Test
	public void testWildcardQuery() {
		CellMapperUUID mapper = new CellMapperUUID();
		WildcardQuery prefixQuery = new WildcardQuery(0.5f, "name", "46ab*");
		Query query = mapper.toLucene(prefixQuery);
		Assert.assertNotNull(query);
		Assert.assertEquals(org.apache.lucene.search.WildcardQuery.class, query.getClass());
		org.apache.lucene.search.WildcardQuery luceneQuery = (org.apache.lucene.search.WildcardQuery) query;
		Assert.assertEquals("name", luceneQuery.getField());
		Assert.assertEquals("46ab*", luceneQuery.getTerm().text());
		Assert.assertEquals(0.5f, query.getBoost(), 0);
	}

	@Test
	public void testFuzzyQuery() {
		CellMapperUUID mapper = new CellMapperUUID();
		FuzzyQuery prefixQuery = new FuzzyQuery(0.5f, "name", "46ab3550-ae7c-11e3-a5e2-0800200c9a6", 1, 2, 49, true);
		Query query = mapper.toLucene(prefixQuery);
		Assert.assertNotNull(query);
		Assert.assertEquals(org.apache.lucene.search.FuzzyQuery.class, query.getClass());
		org.apache.lucene.search.FuzzyQuery luceneQuery = (org.apache.lucene.search.FuzzyQuery) query;
		Assert.assertEquals("name", luceneQuery.getField());
		Assert.assertEquals("46ab3550-ae7c-11e3-a5e2-0800200c9a6", luceneQuery.getTerm().text());
		Assert.assertEquals(1, luceneQuery.getMaxEdits());
		Assert.assertEquals(2, luceneQuery.getPrefixLength());
		Assert.assertEquals(0.5f, query.getBoost(), 0);

	}

	@Test
	public void testPhraseQuery() {

		List<Object> values = new ArrayList<>();
		values.add("46ab3550-ae7c-11e3-a5e2-0800200c9a66");
		values.add("5007c0f0-ae7c-11e3-a5e2-0800200c9a66");

		CellMapperUUID mapper = new CellMapperUUID();
		PhraseQuery phraseQuery = new PhraseQuery(0.5f, "name", values, 2);
		Query query = mapper.toLucene(phraseQuery);
		Assert.assertNotNull(query);
		Assert.assertEquals(org.apache.lucene.search.PhraseQuery.class, query.getClass());
		org.apache.lucene.search.PhraseQuery luceneQuery = (org.apache.lucene.search.PhraseQuery) query;
		Assert.assertEquals(values.size(), luceneQuery.getTerms().length);
	}

	@Test
	public void testExtractAnalyzers() {
		CellMapperUUID mapper = new CellMapperUUID();
		Analyzer analyzer = mapper.analyzer();
		Assert.assertEquals(CellMapper.EMPTY_ANALYZER, analyzer);
	}

	@Test
	public void testParseJSON() throws IOException {
		String json = "{fields:{age:{type:\"uuid\"}}}";
		CellsMapper cellsMapper = CellsMapper.fromJson(json);
		CellMapper<?> cellMapper = cellsMapper.getMapper("age");
		Assert.assertNotNull(cellMapper);
		Assert.assertEquals(CellMapperUUID.class, cellMapper.getClass());
	}

	@Test
	public void testParseJSONEmpty() throws IOException {
		String json = "{fields:{}}";
		CellsMapper cellsMapper = CellsMapper.fromJson(json);
		CellMapper<?> cellMapper = cellsMapper.getMapper("age");
		Assert.assertNotNull(cellMapper);
		Assert.assertEquals(CellMapperText.class, cellMapper.getClass());
	}

	@Test(expected = IllegalArgumentException.class)
	public void testParseJSONInvalid() throws IOException {
		String json = "{fields:{age:{}}";
		CellsMapper.fromJson(json);
	}
}
