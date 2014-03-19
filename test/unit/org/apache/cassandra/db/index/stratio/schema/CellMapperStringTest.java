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

public class CellMapperStringTest {

	@Test()
	public void testValueNull() {
		CellMapperString mapper = new CellMapperString();
		String parsed = mapper.indexValue(null);
		Assert.assertNull(parsed);
	}

	@Test
	public void testValueInteger() {
		CellMapperString mapper = new CellMapperString();
		String parsed = mapper.indexValue(3);
		Assert.assertEquals("3", parsed);
	}

	@Test
	public void testValueLong() {
		CellMapperString mapper = new CellMapperString();
		String parsed = mapper.indexValue(3l);
		Assert.assertEquals("3", parsed);
	}

	@Test
	public void testValueFloatWithoutDecimal() {
		CellMapperString mapper = new CellMapperString();
		String parsed = mapper.indexValue(3f);
		Assert.assertEquals("3.0", parsed);
	}

	@Test
	public void testValueFloatWithDecimalFloor() {
		CellMapperString mapper = new CellMapperString();
		String parsed = mapper.indexValue(3.5f);
		Assert.assertEquals("3.5", parsed);

	}

	@Test
	public void testValueFloatWithDecimalCeil() {
		CellMapperString mapper = new CellMapperString();
		String parsed = mapper.indexValue(3.6f);
		Assert.assertEquals("3.6", parsed);
	}

	@Test
	public void testValueDoubleWithoutDecimal() {
		CellMapperString mapper = new CellMapperString();
		String parsed = mapper.indexValue(3d);
		Assert.assertEquals("3.0", parsed);
	}

	@Test
	public void testValueDoubleWithDecimalFloor() {
		CellMapperString mapper = new CellMapperString();
		String parsed = mapper.indexValue(3.5d);
		Assert.assertEquals("3.5", parsed);

	}

	@Test
	public void testValueDoubleWithDecimalCeil() {
		CellMapperString mapper = new CellMapperString();
		String parsed = mapper.indexValue(3.6d);
		Assert.assertEquals("3.6", parsed);

	}

	@Test
	public void testValueStringWithoutDecimal() {
		CellMapperString mapper = new CellMapperString();
		String parsed = mapper.indexValue("3");
		Assert.assertEquals("3", parsed);
	}

	@Test
	public void testValueStringWithDecimalFloor() {
		CellMapperString mapper = new CellMapperString();
		String parsed = mapper.indexValue("3.2");
		Assert.assertEquals("3.2", parsed);
	}

	@Test
	public void testValueStringWithDecimalCeil() {
		CellMapperString mapper = new CellMapperString();
		String parsed = mapper.indexValue("3.6");
		Assert.assertEquals("3.6", parsed);

	}

	@Test
	public void testValueUUID() {
		CellMapperString mapper = new CellMapperString();
		String parsed = mapper.indexValue(UUID.fromString("550e8400-e29b-41d4-a716-446655440000"));
		Assert.assertEquals("550e8400-e29b-41d4-a716-446655440000", parsed);
	}

	@Test
	public void testField() {
		CellMapperString mapper = new CellMapperString();
		Field field = mapper.field("name", "hello");
		Assert.assertNotNull(field);
		Assert.assertEquals("hello", field.stringValue());
		Assert.assertEquals("name", field.name());
		Assert.assertEquals(false, field.fieldType().stored());
	}

	@Test
	public void testMatchQuery() {
		CellMapperString mapper = new CellMapperString();
		MatchQuery matchQuery = new MatchQuery(0.5f, "name", "hola");
		Query query = mapper.query(matchQuery);
		Assert.assertNotNull(query);
		Assert.assertEquals(TermQuery.class, query.getClass());
		Assert.assertEquals("hola", ((TermQuery) query).getTerm().bytes().utf8ToString());
		Assert.assertEquals(0.5f, query.getBoost(), 0);
	}

	@Test
	public void testRangeQueryClose() {
		CellMapperString mapper = new CellMapperString();
		RangeQuery rangeQuery = new RangeQuery(0.5f, "name", "a", "b", true, false);
		Query query = mapper.query(rangeQuery);
		Assert.assertNotNull(query);
		Assert.assertEquals(TermRangeQuery.class, query.getClass());
		TermRangeQuery termRangeQuery = (TermRangeQuery) query;
		Assert.assertEquals("a", termRangeQuery.getLowerTerm().utf8ToString());
		Assert.assertEquals("b", termRangeQuery.getUpperTerm().utf8ToString());
		Assert.assertEquals(true, termRangeQuery.includesLower());
		Assert.assertEquals(false, termRangeQuery.includesUpper());
		Assert.assertEquals(0.5f, query.getBoost(), 0);
	}

	@Test
	public void testRangeQueryOpen() {
		CellMapperString mapper = new CellMapperString();
		RangeQuery rangeQuery = new RangeQuery(0.5f, "name", "a", null, true, false);
		Query query = mapper.query(rangeQuery);
		Assert.assertNotNull(query);
		Assert.assertEquals(TermRangeQuery.class, query.getClass());
		TermRangeQuery termRangeQuery = (TermRangeQuery) query;
		Assert.assertEquals("a", termRangeQuery.getLowerTerm().utf8ToString());
		Assert.assertNull(termRangeQuery.getUpperTerm());
		Assert.assertEquals(true, termRangeQuery.includesLower());
		Assert.assertEquals(false, termRangeQuery.includesUpper());
		Assert.assertEquals(0.5f, query.getBoost(), 0);
	}

	@Test
	public void testPrefixQuery() {
		CellMapperString mapper = new CellMapperString();
		PrefixQuery prefixQuery = new PrefixQuery(0.5f, "name", "hell");
		Query query = mapper.query(prefixQuery);
		Assert.assertNotNull(query);
		Assert.assertEquals(org.apache.lucene.search.PrefixQuery.class, query.getClass());
		org.apache.lucene.search.PrefixQuery luceneQuery = (org.apache.lucene.search.PrefixQuery) query;
		Assert.assertEquals("name", luceneQuery.getField());
		Assert.assertEquals("hell", luceneQuery.getPrefix().text());
		Assert.assertEquals(0.5f, query.getBoost(), 0);
	}

	@Test
	public void testWildcardQuery() {
		CellMapperString mapper = new CellMapperString();
		WildcardQuery prefixQuery = new WildcardQuery(0.5f, "name", "hell*");
		Query query = mapper.query(prefixQuery);
		Assert.assertNotNull(query);
		Assert.assertEquals(org.apache.lucene.search.WildcardQuery.class, query.getClass());
		org.apache.lucene.search.WildcardQuery luceneQuery = (org.apache.lucene.search.WildcardQuery) query;
		Assert.assertEquals("name", luceneQuery.getField());
		Assert.assertEquals("hell*", luceneQuery.getTerm().text());
		Assert.assertEquals(0.5f, query.getBoost(), 0);
	}

	@Test
	public void testFuzzyQuery() {
		CellMapperString mapper = new CellMapperString();
		FuzzyQuery prefixQuery = new FuzzyQuery(0.5f, "name", "hell", 1, 2, 49, true);
		Query query = mapper.query(prefixQuery);
		Assert.assertNotNull(query);
		Assert.assertEquals(org.apache.lucene.search.FuzzyQuery.class, query.getClass());
		org.apache.lucene.search.FuzzyQuery luceneQuery = (org.apache.lucene.search.FuzzyQuery) query;
		Assert.assertEquals("name", luceneQuery.getField());
		Assert.assertEquals("hell", luceneQuery.getTerm().text());
		Assert.assertEquals(1, luceneQuery.getMaxEdits());
		Assert.assertEquals(2, luceneQuery.getPrefixLength());
		Assert.assertEquals(0.5f, query.getBoost(), 0);

	}

	@Test
	public void testPhraseQuery() {

		List<Object> values = new ArrayList<>();
		values.add("aa");
		values.add("ab");

		CellMapperString mapper = new CellMapperString();
		PhraseQuery phraseQuery = new PhraseQuery(0.5f, "name", values, 2);
		Query query = mapper.query(phraseQuery);
		Assert.assertNotNull(query);
		Assert.assertEquals(org.apache.lucene.search.PhraseQuery.class, query.getClass());
		org.apache.lucene.search.PhraseQuery luceneQuery = (org.apache.lucene.search.PhraseQuery) query;
		Assert.assertEquals(values.size(), luceneQuery.getTerms().length);
	}

	@Test
	public void testExtractAnalyzers() {
		CellMapperString mapper = new CellMapperString();
		Analyzer analyzer = mapper.analyzer();
		Assert.assertEquals(CellMapper.EMPTY_ANALYZER, analyzer);
	}

	@Test
	public void testParseJSON() throws IOException {
		String json = "{fields:{age:{type:\"string\"}}}";
		CellsMapper cellsMapper = CellsMapper.fromJson(json);
		CellMapper<?> cellMapper = cellsMapper.getMapper("age");
		Assert.assertNotNull(cellMapper);
		Assert.assertEquals(CellMapperString.class, cellMapper.getClass());
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
