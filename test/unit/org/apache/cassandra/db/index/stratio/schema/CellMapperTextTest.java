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

public class CellMapperTextTest {

	@Test()
	public void testAnalyzerNull() {
		CellMapperText mapper = new CellMapperText(null);
		String parsed = mapper.indexValue(null);
		Assert.assertNull(parsed);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testAnalyzerInvalid() {
		CellMapperText mapper = new CellMapperText("hello");
		String parsed = mapper.indexValue(null);
		Assert.assertNull(parsed);
	}

	@Test()
	public void testValueNull() {
		CellMapperText mapper = new CellMapperText("org.apache.lucene.analysis.en.EnglishAnalyzer");
		String parsed = mapper.indexValue(null);
		Assert.assertNull(parsed);
	}

	@Test
	public void testValueInteger() {
		CellMapperText mapper = new CellMapperText("org.apache.lucene.analysis.en.EnglishAnalyzer");
		String parsed = mapper.indexValue(3);
		Assert.assertEquals("3", parsed);
	}

	@Test
	public void testValueLong() {
		CellMapperText mapper = new CellMapperText("org.apache.lucene.analysis.en.EnglishAnalyzer");
		String parsed = mapper.indexValue(3l);
		Assert.assertEquals("3", parsed);
	}

	@Test
	public void testValueFloatWithoutDecimal() {
		CellMapperText mapper = new CellMapperText("org.apache.lucene.analysis.en.EnglishAnalyzer");
		String parsed = mapper.indexValue(3f);
		Assert.assertEquals("3.0", parsed);
	}

	@Test
	public void testValueFloatWithDecimalFloor() {
		CellMapperText mapper = new CellMapperText("org.apache.lucene.analysis.en.EnglishAnalyzer");
		String parsed = mapper.indexValue(3.5f);
		Assert.assertEquals("3.5", parsed);

	}

	@Test
	public void testValueFloatWithDecimalCeil() {
		CellMapperText mapper = new CellMapperText("org.apache.lucene.analysis.en.EnglishAnalyzer");
		String parsed = mapper.indexValue(3.6f);
		Assert.assertEquals("3.6", parsed);
	}

	@Test
	public void testValueDoubleWithoutDecimal() {
		CellMapperText mapper = new CellMapperText("org.apache.lucene.analysis.en.EnglishAnalyzer");
		String parsed = mapper.indexValue(3d);
		Assert.assertEquals("3.0", parsed);
	}

	@Test
	public void testValueDoubleWithDecimalFloor() {
		CellMapperText mapper = new CellMapperText("org.apache.lucene.analysis.en.EnglishAnalyzer");
		String parsed = mapper.indexValue(3.5d);
		Assert.assertEquals("3.5", parsed);

	}

	@Test
	public void testValueDoubleWithDecimalCeil() {
		CellMapperText mapper = new CellMapperText("org.apache.lucene.analysis.en.EnglishAnalyzer");
		String parsed = mapper.indexValue(3.6d);
		Assert.assertEquals("3.6", parsed);

	}

	@Test
	public void testValueStringWithoutDecimal() {
		CellMapperText mapper = new CellMapperText("org.apache.lucene.analysis.en.EnglishAnalyzer");
		String parsed = mapper.indexValue("3");
		Assert.assertEquals("3", parsed);
	}

	@Test
	public void testValueStringWithDecimalFloor() {
		CellMapperText mapper = new CellMapperText("org.apache.lucene.analysis.en.EnglishAnalyzer");
		String parsed = mapper.indexValue("3.2");
		Assert.assertEquals("3.2", parsed);
	}

	@Test
	public void testValueStringWithDecimalCeil() {
		CellMapperText mapper = new CellMapperText("org.apache.lucene.analysis.en.EnglishAnalyzer");
		String parsed = mapper.indexValue("3.6");
		Assert.assertEquals("3.6", parsed);

	}

	@Test
	public void testValueUUID() {
		CellMapperText mapper = new CellMapperText("org.apache.lucene.analysis.en.EnglishAnalyzer");
		String parsed = mapper.indexValue(UUID.fromString("550e8400-e29b-41d4-a716-446655440000"));
		Assert.assertEquals("550e8400-e29b-41d4-a716-446655440000", parsed);
	}

	@Test
	public void testField() {
		CellMapperText mapper = new CellMapperText("org.apache.lucene.analysis.en.EnglishAnalyzer");
		Field field = mapper.field("name", "hello");
		Assert.assertNotNull(field);
		Assert.assertEquals("hello", field.stringValue());
		Assert.assertEquals("name", field.name());
		Assert.assertEquals(false, field.fieldType().stored());
	}

	@Test
	public void testMatchQuery() {
		CellMapperText mapper = new CellMapperText("org.apache.lucene.analysis.en.EnglishAnalyzer");
		MatchQuery matchQuery = new MatchQuery(0.5f, "name", "hola");
		Query query = mapper.toLucene(matchQuery);
		Assert.assertNotNull(query);
		Assert.assertEquals(TermQuery.class, query.getClass());
		Assert.assertEquals("hola", ((TermQuery) query).getTerm().bytes().utf8ToString());
		Assert.assertEquals(0.5f, query.getBoost(), 0);
	}

	@Test
	public void testRangeQueryClose() {
		CellMapperText mapper = new CellMapperText("org.apache.lucene.analysis.en.EnglishAnalyzer");
		RangeQuery rangeQuery = new RangeQuery(0.5f, "name", "a", "b", true, false);
		Query query = mapper.toLucene(rangeQuery);
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
		CellMapperText mapper = new CellMapperText("org.apache.lucene.analysis.en.EnglishAnalyzer");
		RangeQuery rangeQuery = new RangeQuery(0.5f, "name", "a", null, true, false);
		Query query = mapper.toLucene(rangeQuery);
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
		CellMapperText mapper = new CellMapperText("org.apache.lucene.analysis.en.EnglishAnalyzer");
		PrefixQuery prefixQuery = new PrefixQuery(0.5f, "name", "hell");
		Query query = mapper.toLucene(prefixQuery);
		Assert.assertNotNull(query);
		Assert.assertEquals(org.apache.lucene.search.PrefixQuery.class, query.getClass());
		org.apache.lucene.search.PrefixQuery luceneQuery = (org.apache.lucene.search.PrefixQuery) query;
		Assert.assertEquals("name", luceneQuery.getField());
		Assert.assertEquals("hell", luceneQuery.getPrefix().text());
		Assert.assertEquals(0.5f, query.getBoost(), 0);
	}

	@Test
	public void testWildcardQuery() {
		CellMapperText mapper = new CellMapperText("org.apache.lucene.analysis.en.EnglishAnalyzer");
		WildcardQuery prefixQuery = new WildcardQuery(0.5f, "name", "hell*");
		Query query = mapper.toLucene(prefixQuery);
		Assert.assertNotNull(query);
		Assert.assertEquals(org.apache.lucene.search.WildcardQuery.class, query.getClass());
		org.apache.lucene.search.WildcardQuery luceneQuery = (org.apache.lucene.search.WildcardQuery) query;
		Assert.assertEquals("name", luceneQuery.getField());
		Assert.assertEquals("hell*", luceneQuery.getTerm().text());
		Assert.assertEquals(0.5f, query.getBoost(), 0);
	}

	@Test
	public void testFuzzyQuery() {
		CellMapperText mapper = new CellMapperText("org.apache.lucene.analysis.en.EnglishAnalyzer");
		FuzzyQuery prefixQuery = new FuzzyQuery(0.5f, "name", "hell", 1, 2, 49, true);
		Query query = mapper.toLucene(prefixQuery);
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

		CellMapperText mapper = new CellMapperText("org.apache.lucene.analysis.en.EnglishAnalyzer");
		PhraseQuery phraseQuery = new PhraseQuery(0.5f, "name", values, 2);
		Query query = mapper.toLucene(phraseQuery);
		Assert.assertNotNull(query);
		Assert.assertEquals(org.apache.lucene.search.PhraseQuery.class, query.getClass());
		org.apache.lucene.search.PhraseQuery luceneQuery = (org.apache.lucene.search.PhraseQuery) query;
		Assert.assertEquals(values.size(), luceneQuery.getTerms().length);
	}

	@Test
	public void testExtractAnalyzers() {
		CellMapperText mapper = new CellMapperText("org.apache.lucene.analysis.en.EnglishAnalyzer");
		Analyzer analyzer = mapper.analyzer();
		Assert.assertEquals(org.apache.lucene.analysis.en.EnglishAnalyzer.class, analyzer.getClass());
	}

	@Test
	public void testParseJSONWithAnayzer() throws IOException {
		String json = "{fields:{age:{type:\"text\", analyzer:\"org.apache.lucene.analysis.en.EnglishAnalyzer\"}}}";
		CellsMapper cellsMapper = CellsMapper.fromJson(json);
		CellMapper<?> cellMapper = cellsMapper.getMapper("age");
		Assert.assertNotNull(cellMapper);
		Assert.assertEquals(CellMapperText.class, cellMapper.getClass());
	}

	@Test
	public void testParseJSONWithoutAnalyzer() throws IOException {
		String json = "{fields:{age:{type:\"text\"}}}";
		CellsMapper cellsMapper = CellsMapper.fromJson(json);
		CellMapper<?> cellMapper = cellsMapper.getMapper("age");
		Assert.assertNotNull(cellMapper);
		Assert.assertEquals(CellMapperText.class, cellMapper.getClass());
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
