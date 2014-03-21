package org.apache.cassandra.db.index.stratio.schema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
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

public class CellMapperBooleanTest {

	@Test()
	public void testValueNull() {
		CellMapperBoolean mapper = new CellMapperBoolean();
		String parsed = mapper.indexValue(null);
		Assert.assertNull(parsed);
	}

	@Test
	public void testValueBooleanTrue() {
		CellMapperBoolean mapper = new CellMapperBoolean();
		String parsed = mapper.indexValue(true);
		Assert.assertEquals("true", parsed);
	}

	@Test
	public void testValueBooleanFalse() {
		CellMapperBoolean mapper = new CellMapperBoolean();
		String parsed = mapper.indexValue(false);
		Assert.assertEquals("false", parsed);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testValueDate() {
		CellMapperBoolean mapper = new CellMapperBoolean();
		mapper.indexValue(new Date());
	}

	@Test(expected = IllegalArgumentException.class)
	public void testValueInteger() {
		CellMapperBoolean mapper = new CellMapperBoolean();
		mapper.indexValue(3);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testValueLong() {
		CellMapperBoolean mapper = new CellMapperBoolean();
		mapper.indexValue(3l);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testValueFloat() {
		CellMapperBoolean mapper = new CellMapperBoolean();
		mapper.indexValue(3.6f);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testValueDouble() {
		CellMapperBoolean mapper = new CellMapperBoolean();
		mapper.indexValue(3.5d);
	}

	@Test
	public void testValueStringTrueLowercase() {
		CellMapperBoolean mapper = new CellMapperBoolean();
		String parsed = mapper.indexValue("true");
		Assert.assertEquals("true", parsed);
	}

	@Test
	public void testValueStringTrueUppercase() {
		CellMapperBoolean mapper = new CellMapperBoolean();
		String parsed = mapper.indexValue("TRUE");
		Assert.assertEquals("true", parsed);
	}

	@Test
	public void testValueStringTrueMixedcase() {
		CellMapperBoolean mapper = new CellMapperBoolean();
		String parsed = mapper.indexValue("TrUe");
		Assert.assertEquals("true", parsed);
	}

	@Test
	public void testValueStringFalseLowercase() {
		CellMapperBoolean mapper = new CellMapperBoolean();
		String parsed = mapper.indexValue("false");
		Assert.assertEquals("false", parsed);
	}

	@Test
	public void testValueStringFalseUppercase() {
		CellMapperBoolean mapper = new CellMapperBoolean();
		String parsed = mapper.indexValue("FALSE");
		Assert.assertEquals("false", parsed);
	}

	@Test
	public void testValueStringFalseMixedcase() {
		CellMapperBoolean mapper = new CellMapperBoolean();
		String parsed = mapper.indexValue("fALsE");
		Assert.assertEquals("false", parsed);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testValueStringInvalid() {
		CellMapperBoolean mapper = new CellMapperBoolean();
		mapper.indexValue("hello");
	}

	@Test(expected = IllegalArgumentException.class)
	public void testValueUUID() {
		CellMapperBoolean mapper = new CellMapperBoolean();
		mapper.indexValue(UUID.fromString("550e8400-e29b-41d4-a716-446655440000"));
	}

	@Test
	public void testField() {
		CellMapperBoolean mapper = new CellMapperBoolean();
		Field field = mapper.field("name", "true");
		Assert.assertNotNull(field);
		Assert.assertEquals("true", field.stringValue());
		Assert.assertEquals("name", field.name());
		Assert.assertFalse(field.fieldType().stored());
	}

	@Test
	public void testMatchQuery() {
		CellMapperBoolean mapper = new CellMapperBoolean();
		MatchQuery matchQuery = new MatchQuery(0.5f, "name", false);
		Query query = mapper.toLucene(matchQuery);
		Assert.assertNotNull(query);
		Assert.assertEquals(TermQuery.class, query.getClass());
		Assert.assertEquals("false", ((TermQuery) query).getTerm().bytes().utf8ToString());
		Assert.assertEquals(0.5f, query.getBoost(), 0);
	}

	@Test
	public void testRangeQueryClose() {
		CellMapperBoolean mapper = new CellMapperBoolean();
		RangeQuery rangeQuery = new RangeQuery(0.5f, "name", true, false, true, false);
		Query query = mapper.toLucene(rangeQuery);
		Assert.assertNotNull(query);
		Assert.assertEquals(TermRangeQuery.class, query.getClass());
		TermRangeQuery termRangeQuery = (TermRangeQuery) query;
		Assert.assertEquals("true", termRangeQuery.getLowerTerm().utf8ToString());
		Assert.assertEquals("false", termRangeQuery.getUpperTerm().utf8ToString());
		Assert.assertEquals(true, termRangeQuery.includesLower());
		Assert.assertEquals(false, termRangeQuery.includesUpper());
		Assert.assertEquals(0.5f, query.getBoost(), 0);
	}

	@Test
	public void testRangeQueryOpen() {
		CellMapperBoolean mapper = new CellMapperBoolean();
		RangeQuery rangeQuery = new RangeQuery(0.5f, "name", "true", null, true, false);
		Query query = mapper.toLucene(rangeQuery);
		Assert.assertNotNull(query);
		Assert.assertEquals(TermRangeQuery.class, query.getClass());
		TermRangeQuery termRangeQuery = (TermRangeQuery) query;
		Assert.assertEquals("true", termRangeQuery.getLowerTerm().utf8ToString());
		Assert.assertNull(termRangeQuery.getUpperTerm());
		Assert.assertEquals(true, termRangeQuery.includesLower());
		Assert.assertEquals(false, termRangeQuery.includesUpper());
		Assert.assertEquals(0.5f, query.getBoost(), 0);
	}

	@Test
	public void testPrefixQuery() {
		CellMapperBoolean mapper = new CellMapperBoolean();
		PrefixQuery prefixQuery = new PrefixQuery(0.5f, "name", "tr");
		Query query = mapper.toLucene(prefixQuery);
		Assert.assertNotNull(query);
		Assert.assertEquals(org.apache.lucene.search.PrefixQuery.class, query.getClass());
		org.apache.lucene.search.PrefixQuery luceneQuery = (org.apache.lucene.search.PrefixQuery) query;
		Assert.assertEquals("name", luceneQuery.getField());
		Assert.assertEquals("tr", luceneQuery.getPrefix().text());
		Assert.assertEquals(0.5f, query.getBoost(), 0);
	}

	@Test
	public void testWildcardQuery() {
		CellMapperBoolean mapper = new CellMapperBoolean();
		WildcardQuery prefixQuery = new WildcardQuery(0.5f, "name", "tr*");
		Query query = mapper.toLucene(prefixQuery);
		Assert.assertNotNull(query);
		Assert.assertEquals(org.apache.lucene.search.WildcardQuery.class, query.getClass());
		org.apache.lucene.search.WildcardQuery luceneQuery = (org.apache.lucene.search.WildcardQuery) query;
		Assert.assertEquals("name", luceneQuery.getField());
		Assert.assertEquals("tr*", luceneQuery.getTerm().text());
		Assert.assertEquals(0.5f, query.getBoost(), 0);
	}

	@Test
	public void testFuzzyQuery() {
		CellMapperBoolean mapper = new CellMapperBoolean();
		FuzzyQuery prefixQuery = new FuzzyQuery(0.5f, "name", "tr", 1, 2, 49, true);
		Query query = mapper.toLucene(prefixQuery);
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
	public void testPhraseQuery() {

		List<Object> values = new ArrayList<>();
		values.add("false");
		values.add(true);

		CellMapperBoolean mapper = new CellMapperBoolean();
		PhraseQuery phraseQuery = new PhraseQuery(0.5f, "name", values, 2);
		mapper.toLucene(phraseQuery);
	}

	@Test
	public void testExtractAnalyzers() {
		CellMapperBoolean mapper = new CellMapperBoolean();
		Analyzer analyzer = mapper.analyzer();
		Assert.assertEquals(CellMapper.EMPTY_ANALYZER, analyzer);
	}

	@Test
	public void testParseJSON() throws IOException {
		String json = "{fields:{age:{type:\"boolean\"}}}";
		CellsMapper cellsMapper = CellsMapper.fromJson(json);
		CellMapper<?> cellMapper = cellsMapper.getMapper("age");
		Assert.assertNotNull(cellMapper);
		Assert.assertEquals(CellMapperBoolean.class, cellMapper.getClass());
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
