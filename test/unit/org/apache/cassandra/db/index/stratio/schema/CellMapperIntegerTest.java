package org.apache.cassandra.db.index.stratio.schema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.db.index.stratio.query.FuzzyQuery;
import org.apache.cassandra.db.index.stratio.query.MatchQuery;
import org.apache.cassandra.db.index.stratio.query.PhraseQuery;
import org.apache.cassandra.db.index.stratio.query.PrefixQuery;
import org.apache.cassandra.db.index.stratio.query.RangeQuery;
import org.apache.cassandra.db.index.stratio.query.WildcardQuery;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Field;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.Query;
import org.junit.Assert;
import org.junit.Test;

public class CellMapperIntegerTest {

	@Test()
	public void testValueNull() {
		CellMapperInteger mapper = new CellMapperInteger(1f);
		Integer parsed = mapper.indexValue(null);
		Assert.assertNull(parsed);
	}

	@Test
	public void testValueInteger() {
		CellMapperInteger mapper = new CellMapperInteger(1f);
		Integer parsed = mapper.indexValue(3);
		Assert.assertEquals(Integer.valueOf(3), parsed);
	}

	@Test
	public void testValueLong() {
		CellMapperInteger mapper = new CellMapperInteger(1f);
		Integer parsed = mapper.indexValue(3l);
		Assert.assertEquals(Integer.valueOf(3), parsed);
	}

	@Test
	public void testValueFloatWithoutDecimal() {
		CellMapperInteger mapper = new CellMapperInteger(1f);
		Integer parsed = mapper.indexValue(3f);
		Assert.assertEquals(Integer.valueOf(3), parsed);
	}

	@Test
	public void testValueFloatWithDecimalFloor() {
		CellMapperInteger mapper = new CellMapperInteger(1f);
		Integer parsed = mapper.indexValue(3.5f);
		Assert.assertEquals(Integer.valueOf(3), parsed);

	}

	@Test
	public void testValueFloatWithDecimalCeil() {
		CellMapperInteger mapper = new CellMapperInteger(1f);
		Integer parsed = mapper.indexValue(3.6f);
		Assert.assertEquals(Integer.valueOf(3), parsed);

	}

	@Test
	public void testValueDoubleWithoutDecimal() {
		CellMapperInteger mapper = new CellMapperInteger(1f);
		Integer parsed = mapper.indexValue(3d);
		Assert.assertEquals(Integer.valueOf(3), parsed);
	}

	@Test
	public void testValueDoubleWithDecimalFloor() {
		CellMapperInteger mapper = new CellMapperInteger(1f);
		Integer parsed = mapper.indexValue(3.5d);
		Assert.assertEquals(Integer.valueOf(3), parsed);

	}

	@Test
	public void testValueDoubleWithDecimalCeil() {
		CellMapperInteger mapper = new CellMapperInteger(1f);
		Integer parsed = mapper.indexValue(3.6d);
		Assert.assertEquals(Integer.valueOf(3), parsed);

	}

	@Test
	public void testValueStringWithoutDecimal() {
		CellMapperInteger mapper = new CellMapperInteger(1f);
		Integer parsed = mapper.indexValue("3");
		Assert.assertEquals(Integer.valueOf(3), parsed);
	}

	@Test
	public void testValueStringWithDecimalFloor() {
		CellMapperInteger mapper = new CellMapperInteger(1f);
		Integer parsed = mapper.indexValue("3.2");
		Assert.assertEquals(Integer.valueOf(3), parsed);

	}

	@Test
	public void testValueStringWithDecimalCeil() {
		CellMapperInteger mapper = new CellMapperInteger(1f);
		Integer parsed = mapper.indexValue("3.2");
		Assert.assertEquals(Integer.valueOf(3), parsed);

	}

	@Test
	public void testField() {
		CellMapperInteger mapper = new CellMapperInteger(1f);
		Field field = mapper.field("name", "3.2");
		Assert.assertNotNull(field);
		Assert.assertEquals(Integer.valueOf(3), field.numericValue());
		Assert.assertEquals("name", field.name());
		Assert.assertEquals(false, field.fieldType().stored());
	}

	@Test
	public void testMatchQuery() {
		CellMapperInteger mapper = new CellMapperInteger(1f);
		MatchQuery matchQuery = new MatchQuery(0.5f, "name", 3);
		Query query = mapper.toLucene(matchQuery);
		Assert.assertNotNull(query);
		Assert.assertEquals(NumericRangeQuery.class, query.getClass());
		Assert.assertEquals(0.5f, query.getBoost(), 0);
	}

	@Test
	public void testRangeQueryClose() {
		CellMapperInteger mapper = new CellMapperInteger(1f);
		RangeQuery rangeQuery = new RangeQuery(0.5f, "name", 2, 3, true, false);
		Query query = mapper.toLucene(rangeQuery);
		Assert.assertNotNull(query);
		Assert.assertEquals(NumericRangeQuery.class, query.getClass());
		NumericRangeQuery<?> numericQuery = (NumericRangeQuery<?>) query;
		Assert.assertEquals(Integer.valueOf(2), (Integer) numericQuery.getMin());
		Assert.assertEquals(Integer.valueOf(3), (Integer) numericQuery.getMax());
		Assert.assertEquals(true, numericQuery.includesMin());
		Assert.assertEquals(false, numericQuery.includesMax());
		Assert.assertEquals(0.5f, query.getBoost(), 0);
	}

	@Test
	public void testRangeQueryOpen() {
		CellMapperInteger mapper = new CellMapperInteger(1f);
		RangeQuery rangeQuery = new RangeQuery(0.5f, "name", 2, null, true, false);
		Query query = mapper.toLucene(rangeQuery);
		Assert.assertNotNull(query);
		Assert.assertEquals(NumericRangeQuery.class, query.getClass());
		NumericRangeQuery<?> numericQuery = (NumericRangeQuery<?>) query;
		Assert.assertEquals(Integer.valueOf(2), (Integer) numericQuery.getMin());
		Assert.assertNull(numericQuery.getMax());
		Assert.assertEquals(true, numericQuery.includesMin());
		Assert.assertEquals(false, numericQuery.includesMax());
		Assert.assertEquals(0.5f, query.getBoost(), 0);
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testPrefixQuery() {
		CellMapperInteger mapper = new CellMapperInteger(1f);
		PrefixQuery prefixQuery = new PrefixQuery(1f, "name", 3);
		mapper.toLucene(prefixQuery);
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testWildcardQuery() {
		CellMapperInteger mapper = new CellMapperInteger(1f);
		WildcardQuery prefixQuery = new WildcardQuery(1f, "name", 3);
		mapper.toLucene(prefixQuery);
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testFuzzyQuery() {
		CellMapperInteger mapper = new CellMapperInteger(1f);
		FuzzyQuery prefixQuery = new FuzzyQuery(1f, "name", 3, 2, 0, 50, true);
		mapper.toLucene(prefixQuery);
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testPhraseQuery() {
		CellMapperInteger mapper = new CellMapperInteger(1f);
		List<Object> values = new ArrayList<>();
		values.add(3);
		PhraseQuery prefixQuery = new PhraseQuery(1f, "name", values, 2);
		mapper.toLucene(prefixQuery);
	}

	@Test
	public void testExtractAnalyzers() {
		CellMapperInteger mapper = new CellMapperInteger(1f);
		Analyzer analyzer = mapper.analyzer();
		Assert.assertEquals(CellMapper.EMPTY_ANALYZER, analyzer);
	}

	@Test
	public void testParseJSON() throws IOException {
		String json = "{fields:{age:{type:\"integer\"}}}";
		CellsMapper cellsMapper = CellsMapper.fromJson(json);
		CellMapper<?> cellMapper = cellsMapper.getMapper("age");
		Assert.assertNotNull(cellMapper);
		Assert.assertEquals(CellMapperInteger.class, cellMapper.getClass());
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
