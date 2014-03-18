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

public class CellMapperFloatTest {

	@Test()
	public void testValueNull() {
		CellMapperFloat mapper = new CellMapperFloat(1f);
		Float parsed = mapper.indexValue(null);
		Assert.assertNull(parsed);
	}

	@Test
	public void testValueInteger() {
		CellMapperFloat mapper = new CellMapperFloat(1f);
		Float parsed = mapper.indexValue(3);
		Assert.assertEquals(Float.valueOf(3), parsed);
	}

	@Test
	public void testValueLong() {
		CellMapperFloat mapper = new CellMapperFloat(1f);
		Float parsed = mapper.indexValue(3l);
		Assert.assertEquals(Float.valueOf(3), parsed);
	}

	@Test
	public void testValueFloatWithoutDecimal() {
		CellMapperFloat mapper = new CellMapperFloat(1f);
		Float parsed = mapper.indexValue(3f);
		Assert.assertEquals(Float.valueOf(3), parsed);
	}

	@Test
	public void testValueFloatWithDecimalFloor() {
		CellMapperFloat mapper = new CellMapperFloat(1f);
		Float parsed = mapper.indexValue(3.5f);
		Assert.assertEquals(Float.valueOf(3.5f), parsed);

	}

	@Test
	public void testValueFloatWithDecimalCeil() {
		CellMapperFloat mapper = new CellMapperFloat(1f);
		Float parsed = mapper.indexValue(3.6f);
		Assert.assertEquals(Float.valueOf(3.6f), parsed);

	}

	@Test
	public void testValueDoubleWithoutDecimal() {
		CellMapperFloat mapper = new CellMapperFloat(1f);
		Float parsed = mapper.indexValue(3d);
		Assert.assertEquals(Float.valueOf(3), parsed);
	}

	@Test
	public void testValueDoubleWithDecimalFloor() {
		CellMapperFloat mapper = new CellMapperFloat(1f);
		Float parsed = mapper.indexValue(3.5d);
		Assert.assertEquals(Float.valueOf(3.5f), parsed);

	}

	@Test
	public void testValueDoubleWithDecimalCeil() {
		CellMapperFloat mapper = new CellMapperFloat(1f);
		Float parsed = mapper.indexValue(3.6d);
		Assert.assertEquals(Float.valueOf(3.6f), parsed);

	}

	@Test
	public void testValueStringWithoutDecimal() {
		CellMapperFloat mapper = new CellMapperFloat(1f);
		Float parsed = mapper.indexValue("3");
		Assert.assertEquals(Float.valueOf(3), parsed);
	}

	@Test
	public void testValueStringWithDecimalFloor() {
		CellMapperFloat mapper = new CellMapperFloat(1f);
		Float parsed = mapper.indexValue("3.2");
		Assert.assertEquals(Float.valueOf(3.2f), parsed);
	}

	@Test
	public void testValueStringWithDecimalCeil() {
		CellMapperFloat mapper = new CellMapperFloat(1f);
		Float parsed = mapper.indexValue("3.6");
		Assert.assertEquals(Float.valueOf(3.6f), parsed);

	}

	@Test
	public void testField() {
		CellMapperFloat mapper = new CellMapperFloat(1f);
		Field field = mapper.field("name", "3.2");
		Assert.assertNotNull(field);
		Assert.assertEquals(Float.valueOf(3.2f), field.numericValue());
		Assert.assertEquals("name", field.name());
		Assert.assertEquals(false, field.fieldType().stored());
	}

	@Test
	public void testMatchQuery() {
		CellMapperFloat mapper = new CellMapperFloat(1f);
		MatchQuery matchQuery = new MatchQuery(0.5f, "name", 3l);
		Query query = mapper.query(matchQuery);
		Assert.assertNotNull(query);
		Assert.assertEquals(NumericRangeQuery.class, query.getClass());
		Assert.assertEquals(0.5f, query.getBoost(), 0);
	}

	@Test
	public void testRangeQueryClose() {
		CellMapperFloat mapper = new CellMapperFloat(1f);
		RangeQuery rangeQuery = new RangeQuery(0.5f, "name", 2L, 3f, true, false);
		Query query = mapper.query(rangeQuery);
		Assert.assertNotNull(query);
		Assert.assertEquals(NumericRangeQuery.class, query.getClass());
		NumericRangeQuery<?> numericQuery = (NumericRangeQuery<?>) query;
		Assert.assertEquals(Float.valueOf(2), (Float) numericQuery.getMin());
		Assert.assertEquals(Float.valueOf(3), (Float) numericQuery.getMax());
		Assert.assertEquals(true, numericQuery.includesMin());
		Assert.assertEquals(false, numericQuery.includesMax());
		Assert.assertEquals(0.5f, query.getBoost(), 0);
	}

	@Test
	public void testRangeQueryOpen() {
		CellMapperFloat mapper = new CellMapperFloat(1f);
		RangeQuery rangeQuery = new RangeQuery(0.5f, "name", 2, null, true, false);
		Query query = mapper.query(rangeQuery);
		Assert.assertNotNull(query);
		Assert.assertEquals(NumericRangeQuery.class, query.getClass());
		NumericRangeQuery<?> numericQuery = (NumericRangeQuery<?>) query;
		Assert.assertEquals(Float.valueOf(2), (Float) numericQuery.getMin());
		Assert.assertNull(numericQuery.getMax());
		Assert.assertEquals(true, numericQuery.includesMin());
		Assert.assertEquals(false, numericQuery.includesMax());
		Assert.assertEquals(0.5f, query.getBoost(), 0);
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testPrefixQuery() {
		CellMapperFloat mapper = new CellMapperFloat(1f);
		PrefixQuery prefixQuery = new PrefixQuery(1f, "name", 3);
		mapper.query(prefixQuery);
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testWildcardQuery() {
		CellMapperFloat mapper = new CellMapperFloat(1f);
		WildcardQuery prefixQuery = new WildcardQuery(1f, "name", 3);
		mapper.query(prefixQuery);
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testFuzzyQuery() {
		CellMapperFloat mapper = new CellMapperFloat(1f);
		FuzzyQuery prefixQuery = new FuzzyQuery(1f, "name", 3, 2, 0, 50, true);
		mapper.query(prefixQuery);
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testPhraseQuery() {
		CellMapperFloat mapper = new CellMapperFloat(1f);
		List<Object> values = new ArrayList<>();
		values.add(3);
		PhraseQuery prefixQuery = new PhraseQuery(1f, "name", values, 2);
		mapper.query(prefixQuery);
	}

	@Test
	public void testExtractAnalyzers() {
		CellMapperFloat mapper = new CellMapperFloat(1f);
		Analyzer analyzer = mapper.analyzer();
		Assert.assertEquals(CellMapper.EMPTY_ANALYZER, analyzer);
	}

	@Test
	public void testParseJSON() throws IOException {
		String json = "{fields:{age:{type:\"float\"}}}";
		CellsMapper cellsMapper = CellsMapper.fromJson(json);
		CellMapper<?> cellMapper = cellsMapper.getMapper("age");
		Assert.assertNotNull(cellMapper);
		Assert.assertEquals(CellMapperFloat.class, cellMapper.getClass());
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
