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

public class CellMapperDoubleTest {

	@Test()
	public void testValueNull() {
		CellMapperDouble mapper = new CellMapperDouble(1f);
		Double parsed = mapper.indexValue(null);
		Assert.assertNull(parsed);
	}

	@Test
	public void testValueInteger() {
		CellMapperDouble mapper = new CellMapperDouble(1f);
		Double parsed = mapper.indexValue(3);
		Assert.assertEquals(Double.valueOf(3), parsed);
	}

	@Test
	public void testValueLong() {
		CellMapperDouble mapper = new CellMapperDouble(1f);
		Double parsed = mapper.indexValue(3l);
		Assert.assertEquals(Double.valueOf(3), parsed);
	}

	@Test
	public void testValueFloatWithoutDecimal() {
		CellMapperDouble mapper = new CellMapperDouble(1f);
		Double parsed = mapper.indexValue(3f);
		Assert.assertEquals(Double.valueOf(3), parsed);
	}

	@Test
	public void testValueFloatWithDecimalFloor() {
		CellMapperDouble mapper = new CellMapperDouble(1f);
		Double parsed = mapper.indexValue(3.5f);
		Assert.assertEquals(Double.valueOf(3.5d), parsed);

	}

	@Test
	public void testValueFloatWithDecimalCeil() {
		CellMapperDouble mapper = new CellMapperDouble(1f);
		Double parsed = mapper.indexValue(3.6f);
		Assert.assertEquals(Double.valueOf(3.6f), parsed);
	}

	@Test
	public void testValueDoubleWithoutDecimal() {
		CellMapperDouble mapper = new CellMapperDouble(1f);
		Double parsed = mapper.indexValue(3d);
		Assert.assertEquals(Double.valueOf(3), parsed);
	}

	@Test
	public void testValueDoubleWithDecimalFloor() {
		CellMapperDouble mapper = new CellMapperDouble(1f);
		Double parsed = mapper.indexValue(3.5d);
		Assert.assertEquals(Double.valueOf(3.5d), parsed);

	}

	@Test
	public void testValueDoubleWithDecimalCeil() {
		CellMapperDouble mapper = new CellMapperDouble(1f);
		Double parsed = mapper.indexValue(3.6d);
		Assert.assertEquals(Double.valueOf(3.6d), parsed);

	}

	@Test
	public void testValueStringWithoutDecimal() {
		CellMapperDouble mapper = new CellMapperDouble(1f);
		Double parsed = mapper.indexValue("3");
		Assert.assertEquals(Double.valueOf(3), parsed);
	}

	@Test
	public void testValueStringWithDecimalFloor() {
		CellMapperDouble mapper = new CellMapperDouble(1f);
		Double parsed = mapper.indexValue("3.2");
		Assert.assertEquals(Double.valueOf(3.2d), parsed);
	}

	@Test
	public void testValueStringWithDecimalCeil() {
		CellMapperDouble mapper = new CellMapperDouble(1f);
		Double parsed = mapper.indexValue("3.6");
		Assert.assertEquals(Double.valueOf(3.6d), parsed);

	}

	@Test
	public void testField() {
		CellMapperDouble mapper = new CellMapperDouble(1f);
		Field field = mapper.field("name", "3.2");
		Assert.assertNotNull(field);
		Assert.assertEquals(Double.valueOf(3.2d), field.numericValue());
		Assert.assertEquals("name", field.name());
		Assert.assertEquals(false, field.fieldType().stored());
	}

	@Test
	public void testMatchQuery() {
		CellMapperDouble mapper = new CellMapperDouble(1f);
		MatchQuery matchQuery = new MatchQuery(0.5f, "name", 3l);
		Query query = mapper.toLucene(matchQuery);
		Assert.assertNotNull(query);
		Assert.assertEquals(NumericRangeQuery.class, query.getClass());
		NumericRangeQuery<?> numericRangeQuery = (NumericRangeQuery<?>) query;
		Assert.assertEquals(3d, numericRangeQuery.getMin().doubleValue(), 0);
		Assert.assertEquals(3d, numericRangeQuery.getMax().doubleValue(), 0);
		Assert.assertEquals(true, numericRangeQuery.includesMin());
		Assert.assertEquals(true, numericRangeQuery.includesMax());
		Assert.assertEquals(0.5f, query.getBoost(), 0);
	}

	@Test
	public void testRangeQueryClose() {
		CellMapperDouble mapper = new CellMapperDouble(1f);
		RangeQuery rangeQuery = new RangeQuery(0.5f, "name", 2L, 3f, true, false);
		Query query = mapper.toLucene(rangeQuery);
		Assert.assertNotNull(query);
		Assert.assertEquals(NumericRangeQuery.class, query.getClass());
		NumericRangeQuery<?> numericRangeQuery = (NumericRangeQuery<?>) query;
		Assert.assertEquals(Double.valueOf(2), (Double) numericRangeQuery.getMin());
		Assert.assertEquals(Double.valueOf(3), (Double) numericRangeQuery.getMax());
		Assert.assertEquals(true, numericRangeQuery.includesMin());
		Assert.assertEquals(false, numericRangeQuery.includesMax());
		Assert.assertEquals(0.5f, query.getBoost(), 0);
	}

	@Test
	public void testRangeQueryOpen() {
		CellMapperDouble mapper = new CellMapperDouble(1f);
		RangeQuery rangeQuery = new RangeQuery(0.5f, "name", 2, null, true, false);
		Query query = mapper.toLucene(rangeQuery);
		Assert.assertNotNull(query);
		Assert.assertEquals(NumericRangeQuery.class, query.getClass());
		NumericRangeQuery<?> numericRangeQuery = (NumericRangeQuery<?>) query;
		Assert.assertEquals(2d, numericRangeQuery.getMin().doubleValue(), 0);
		Assert.assertNull(numericRangeQuery.getMax());
		Assert.assertEquals(true, numericRangeQuery.includesMin());
		Assert.assertEquals(false, numericRangeQuery.includesMax());
		Assert.assertEquals(0.5f, query.getBoost(), 0);
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testPrefixQuery() {
		CellMapperDouble mapper = new CellMapperDouble(1f);
		PrefixQuery prefixQuery = new PrefixQuery(1f, "name", 3);
		mapper.toLucene(prefixQuery);
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testWildcardQuery() {
		CellMapperDouble mapper = new CellMapperDouble(1f);
		WildcardQuery prefixQuery = new WildcardQuery(1f, "name", 3);
		mapper.toLucene(prefixQuery);
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testFuzzyQuery() {
		CellMapperDouble mapper = new CellMapperDouble(1f);
		FuzzyQuery prefixQuery = new FuzzyQuery(1f, "name", 3, 2, 0, 50, true);
		mapper.toLucene(prefixQuery);
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testPhraseQuery() {
		CellMapperDouble mapper = new CellMapperDouble(1f);
		List<Object> values = new ArrayList<>();
		values.add(3);
		PhraseQuery prefixQuery = new PhraseQuery(1f, "name", values, 2);
		mapper.toLucene(prefixQuery);
	}

	@Test
	public void testExtractAnalyzers() {
		CellMapperDouble mapper = new CellMapperDouble(1f);
		Analyzer analyzer = mapper.analyzer();
		Assert.assertEquals(CellMapper.EMPTY_ANALYZER, analyzer);
	}

	@Test
	public void testParseJSON() throws IOException {
		String json = "{fields:{age:{type:\"double\"}}}";
		CellsMapper cellsMapper = CellsMapper.fromJson(json);
		CellMapper<?> cellMapper = cellsMapper.getMapper("age");
		Assert.assertNotNull(cellMapper);
		Assert.assertEquals(CellMapperDouble.class, cellMapper.getClass());
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
