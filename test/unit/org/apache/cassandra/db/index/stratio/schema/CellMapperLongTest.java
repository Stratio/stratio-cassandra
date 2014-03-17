package org.apache.cassandra.db.index.stratio.schema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.db.index.stratio.MappingException;
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

public class CellMapperLongTest {

	@Test()
	public void testValueNull() {
		CellMapperLong mapper = new CellMapperLong(1f);
		Long parsed = mapper.value(null);
		Assert.assertNull(parsed);
	}

	@Test
	public void testValueInteger() {
		CellMapperLong mapper = new CellMapperLong(1f);
		Long parsed = mapper.value(3);
		Assert.assertEquals(Long.valueOf(3), parsed);
	}

	@Test
	public void testValueLong() {
		CellMapperLong mapper = new CellMapperLong(1f);
		Long parsed = mapper.value(3l);
		Assert.assertEquals(Long.valueOf(3), parsed);
	}

	@Test
	public void testValueFloatWithoutDecimal() {
		CellMapperLong mapper = new CellMapperLong(1f);
		Long parsed = mapper.value(3f);
		Assert.assertEquals(Long.valueOf(3), parsed);
	}

	@Test
	public void testValueFloatWithDecimalFloor() {
		CellMapperLong mapper = new CellMapperLong(1f);
		Long parsed = mapper.value(3.5f);
		Assert.assertEquals(Long.valueOf(3), parsed);

	}

	@Test
	public void testValueFloatWithDecimalCeil() {
		CellMapperLong mapper = new CellMapperLong(1f);
		Long parsed = mapper.value(3.6f);
		Assert.assertEquals(Long.valueOf(3), parsed);

	}

	@Test
	public void testValueDoubleWithoutDecimal() {
		CellMapperLong mapper = new CellMapperLong(1f);
		Long parsed = mapper.value(3d);
		Assert.assertEquals(Long.valueOf(3), parsed);
	}

	@Test
	public void testValueDoubleWithDecimalFloor() {
		CellMapperLong mapper = new CellMapperLong(1f);
		Long parsed = mapper.value(3.5d);
		Assert.assertEquals(Long.valueOf(3), parsed);

	}

	@Test
	public void testValueDoubleWithDecimalCeil() {
		CellMapperLong mapper = new CellMapperLong(1f);
		Long parsed = mapper.value(3.6d);
		Assert.assertEquals(Long.valueOf(3), parsed);

	}

	@Test
	public void testValueStringWithoutDecimal() {
		CellMapperLong mapper = new CellMapperLong(1f);
		Long parsed = mapper.value("3");
		Assert.assertEquals(Long.valueOf(3), parsed);
	}

	@Test
	public void testValueStringWithDecimalFloor() {
		CellMapperLong mapper = new CellMapperLong(1f);
		Long parsed = mapper.value("3.2");
		Assert.assertEquals(Long.valueOf(3), parsed);

	}

	@Test
	public void testValueStringWithDecimalCeil() {
		CellMapperLong mapper = new CellMapperLong(1f);
		Long parsed = mapper.value("3.2");
		Assert.assertEquals(Long.valueOf(3), parsed);

	}

	@Test
	public void testField() {
		CellMapperLong mapper = new CellMapperLong(1f);
		Field field = mapper.field("name", "3.2");
		Assert.assertNotNull(field);
		Assert.assertEquals(Long.valueOf(3), field.numericValue());
		Assert.assertEquals("name", field.name());
		Assert.assertEquals(false, field.fieldType().stored());
	}

	@Test
	public void testMatchQuery() {
		CellMapperLong mapper = new CellMapperLong(1f);
		MatchQuery matchQuery = new MatchQuery(0.5f, "name", 3l);
		Query query = mapper.query(matchQuery);
		Assert.assertNotNull(query);
		Assert.assertEquals(NumericRangeQuery.class, query.getClass());
		Assert.assertEquals(0.5f, query.getBoost(), 0);
	}

	@Test
	public void testRangeQueryClose() {
		CellMapperLong mapper = new CellMapperLong(1f);
		RangeQuery rangeQuery = new RangeQuery(0.5f, "name", 2L, 3f, true, false);
		Query query = mapper.query(rangeQuery);
		Assert.assertNotNull(query);
		Assert.assertEquals(NumericRangeQuery.class, query.getClass());
		NumericRangeQuery<?> numericQuery = (NumericRangeQuery<?>) query;
		Assert.assertEquals(Long.valueOf(2), (Long) numericQuery.getMin());
		Assert.assertEquals(Long.valueOf(3), (Long) numericQuery.getMax());
		Assert.assertEquals(true, numericQuery.includesMin());
		Assert.assertEquals(false, numericQuery.includesMax());
		Assert.assertEquals(0.5f, query.getBoost(), 0);
	}

	@Test
	public void testRangeQueryOpen() {
		CellMapperLong mapper = new CellMapperLong(1f);
		RangeQuery rangeQuery = new RangeQuery(0.5f, "name", 2, null, true, false);
		Query query = mapper.query(rangeQuery);
		Assert.assertNotNull(query);
		Assert.assertEquals(NumericRangeQuery.class, query.getClass());
		Assert.assertEquals(0.5f, query.getBoost(), 0);
		NumericRangeQuery<?> numericQuery = (NumericRangeQuery<?>) query;
		Assert.assertEquals(Long.valueOf(2), (Long) numericQuery.getMin());
		Assert.assertNull(numericQuery.getMax());
		Assert.assertEquals(true, numericQuery.includesMin());
		Assert.assertEquals(false, numericQuery.includesMax());
	}

	@Test(expected = MappingException.class)
	public void testPrefixQuery() {
		CellMapperLong mapper = new CellMapperLong(1f);
		PrefixQuery prefixQuery = new PrefixQuery(1f, "name", 3);
		mapper.query(prefixQuery);
	}

	@Test(expected = MappingException.class)
	public void testWildcardQuery() {
		CellMapperLong mapper = new CellMapperLong(1f);
		WildcardQuery prefixQuery = new WildcardQuery(1f, "name", 3);
		mapper.query(prefixQuery);
	}

	@Test(expected = MappingException.class)
	public void testFuzzyQuery() {
		CellMapperLong mapper = new CellMapperLong(1f);
		FuzzyQuery prefixQuery = new FuzzyQuery(1f, "name", 3, 2, 0, 50, true);
		mapper.query(prefixQuery);
	}

	@Test(expected = MappingException.class)
	public void testPhraseQuery() {
		CellMapperLong mapper = new CellMapperLong(1f);
		List<Object> values = new ArrayList<>();
		values.add(3);
		PhraseQuery prefixQuery = new PhraseQuery(1f, "name", values, 2);
		mapper.query(prefixQuery);
	}

	@Test
	public void testExtractAnalyzers() {
		CellMapperLong mapper = new CellMapperLong(1f);
		Analyzer analyzer = mapper.analyzer();
		Assert.assertEquals(CellMapper.EMPTY_ANALYZER, analyzer);
	}

	@Test
	public void testParseJSON() throws IOException {
		String json = "{fields:{age:{type:\"long\"}}}";
		CellsMapper cellsMapper = CellsMapper.fromJson(json);
		CellMapper<?> cellMapper = cellsMapper.getMapper("age");
		Assert.assertNotNull(cellMapper);
		Assert.assertEquals(CellMapperLong.class, cellMapper.getClass());
	}

	@Test
	public void testParseJSONEmpty() throws IOException {
		String json = "{fields:{}}";
		CellsMapper cellsMapper = CellsMapper.fromJson(json);
		CellMapper<?> cellMapper = cellsMapper.getMapper("age");
		Assert.assertNotNull(cellMapper);
		Assert.assertEquals(CellMapperText.class, cellMapper.getClass());
	}

	@Test(expected = MappingException.class)
	public void testParseJSONInvalid() throws IOException {
		String json = "{fields:{age:{}}";
		CellsMapper.fromJson(json);
	}
}
