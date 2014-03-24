package org.apache.cassandra.db.index.stratio.schema;

import java.io.IOException;
import java.util.Date;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Field;
import org.junit.Assert;
import org.junit.Test;

public class CellMapperDateTest {

	private static final String PATTERN = "yyyy-MM-dd";

	@Test()
	public void testValueNull() {
		CellMapperDate mapper = new CellMapperDate(PATTERN);
		Long parsed = mapper.indexValue(null);
		Assert.assertNull(parsed);
	}

	@Test
	public void testValueDate() {
		CellMapperDate mapper = new CellMapperDate(PATTERN);
		Date date = new Date();
		long parsed = mapper.indexValue(date);
		Assert.assertEquals(date.getTime(), parsed);
	}

	@Test
	public void testValueInteger() {
		CellMapperDate mapper = new CellMapperDate(PATTERN);
		Long parsed = mapper.indexValue(3);
		Assert.assertEquals(Long.valueOf(3), parsed);
	}

	@Test
	public void testValueLong() {
		CellMapperDate mapper = new CellMapperDate(PATTERN);
		Long parsed = mapper.indexValue(3l);
		Assert.assertEquals(Long.valueOf(3), parsed);
	}

	@Test
	public void testValueFloatWithoutDecimal() {
		CellMapperDate mapper = new CellMapperDate(PATTERN);
		Long parsed = mapper.indexValue(3f);
		Assert.assertEquals(Long.valueOf(3), parsed);
	}

	@Test
	public void testValueFloatWithDecimalFloor() {
		CellMapperDate mapper = new CellMapperDate(PATTERN);
		Long parsed = mapper.indexValue(3.5f);
		Assert.assertEquals(Long.valueOf(3), parsed);

	}

	@Test
	public void testValueFloatWithDecimalCeil() {
		CellMapperDate mapper = new CellMapperDate(PATTERN);
		Long parsed = mapper.indexValue(3.6f);
		Assert.assertEquals(Long.valueOf(3), parsed);

	}

	@Test
	public void testValueDoubleWithoutDecimal() {
		CellMapperDate mapper = new CellMapperDate(PATTERN);
		Long parsed = mapper.indexValue(3d);
		Assert.assertEquals(Long.valueOf(3), parsed);
	}

	@Test
	public void testValueDoubleWithDecimalFloor() {
		CellMapperDate mapper = new CellMapperDate(PATTERN);
		Long parsed = mapper.indexValue(3.5d);
		Assert.assertEquals(Long.valueOf(3), parsed);

	}

	@Test
	public void testValueDoubleWithDecimalCeil() {
		CellMapperDate mapper = new CellMapperDate(PATTERN);
		Long parsed = mapper.indexValue(3.6d);
		Assert.assertEquals(Long.valueOf(3), parsed);

	}

	@Test
	public void testValueStringWithPattern() {
		CellMapperDate mapper = new CellMapperDate(PATTERN);
		Long parsed = mapper.indexValue("2014-03-19");
		Assert.assertEquals(Long.valueOf(1395183600000L), parsed);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testValueStringWithPatternInvalid() {
		CellMapperDate mapper = new CellMapperDate(PATTERN);
		Long parsed = mapper.indexValue("2014/03/19");
		Assert.assertEquals(Long.valueOf(1395183600000L), parsed);
	}

	@Test
	public void testValueStringWithoutPattern() {
		CellMapperDate mapper = new CellMapperDate(null);
		Long parsed = mapper.indexValue("2014/03/19 00:00:00.000");
		Assert.assertEquals(Long.valueOf(1395183600000L), parsed);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testValueStringWithoutPatternInvalid() {
		CellMapperDate mapper = new CellMapperDate(null);
		Long parsed = mapper.indexValue("2014-03-19");
		Assert.assertEquals(Long.valueOf(1395183600000L), parsed);
	}

	@Test
	public void testField() {
		CellMapperDate mapper = new CellMapperDate(PATTERN);
		Field field = mapper.field("name", "2014-03-19");
		Assert.assertNotNull(field);
		Assert.assertEquals(Long.valueOf(1395183600000L), field.numericValue());
		Assert.assertEquals("name", field.name());
		Assert.assertEquals(false, field.fieldType().stored());
	}

	@Test
	public void testExtractAnalyzers() {
		CellMapperDate mapper = new CellMapperDate(PATTERN);
		Analyzer analyzer = mapper.analyzer();
		Assert.assertEquals(CellMapper.EMPTY_ANALYZER, analyzer);
	}

	@Test
	public void testParseJSON() throws IOException {
		String json = "{fields:{age:{type:\"date\"}}}";
		CellsMapper cellsMapper = CellsMapper.fromJson(json);
		CellMapper<?> cellMapper = cellsMapper.getMapper("age");
		Assert.assertNotNull(cellMapper);
		Assert.assertEquals(CellMapperDate.class, cellMapper.getClass());
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
