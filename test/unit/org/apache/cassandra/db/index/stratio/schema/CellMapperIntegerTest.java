package org.apache.cassandra.db.index.stratio.schema;

import java.io.IOException;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Field;
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
	public void testExtractAnalyzers() {
		CellMapperInteger mapper = new CellMapperInteger(1f);
		Analyzer analyzer = mapper.analyzer();
		Assert.assertEquals(CellMapper.EMPTY_ANALYZER, analyzer);
	}

	@Test
	public void testParseJSON() throws IOException {
		String json = "{fields:{age:{type:\"integer\"}}}";
		Schema schema = Schema.fromJson(json);
		CellMapper<?> cellMapper = schema.getMapper("age");
		Assert.assertNotNull(cellMapper);
		Assert.assertEquals(CellMapperInteger.class, cellMapper.getClass());
	}

	@Test(expected=IllegalArgumentException.class)
	public void testParseJSONEmpty() throws IOException {
		String json = "{fields:{}}";
		Schema schema = Schema.fromJson(json);
		schema.getMapper("age");
	}

	@Test(expected = IllegalArgumentException.class)
	public void testParseJSONInvalid() throws IOException {
		String json = "{fields:{age:{}}";
		Schema.fromJson(json);
	}
}
