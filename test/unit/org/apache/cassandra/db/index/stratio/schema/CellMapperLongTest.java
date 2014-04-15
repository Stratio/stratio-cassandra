package org.apache.cassandra.db.index.stratio.schema;

import java.io.IOException;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Field;
import org.junit.Assert;
import org.junit.Test;

public class CellMapperLongTest {

	@Test()
	public void testValueNull() {
		CellMapperLong mapper = new CellMapperLong(1f);
		Long parsed = mapper.indexValue("test", null);
		Assert.assertNull(parsed);
	}

	@Test
	public void testValueInteger() {
		CellMapperLong mapper = new CellMapperLong(1f);
		Long parsed = mapper.indexValue("test", 3);
		Assert.assertEquals(Long.valueOf(3), parsed);
	}

	@Test
	public void testValueLong() {
		CellMapperLong mapper = new CellMapperLong(1f);
		Long parsed = mapper.indexValue("test", 3l);
		Assert.assertEquals(Long.valueOf(3), parsed);
	}

	@Test
	public void testValueFloatWithoutDecimal() {
		CellMapperLong mapper = new CellMapperLong(1f);
		Long parsed = mapper.indexValue("test", 3f);
		Assert.assertEquals(Long.valueOf(3), parsed);
	}

	@Test
	public void testValueFloatWithDecimalFloor() {
		CellMapperLong mapper = new CellMapperLong(1f);
		Long parsed = mapper.indexValue("test", 3.5f);
		Assert.assertEquals(Long.valueOf(3), parsed);

	}

	@Test
	public void testValueFloatWithDecimalCeil() {
		CellMapperLong mapper = new CellMapperLong(1f);
		Long parsed = mapper.indexValue("test", 3.6f);
		Assert.assertEquals(Long.valueOf(3), parsed);

	}

	@Test
	public void testValueDoubleWithoutDecimal() {
		CellMapperLong mapper = new CellMapperLong(1f);
		Long parsed = mapper.indexValue("test", 3d);
		Assert.assertEquals(Long.valueOf(3), parsed);
	}

	@Test
	public void testValueDoubleWithDecimalFloor() {
		CellMapperLong mapper = new CellMapperLong(1f);
		Long parsed = mapper.indexValue("test", 3.5d);
		Assert.assertEquals(Long.valueOf(3), parsed);

	}

	@Test
	public void testValueDoubleWithDecimalCeil() {
		CellMapperLong mapper = new CellMapperLong(1f);
		Long parsed = mapper.indexValue("test", 3.6d);
		Assert.assertEquals(Long.valueOf(3), parsed);

	}

	@Test
	public void testValueStringWithoutDecimal() {
		CellMapperLong mapper = new CellMapperLong(1f);
		Long parsed = mapper.indexValue("test", "3");
		Assert.assertEquals(Long.valueOf(3), parsed);
	}

	@Test
	public void testValueStringWithDecimalFloor() {
		CellMapperLong mapper = new CellMapperLong(1f);
		Long parsed = mapper.indexValue("test", "3.2");
		Assert.assertEquals(Long.valueOf(3), parsed);

	}

	@Test
	public void testValueStringWithDecimalCeil() {
		CellMapperLong mapper = new CellMapperLong(1f);
		Long parsed = mapper.indexValue("test", "3.2");
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
	public void testExtractAnalyzers() {
		CellMapperLong mapper = new CellMapperLong(1f);
		Analyzer analyzer = mapper.analyzer();
		Assert.assertEquals(CellMapper.EMPTY_ANALYZER, analyzer);
	}

	@Test
	public void testParseJSON() throws IOException {
		String json = "{fields:{age:{type:\"long\"}}}";
		Schema schema = Schema.fromJson(json);
		CellMapper<?> cellMapper = schema.getMapper("age");
		Assert.assertNotNull(cellMapper);
		Assert.assertEquals(CellMapperLong.class, cellMapper.getClass());
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
