package org.apache.cassandra.db.index.stratio.schema;

import java.io.IOException;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Field;
import org.junit.Assert;
import org.junit.Test;

public class CellMapperFloatTest {

	@Test()
	public void testValueNull() {
		CellMapperFloat mapper = new CellMapperFloat(1f);
		Float parsed = mapper.indexValue("test", null);
		Assert.assertNull(parsed);
	}

	@Test
	public void testValueInteger() {
		CellMapperFloat mapper = new CellMapperFloat(1f);
		Float parsed = mapper.indexValue("test", 3);
		Assert.assertEquals(Float.valueOf(3), parsed);
	}

	@Test
	public void testValueLong() {
		CellMapperFloat mapper = new CellMapperFloat(1f);
		Float parsed = mapper.indexValue("test", 3l);
		Assert.assertEquals(Float.valueOf(3), parsed);
	}

	@Test
	public void testValueFloatWithoutDecimal() {
		CellMapperFloat mapper = new CellMapperFloat(1f);
		Float parsed = mapper.indexValue("test", 3f);
		Assert.assertEquals(Float.valueOf(3), parsed);
	}

	@Test
	public void testValueFloatWithDecimalFloor() {
		CellMapperFloat mapper = new CellMapperFloat(1f);
		Float parsed = mapper.indexValue("test", 3.5f);
		Assert.assertEquals(Float.valueOf(3.5f), parsed);

	}

	@Test
	public void testValueFloatWithDecimalCeil() {
		CellMapperFloat mapper = new CellMapperFloat(1f);
		Float parsed = mapper.indexValue("test", 3.6f);
		Assert.assertEquals(Float.valueOf(3.6f), parsed);

	}

	@Test
	public void testValueDoubleWithoutDecimal() {
		CellMapperFloat mapper = new CellMapperFloat(1f);
		Float parsed = mapper.indexValue("test", 3d);
		Assert.assertEquals(Float.valueOf(3), parsed);
	}

	@Test
	public void testValueDoubleWithDecimalFloor() {
		CellMapperFloat mapper = new CellMapperFloat(1f);
		Float parsed = mapper.indexValue("test", 3.5d);
		Assert.assertEquals(Float.valueOf(3.5f), parsed);

	}

	@Test
	public void testValueDoubleWithDecimalCeil() {
		CellMapperFloat mapper = new CellMapperFloat(1f);
		Float parsed = mapper.indexValue("test", 3.6d);
		Assert.assertEquals(Float.valueOf(3.6f), parsed);

	}

	@Test
	public void testValueStringWithoutDecimal() {
		CellMapperFloat mapper = new CellMapperFloat(1f);
		Float parsed = mapper.indexValue("test", "3");
		Assert.assertEquals(Float.valueOf(3), parsed);
	}

	@Test
	public void testValueStringWithDecimalFloor() {
		CellMapperFloat mapper = new CellMapperFloat(1f);
		Float parsed = mapper.indexValue("test", "3.2");
		Assert.assertEquals(Float.valueOf(3.2f), parsed);
	}

	@Test
	public void testValueStringWithDecimalCeil() {
		CellMapperFloat mapper = new CellMapperFloat(1f);
		Float parsed = mapper.indexValue("test", "3.6");
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
	public void testExtractAnalyzers() {
		CellMapperFloat mapper = new CellMapperFloat(1f);
		Analyzer analyzer = mapper.analyzer();
		Assert.assertEquals(CellMapper.EMPTY_ANALYZER, analyzer);
	}

	@Test
	public void testParseJSON() throws IOException {
		String json = "{fields:{age:{type:\"float\"}}}";
		Schema schema = Schema.fromJson(json);
		CellMapper<?> cellMapper = schema.getMapper("age");
		Assert.assertNotNull(cellMapper);
		Assert.assertEquals(CellMapperFloat.class, cellMapper.getClass());
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
