package org.apache.cassandra.db.index.stratio.schema;

import java.io.IOException;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Field;
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
