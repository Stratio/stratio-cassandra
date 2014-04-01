package org.apache.cassandra.db.index.stratio.schema;

import java.io.IOException;
import java.util.UUID;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Field;
import org.junit.Assert;
import org.junit.Test;

public class CellMapperTextTest {

	@Test()
	public void testAnalyzerNull() {
		CellMapperText mapper = new CellMapperText(null);
		String parsed = mapper.indexValue(null);
		Assert.assertNull(parsed);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testAnalyzerInvalid() {
		CellMapperText mapper = new CellMapperText("hello");
		String parsed = mapper.indexValue(null);
		Assert.assertNull(parsed);
	}

	@Test()
	public void testValueNull() {
		CellMapperText mapper = new CellMapperText("org.apache.lucene.analysis.en.EnglishAnalyzer");
		String parsed = mapper.indexValue(null);
		Assert.assertNull(parsed);
	}

	@Test
	public void testValueInteger() {
		CellMapperText mapper = new CellMapperText("org.apache.lucene.analysis.en.EnglishAnalyzer");
		String parsed = mapper.indexValue(3);
		Assert.assertEquals("3", parsed);
	}

	@Test
	public void testValueLong() {
		CellMapperText mapper = new CellMapperText("org.apache.lucene.analysis.en.EnglishAnalyzer");
		String parsed = mapper.indexValue(3l);
		Assert.assertEquals("3", parsed);
	}

	@Test
	public void testValueFloatWithoutDecimal() {
		CellMapperText mapper = new CellMapperText("org.apache.lucene.analysis.en.EnglishAnalyzer");
		String parsed = mapper.indexValue(3f);
		Assert.assertEquals("3.0", parsed);
	}

	@Test
	public void testValueFloatWithDecimalFloor() {
		CellMapperText mapper = new CellMapperText("org.apache.lucene.analysis.en.EnglishAnalyzer");
		String parsed = mapper.indexValue(3.5f);
		Assert.assertEquals("3.5", parsed);

	}

	@Test
	public void testValueFloatWithDecimalCeil() {
		CellMapperText mapper = new CellMapperText("org.apache.lucene.analysis.en.EnglishAnalyzer");
		String parsed = mapper.indexValue(3.6f);
		Assert.assertEquals("3.6", parsed);
	}

	@Test
	public void testValueDoubleWithoutDecimal() {
		CellMapperText mapper = new CellMapperText("org.apache.lucene.analysis.en.EnglishAnalyzer");
		String parsed = mapper.indexValue(3d);
		Assert.assertEquals("3.0", parsed);
	}

	@Test
	public void testValueDoubleWithDecimalFloor() {
		CellMapperText mapper = new CellMapperText("org.apache.lucene.analysis.en.EnglishAnalyzer");
		String parsed = mapper.indexValue(3.5d);
		Assert.assertEquals("3.5", parsed);

	}

	@Test
	public void testValueDoubleWithDecimalCeil() {
		CellMapperText mapper = new CellMapperText("org.apache.lucene.analysis.en.EnglishAnalyzer");
		String parsed = mapper.indexValue(3.6d);
		Assert.assertEquals("3.6", parsed);

	}

	@Test
	public void testValueStringWithoutDecimal() {
		CellMapperText mapper = new CellMapperText("org.apache.lucene.analysis.en.EnglishAnalyzer");
		String parsed = mapper.indexValue("3");
		Assert.assertEquals("3", parsed);
	}

	@Test
	public void testValueStringWithDecimalFloor() {
		CellMapperText mapper = new CellMapperText("org.apache.lucene.analysis.en.EnglishAnalyzer");
		String parsed = mapper.indexValue("3.2");
		Assert.assertEquals("3.2", parsed);
	}

	@Test
	public void testValueStringWithDecimalCeil() {
		CellMapperText mapper = new CellMapperText("org.apache.lucene.analysis.en.EnglishAnalyzer");
		String parsed = mapper.indexValue("3.6");
		Assert.assertEquals("3.6", parsed);

	}

	@Test
	public void testValueUUID() {
		CellMapperText mapper = new CellMapperText("org.apache.lucene.analysis.en.EnglishAnalyzer");
		String parsed = mapper.indexValue(UUID.fromString("550e8400-e29b-41d4-a716-446655440000"));
		Assert.assertEquals("550e8400-e29b-41d4-a716-446655440000", parsed);
	}

	@Test
	public void testField() {
		CellMapperText mapper = new CellMapperText("org.apache.lucene.analysis.en.EnglishAnalyzer");
		Field field = mapper.field("name", "hello");
		Assert.assertNotNull(field);
		Assert.assertEquals("hello", field.stringValue());
		Assert.assertEquals("name", field.name());
		Assert.assertEquals(false, field.fieldType().stored());
	}

	@Test
	public void testExtractAnalyzers() {
		CellMapperText mapper = new CellMapperText("org.apache.lucene.analysis.en.EnglishAnalyzer");
		Analyzer analyzer = mapper.analyzer();
		Assert.assertEquals(org.apache.lucene.analysis.en.EnglishAnalyzer.class, analyzer.getClass());
	}

	@Test
	public void testParseJSONWithAnayzer() throws IOException {
		String json = "{fields:{age:{type:\"text\", analyzer:\"org.apache.lucene.analysis.en.EnglishAnalyzer\"}}}";
		CellsMapper cellsMapper = CellsMapper.fromJson(json);
		CellMapper<?> cellMapper = cellsMapper.getMapper("age");
		Assert.assertNotNull(cellMapper);
		Assert.assertEquals(CellMapperText.class, cellMapper.getClass());
	}

	@Test
	public void testParseJSONWithoutAnalyzer() throws IOException {
		String json = "{fields:{age:{type:\"text\"}}}";
		CellsMapper cellsMapper = CellsMapper.fromJson(json);
		CellMapper<?> cellMapper = cellsMapper.getMapper("age");
		Assert.assertNotNull(cellMapper);
		Assert.assertEquals(CellMapperText.class, cellMapper.getClass());
	}

	@Test(expected=IllegalArgumentException.class)
	public void testParseJSONEmpty() throws IOException {
		String json = "{fields:{}}";
		CellsMapper cellsMapper = CellsMapper.fromJson(json);
		cellsMapper.getMapper("age");
	}

	@Test(expected = IllegalArgumentException.class)
	public void testParseJSONInvalid() throws IOException {
		String json = "{fields:{age:{}}";
		CellsMapper.fromJson(json);
	}
}
