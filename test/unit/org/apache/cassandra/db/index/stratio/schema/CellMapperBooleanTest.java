package org.apache.cassandra.db.index.stratio.schema;

import java.io.IOException;
import java.util.Date;
import java.util.UUID;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Field;
import org.junit.Assert;
import org.junit.Test;

public class CellMapperBooleanTest {

	@Test()
	public void testValueNull() {
		CellMapperBoolean mapper = new CellMapperBoolean();
		String parsed = mapper.indexValue(null);
		Assert.assertNull(parsed);
	}

	@Test
	public void testValueBooleanTrue() {
		CellMapperBoolean mapper = new CellMapperBoolean();
		String parsed = mapper.indexValue(true);
		Assert.assertEquals("true", parsed);
	}

	@Test
	public void testValueBooleanFalse() {
		CellMapperBoolean mapper = new CellMapperBoolean();
		String parsed = mapper.indexValue(false);
		Assert.assertEquals("false", parsed);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testValueDate() {
		CellMapperBoolean mapper = new CellMapperBoolean();
		mapper.indexValue(new Date());
	}

	@Test(expected = IllegalArgumentException.class)
	public void testValueInteger() {
		CellMapperBoolean mapper = new CellMapperBoolean();
		mapper.indexValue(3);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testValueLong() {
		CellMapperBoolean mapper = new CellMapperBoolean();
		mapper.indexValue(3l);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testValueFloat() {
		CellMapperBoolean mapper = new CellMapperBoolean();
		mapper.indexValue(3.6f);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testValueDouble() {
		CellMapperBoolean mapper = new CellMapperBoolean();
		mapper.indexValue(3.5d);
	}

	@Test
	public void testValueStringTrueLowercase() {
		CellMapperBoolean mapper = new CellMapperBoolean();
		String parsed = mapper.indexValue("true");
		Assert.assertEquals("true", parsed);
	}

	@Test
	public void testValueStringTrueUppercase() {
		CellMapperBoolean mapper = new CellMapperBoolean();
		String parsed = mapper.indexValue("TRUE");
		Assert.assertEquals("true", parsed);
	}

	@Test
	public void testValueStringTrueMixedcase() {
		CellMapperBoolean mapper = new CellMapperBoolean();
		String parsed = mapper.indexValue("TrUe");
		Assert.assertEquals("true", parsed);
	}

	@Test
	public void testValueStringFalseLowercase() {
		CellMapperBoolean mapper = new CellMapperBoolean();
		String parsed = mapper.indexValue("false");
		Assert.assertEquals("false", parsed);
	}

	@Test
	public void testValueStringFalseUppercase() {
		CellMapperBoolean mapper = new CellMapperBoolean();
		String parsed = mapper.indexValue("FALSE");
		Assert.assertEquals("false", parsed);
	}

	@Test
	public void testValueStringFalseMixedcase() {
		CellMapperBoolean mapper = new CellMapperBoolean();
		String parsed = mapper.indexValue("fALsE");
		Assert.assertEquals("false", parsed);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testValueStringInvalid() {
		CellMapperBoolean mapper = new CellMapperBoolean();
		mapper.indexValue("hello");
	}

	@Test(expected = IllegalArgumentException.class)
	public void testValueUUID() {
		CellMapperBoolean mapper = new CellMapperBoolean();
		mapper.indexValue(UUID.fromString("550e8400-e29b-41d4-a716-446655440000"));
	}

	@Test
	public void testField() {
		CellMapperBoolean mapper = new CellMapperBoolean();
		Field field = mapper.field("name", "true");
		Assert.assertNotNull(field);
		Assert.assertEquals("true", field.stringValue());
		Assert.assertEquals("name", field.name());
		Assert.assertFalse(field.fieldType().stored());
	}

	@Test
	public void testExtractAnalyzers() {
		CellMapperBoolean mapper = new CellMapperBoolean();
		Analyzer analyzer = mapper.analyzer();
		Assert.assertEquals(CellMapper.EMPTY_ANALYZER, analyzer);
	}

	@Test
	public void testParseJSON() throws IOException {
		String json = "{fields:{age:{type:\"boolean\"}}}";
		Schema schema = Schema.fromJson(json);
		CellMapper<?> cellMapper = schema.getMapper("age");
		Assert.assertNotNull(cellMapper);
		Assert.assertEquals(CellMapperBoolean.class, cellMapper.getClass());
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
