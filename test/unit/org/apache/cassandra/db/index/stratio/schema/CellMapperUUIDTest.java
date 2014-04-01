package org.apache.cassandra.db.index.stratio.schema;

import java.io.IOException;
import java.util.UUID;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Field;
import org.junit.Assert;
import org.junit.Test;

public class CellMapperUUIDTest {

	@Test()
	public void testValueNull() {
		CellMapperUUID mapper = new CellMapperUUID();
		String parsed = mapper.indexValue(null);
		Assert.assertNull(parsed);
	}

	@Test
	public void testValueUUID() {
		CellMapperUUID mapper = new CellMapperUUID();
		String parsed = mapper.indexValue(UUID.fromString("550e8400-e29b-41d4-a716-446655440000"));
		Assert.assertEquals("550e8400-e29b-41d4-a716-446655440000", parsed);
	}

	@Test
	public void testValueString() {
		CellMapperUUID mapper = new CellMapperUUID();
		String parsed = mapper.indexValue("550e8400-e29b-41d4-a716-446655440000");
		Assert.assertEquals("550e8400-e29b-41d4-a716-446655440000", parsed);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testValueStringInvalid() {
		CellMapperUUID mapper = new CellMapperUUID();
		mapper.indexValue("550e840");
	}

	@Test(expected = IllegalArgumentException.class)
	public void testValueInteger() {
		CellMapperUUID mapper = new CellMapperUUID();
		String parsed = mapper.indexValue(3);
		Assert.assertEquals("3", parsed);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testValueLong() {
		CellMapperUUID mapper = new CellMapperUUID();
		String parsed = mapper.indexValue(3l);
		Assert.assertEquals("3", parsed);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testValueFloat() {
		CellMapperUUID mapper = new CellMapperUUID();
		String parsed = mapper.indexValue(3.6f);
		Assert.assertEquals("3.6", parsed);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testValueDouble() {
		CellMapperUUID mapper = new CellMapperUUID();
		String parsed = mapper.indexValue(3d);
		Assert.assertEquals("3.0", parsed);
	}

	@Test
	public void testField() {
		CellMapperUUID mapper = new CellMapperUUID();
		UUID uuid = UUID.fromString("550e8400-e29b-41d4-a716-446655440000");
		Field field = mapper.field("name", uuid);
		Assert.assertNotNull(field);
		Assert.assertEquals("name", field.name());
		Assert.assertEquals(uuid.toString(), field.stringValue());
		Assert.assertFalse(field.fieldType().stored());
	}

	@Test
	public void testExtractAnalyzers() {
		CellMapperUUID mapper = new CellMapperUUID();
		Analyzer analyzer = mapper.analyzer();
		Assert.assertEquals(CellMapper.EMPTY_ANALYZER, analyzer);
	}

	@Test
	public void testParseJSON() throws IOException {
		String json = "{fields:{age:{type:\"uuid\"}}}";
		CellsMapper cellsMapper = CellsMapper.fromJson(json);
		CellMapper<?> cellMapper = cellsMapper.getMapper("age");
		Assert.assertNotNull(cellMapper);
		Assert.assertEquals(CellMapperUUID.class, cellMapper.getClass());
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
