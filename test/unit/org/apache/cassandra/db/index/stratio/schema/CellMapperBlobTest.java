package org.apache.cassandra.db.index.stratio.schema;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.UUID;

import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Hex;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Field;
import org.junit.Assert;
import org.junit.Test;

public class CellMapperBlobTest {

	@Test()
	public void testValueNull() {
		CellMapperBlob mapper = new CellMapperBlob();
		String parsed = mapper.indexValue(null);
		Assert.assertNull(parsed);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testValueInteger() {
		CellMapperBlob mapper = new CellMapperBlob();
		mapper.indexValue(3);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testValueLong() {
		CellMapperBlob mapper = new CellMapperBlob();
		mapper.indexValue(3l);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testValueFloat() {
		CellMapperBlob mapper = new CellMapperBlob();
		mapper.indexValue(3.5f);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testValueDouble() {
		CellMapperBlob mapper = new CellMapperBlob();
		mapper.indexValue(3.6d);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testValueUUID() {
		CellMapperBlob mapper = new CellMapperBlob();
		mapper.indexValue(UUID.randomUUID());
	}

	@Test(expected = IllegalArgumentException.class)
	public void testValueStringInvalid() {
		CellMapperBlob mapper = new CellMapperBlob();
		mapper.indexValue("Hello");
	}

	@Test
	public void testValueStringLowerCase() {
		CellMapperBlob mapper = new CellMapperBlob();
		String parsed = mapper.indexValue("f1");
		Assert.assertEquals("f1", parsed);
	}

	@Test
	public void testValueStringUpperCase() {
		CellMapperBlob mapper = new CellMapperBlob();
		String parsed = mapper.indexValue("F1");
		Assert.assertEquals("f1", parsed);
	}

	@Test
	public void testValueStringMixedCase() {
		CellMapperBlob mapper = new CellMapperBlob();
		String parsed = mapper.indexValue("F1a2B3");
		Assert.assertEquals("f1a2b3", parsed);
	}

	@Test(expected = NumberFormatException.class)
	public void testValueStringOdd() {
		CellMapperBlob mapper = new CellMapperBlob();
		mapper.indexValue("f");
	}

	@Test
	public void testValueByteBuffer() {
		CellMapperBlob mapper = new CellMapperBlob();
		ByteBuffer bb = ByteBufferUtil.hexToBytes("f1");
		String parsed = mapper.indexValue(bb);
		Assert.assertEquals("f1", parsed);
	}

	@Test
	public void testValueBytes() {
		CellMapperBlob mapper = new CellMapperBlob();
		byte[] bytes = Hex.hexToBytes("f1");
		String parsed = mapper.indexValue(bytes);
		Assert.assertEquals("f1", parsed);
	}

	@Test
	public void testField() {
		CellMapperBlob mapper = new CellMapperBlob();
		Field field = mapper.field("name", "f1B2");
		Assert.assertNotNull(field);
		Assert.assertEquals("f1b2", field.stringValue());
		Assert.assertEquals("name", field.name());
		Assert.assertEquals(false, field.fieldType().stored());
	}

	@Test
	public void testExtractAnalyzers() {
		CellMapperBlob mapper = new CellMapperBlob();
		Analyzer analyzer = mapper.analyzer();
		Assert.assertEquals(CellMapper.EMPTY_ANALYZER, analyzer);
	}

	@Test
	public void testParseJSON() throws IOException {
		String json = "{fields:{age:{type:\"bytes\"}}}";
		CellsMapper cellsMapper = CellsMapper.fromJson(json);
		CellMapper<?> cellMapper = cellsMapper.getMapper("age");
		Assert.assertNotNull(cellMapper);
		Assert.assertEquals(CellMapperBlob.class, cellMapper.getClass());
	}

	@Test(expected = IllegalArgumentException.class)
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
