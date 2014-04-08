package org.apache.cassandra.db.index.stratio.schema;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.UUID;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Field;
import org.junit.Assert;
import org.junit.Test;

public class CellMapperInetTest {

	@Test()
	public void testValueNull() {
		CellMapperInet mapper = new CellMapperInet();
		String parsed = mapper.indexValue(null);
		Assert.assertNull(parsed);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testValueInteger() {
		CellMapperInet mapper = new CellMapperInet();
		mapper.indexValue(3);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testValueLong() {
		CellMapperInet mapper = new CellMapperInet();
		mapper.indexValue(3l);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testValueFloat() {
		CellMapperInet mapper = new CellMapperInet();
		mapper.indexValue(3.5f);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testValueDouble() {
		CellMapperInet mapper = new CellMapperInet();
		mapper.indexValue(3.6d);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testValueUUID() {
		CellMapperInet mapper = new CellMapperInet();
		mapper.indexValue(UUID.randomUUID());
	}

	@Test(expected = IllegalArgumentException.class)
	public void testValueStringInvalid() {
		CellMapperInet mapper = new CellMapperInet();
		mapper.indexValue("Hello");
	}

	@Test
	public void testValueStringV4WithoutZeros() {
		CellMapperInet mapper = new CellMapperInet();
		String parsed = mapper.indexValue("192.168.0.1");
		Assert.assertEquals("192.168.0.1", parsed);
	}

	@Test
	public void testValueStringV4WithZeros() {
		CellMapperInet mapper = new CellMapperInet();
		String parsed = mapper.indexValue("192.168.000.001");
		Assert.assertEquals("192.168.0.1", parsed);
	}

	@Test
	public void testValueStringV6WithoutZeros() {
		CellMapperInet mapper = new CellMapperInet();
		String parsed = mapper.indexValue("2001:db8:2de:0:0:0:0:e13");
		Assert.assertEquals("2001:db8:2de:0:0:0:0:e13", parsed);
	}

	@Test
	public void testValueStringV6WithZeros() {
		CellMapperInet mapper = new CellMapperInet();
		String parsed = mapper.indexValue("2001:0db8:02de:0000:0000:0000:0000:0e13");
		Assert.assertEquals("2001:db8:2de:0:0:0:0:e13", parsed);
	}

	@Test
	public void testValueStringV6Compact() {
		CellMapperInet mapper = new CellMapperInet();
		String parsed = mapper.indexValue("2001:DB8:2de::0e13");
		Assert.assertEquals("2001:db8:2de:0:0:0:0:e13", parsed);
	}

	@Test
	public void testValueInetV4() throws UnknownHostException {
		CellMapperInet mapper = new CellMapperInet();
		InetAddress inet = InetAddress.getByName("192.168.0.13");
		String parsed = mapper.indexValue(inet);
		Assert.assertEquals("192.168.0.13", parsed);
	}

	@Test
	public void testValueInetV6() throws UnknownHostException {
		CellMapperInet mapper = new CellMapperInet();
		InetAddress inet = InetAddress.getByName("2001:db8:2de:0:0:0:0:e13");
		String parsed = mapper.indexValue(inet);
		Assert.assertEquals("2001:db8:2de:0:0:0:0:e13", parsed);
	}

	@Test
	public void testField() {
		CellMapperInet mapper = new CellMapperInet();
		Field field = mapper.field("name", "192.168.0.13");
		Assert.assertNotNull(field);
		Assert.assertEquals("192.168.0.13", field.stringValue());
		Assert.assertEquals("name", field.name());
		Assert.assertEquals(false, field.fieldType().stored());
	}

	@Test
	public void testExtractAnalyzers() {
		CellMapperInet mapper = new CellMapperInet();
		Analyzer analyzer = mapper.analyzer();
		Assert.assertEquals(CellMapper.EMPTY_ANALYZER, analyzer);
	}

	@Test
	public void testParseJSON() throws IOException {
		String json = "{fields:{age:{type:\"inet\"}}}";
		CellsMapper cellsMapper = CellsMapper.fromJson(json);
		CellMapper<?> cellMapper = cellsMapper.getMapper("age");
		Assert.assertNotNull(cellMapper);
		Assert.assertEquals(CellMapperInet.class, cellMapper.getClass());
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
