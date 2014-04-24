/*
 * Copyright 2014, Stratio.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
		String parsed = mapper.indexValue("test", null);
		Assert.assertNull(parsed);
	}

	@Test
	public void testValueBooleanTrue() {
		CellMapperBoolean mapper = new CellMapperBoolean();
		String parsed = mapper.indexValue("test", true);
		Assert.assertEquals("true", parsed);
	}

	@Test
	public void testValueBooleanFalse() {
		CellMapperBoolean mapper = new CellMapperBoolean();
		String parsed = mapper.indexValue("test", false);
		Assert.assertEquals("false", parsed);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testValueDate() {
		CellMapperBoolean mapper = new CellMapperBoolean();
		mapper.indexValue("test", new Date());
	}

	@Test(expected = IllegalArgumentException.class)
	public void testValueInteger() {
		CellMapperBoolean mapper = new CellMapperBoolean();
		mapper.indexValue("test", 3);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testValueLong() {
		CellMapperBoolean mapper = new CellMapperBoolean();
		mapper.indexValue("test", 3l);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testValueFloat() {
		CellMapperBoolean mapper = new CellMapperBoolean();
		mapper.indexValue("test", 3.6f);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testValueDouble() {
		CellMapperBoolean mapper = new CellMapperBoolean();
		mapper.indexValue("test", 3.5d);
	}

	@Test
	public void testValueStringTrueLowercase() {
		CellMapperBoolean mapper = new CellMapperBoolean();
		String parsed = mapper.indexValue("test", "true");
		Assert.assertEquals("true", parsed);
	}

	@Test
	public void testValueStringTrueUppercase() {
		CellMapperBoolean mapper = new CellMapperBoolean();
		String parsed = mapper.indexValue("test", "TRUE");
		Assert.assertEquals("true", parsed);
	}

	@Test
	public void testValueStringTrueMixedcase() {
		CellMapperBoolean mapper = new CellMapperBoolean();
		String parsed = mapper.indexValue("test", "TrUe");
		Assert.assertEquals("true", parsed);
	}

	@Test
	public void testValueStringFalseLowercase() {
		CellMapperBoolean mapper = new CellMapperBoolean();
		String parsed = mapper.indexValue("test", "false");
		Assert.assertEquals("false", parsed);
	}

	@Test
	public void testValueStringFalseUppercase() {
		CellMapperBoolean mapper = new CellMapperBoolean();
		String parsed = mapper.indexValue("test", "FALSE");
		Assert.assertEquals("false", parsed);
	}

	@Test
	public void testValueStringFalseMixedcase() {
		CellMapperBoolean mapper = new CellMapperBoolean();
		String parsed = mapper.indexValue("test", "fALsE");
		Assert.assertEquals("false", parsed);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testValueStringInvalid() {
		CellMapperBoolean mapper = new CellMapperBoolean();
		mapper.indexValue("test", "hello");
	}

	@Test(expected = IllegalArgumentException.class)
	public void testValueUUID() {
		CellMapperBoolean mapper = new CellMapperBoolean();
		mapper.indexValue("test", UUID.fromString("550e8400-e29b-41d4-a716-446655440000"));
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
