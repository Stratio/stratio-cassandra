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
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Field;
import org.junit.Assert;
import org.junit.Test;

public class CellMapperDateTest {

	private static final String PATTERN = "yyyy-MM-dd";
	private static final SimpleDateFormat sdf = new SimpleDateFormat(PATTERN);

	@Test()
	public void testValueNull() {
		CellMapperDate mapper = new CellMapperDate(PATTERN);
		Long parsed = mapper.indexValue("test", null);
		Assert.assertNull(parsed);
	}

	@Test
	public void testValueDate() {
		CellMapperDate mapper = new CellMapperDate(PATTERN);
		Date date = new Date();
		long parsed = mapper.indexValue("test", date);
		Assert.assertEquals(date.getTime(), parsed);
	}

	@Test
	public void testValueInteger() {
		CellMapperDate mapper = new CellMapperDate(PATTERN);
		Long parsed = mapper.indexValue("test", 3);
		Assert.assertEquals(Long.valueOf(3), parsed);
	}

	@Test
	public void testValueLong() {
		CellMapperDate mapper = new CellMapperDate(PATTERN);
		Long parsed = mapper.indexValue("test", 3l);
		Assert.assertEquals(Long.valueOf(3), parsed);
	}

	@Test
	public void testValueFloatWithoutDecimal() {
		CellMapperDate mapper = new CellMapperDate(PATTERN);
		Long parsed = mapper.indexValue("test", 3f);
		Assert.assertEquals(Long.valueOf(3), parsed);
	}

	@Test
	public void testValueFloatWithDecimalFloor() {
		CellMapperDate mapper = new CellMapperDate(PATTERN);
		Long parsed = mapper.indexValue("test", 3.5f);
		Assert.assertEquals(Long.valueOf(3), parsed);

	}

	@Test
	public void testValueFloatWithDecimalCeil() {
		CellMapperDate mapper = new CellMapperDate(PATTERN);
		Long parsed = mapper.indexValue("test", 3.6f);
		Assert.assertEquals(Long.valueOf(3), parsed);

	}

	@Test
	public void testValueDoubleWithoutDecimal() {
		CellMapperDate mapper = new CellMapperDate(PATTERN);
		Long parsed = mapper.indexValue("test", 3d);
		Assert.assertEquals(Long.valueOf(3), parsed);
	}

	@Test
	public void testValueDoubleWithDecimalFloor() {
		CellMapperDate mapper = new CellMapperDate(PATTERN);
		Long parsed = mapper.indexValue("test", 3.5d);
		Assert.assertEquals(Long.valueOf(3), parsed);

	}

	@Test
	public void testValueDoubleWithDecimalCeil() {
		CellMapperDate mapper = new CellMapperDate(PATTERN);
		Long parsed = mapper.indexValue("test", 3.6d);
		Assert.assertEquals(Long.valueOf(3), parsed);

	}

	@Test
	public void testValueStringWithPattern() throws ParseException {
		CellMapperDate mapper = new CellMapperDate(PATTERN);
		long parsed = mapper.indexValue("test", "2014-03-19");
		Assert.assertEquals(sdf.parse("2014-03-19").getTime(), parsed);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testValueStringWithPatternInvalid() {
		CellMapperDate mapper = new CellMapperDate(PATTERN);
		mapper.indexValue("test", "2014/03/19");
	}

	@Test
	public void testValueStringWithoutPattern() throws ParseException {
		CellMapperDate mapper = new CellMapperDate(null);
		long parsed = mapper.indexValue("test", "2014/03/19 00:00:00.000");
		Assert.assertEquals(sdf.parse("2014-03-19").getTime(), parsed);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testValueStringWithoutPatternInvalid() throws ParseException {
		CellMapperDate mapper = new CellMapperDate(null);
		mapper.indexValue("test", "2014-03-19");
	}

	@Test
	public void testField() throws ParseException {
		CellMapperDate mapper = new CellMapperDate(PATTERN);
		Field field = mapper.field("name", "2014-03-19");
		Assert.assertNotNull(field);
		Assert.assertEquals(sdf.parse("2014-03-19").getTime(), field.numericValue().longValue());
		Assert.assertEquals("name", field.name());
		Assert.assertEquals(false, field.fieldType().stored());
	}

	@Test
	public void testExtractAnalyzers() {
		CellMapperDate mapper = new CellMapperDate(PATTERN);
		Analyzer analyzer = mapper.analyzer();
		Assert.assertEquals(CellMapper.EMPTY_ANALYZER, analyzer);
	}

	@Test
	public void testParseJSON() throws IOException {
		String json = "{fields:{age:{type:\"date\"}}}";
		Schema schema = Schema.fromJson(json);
		CellMapper<?> cellMapper = schema.getMapper("age");
		Assert.assertNotNull(cellMapper);
		Assert.assertEquals(CellMapperDate.class, cellMapper.getClass());
	}

	@Test(expected = IllegalArgumentException.class)
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
