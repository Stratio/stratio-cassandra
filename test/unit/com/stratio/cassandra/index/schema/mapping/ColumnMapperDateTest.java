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
package com.stratio.cassandra.index.schema.mapping;

import com.stratio.cassandra.index.schema.Schema;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.DocValuesType;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

public class ColumnMapperDateTest {

    private static final String PATTERN = "yyyy-MM-dd";
    private static final SimpleDateFormat sdf = new SimpleDateFormat(PATTERN);

    @Test
    public void testConstructorWithoutArgs() {
        ColumnMapperDate mapper = new ColumnMapperDate(null, null, null);
        Assert.assertEquals(ColumnMapper.INDEXED, mapper.isIndexed());
        Assert.assertEquals(ColumnMapper.SORTED, mapper.isSorted());
        Assert.assertEquals(ColumnMapperDate.DEFAULT_PATTERN, mapper.getPattern());
    }

    @Test
    public void testConstructorWithAllArgs() {
        ColumnMapperDate mapper = new ColumnMapperDate(false, true, PATTERN);
        Assert.assertFalse(mapper.isIndexed());
        Assert.assertTrue(mapper.isSorted());
        Assert.assertEquals(PATTERN, mapper.getPattern());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorWithWrongPattern() {
        new ColumnMapperDate(!ColumnMapper.INDEXED, !ColumnMapper.SORTED, "hello");
    }

    @Test()
    public void testValueNull() {
        ColumnMapperDate mapper = new ColumnMapperDate(null, null, PATTERN);
        Long parsed = mapper.base("test", null);
        Assert.assertNull(parsed);
    }

    @Test
    public void testValueDate() {
        ColumnMapperDate mapper = new ColumnMapperDate(null, null, PATTERN);
        Date date = new Date();
        long parsed = mapper.base("test", date);
        Assert.assertEquals(date.getTime(), parsed);
    }

    @Test
    public void testValueInteger() {
        ColumnMapperDate mapper = new ColumnMapperDate(null, null, PATTERN);
        Long parsed = mapper.base("test", 3);
        Assert.assertEquals(Long.valueOf(3), parsed);
    }

    @Test
    public void testValueLong() {
        ColumnMapperDate mapper = new ColumnMapperDate(null, null, PATTERN);
        Long parsed = mapper.base("test", 3l);
        Assert.assertEquals(Long.valueOf(3), parsed);
    }

    @Test
    public void testValueFloatWithoutDecimal() {
        ColumnMapperDate mapper = new ColumnMapperDate(null, null, PATTERN);
        Long parsed = mapper.base("test", 3f);
        Assert.assertEquals(Long.valueOf(3), parsed);
    }

    @Test
    public void testValueFloatWithDecimalFloor() {
        ColumnMapperDate mapper = new ColumnMapperDate(null, null, PATTERN);
        Long parsed = mapper.base("test", 3.5f);
        Assert.assertEquals(Long.valueOf(3), parsed);

    }

    @Test
    public void testValueFloatWithDecimalCeil() {
        ColumnMapperDate mapper = new ColumnMapperDate(null, null, PATTERN);
        Long parsed = mapper.base("test", 3.6f);
        Assert.assertEquals(Long.valueOf(3), parsed);

    }

    @Test
    public void testValueDoubleWithoutDecimal() {
        ColumnMapperDate mapper = new ColumnMapperDate(null, null, PATTERN);
        Long parsed = mapper.base("test", 3d);
        Assert.assertEquals(Long.valueOf(3), parsed);
    }

    @Test
    public void testValueDoubleWithDecimalFloor() {
        ColumnMapperDate mapper = new ColumnMapperDate(null, null, PATTERN);
        Long parsed = mapper.base("test", 3.5d);
        Assert.assertEquals(Long.valueOf(3), parsed);

    }

    @Test
    public void testValueDoubleWithDecimalCeil() {
        ColumnMapperDate mapper = new ColumnMapperDate(null, null, PATTERN);
        Long parsed = mapper.base("test", 3.6d);
        Assert.assertEquals(Long.valueOf(3), parsed);

    }

    @Test
    public void testValueStringWithPattern() throws ParseException {
        ColumnMapperDate mapper = new ColumnMapperDate(null, null, PATTERN);
        long parsed = mapper.base("test", "2014-03-19");
        Assert.assertEquals(sdf.parse("2014-03-19").getTime(), parsed);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValueStringWithPatternInvalid() {
        ColumnMapperDate mapper = new ColumnMapperDate(null, null, PATTERN);
        mapper.base("test", "2014/03/19");
    }

    @Test
    public void testValueStringWithoutPattern() throws ParseException {
        ColumnMapperDate mapper = new ColumnMapperDate(null, null, null);
        long parsed = mapper.base("test", "2014/03/19 00:00:00.000");
        Assert.assertEquals(sdf.parse("2014-03-19").getTime(), parsed);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValueStringWithoutPatternInvalid() throws ParseException {
        ColumnMapperDate mapper = new ColumnMapperDate(null, null, null);
        mapper.base("test", "2014-03-19");
    }

    @Test
    public void testFieldsIndexedSorted() throws ParseException {
        ColumnMapperDate mapper = new ColumnMapperDate(true, true, PATTERN);
        List<Field> fields = mapper.fields("name", "2014-03-19");
        Assert.assertNotNull(fields);
        Assert.assertEquals(2, fields.size());
        Field field = fields.get(0);
        Assert.assertNotNull(field);
        Assert.assertEquals(sdf.parse("2014-03-19").getTime(), field.numericValue().longValue());
        Assert.assertEquals("name", field.name());
        Assert.assertEquals(false, field.fieldType().stored());
        field = fields.get(1);
        Assert.assertEquals(DocValuesType.NUMERIC, field.fieldType().docValuesType());
    }

    @Test
    public void testFieldsIndexedUnsorted() throws ParseException {
        ColumnMapperDate mapper = new ColumnMapperDate(true, false, PATTERN);
        List<Field> fields = mapper.fields("name", "2014-03-19");
        Assert.assertNotNull(fields);
        Assert.assertEquals(1, fields.size());
        Field field = fields.get(0);
        Assert.assertNotNull(field);
        Assert.assertEquals(sdf.parse("2014-03-19").getTime(), field.numericValue().longValue());
        Assert.assertEquals("name", field.name());
        Assert.assertEquals(false, field.fieldType().stored());
    }

    @Test
    public void testFieldsUnindexedSorted() throws ParseException {
        ColumnMapperDate mapper = new ColumnMapperDate(false, true, PATTERN);
        List<Field> fields = mapper.fields("name", "2014-03-19");
        Assert.assertNotNull(fields);
        Assert.assertEquals(1, fields.size());
        Field field = fields.get(0);
        Assert.assertEquals(DocValuesType.NUMERIC, field.fieldType().docValuesType());
    }

    @Test
    public void testFieldsUnindexedUnsorted() throws ParseException {
        ColumnMapperDate mapper = new ColumnMapperDate(false, false, PATTERN);
        List<Field> fields = mapper.fields("name", "2014-03-19");
        Assert.assertNotNull(fields);
        Assert.assertEquals(0, fields.size());
    }

    @Test
    public void testExtractAnalyzers() {
        ColumnMapperDate mapper = new ColumnMapperDate(null, null, PATTERN);
        String analyzer = mapper.getAnalyzer();
        Assert.assertEquals(ColumnMapper.KEYWORD_ANALYZER, analyzer);
    }

    @Test
    public void testParseJSONWithoutArgs() throws IOException {
        String json = "{fields:{age:{type:\"date\"}}}";
        Schema schema = Schema.fromJson(json);
        ColumnMapper columnMapper = schema.getMapper("age");
        Assert.assertNotNull(columnMapper);
        Assert.assertEquals(ColumnMapperDate.class, columnMapper.getClass());
        Assert.assertEquals(ColumnMapper.INDEXED, columnMapper.isIndexed());
        Assert.assertEquals(ColumnMapper.SORTED, columnMapper.isSorted());
        Assert.assertEquals(ColumnMapperDate.DEFAULT_PATTERN, ((ColumnMapperDate) columnMapper).getPattern());
    }

    @Test
    public void testParseJSONWithAllArgs() throws IOException {
        String json = "{fields:{age:{type:\"date\", indexed:\"false\", sorted:\"true\"," +
                      " pattern:\"" + PATTERN + "\"}}}";
        Schema schema = Schema.fromJson(json);
        ColumnMapper columnMapper = schema.getMapper("age");
        Assert.assertNotNull(columnMapper);
        Assert.assertEquals(ColumnMapperDate.class, columnMapper.getClass());
        Assert.assertFalse(columnMapper.isIndexed());
        Assert.assertTrue(columnMapper.isSorted());
        Assert.assertEquals(PATTERN, ((ColumnMapperDate) columnMapper).getPattern());
    }

    @Test
    public void testParseJSONEmpty() throws IOException {
        String json = "{fields:{}}";
        Schema schema = Schema.fromJson(json);
        ColumnMapper columnMapper = schema.getMapper("age");
        Assert.assertNull(columnMapper);
    }

    @Test(expected = IOException.class)
    public void testParseJSONInvalid() throws IOException {
        String json = "{fields:{age:{}}";
        Schema.fromJson(json);
    }
}
