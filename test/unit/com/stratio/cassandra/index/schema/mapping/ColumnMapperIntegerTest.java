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
import java.util.List;

public class ColumnMapperIntegerTest {

    @Test()
    public void testValueNull() {
        ColumnMapperInteger mapper = new ColumnMapperInteger(null, null, 1f);
        Integer parsed = mapper.indexValue("test", null);
        Assert.assertNull(parsed);
    }

    @Test
    public void testValueInteger() {
        ColumnMapperInteger mapper = new ColumnMapperInteger(null, null, 1f);
        Integer parsed = mapper.indexValue("test", 3);
        Assert.assertEquals(Integer.valueOf(3), parsed);
    }

    @Test
    public void testValueLong() {
        ColumnMapperInteger mapper = new ColumnMapperInteger(null, null, 1f);
        Integer parsed = mapper.indexValue("test", 3l);
        Assert.assertEquals(Integer.valueOf(3), parsed);
    }

    @Test
    public void testValueFloatWithoutDecimal() {
        ColumnMapperInteger mapper = new ColumnMapperInteger(null, null, 1f);
        Integer parsed = mapper.indexValue("test", 3f);
        Assert.assertEquals(Integer.valueOf(3), parsed);
    }

    @Test
    public void testValueFloatWithDecimalFloor() {
        ColumnMapperInteger mapper = new ColumnMapperInteger(null, null, 1f);
        Integer parsed = mapper.indexValue("test", 3.5f);
        Assert.assertEquals(Integer.valueOf(3), parsed);

    }

    @Test
    public void testValueFloatWithDecimalCeil() {
        ColumnMapperInteger mapper = new ColumnMapperInteger(null, null, 1f);
        Integer parsed = mapper.indexValue("test", 3.6f);
        Assert.assertEquals(Integer.valueOf(3), parsed);

    }

    @Test
    public void testValueDoubleWithoutDecimal() {
        ColumnMapperInteger mapper = new ColumnMapperInteger(null, null, 1f);
        Integer parsed = mapper.indexValue("test", 3d);
        Assert.assertEquals(Integer.valueOf(3), parsed);
    }

    @Test
    public void testValueDoubleWithDecimalFloor() {
        ColumnMapperInteger mapper = new ColumnMapperInteger(null, null, 1f);
        Integer parsed = mapper.indexValue("test", 3.5d);
        Assert.assertEquals(Integer.valueOf(3), parsed);

    }

    @Test
    public void testValueDoubleWithDecimalCeil() {
        ColumnMapperInteger mapper = new ColumnMapperInteger(null, null, 1f);
        Integer parsed = mapper.indexValue("test", 3.6d);
        Assert.assertEquals(Integer.valueOf(3), parsed);

    }

    @Test
    public void testValueStringWithoutDecimal() {
        ColumnMapperInteger mapper = new ColumnMapperInteger(null, null, 1f);
        Integer parsed = mapper.indexValue("test", "3");
        Assert.assertEquals(Integer.valueOf(3), parsed);
    }

    @Test
    public void testValueStringWithDecimalFloor() {
        ColumnMapperInteger mapper = new ColumnMapperInteger(null, null, 1f);
        Integer parsed = mapper.indexValue("test", "3.2");
        Assert.assertEquals(Integer.valueOf(3), parsed);

    }

    @Test
    public void testValueStringWithDecimalCeil() {
        ColumnMapperInteger mapper = new ColumnMapperInteger(null, null, 1f);
        Integer parsed = mapper.indexValue("test", "3.2");
        Assert.assertEquals(Integer.valueOf(3), parsed);

    }

    @Test
    public void testField() {
        ColumnMapperInteger mapper = new ColumnMapperInteger(null, null, 1f);
        List<Field> fields = mapper.fields("name", "3.2");
        Assert.assertNotNull(fields);
        Assert.assertEquals(2, fields.size());
        Field field = fields.get(0);
        Assert.assertNotNull(field);
        Assert.assertEquals(3, field.numericValue());
        Assert.assertEquals("name", field.name());
        Assert.assertEquals(false, field.fieldType().stored());
        field = fields.get(1);
        Assert.assertEquals(DocValuesType.NUMERIC, field.fieldType().docValuesType());
    }

    @Test
    public void testExtractAnalyzers() {
        ColumnMapperInteger mapper = new ColumnMapperInteger(null, null, 1f);
        String analyzer = mapper.analyzer();
        Assert.assertEquals(ColumnMapper.KEYWORD_ANALYZER, analyzer);
    }

    @Test
    public void testParseJSON() throws IOException {
        String json = "{fields:{age:{type:\"integer\"}}}";
        Schema schema = Schema.fromJson(json);
        ColumnMapper columnMapper = schema.getMapper("age");
        Assert.assertNotNull(columnMapper);
        Assert.assertEquals(ColumnMapperInteger.class, columnMapper.getClass());
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
