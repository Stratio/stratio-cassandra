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

public class ColumnMapperFloatTest {

    @Test
    public void testConstructorWithoutArgs() {
        ColumnMapperFloat mapper = new ColumnMapperFloat(null, null, null);
        Assert.assertEquals(ColumnMapper.INDEXED, mapper.isIndexed());
        Assert.assertEquals(ColumnMapper.SORTED, mapper.isSorted());
        Assert.assertEquals(ColumnMapperDouble.DEFAULT_BOOST, mapper.getBoost(), 1);
    }

    @Test
    public void testConstructorWithAllArgs() {
        ColumnMapperFloat mapper = new ColumnMapperFloat(false, true, 2.3f);
        Assert.assertFalse(mapper.isIndexed());
        Assert.assertTrue(mapper.isSorted());
        Assert.assertEquals(2.3f, mapper.getBoost(), 1);
    }

    @Test()
    public void testValueNull() {
        ColumnMapperFloat mapper = new ColumnMapperFloat(null, null, 1f);
        Float parsed = mapper.base("test", null);
        Assert.assertNull(parsed);
    }

    @Test
    public void testValueInteger() {
        ColumnMapperFloat mapper = new ColumnMapperFloat(null, null, 1f);
        Float parsed = mapper.base("test", 3);
        Assert.assertEquals(Float.valueOf(3), parsed);
    }

    @Test
    public void testValueLong() {
        ColumnMapperFloat mapper = new ColumnMapperFloat(null, null, 1f);
        Float parsed = mapper.base("test", 3l);
        Assert.assertEquals(Float.valueOf(3), parsed);
    }

    @Test
    public void testValueFloatWithoutDecimal() {
        ColumnMapperFloat mapper = new ColumnMapperFloat(null, null, 1f);
        Float parsed = mapper.base("test", 3f);
        Assert.assertEquals(Float.valueOf(3), parsed);
    }

    @Test
    public void testValueFloatWithDecimalFloor() {
        ColumnMapperFloat mapper = new ColumnMapperFloat(null, null, 1f);
        Float parsed = mapper.base("test", 3.5f);
        Assert.assertEquals(Float.valueOf(3.5f), parsed);

    }

    @Test
    public void testValueFloatWithDecimalCeil() {
        ColumnMapperFloat mapper = new ColumnMapperFloat(null, null, 1f);
        Float parsed = mapper.base("test", 3.6f);
        Assert.assertEquals(Float.valueOf(3.6f), parsed);

    }

    @Test
    public void testValueDoubleWithoutDecimal() {
        ColumnMapperFloat mapper = new ColumnMapperFloat(null, null, 1f);
        Float parsed = mapper.base("test", 3d);
        Assert.assertEquals(Float.valueOf(3), parsed);
    }

    @Test
    public void testValueDoubleWithDecimalFloor() {
        ColumnMapperFloat mapper = new ColumnMapperFloat(null, null, 1f);
        Float parsed = mapper.base("test", 3.5d);
        Assert.assertEquals(Float.valueOf(3.5f), parsed);

    }

    @Test
    public void testValueDoubleWithDecimalCeil() {
        ColumnMapperFloat mapper = new ColumnMapperFloat(null, null, 1f);
        Float parsed = mapper.base("test", 3.6d);
        Assert.assertEquals(Float.valueOf(3.6f), parsed);

    }

    @Test
    public void testValueStringWithoutDecimal() {
        ColumnMapperFloat mapper = new ColumnMapperFloat(null, null, 1f);
        Float parsed = mapper.base("test", "3");
        Assert.assertEquals(Float.valueOf(3), parsed);
    }

    @Test
    public void testValueStringWithDecimalFloor() {
        ColumnMapperFloat mapper = new ColumnMapperFloat(null, null, 1f);
        Float parsed = mapper.base("test", "3.2");
        Assert.assertEquals(Float.valueOf(3.2f), parsed);
    }

    @Test
    public void testValueStringWithDecimalCeil() {
        ColumnMapperFloat mapper = new ColumnMapperFloat(null, null, 1f);
        Float parsed = mapper.base("test", "3.6");
        Assert.assertEquals(Float.valueOf(3.6f), parsed);

    }

    @Test
    public void testFieldsIndexedSorted() {
        ColumnMapperFloat mapper = new ColumnMapperFloat(true, true, 1f);
        List<Field> fields = mapper.fields("name", "3.2");
        Assert.assertNotNull(fields);
        Assert.assertEquals(2, fields.size());
        Field field = fields.get(0);
        Assert.assertNotNull(field);
        Assert.assertEquals(3.2f, field.numericValue());
        Assert.assertEquals("name", field.name());
        Assert.assertEquals(false, field.fieldType().stored());
        field = fields.get(1);
        Assert.assertEquals(DocValuesType.NUMERIC, field.fieldType().docValuesType());
    }

    @Test
    public void testFieldsIndexedUnsorted() {
        ColumnMapperFloat mapper = new ColumnMapperFloat(true, false, 1f);
        List<Field> fields = mapper.fields("name", "3.2");
        Assert.assertNotNull(fields);
        Assert.assertEquals(1, fields.size());
        Field field = fields.get(0);
        Assert.assertNotNull(field);
        Assert.assertEquals(3.2f, field.numericValue());
        Assert.assertEquals("name", field.name());
        Assert.assertEquals(false, field.fieldType().stored());
    }

    @Test
    public void testFieldsUnindexedSorted() {
        ColumnMapperFloat mapper = new ColumnMapperFloat(false, true, 1f);
        List<Field> fields = mapper.fields("name", "3.2");
        Assert.assertNotNull(fields);
        Assert.assertEquals(1, fields.size());
        Field field = fields.get(0);
        Assert.assertEquals(DocValuesType.NUMERIC, field.fieldType().docValuesType());
    }

    @Test
    public void testFieldsUnindexedUnsorted() {
        ColumnMapperFloat mapper = new ColumnMapperFloat(false, false, 1f);
        List<Field> fields = mapper.fields("name", "3.2");
        Assert.assertNotNull(fields);
        Assert.assertEquals(0, fields.size());
    }

    @Test
    public void testExtractAnalyzers() {
        ColumnMapperFloat mapper = new ColumnMapperFloat(null, null, 1f);
        String analyzer = mapper.getAnalyzer();
        Assert.assertEquals(ColumnMapper.KEYWORD_ANALYZER, analyzer);
    }

    @Test
    public void testParseJSONWithoutArgs() throws IOException {
        String json = "{fields:{age:{type:\"float\"}}}";
        Schema schema = Schema.fromJson(json);
        ColumnMapper columnMapper = schema.getMapper("age");
        Assert.assertNotNull(columnMapper);
        Assert.assertEquals(ColumnMapperFloat.class, columnMapper.getClass());
        Assert.assertEquals(ColumnMapper.INDEXED, columnMapper.isIndexed());
        Assert.assertEquals(ColumnMapper.SORTED, columnMapper.isSorted());
        Assert.assertEquals(ColumnMapperFloat.DEFAULT_BOOST, ((ColumnMapperFloat) columnMapper).getBoost(), 1);
    }

    @Test
    public void testParseJSONWithAllArgs() throws IOException {
        String json = "{fields:{age:{type:\"float\", indexed:\"false\", sorted:\"true\", boost:\"5\"}}}";
        Schema schema = Schema.fromJson(json);
        ColumnMapper columnMapper = schema.getMapper("age");
        Assert.assertNotNull(columnMapper);
        Assert.assertEquals(ColumnMapperFloat.class, columnMapper.getClass());
        Assert.assertFalse(columnMapper.isIndexed());
        Assert.assertTrue(columnMapper.isSorted());
        Assert.assertEquals(5, ((ColumnMapperFloat) columnMapper).getBoost(), 1);
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
