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
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Hex;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.DocValuesType;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.UUID;

public class ColumnMapperBlobTest {

    @Test
    public void testConstructorWithoutArgs() {
        ColumnMapperBlob mapper = new ColumnMapperBlob(null, null);
        Assert.assertEquals(ColumnMapper.INDEXED, mapper.isIndexed());
        Assert.assertEquals(ColumnMapper.SORTED, mapper.isSorted());
    }

    @Test
    public void testConstructorWithAllArgs() {
        ColumnMapperBlob mapper = new ColumnMapperBlob(false, true);
        Assert.assertFalse(mapper.isIndexed());
        Assert.assertTrue(mapper.isSorted());
    }

    @Test()
    public void testValueNull() {
        ColumnMapperBlob mapper = new ColumnMapperBlob(null, null);
        String parsed = mapper.base("test", null);
        Assert.assertNull(parsed);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValueInteger() {
        ColumnMapperBlob mapper = new ColumnMapperBlob(null, null);
        mapper.base("test", 3);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValueLong() {
        ColumnMapperBlob mapper = new ColumnMapperBlob(null, null);
        mapper.base("test", 3l);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValueFloat() {
        ColumnMapperBlob mapper = new ColumnMapperBlob(null, null);
        mapper.base("test", 3.5f);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValueDouble() {
        ColumnMapperBlob mapper = new ColumnMapperBlob(null, null);
        mapper.base("test", 3.6d);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValueUUID() {
        ColumnMapperBlob mapper = new ColumnMapperBlob(null, null);
        mapper.base("test", UUID.randomUUID());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValueStringInvalid() {
        ColumnMapperBlob mapper = new ColumnMapperBlob(null, null);
        mapper.base("test", "Hello");
    }

    @Test
    public void testValueStringLowerCaseWithoutPrefix() {
        ColumnMapperBlob mapper = new ColumnMapperBlob(null, null);
        String parsed = mapper.base("test", "f1");
        Assert.assertEquals("f1", parsed);
    }

    @Test
    public void testValueStringUpperCaseWithoutPrefix() {
        ColumnMapperBlob mapper = new ColumnMapperBlob(null, null);
        String parsed = mapper.base("test", "F1");
        Assert.assertEquals("f1", parsed);
    }

    @Test
    public void testValueStringMixedCaseWithoutPrefix() {
        ColumnMapperBlob mapper = new ColumnMapperBlob(null, null);
        String parsed = mapper.base("test", "F1a2B3");
        Assert.assertEquals("f1a2b3", parsed);
    }

    @Test
    public void testValueStringLowerCaseWithPrefix() {
        ColumnMapperBlob mapper = new ColumnMapperBlob(null, null);
        String parsed = mapper.base("test", "0xf1");
        Assert.assertEquals("f1", parsed);
    }

    @Test
    public void testValueStringUpperCaseWithPrefix() {
        ColumnMapperBlob mapper = new ColumnMapperBlob(null, null);
        String parsed = mapper.base("test", "0xF1");
        Assert.assertEquals("f1", parsed);
    }

    @Test
    public void testValueStringMixedCaseWithPrefix() {
        ColumnMapperBlob mapper = new ColumnMapperBlob(null, null);
        String parsed = mapper.base("test", "0xF1a2B3");
        Assert.assertEquals("f1a2b3", parsed);
    }

    @Test(expected = NumberFormatException.class)
    public void testValueStringOdd() {
        ColumnMapperBlob mapper = new ColumnMapperBlob(null, null);
        mapper.base("test", "f");
    }

    @Test
    public void testValueByteBuffer() {
        ColumnMapperBlob mapper = new ColumnMapperBlob(null, null);
        ByteBuffer bb = ByteBufferUtil.hexToBytes("f1");
        String parsed = mapper.base("test", bb);
        Assert.assertEquals("f1", parsed);
    }

    @Test
    public void testValueBytes() {
        ColumnMapperBlob mapper = new ColumnMapperBlob(null, null);
        byte[] bytes = Hex.hexToBytes("f1");
        String parsed = mapper.base("test", bytes);
        Assert.assertEquals("f1", parsed);
    }

    @Test
    public void testFieldsIndexedSorted() {
        ColumnMapperBlob mapper = new ColumnMapperBlob(true, true);
        List<Field> fields = mapper.fields("name", "f1B2");
        Assert.assertNotNull(fields);
        Assert.assertEquals(2, fields.size());
        Field field = fields.get(0);
        Assert.assertNotNull(field);
        Assert.assertEquals("f1b2", field.stringValue());
        Assert.assertEquals("name", field.name());
        Assert.assertEquals(false, field.fieldType().stored());
        field = fields.get(1);
        Assert.assertEquals(DocValuesType.SORTED, field.fieldType().docValuesType());
    }

    @Test
    public void testFieldsIndexedUnsorted() {
        ColumnMapperBlob mapper = new ColumnMapperBlob(true, false);
        List<Field> fields = mapper.fields("name", "f1B2");
        Assert.assertNotNull(fields);
        Assert.assertEquals(1, fields.size());
        Field field = fields.get(0);
        Assert.assertNotNull(field);
        Assert.assertEquals("f1b2", field.stringValue());
        Assert.assertEquals("name", field.name());
        Assert.assertEquals(false, field.fieldType().stored());
    }

    @Test
    public void testFieldsUnindexedSorted() {
        ColumnMapperBlob mapper = new ColumnMapperBlob(false, true);
        List<Field> fields = mapper.fields("name", "f1B2");
        Assert.assertNotNull(fields);
        Assert.assertEquals(1, fields.size());
        Field field = fields.get(0);
        Assert.assertEquals(DocValuesType.SORTED, field.fieldType().docValuesType());
    }

    @Test
    public void testFieldsUnindexedUnsorted() {
        ColumnMapperBlob mapper = new ColumnMapperBlob(false, false);
        List<Field> fields = mapper.fields("name", "f1B2");
        Assert.assertNotNull(fields);
        Assert.assertEquals(0, fields.size());
    }

    @Test
    public void testExtractAnalyzers() {
        ColumnMapperBlob mapper = new ColumnMapperBlob(null, null);
        String analyzer = mapper.getAnalyzer();
        Assert.assertEquals(ColumnMapper.KEYWORD_ANALYZER, analyzer);
    }

    @Test
    public void testParseJSONWithoutArgs() throws IOException {
        String json = "{fields:{age:{type:\"bytes\"}}}";
        Schema schema = Schema.fromJson(json);
        ColumnMapper columnMapper = schema.getMapper("age");
        Assert.assertNotNull(columnMapper);
        Assert.assertEquals(ColumnMapperBlob.class, columnMapper.getClass());
        Assert.assertEquals(ColumnMapper.INDEXED, columnMapper.isIndexed());
        Assert.assertEquals(ColumnMapper.SORTED, columnMapper.isSorted());
    }

    @Test
    public void testParseJSONWithAllArgs() throws IOException {
        String json = "{fields:{age:{type:\"bytes\", indexed:\"false\", sorted:\"true\"}}}";
        Schema schema = Schema.fromJson(json);
        ColumnMapper columnMapper = schema.getMapper("age");
        Assert.assertNotNull(columnMapper);
        Assert.assertEquals(ColumnMapperBlob.class, columnMapper.getClass());
        Assert.assertFalse(columnMapper.isIndexed());
        Assert.assertTrue(columnMapper.isSorted());
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
