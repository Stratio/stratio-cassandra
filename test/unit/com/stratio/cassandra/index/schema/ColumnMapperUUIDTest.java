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
package com.stratio.cassandra.index.schema;

import java.io.IOException;
import java.util.UUID;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Field;
import org.junit.Assert;
import org.junit.Test;

public class ColumnMapperUUIDTest
{

    @Test()
    public void testValueNull()
    {
        ColumnMapperUUID mapper = new ColumnMapperUUID();
        String parsed = mapper.indexValue("test", null);
        Assert.assertNull(parsed);
    }

    @Test
    public void testValueUUID()
    {
        ColumnMapperUUID mapper = new ColumnMapperUUID();
        String parsed = mapper.indexValue("test", UUID.fromString("550e8400-e29b-41d4-a716-446655440000"));
        Assert.assertEquals("550e8400-e29b-41d4-a716-446655440000", parsed);
    }

    @Test
    public void testValueString()
    {
        ColumnMapperUUID mapper = new ColumnMapperUUID();
        String parsed = mapper.indexValue("test", "550e8400-e29b-41d4-a716-446655440000");
        Assert.assertEquals("550e8400-e29b-41d4-a716-446655440000", parsed);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValueStringInvalid()
    {
        ColumnMapperUUID mapper = new ColumnMapperUUID();
        mapper.indexValue("test", "550e840");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValueInteger()
    {
        ColumnMapperUUID mapper = new ColumnMapperUUID();
        String parsed = mapper.indexValue("test", 3);
        Assert.assertEquals("3", parsed);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValueLong()
    {
        ColumnMapperUUID mapper = new ColumnMapperUUID();
        String parsed = mapper.indexValue("test", 3l);
        Assert.assertEquals("3", parsed);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValueFloat()
    {
        ColumnMapperUUID mapper = new ColumnMapperUUID();
        String parsed = mapper.indexValue("test", 3.6f);
        Assert.assertEquals("3.6", parsed);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValueDouble()
    {
        ColumnMapperUUID mapper = new ColumnMapperUUID();
        String parsed = mapper.indexValue("test", 3d);
        Assert.assertEquals("3.0", parsed);
    }

    @Test
    public void testField()
    {
        ColumnMapperUUID mapper = new ColumnMapperUUID();
        UUID uuid = UUID.fromString("550e8400-e29b-41d4-a716-446655440000");
        Field field = mapper.field("name", uuid);
        Assert.assertNotNull(field);
        Assert.assertEquals("name", field.name());
        Assert.assertEquals(uuid.toString(), field.stringValue());
        Assert.assertFalse(field.fieldType().stored());
    }

    @Test
    public void testExtractAnalyzers()
    {
        ColumnMapperUUID mapper = new ColumnMapperUUID();
        Analyzer analyzer = mapper.analyzer();
        Assert.assertEquals(ColumnMapper.EMPTY_ANALYZER, analyzer);
    }

    @Test
    public void testParseJSON() throws IOException
    {
        String json = "{fields:{age:{type:\"uuid\"}}}";
        Schema schema = Schema.fromJson(json);
        ColumnMapper<?> columnMapper = schema.getMapper("age");
        Assert.assertNotNull(columnMapper);
        Assert.assertEquals(ColumnMapperUUID.class, columnMapper.getClass());
    }

    @Test
    public void testParseJSONEmpty() throws IOException
    {
        String json = "{fields:{}}";
        Schema schema = Schema.fromJson(json);
        ColumnMapper<?> columnMapper = schema.getMapper("age");
        Assert.assertNull(columnMapper);
    }

    @Test(expected = IOException.class)
    public void testParseJSONInvalid() throws IOException
    {
        String json = "{fields:{age:{}}";
        Schema.fromJson(json);
    }
}
