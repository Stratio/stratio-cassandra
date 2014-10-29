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
import java.util.Date;
import java.util.UUID;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Field;
import org.junit.Assert;
import org.junit.Test;


public class ColumnMapperBooleanTest
{

    @Test()
    public void testValueNull()
    {
        ColumnMapperBoolean mapper = new ColumnMapperBoolean();
        String parsed = mapper.indexValue("test", null);
        Assert.assertNull(parsed);
    }

    @Test
    public void testValueBooleanTrue()
    {
        ColumnMapperBoolean mapper = new ColumnMapperBoolean();
        String parsed = mapper.indexValue("test", true);
        Assert.assertEquals("true", parsed);
    }

    @Test
    public void testValueBooleanFalse()
    {
        ColumnMapperBoolean mapper = new ColumnMapperBoolean();
        String parsed = mapper.indexValue("test", false);
        Assert.assertEquals("false", parsed);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValueDate()
    {
        ColumnMapperBoolean mapper = new ColumnMapperBoolean();
        mapper.indexValue("test", new Date());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValueInteger()
    {
        ColumnMapperBoolean mapper = new ColumnMapperBoolean();
        mapper.indexValue("test", 3);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValueLong()
    {
        ColumnMapperBoolean mapper = new ColumnMapperBoolean();
        mapper.indexValue("test", 3l);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValueFloat()
    {
        ColumnMapperBoolean mapper = new ColumnMapperBoolean();
        mapper.indexValue("test", 3.6f);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValueDouble()
    {
        ColumnMapperBoolean mapper = new ColumnMapperBoolean();
        mapper.indexValue("test", 3.5d);
    }

    @Test
    public void testValueStringTrueLowercase()
    {
        ColumnMapperBoolean mapper = new ColumnMapperBoolean();
        String parsed = mapper.indexValue("test", "true");
        Assert.assertEquals("true", parsed);
    }

    @Test
    public void testValueStringTrueUppercase()
    {
        ColumnMapperBoolean mapper = new ColumnMapperBoolean();
        String parsed = mapper.indexValue("test", "TRUE");
        Assert.assertEquals("true", parsed);
    }

    @Test
    public void testValueStringTrueMixedcase()
    {
        ColumnMapperBoolean mapper = new ColumnMapperBoolean();
        String parsed = mapper.indexValue("test", "TrUe");
        Assert.assertEquals("true", parsed);
    }

    @Test
    public void testValueStringFalseLowercase()
    {
        ColumnMapperBoolean mapper = new ColumnMapperBoolean();
        String parsed = mapper.indexValue("test", "false");
        Assert.assertEquals("false", parsed);
    }

    @Test
    public void testValueStringFalseUppercase()
    {
        ColumnMapperBoolean mapper = new ColumnMapperBoolean();
        String parsed = mapper.indexValue("test", "FALSE");
        Assert.assertEquals("false", parsed);
    }

    @Test
    public void testValueStringFalseMixedcase()
    {
        ColumnMapperBoolean mapper = new ColumnMapperBoolean();
        String parsed = mapper.indexValue("test", "fALsE");
        Assert.assertEquals("false", parsed);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValueStringInvalid()
    {
        ColumnMapperBoolean mapper = new ColumnMapperBoolean();
        mapper.indexValue("test", "hello");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValueUUID()
    {
        ColumnMapperBoolean mapper = new ColumnMapperBoolean();
        mapper.indexValue("test", UUID.fromString("550e8400-e29b-41d4-a716-446655440000"));
    }

    @Test
    public void testField()
    {
        ColumnMapperBoolean mapper = new ColumnMapperBoolean();
        Field field = mapper.field("name", "true");
        Assert.assertNotNull(field);
        Assert.assertEquals("true", field.stringValue());
        Assert.assertEquals("name", field.name());
        Assert.assertFalse(field.fieldType().stored());
    }

    @Test
    public void testExtractAnalyzers()
    {
        ColumnMapperBoolean mapper = new ColumnMapperBoolean();
        Analyzer analyzer = mapper.analyzer();
        Assert.assertEquals(ColumnMapper.EMPTY_ANALYZER, analyzer);
    }

    @Test
    public void testParseJSON() throws IOException
    {
        String json = "{fields:{age:{type:\"boolean\"}}}";
        Schema schema = Schema.fromJson(json);
        ColumnMapper<?> columnMapper = schema.getMapper("age");
        Assert.assertNotNull(columnMapper);
        Assert.assertEquals(ColumnMapperBoolean.class, columnMapper.getClass());
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
