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
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class ColumnMapperLongTest
{

    @Test
    public void testValueNull()
    {
        ColumnMapperLong mapper = new ColumnMapperLong(1f);
        Long parsed = mapper.indexValue("test", null);
        Assert.assertNull(parsed);
    }

    @Test
    public void testValueInteger()
    {
        ColumnMapperLong mapper = new ColumnMapperLong(1f);
        Long parsed = mapper.indexValue("test", 3);
        Assert.assertEquals(Long.valueOf(3), parsed);
    }

    @Test
    public void testValueLong()
    {
        ColumnMapperLong mapper = new ColumnMapperLong(1f);
        Long parsed = mapper.indexValue("test", 3l);
        Assert.assertEquals(Long.valueOf(3), parsed);
    }

    @Test
    public void testValueFloatWithoutDecimal()
    {
        ColumnMapperLong mapper = new ColumnMapperLong(1f);
        Long parsed = mapper.indexValue("test", 3f);
        Assert.assertEquals(Long.valueOf(3), parsed);
    }

    @Test
    public void testValueFloatWithDecimalFloor()
    {
        ColumnMapperLong mapper = new ColumnMapperLong(1f);
        Long parsed = mapper.indexValue("test", 3.5f);
        Assert.assertEquals(Long.valueOf(3), parsed);

    }

    @Test
    public void testValueFloatWithDecimalCeil()
    {
        ColumnMapperLong mapper = new ColumnMapperLong(1f);
        Long parsed = mapper.indexValue("test", 3.6f);
        Assert.assertEquals(Long.valueOf(3), parsed);

    }

    @Test
    public void testValueDoubleWithoutDecimal()
    {
        ColumnMapperLong mapper = new ColumnMapperLong(1f);
        Long parsed = mapper.indexValue("test", 3d);
        Assert.assertEquals(Long.valueOf(3), parsed);
    }

    @Test
    public void testValueDoubleWithDecimalFloor()
    {
        ColumnMapperLong mapper = new ColumnMapperLong(1f);
        Long parsed = mapper.indexValue("test", 3.5d);
        Assert.assertEquals(Long.valueOf(3), parsed);

    }

    @Test
    public void testValueDoubleWithDecimalCeil()
    {
        ColumnMapperLong mapper = new ColumnMapperLong(1f);
        Long parsed = mapper.indexValue("test", 3.6d);
        Assert.assertEquals(Long.valueOf(3), parsed);

    }

    @Test
    public void testValueStringWithoutDecimal()
    {
        ColumnMapperLong mapper = new ColumnMapperLong(1f);
        Long parsed = mapper.indexValue("test", "3");
        Assert.assertEquals(Long.valueOf(3), parsed);
    }

    @Test
    public void testValueStringWithDecimalFloor()
    {
        ColumnMapperLong mapper = new ColumnMapperLong(1f);
        Long parsed = mapper.indexValue("test", "3.2");
        Assert.assertEquals(Long.valueOf(3), parsed);

    }

    @Test
    public void testValueStringWithDecimalCeil()
    {
        ColumnMapperLong mapper = new ColumnMapperLong(1f);
        Long parsed = mapper.indexValue("test", "3.2");
        Assert.assertEquals(Long.valueOf(3), parsed);

    }

    @Test
    public void testField()
    {
        ColumnMapperLong mapper = new ColumnMapperLong(1f);
        Field field = mapper.field("name", "3.2");
        Assert.assertNotNull(field);
        Assert.assertEquals(3L, field.numericValue());
        Assert.assertEquals("name", field.name());
        Assert.assertEquals(false, field.fieldType().stored());
    }

    @Test
    public void testExtractAnalyzers()
    {
        ColumnMapperLong mapper = new ColumnMapperLong(1f);
        String analyzer = mapper.analyzer();
        Assert.assertEquals(ColumnMapper.KEYWORD_ANALYZER, analyzer);
    }

    @Test
    public void testParseJSON() throws IOException
    {
        String json = "{fields:{age:{type:\"long\"}}}";
        Schema schema = Schema.fromJson(json);
        ColumnMapper columnMapper = schema.getMapper("age");
        Assert.assertNotNull(columnMapper);
        Assert.assertEquals(ColumnMapperLong.class, columnMapper.getClass());
    }

    @Test
    public void testParseJSONEmpty() throws IOException
    {
        String json = "{fields:{}}";
        Schema schema = Schema.fromJson(json);
        ColumnMapper columnMapper = schema.getMapper("age");
        Assert.assertNull(columnMapper);
    }

    @Test(expected = IOException.class)
    public void testParseJSONInvalid() throws IOException
    {
        String json = "{fields:{age:{}}";
        Schema.fromJson(json);
    }
}
