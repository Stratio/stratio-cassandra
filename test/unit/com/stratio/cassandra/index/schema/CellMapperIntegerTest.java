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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Field;
import org.junit.Assert;
import org.junit.Test;

public class CellMapperIntegerTest
{

    @Test()
    public void testValueNull()
    {
        CellMapperInteger mapper = new CellMapperInteger(1f);
        Integer parsed = mapper.indexValue("test", null);
        Assert.assertNull(parsed);
    }

    @Test
    public void testValueInteger()
    {
        CellMapperInteger mapper = new CellMapperInteger(1f);
        Integer parsed = mapper.indexValue("test", 3);
        Assert.assertEquals(Integer.valueOf(3), parsed);
    }

    @Test
    public void testValueLong()
    {
        CellMapperInteger mapper = new CellMapperInteger(1f);
        Integer parsed = mapper.indexValue("test", 3l);
        Assert.assertEquals(Integer.valueOf(3), parsed);
    }

    @Test
    public void testValueFloatWithoutDecimal()
    {
        CellMapperInteger mapper = new CellMapperInteger(1f);
        Integer parsed = mapper.indexValue("test", 3f);
        Assert.assertEquals(Integer.valueOf(3), parsed);
    }

    @Test
    public void testValueFloatWithDecimalFloor()
    {
        CellMapperInteger mapper = new CellMapperInteger(1f);
        Integer parsed = mapper.indexValue("test", 3.5f);
        Assert.assertEquals(Integer.valueOf(3), parsed);

    }

    @Test
    public void testValueFloatWithDecimalCeil()
    {
        CellMapperInteger mapper = new CellMapperInteger(1f);
        Integer parsed = mapper.indexValue("test", 3.6f);
        Assert.assertEquals(Integer.valueOf(3), parsed);

    }

    @Test
    public void testValueDoubleWithoutDecimal()
    {
        CellMapperInteger mapper = new CellMapperInteger(1f);
        Integer parsed = mapper.indexValue("test", 3d);
        Assert.assertEquals(Integer.valueOf(3), parsed);
    }

    @Test
    public void testValueDoubleWithDecimalFloor()
    {
        CellMapperInteger mapper = new CellMapperInteger(1f);
        Integer parsed = mapper.indexValue("test", 3.5d);
        Assert.assertEquals(Integer.valueOf(3), parsed);

    }

    @Test
    public void testValueDoubleWithDecimalCeil()
    {
        CellMapperInteger mapper = new CellMapperInteger(1f);
        Integer parsed = mapper.indexValue("test", 3.6d);
        Assert.assertEquals(Integer.valueOf(3), parsed);

    }

    @Test
    public void testValueStringWithoutDecimal()
    {
        CellMapperInteger mapper = new CellMapperInteger(1f);
        Integer parsed = mapper.indexValue("test", "3");
        Assert.assertEquals(Integer.valueOf(3), parsed);
    }

    @Test
    public void testValueStringWithDecimalFloor()
    {
        CellMapperInteger mapper = new CellMapperInteger(1f);
        Integer parsed = mapper.indexValue("test", "3.2");
        Assert.assertEquals(Integer.valueOf(3), parsed);

    }

    @Test
    public void testValueStringWithDecimalCeil()
    {
        CellMapperInteger mapper = new CellMapperInteger(1f);
        Integer parsed = mapper.indexValue("test", "3.2");
        Assert.assertEquals(Integer.valueOf(3), parsed);

    }

    @Test
    public void testField()
    {
        CellMapperInteger mapper = new CellMapperInteger(1f);
        Field field = mapper.field("name", "3.2");
        Assert.assertNotNull(field);
        Assert.assertEquals(Integer.valueOf(3), field.numericValue());
        Assert.assertEquals("name", field.name());
        Assert.assertEquals(false, field.fieldType().stored());
    }

    @Test
    public void testExtractAnalyzers()
    {
        CellMapperInteger mapper = new CellMapperInteger(1f);
        Analyzer analyzer = mapper.analyzer();
        Assert.assertEquals(CellMapper.EMPTY_ANALYZER, analyzer);
    }

    @Test
    public void testParseJSON() throws IOException
    {
        String json = "{fields:{age:{type:\"integer\"}}}";
        Schema schema = Schema.fromJson(json);
        CellMapper<?> cellMapper = schema.getMapper("age");
        Assert.assertNotNull(cellMapper);
        Assert.assertEquals(CellMapperInteger.class, cellMapper.getClass());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testParseJSONEmpty() throws IOException
    {
        String json = "{fields:{}}";
        Schema schema = Schema.fromJson(json);
        schema.getMapper("age");
    }

    @Test(expected = IOException.class)
    public void testParseJSONInvalid() throws IOException
    {
        String json = "{fields:{age:{}}";
        Schema.fromJson(json);
    }
}
