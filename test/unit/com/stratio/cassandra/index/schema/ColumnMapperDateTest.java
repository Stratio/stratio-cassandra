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
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Field;
import org.junit.Assert;
import org.junit.Test;


public class ColumnMapperDateTest
{

    private static final String PATTERN = "yyyy-MM-dd";
    private static final SimpleDateFormat sdf = new SimpleDateFormat(PATTERN);

    @Test()
    public void testValueNull()
    {
        ColumnMapperDate mapper = new ColumnMapperDate(PATTERN);
        Long parsed = mapper.indexValue("test", null);
        Assert.assertNull(parsed);
    }

    @Test
    public void testValueDate()
    {
        ColumnMapperDate mapper = new ColumnMapperDate(PATTERN);
        Date date = new Date();
        long parsed = mapper.indexValue("test", date);
        Assert.assertEquals(date.getTime(), parsed);
    }

    @Test
    public void testValueInteger()
    {
        ColumnMapperDate mapper = new ColumnMapperDate(PATTERN);
        Long parsed = mapper.indexValue("test", 3);
        Assert.assertEquals(Long.valueOf(3), parsed);
    }

    @Test
    public void testValueLong()
    {
        ColumnMapperDate mapper = new ColumnMapperDate(PATTERN);
        Long parsed = mapper.indexValue("test", 3l);
        Assert.assertEquals(Long.valueOf(3), parsed);
    }

    @Test
    public void testValueFloatWithoutDecimal()
    {
        ColumnMapperDate mapper = new ColumnMapperDate(PATTERN);
        Long parsed = mapper.indexValue("test", 3f);
        Assert.assertEquals(Long.valueOf(3), parsed);
    }

    @Test
    public void testValueFloatWithDecimalFloor()
    {
        ColumnMapperDate mapper = new ColumnMapperDate(PATTERN);
        Long parsed = mapper.indexValue("test", 3.5f);
        Assert.assertEquals(Long.valueOf(3), parsed);

    }

    @Test
    public void testValueFloatWithDecimalCeil()
    {
        ColumnMapperDate mapper = new ColumnMapperDate(PATTERN);
        Long parsed = mapper.indexValue("test", 3.6f);
        Assert.assertEquals(Long.valueOf(3), parsed);

    }

    @Test
    public void testValueDoubleWithoutDecimal()
    {
        ColumnMapperDate mapper = new ColumnMapperDate(PATTERN);
        Long parsed = mapper.indexValue("test", 3d);
        Assert.assertEquals(Long.valueOf(3), parsed);
    }

    @Test
    public void testValueDoubleWithDecimalFloor()
    {
        ColumnMapperDate mapper = new ColumnMapperDate(PATTERN);
        Long parsed = mapper.indexValue("test", 3.5d);
        Assert.assertEquals(Long.valueOf(3), parsed);

    }

    @Test
    public void testValueDoubleWithDecimalCeil()
    {
        ColumnMapperDate mapper = new ColumnMapperDate(PATTERN);
        Long parsed = mapper.indexValue("test", 3.6d);
        Assert.assertEquals(Long.valueOf(3), parsed);

    }

    @Test
    public void testValueStringWithPattern() throws ParseException
    {
        ColumnMapperDate mapper = new ColumnMapperDate(PATTERN);
        long parsed = mapper.indexValue("test", "2014-03-19");
        Assert.assertEquals(sdf.parse("2014-03-19").getTime(), parsed);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValueStringWithPatternInvalid()
    {
        ColumnMapperDate mapper = new ColumnMapperDate(PATTERN);
        mapper.indexValue("test", "2014/03/19");
    }

    @Test
    public void testValueStringWithoutPattern() throws ParseException
    {
        ColumnMapperDate mapper = new ColumnMapperDate(null);
        long parsed = mapper.indexValue("test", "2014/03/19 00:00:00.000");
        Assert.assertEquals(sdf.parse("2014-03-19").getTime(), parsed);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValueStringWithoutPatternInvalid() throws ParseException
    {
        ColumnMapperDate mapper = new ColumnMapperDate(null);
        mapper.indexValue("test", "2014-03-19");
    }

    @Test
    public void testField() throws ParseException
    {
        ColumnMapperDate mapper = new ColumnMapperDate(PATTERN);
        Field field = mapper.field("name", "2014-03-19");
        Assert.assertNotNull(field);
        Assert.assertEquals(sdf.parse("2014-03-19").getTime(), field.numericValue().longValue());
        Assert.assertEquals("name", field.name());
        Assert.assertEquals(false, field.fieldType().stored());
    }

    @Test
    public void testExtractAnalyzers()
    {
        ColumnMapperDate mapper = new ColumnMapperDate(PATTERN);
        Analyzer analyzer = mapper.analyzer();
        Assert.assertEquals(ColumnMapper.EMPTY_ANALYZER, analyzer);
    }

    @Test
    public void testParseJSON() throws IOException
    {
        String json = "{fields:{age:{type:\"date\"}}}";
        Schema schema = Schema.fromJson(json);
        ColumnMapper columnMapper = schema.getMapper("age");
        Assert.assertNotNull(columnMapper);
        Assert.assertEquals(ColumnMapperDate.class, columnMapper.getClass());
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
