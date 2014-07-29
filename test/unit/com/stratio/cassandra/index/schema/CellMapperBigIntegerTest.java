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
import java.math.BigInteger;
import java.util.Date;
import java.util.UUID;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Field;
import org.junit.Assert;
import org.junit.Test;

public class CellMapperBigIntegerTest
{

    @Test()
    public void testValueNull()
    {
        CellMapperBigInteger mapper = new CellMapperBigInteger(10);
        String parsed = mapper.indexValue("test", null);
        Assert.assertNull(parsed);
    }

    @Test
    public void testValueDigitsNull()
    {
        CellMapperBigInteger mapper = new CellMapperBigInteger(null);
        Assert.assertEquals(CellMapperBigInteger.DEFAULT_DIGITS, mapper.getDigits());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValueDigitsZero()
    {
        new CellMapperBigInteger(0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValueDigitsNegative()
    {
        new CellMapperBigInteger(-1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValueBooleanTrue()
    {
        CellMapperBigInteger mapper = new CellMapperBigInteger(10);
        mapper.indexValue("test", true);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValueBooleanFalse()
    {
        CellMapperBigInteger mapper = new CellMapperBigInteger(10);
        mapper.indexValue("test", false);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValueUUID()
    {
        CellMapperBigInteger mapper = new CellMapperBigInteger(100);
        mapper.indexValue("test", UUID.fromString("550e8400-e29b-41d4-a716-446655440000"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValueDate()
    {
        CellMapperBigInteger mapper = new CellMapperBigInteger(100);
        mapper.indexValue("test", new Date());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValueStringInvalid()
    {
        CellMapperBigInteger mapper = new CellMapperBigInteger(10);
        mapper.indexValue("test", "0s0");
    }

    @Test
    public void testValueStringMinPositive()
    {
        CellMapperBigInteger mapper = new CellMapperBigInteger(8);
        String parsed = mapper.indexValue("test", "1");
        Assert.assertEquals("01njchs", parsed);
    }

    @Test
    public void testValueStringMaxPositive()
    {
        CellMapperBigInteger mapper = new CellMapperBigInteger(8);
        String parsed = mapper.indexValue("test", "99999999");
        Assert.assertEquals("03b2ozi", parsed);
    }

    @Test
    public void testValueStringMinNegative()
    {
        CellMapperBigInteger mapper = new CellMapperBigInteger(8);
        String parsed = mapper.indexValue("test", "-1");
        Assert.assertEquals("01njchq", parsed);
    }

    @Test
    public void testValueStringMaxNegative()
    {
        CellMapperBigInteger mapper = new CellMapperBigInteger(8);
        String parsed = mapper.indexValue("test", "-99999999");
        Assert.assertEquals("0000000", parsed);
    }

    @Test
    public void testValueStringZero()
    {
        CellMapperBigInteger mapper = new CellMapperBigInteger(8);
        String parsed = mapper.indexValue("test", "0");
        Assert.assertEquals("01njchr", parsed);
    }

    @Test
    public void testValueStringLeadingZeros()
    {
        CellMapperBigInteger mapper = new CellMapperBigInteger(8);
        String parsed = mapper.indexValue("test", "000042");
        Assert.assertEquals("01njcix", parsed);
    }

    // ///

    @Test
    public void testValueIntegerMinPositive()
    {
        CellMapperBigInteger mapper = new CellMapperBigInteger(8);
        String parsed = mapper.indexValue("test", 1);
        Assert.assertEquals("01njchs", parsed);
    }

    @Test
    public void testValueIntegerMaxPositive()
    {
        CellMapperBigInteger mapper = new CellMapperBigInteger(8);
        String parsed = mapper.indexValue("test", 99999999);
        Assert.assertEquals("03b2ozi", parsed);
    }

    @Test
    public void testValueIntegerMinNegative()
    {
        CellMapperBigInteger mapper = new CellMapperBigInteger(8);
        String parsed = mapper.indexValue("test", -1);
        Assert.assertEquals("01njchq", parsed);
    }

    @Test
    public void testValueIntegerMaxNegative()
    {
        CellMapperBigInteger mapper = new CellMapperBigInteger(8);
        String parsed = mapper.indexValue("test", -99999999);
        Assert.assertEquals("0000000", parsed);
    }

    @Test
    public void testValueIntegerZero()
    {
        CellMapperBigInteger mapper = new CellMapperBigInteger(8);
        String parsed = mapper.indexValue("test", 0);
        Assert.assertEquals("01njchr", parsed);
    }

    // ///

    @Test
    public void testValueLongMinPositive()
    {
        CellMapperBigInteger mapper = new CellMapperBigInteger(10);
        String parsed = mapper.indexValue("test", 1L);
        Assert.assertEquals("04ldqpds", parsed);
    }

    @Test
    public void testValueLongMaxPositive()
    {
        CellMapperBigInteger mapper = new CellMapperBigInteger(10);
        String parsed = mapper.indexValue("test", 9999999999L);
        Assert.assertEquals("096rheri", parsed);
    }

    @Test
    public void testValueLongMinNegative()
    {
        CellMapperBigInteger mapper = new CellMapperBigInteger(10);
        String parsed = mapper.indexValue("test", -1L);
        Assert.assertEquals("04ldqpdq", parsed);
    }

    @Test
    public void testValueLongMaxNegative()
    {
        CellMapperBigInteger mapper = new CellMapperBigInteger(10);
        String parsed = mapper.indexValue("test", -9999999999L);
        Assert.assertEquals("00000000", parsed);
    }

    @Test
    public void testValueLongZero()
    {
        CellMapperBigInteger mapper = new CellMapperBigInteger(10);
        String parsed = mapper.indexValue("test", 0L);
        Assert.assertEquals("04ldqpdr", parsed);
    }

    // ///

    @Test
    public void testValueBigIntegerMinPositive()
    {
        CellMapperBigInteger mapper = new CellMapperBigInteger(20);
        String parsed = mapper.indexValue("test", new BigInteger("1"));
        Assert.assertEquals("00l3r41ifs0q5ts", parsed);
    }

    @Test
    public void testValueBigIntegerMaxPositive()
    {
        CellMapperBigInteger mapper = new CellMapperBigInteger(20);
        String parsed = mapper.indexValue("test", new BigInteger("99999999999999999999"));
        Assert.assertEquals("0167i830vk1gbni", parsed);
    }

    @Test
    public void testValueBigIntegerMinNegative()
    {
        CellMapperBigInteger mapper = new CellMapperBigInteger(20);
        String parsed = mapper.indexValue("test", new BigInteger("-1"));
        Assert.assertEquals("00l3r41ifs0q5tq", parsed);
    }

    @Test
    public void testValueBigIntegerMaxNegative()
    {
        CellMapperBigInteger mapper = new CellMapperBigInteger(20);
        String parsed = mapper.indexValue("test", new BigInteger("-99999999999999999999"));
        Assert.assertEquals("000000000000000", parsed);
    }

    @Test
    public void testValueBigIntegerZero()
    {
        CellMapperBigInteger mapper = new CellMapperBigInteger(20);
        String parsed = mapper.indexValue("test", new BigInteger("0"));
        Assert.assertEquals("00l3r41ifs0q5tr", parsed);
    }

    // ///

    @Test(expected = IllegalArgumentException.class)
    public void testValueFloatMinPositive()
    {
        CellMapperBigInteger mapper = new CellMapperBigInteger(8);
        mapper.indexValue("test", 1.0f);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValueFloatMaxPositive()
    {
        CellMapperBigInteger mapper = new CellMapperBigInteger(8);
        mapper.indexValue("test", 99999999.0f);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValueFloatMinNegative()
    {
        CellMapperBigInteger mapper = new CellMapperBigInteger(8);
        mapper.indexValue("test", -1.0f);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValueFloatMaxNegative()
    {
        CellMapperBigInteger mapper = new CellMapperBigInteger(8);
        mapper.indexValue("test", -99999999.0f);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValueFloatZero()
    {
        CellMapperBigInteger mapper = new CellMapperBigInteger(8);
        mapper.indexValue("test", 0.0f);
    }

    // ///

    @Test(expected = IllegalArgumentException.class)
    public void testValueDoubleMinPositive()
    {
        CellMapperBigInteger mapper = new CellMapperBigInteger(10);
        mapper.indexValue("test", 1.0d);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValueDoubleMaxPositive()
    {
        CellMapperBigInteger mapper = new CellMapperBigInteger(10);
        mapper.indexValue("test", 9999999999.0d);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValueDoubleMinNegative()
    {
        CellMapperBigInteger mapper = new CellMapperBigInteger(10);
        mapper.indexValue("test", -1.0d);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValueDoubleMaxNegative()
    {
        CellMapperBigInteger mapper = new CellMapperBigInteger(10);
        mapper.indexValue("test", -9999999999.0d);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValueDoubleZero()
    {
        CellMapperBigInteger mapper = new CellMapperBigInteger(10);
        mapper.indexValue("test", 0.0d);
    }

    // /

    @Test(expected = IllegalArgumentException.class)
    public void testValueTooBig()
    {
        CellMapperBigInteger mapper = new CellMapperBigInteger(8);
        mapper.indexValue("test", 100000000);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValueTooSmall()
    {
        CellMapperBigInteger mapper = new CellMapperBigInteger(8);
        mapper.indexValue("test", -100000000);
    }

    @Test
    public void testValueNegativeMaxSort()
    {
        CellMapperBigInteger mapper = new CellMapperBigInteger(8);
        String lower = mapper.indexValue("test", -99999999);
        String upper = mapper.indexValue("test", -99999998);
        int compare = lower.compareTo(upper);
        Assert.assertEquals(-1, compare);
    }

    @Test
    public void testValueNegativeMinSort()
    {
        CellMapperBigInteger mapper = new CellMapperBigInteger(8);
        String lower = mapper.indexValue("test", -2);
        String upper = mapper.indexValue("test", -1);
        int compare = lower.compareTo(upper);
        Assert.assertEquals(-1, compare);
    }

    @Test
    public void testValuePositiveMaxSort()
    {
        CellMapperBigInteger mapper = new CellMapperBigInteger(8);
        String lower = mapper.indexValue("test", 99999998);
        String upper = mapper.indexValue("test", 99999999);
        int compare = lower.compareTo(upper);
        Assert.assertEquals(-1, compare);
    }

    @Test
    public void testValuePositiveMinSort()
    {
        CellMapperBigInteger mapper = new CellMapperBigInteger(8);
        String lower = mapper.indexValue("test", 1);
        String upper = mapper.indexValue("test", 2);
        int compare = lower.compareTo(upper);
        Assert.assertEquals(-1, compare);
    }

    @Test
    public void testValueNegativeZeroSort()
    {
        CellMapperBigInteger mapper = new CellMapperBigInteger(8);
        String lower = mapper.indexValue("test", -1);
        String upper = mapper.indexValue("test", 0);
        int compare = lower.compareTo(upper);
        Assert.assertEquals(-1, compare);
    }

    @Test
    public void testValuePositiveZeroSort()
    {
        CellMapperBigInteger mapper = new CellMapperBigInteger(8);
        String lower = mapper.indexValue("test", 0);
        String upper = mapper.indexValue("test", 1);
        int compare = lower.compareTo(upper);
        Assert.assertEquals(-1, compare);
    }

    @Test
    public void testValueExtremeSort()
    {
        CellMapperBigInteger mapper = new CellMapperBigInteger(8);
        String lower = mapper.indexValue("test", -99999999);
        String upper = mapper.indexValue("test", 99999999);
        int compare = lower.compareTo(upper);
        Assert.assertEquals(-3, compare);
    }

    @Test
    public void testValueNegativePositiveSort()
    {
        CellMapperBigInteger mapper = new CellMapperBigInteger(8);
        String lower = mapper.indexValue("test", -1);
        String upper = mapper.indexValue("test", 1);
        int compare = lower.compareTo(upper);
        Assert.assertEquals(-2, compare);
    }

    @Test
    public void testField()
    {
        CellMapperBigInteger mapper = new CellMapperBigInteger(10);
        Field field = mapper.field("name", 42);
        Assert.assertNotNull(field);
        Assert.assertEquals("04ldqpex", field.stringValue());
        Assert.assertEquals("name", field.name());
        Assert.assertFalse(field.fieldType().stored());
    }

    @Test
    public void testExtractAnalyzers()
    {
        CellMapperBigInteger mapper = new CellMapperBigInteger(10);
        Analyzer analyzer = mapper.analyzer();
        Assert.assertEquals(CellMapper.EMPTY_ANALYZER, analyzer);
    }

    @Test
    public void testParseJSONWithoutDigits() throws IOException
    {
        String json = "{fields:{age:{type:\"bigint\"}}}";
        Schema schema = Schema.fromJson(json);
        CellMapper<?> cellMapper = schema.getMapper("age");
        Assert.assertNotNull(cellMapper);
        Assert.assertEquals(CellMapperBigInteger.class, cellMapper.getClass());
    }

    @Test
    public void testParseJSONWithDigits() throws IOException
    {
        String json = "{fields:{age:{type:\"bigint\", digits:20}}}";
        Schema schema = Schema.fromJson(json);
        CellMapper<?> cellMapper = schema.getMapper("age");
        Assert.assertNotNull(cellMapper);
        Assert.assertEquals(CellMapperBigInteger.class, cellMapper.getClass());
        Assert.assertEquals(20, ((CellMapperBigInteger) cellMapper).getDigits());
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
