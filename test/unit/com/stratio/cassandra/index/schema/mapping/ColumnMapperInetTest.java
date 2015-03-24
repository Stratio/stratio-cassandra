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

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.UUID;

import com.stratio.cassandra.index.schema.Schema;
import com.stratio.cassandra.index.schema.mapping.ColumnMapper;
import com.stratio.cassandra.index.schema.mapping.ColumnMapperInet;
import org.apache.lucene.document.Field;
import org.junit.Assert;
import org.junit.Test;

public class ColumnMapperInetTest
{

    @Test()
    public void testValueNull()
    {
        ColumnMapperInet mapper = new ColumnMapperInet();
        String parsed = mapper.indexValue("test", null);
        Assert.assertNull(parsed);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValueInteger()
    {
        ColumnMapperInet mapper = new ColumnMapperInet();
        mapper.indexValue("test", 3);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValueLong()
    {
        ColumnMapperInet mapper = new ColumnMapperInet();
        mapper.indexValue("test", 3l);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValueFloat()
    {
        ColumnMapperInet mapper = new ColumnMapperInet();
        mapper.indexValue("test", 3.5f);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValueDouble()
    {
        ColumnMapperInet mapper = new ColumnMapperInet();
        mapper.indexValue("test", 3.6d);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValueUUID()
    {
        ColumnMapperInet mapper = new ColumnMapperInet();
        mapper.indexValue("test", UUID.randomUUID());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValueStringInvalid()
    {
        ColumnMapperInet mapper = new ColumnMapperInet();
        mapper.indexValue("test", "Hello");
    }

    @Test
    public void testValueStringV4WithoutZeros()
    {
        ColumnMapperInet mapper = new ColumnMapperInet();
        String parsed = mapper.indexValue("test", "192.168.0.1");
        Assert.assertEquals("192.168.0.1", parsed);
    }

    @Test
    public void testValueStringV4WithZeros()
    {
        ColumnMapperInet mapper = new ColumnMapperInet();
        String parsed = mapper.indexValue("test", "192.168.000.001");
        Assert.assertEquals("192.168.0.1", parsed);
    }

    @Test
    public void testValueStringV6WithoutZeros()
    {
        ColumnMapperInet mapper = new ColumnMapperInet();
        String parsed = mapper.indexValue("test", "2001:db8:2de:0:0:0:0:e13");
        Assert.assertEquals("2001:db8:2de:0:0:0:0:e13", parsed);
    }

    @Test
    public void testValueStringV6WithZeros()
    {
        ColumnMapperInet mapper = new ColumnMapperInet();
        String parsed = mapper.indexValue("test", "2001:0db8:02de:0000:0000:0000:0000:0e13");
        Assert.assertEquals("2001:db8:2de:0:0:0:0:e13", parsed);
    }

    @Test
    public void testValueStringV6Compact()
    {
        ColumnMapperInet mapper = new ColumnMapperInet();
        String parsed = mapper.indexValue("test", "2001:DB8:2de::0e13");
        Assert.assertEquals("2001:db8:2de:0:0:0:0:e13", parsed);
    }

    @Test
    public void testValueInetV4() throws UnknownHostException
    {
        ColumnMapperInet mapper = new ColumnMapperInet();
        InetAddress inet = InetAddress.getByName("192.168.0.13");
        String parsed = mapper.indexValue("test", inet);
        Assert.assertEquals("192.168.0.13", parsed);
    }

    @Test
    public void testValueInetV6() throws UnknownHostException
    {
        ColumnMapperInet mapper = new ColumnMapperInet();
        InetAddress inet = InetAddress.getByName("2001:db8:2de:0:0:0:0:e13");
        String parsed = mapper.indexValue("test", inet);
        Assert.assertEquals("2001:db8:2de:0:0:0:0:e13", parsed);
    }

    @Test
    public void testField()
    {
        ColumnMapperInet mapper = new ColumnMapperInet();
        Field field = mapper.field("name", "192.168.0.13");
        Assert.assertNotNull(field);
        Assert.assertEquals("192.168.0.13", field.stringValue());
        Assert.assertEquals("name", field.name());
        Assert.assertEquals(false, field.fieldType().stored());
    }

    @Test
    public void testExtractAnalyzers()
    {
        ColumnMapperInet mapper = new ColumnMapperInet();
        String analyzer = mapper.analyzer();
        Assert.assertEquals(ColumnMapper.KEYWORD_ANALYZER, analyzer);
    }

    @Test
    public void testParseJSON() throws IOException
    {
        String json = "{fields:{age:{type:\"inet\"}}}";
        Schema schema = Schema.fromJson(json);
        ColumnMapper columnMapper = schema.getMapper("age");
        Assert.assertNotNull(columnMapper);
        Assert.assertEquals(ColumnMapperInet.class, columnMapper.getClass());
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
