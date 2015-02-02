package com.stratio.cassandra.index.schema;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class SchemaTest {

    @Test
    public void testGetMapper() {

        Map<String, ColumnMapper> columnMappers = new HashMap<>();

        ColumnMapperString columnMapper1 = new ColumnMapperString();
        ColumnMapperString columnMapper2 = new ColumnMapperString();
        ColumnMapperString columnMapper3 = new ColumnMapperString();

        columnMappers.put("a", columnMapper1);
        columnMappers.put("a.b", columnMapper2);
        columnMappers.put("a.b.c", columnMapper3);

        Schema schema = new Schema(null, columnMappers);
        Assert.assertEquals(columnMapper1, schema.getMapper("a"));
        Assert.assertEquals(columnMapper2, schema.getMapper("a.b"));
        Assert.assertEquals(columnMapper3, schema.getMapper("a.b.c"));
        Assert.assertEquals(columnMapper3, schema.getMapper("a.b.c.d"));
        Assert.assertNotSame(columnMapper1, schema.getMapper("a.b.c.d"));
    }
}
