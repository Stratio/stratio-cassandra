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

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.DecimalType;
import org.apache.cassandra.db.marshal.DoubleType;
import org.apache.cassandra.db.marshal.FloatType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntField;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortField.Type;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 * A {@link ColumnMapper} to map an integer field.
 * 
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class ColumnMapperInteger extends ColumnMapper<Integer>
{

    private Float DEFAULT_BOOST = 1.0f;

    private final Float boost;

    @JsonCreator
    public ColumnMapperInteger(@JsonProperty("boost") Float boost)
    {
        super(new AbstractType<?>[] { AsciiType.instance,
                UTF8Type.instance,
                Int32Type.instance,
                LongType.instance,
                IntegerType.instance,
                FloatType.instance,
                DoubleType.instance,
                DecimalType.instance });
        this.boost = boost == null ? DEFAULT_BOOST : boost;
    }

    @Override
    public Analyzer analyzer()
    {
        return EMPTY_ANALYZER;
    }

    @Override
    public Integer indexValue(String name, Object value)
    {
        if (value == null)
        {
            return null;
        }
        else if (value instanceof Number)
        {
            return ((Number) value).intValue();
        }
        else if (value instanceof String)
        {
            String svalue = (String) value;
            try
            {
                return Double.valueOf(svalue).intValue();
            }
            catch (NumberFormatException e)
            {
                String message = String.format("Field %s requires a base 10 integer, but found \"%s\"", name, svalue);
                throw new IllegalArgumentException(message);
            }
        }
        else
        {
            String message = String.format("Field %s requires a base 10 integer, but found \"%s\"", name, value);
            throw new IllegalArgumentException(message);
        }
    }

    @Override
    public Integer queryValue(String name, Object value)
    {
        return indexValue(name, value);
    }

    @Override
    public Field field(String name, Object value)
    {
        Integer number = indexValue(name, value);
        Field field = new IntField(name, number, STORE);
        field.setBoost(boost);
        return field;
    }

    @Override
    public SortField sortField(String field, boolean reverse)
    {
        return new SortField(field, Type.INT, reverse);
    }

    @Override
    public Class<Integer> baseClass()
    {
        return Integer.class;
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder();
        builder.append("CellMapperInteger [boost=");
        builder.append(boost);
        builder.append("]");
        return builder.toString();
    }

}
