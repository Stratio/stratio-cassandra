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

import org.apache.cassandra.db.marshal.*;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FloatField;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortField.Type;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 * A {@link ColumnMapper} to map a float field.
 *
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class ColumnMapperFloat extends ColumnMapper<Float>
{

    private Float DEFAULT_BOOST = 1.0f;

    private final Float boost;

    @JsonCreator
    public ColumnMapperFloat(@JsonProperty("boost") Float boost)
    {
        super(new AbstractType<?>[]{
                AsciiType.instance,
                UTF8Type.instance,
                Int32Type.instance,
                LongType.instance,
                IntegerType.instance,
                FloatType.instance,
                DoubleType.instance,
                DecimalType.instance});
        this.boost = boost == null ? DEFAULT_BOOST : boost;
    }

    @Override
    public Analyzer analyzer()
    {
        return EMPTY_ANALYZER;
    }

    @Override
    public Float indexValue(String name, Object value)
    {
        if (value == null)
        {
            return null;
        }
        else if (value instanceof Number)
        {
            return ((Number) value).floatValue();
        }
        else if (value instanceof String)
        {
            String svalue = (String) value;
            try
            {
                return Double.valueOf(svalue).floatValue();
            }
            catch (NumberFormatException e)
            {
                String message = String.format("Field %s requires a base 10 float, but found \"%s\"", name, svalue);
                throw new IllegalArgumentException(message);
            }
        }
        else
        {
            String message = String.format("Field %s requires a base 10 float, but found \"%s\"", name, value);
            throw new IllegalArgumentException(message);
        }
    }

    @Override
    public Float queryValue(String name, Object value)
    {
        return indexValue(name, value);
    }

    @Override
    public Field field(String name, Object value)
    {
        Float number = indexValue(name, value);
        Field field = new FloatField(name, number, STORE);
        field.setBoost(boost);
        return field;
    }

    @Override
    public SortField sortField(String field, boolean reverse)
    {
        return new SortField(field, Type.FLOAT, reverse);
    }

    @Override
    public Class<Float> baseClass()
    {
        return Float.class;
    }

    @Override
    public String toString()
    {
        return new ToStringBuilder(this).append("boost", boost).toString();
    }

}
