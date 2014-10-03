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

import java.math.BigInteger;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortField.Type;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 * A {@link ColumnMapper} to map a string, not tokenized field.
 * 
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class ColumnMapperBigInteger extends ColumnMapper<String>
{

    public static final int DEFAULT_DIGITS = 32;

    private final int digits;
    private final BigInteger complement;
    private final int hexDigits;

    @JsonCreator
    public ColumnMapperBigInteger(@JsonProperty("digits") Integer digits)
    {
        super(new AbstractType<?>[] { AsciiType.instance,
                UTF8Type.instance,
                Int32Type.instance,
                LongType.instance,
                IntegerType.instance });

        if (digits != null && digits <= 0)
        {
            throw new IllegalArgumentException("Positive digits required");
        }

        this.digits = digits == null ? DEFAULT_DIGITS : digits;
        complement = BigInteger.valueOf(10).pow(this.digits).subtract(BigInteger.valueOf(1));
        BigInteger maxValue = complement.multiply(BigInteger.valueOf(2));
        hexDigits = encode(maxValue).length();
    }

    @Override
    public Analyzer analyzer()
    {
        return EMPTY_ANALYZER;
    }

    @Override
    public String indexValue(String name, Object value)
    {

        // Check not null
        if (value == null)
        {
            return null;
        }

        // Parse big decimal
        String svalue = value.toString();
        BigInteger bi;
        try
        {
            bi = new BigInteger(svalue);
        }
        catch (NumberFormatException e)
        {
            String message = String.format("Field %s requires a base 10 integer, but found \"%s\"", name, svalue);
            throw new IllegalArgumentException(message);
        }

        // Check size
        if (bi.abs().toString().length() > digits)
        {
            throw new IllegalArgumentException("Value has more than " + digits + " digits");
        }

        // Map
        bi = bi.add(complement);
        String bis = encode(bi);
        return StringUtils.leftPad(bis, hexDigits + 1, '0');
    }

    private static String encode(BigInteger bi)
    {
        return bi.toString(Character.MAX_RADIX);
    }

    public int getDigits()
    {
        return digits;
    }

    @Override
    public String queryValue(String name, Object value)
    {
        return indexValue(name, value);
    }

    @Override
    public Field field(String name, Object value)
    {
        String string = indexValue(name, value);
        return new StringField(name, string, STORE);
    }

    @Override
    public SortField sortField(String field, boolean reverse)
    {
        return new SortField(field, Type.STRING, reverse);
    }

    @Override
    public Class<String> baseClass()
    {
        return String.class;
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder();
        builder.append(getClass().getSimpleName());
        builder.append(" []");
        return builder.toString();
    }

}
