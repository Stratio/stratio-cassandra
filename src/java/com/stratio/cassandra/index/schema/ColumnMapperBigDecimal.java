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
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortField.Type;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

import java.math.BigDecimal;

/**
 * A {@link ColumnMapper} to map a string, not tokenized field.
 *
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class ColumnMapperBigDecimal extends ColumnMapper<String>
{

    public static final int DEFAULT_INTEGER_DIGITS = 32;
    public static final int DEFAULT_DECIMAL_DIGITS = 32;

    private final int integerDigits;
    private final int decimalDigits;

    private final BigDecimal complement;

    @JsonCreator
    public ColumnMapperBigDecimal(@JsonProperty("integer_digits") Integer integerDigits,
                                  @JsonProperty("decimal_digits") Integer decimalDigits)
    {
        super(new AbstractType<?>[]{
                AsciiType.instance,
                UTF8Type.instance,
                Int32Type.instance,
                LongType.instance,
                IntegerType.instance,
                FloatType.instance,
                DoubleType.instance,
                DecimalType.instance}, new AbstractType[]{});

        // Setup integer part mapping
        if (integerDigits != null && integerDigits <= 0)
        {
            throw new IllegalArgumentException("Positive integer part digits required");
        }
        this.integerDigits = integerDigits == null ? DEFAULT_INTEGER_DIGITS : integerDigits;

        // Setup decimal part mapping
        if (decimalDigits != null && decimalDigits <= 0)
        {
            throw new IllegalArgumentException("Positive decimal part digits required");
        }
        this.decimalDigits = decimalDigits == null ? DEFAULT_DECIMAL_DIGITS : decimalDigits;

        int totalDigits = this.integerDigits + this.decimalDigits;
        BigDecimal divisor = BigDecimal.valueOf(10).pow(this.decimalDigits);
        BigDecimal dividend = BigDecimal.valueOf(10).pow(totalDigits).subtract(BigDecimal.valueOf(1));
        complement = dividend.divide(divisor);
    }

    public int getIntegerDigits()
    {
        return integerDigits;
    }

    public int getDecimalDigits()
    {
        return decimalDigits;
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
        BigDecimal bd;
        try
        {
            bd = new BigDecimal(value.toString());
        }
        catch (NumberFormatException e)
        {
            String message = String.format("Field %s requires a base 10 decimal, but found \"%s\"", name, svalue);
            throw new IllegalArgumentException(message);
        }

        // Split integer and decimal part
        bd = bd.stripTrailingZeros();
        String[] parts = bd.toPlainString().split("\\.");
        String integerPart = parts[0];
        String decimalPart = parts.length == 1 ? "0" : parts[1];

        if (integerPart.replaceFirst("-", "").length() > integerDigits)
        {
            throw new IllegalArgumentException("Too much digits in integer part");
        }
        if (decimalPart.length() > decimalDigits)
        {
            throw new IllegalArgumentException("Too much digits in decimal part");
        }

        BigDecimal complemented = bd.add(complement);
        String bds[] = complemented.toString().split("\\.");
        integerPart = bds[0];
        decimalPart = bds.length == 2 ? bds[1] : "0";
        integerPart = StringUtils.leftPad(integerPart, integerDigits + 1, '0');

        return integerPart + "." + decimalPart;
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
        return new ToStringBuilder(this).append("integerDigits", integerDigits)
                                        .append("decimalDigits", decimalDigits)
                                        .toString();
    }
}
