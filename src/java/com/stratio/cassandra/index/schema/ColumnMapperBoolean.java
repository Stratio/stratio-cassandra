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
import org.apache.cassandra.db.marshal.BooleanType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortField.Type;
import org.codehaus.jackson.annotate.JsonCreator;

/**
 * A {@link ColumnMapper} to map a boolean field.
 *
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class ColumnMapperBoolean extends ColumnMapper<String>
{

    private static final String TRUE = "true";
    private static final String FALSE = "false";

    @JsonCreator
    public ColumnMapperBoolean()
    {
        super(new AbstractType<?>[]{AsciiType.instance, UTF8Type.instance, BooleanType.instance}, new AbstractType[]{});
    }

    @Override
    public Analyzer analyzer()
    {
        return EMPTY_ANALYZER;
    }

    @Override
    public String indexValue(String name, Object value)
    {
        if (value == null)
        {
            return null;
        }
        else if (value instanceof Boolean)
        {
            return (Boolean) value ? TRUE : FALSE;
        }
        else if (value instanceof String)
        {
            String s = (String) value;
            if (s.equalsIgnoreCase(TRUE))
            {
                return TRUE;
            }
            else if (s.equalsIgnoreCase(FALSE))
            {
                return FALSE;
            }
        }
        throw new IllegalArgumentException();
    }

    @Override
    public String queryValue(String name, Object value)
    {
        if (value == null)
        {
            return null;
        }
        else if (value instanceof Boolean)
        {
            return (Boolean) value ? TRUE : FALSE;
        }
        else
        {
            return value.toString();
        }
    }

    @Override
    public Field field(String name, Object value)
    {
        return new StringField(name, indexValue(name, value), STORE);
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
        return new ToStringBuilder(this).toString();
    }

}
