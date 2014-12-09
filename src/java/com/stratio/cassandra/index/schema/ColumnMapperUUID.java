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
import org.apache.lucene.document.StringField;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortField.Type;
import org.codehaus.jackson.annotate.JsonCreator;

import java.util.UUID;

/**
 * A {@link ColumnMapper} to map a UUID field.
 *
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class ColumnMapperUUID extends ColumnMapper<String>
{

    @JsonCreator
    public ColumnMapperUUID()
    {
        super(new AbstractType<?>[]{AsciiType.instance, UTF8Type.instance, UUIDType.instance, TimeUUIDType.instance},
              new AbstractType[]{});
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
        else if (value instanceof UUID)
        {
            return value.toString();
        }
        else if (value instanceof String)
        {
            return java.util.UUID.fromString((String) value).toString();
        }
        else
        {
            throw new IllegalArgumentException();
        }
    }

    @Override
    public String queryValue(String name, Object value)
    {
        if (value == null)
        {
            return null;
        }
        else
        {
            return value.toString();
        }
    }

    @Override
    public Field field(String name, Object value)
    {
        String uuid = indexValue(name, value);
        return new StringField(name, uuid, STORE);
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
