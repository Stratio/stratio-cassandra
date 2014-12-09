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
import org.apache.commons.lang3.builder.ToStringBuilder;

import java.nio.ByteBuffer;

/**
 * A cell of a CQL3 logic {@link Column}, which in most cases is different from a storage engine column.
 *
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class Column
{

    /**
     * The column's name
     */
    private String name;

    private String nameSufix;

    private ByteBuffer value;

    private AbstractType<?> type;

    public Column(String name, ByteBuffer value, AbstractType<?> type)
    {
        this.name = name;
        this.value = value;
        this.type = type;
    }

    public Column(String name, String nameSufix, ByteBuffer value, AbstractType<?> type)
    {
        this.name = name;
        this.nameSufix = nameSufix;
        this.value = value;
        this.type = type;
    }

    /**
     * Returns the name.
     *
     * @return the name.
     */
    public String getName()
    {
        return name;
    }

    public String getFieldName()
    {
        return nameSufix == null ? name : name + "." + nameSufix;
    }

    /**
     * Returns the value.
     *
     * @return the value.
     */
    public ByteBuffer getRawValue()
    {
        return value;
    }

    public Object getValue()
    {
        return type.compose(value);
    }

    public AbstractType<?> getType()
    {
        return type;
    }

    @Override
    public String toString()
    {
        return new ToStringBuilder(this).append("name", name)
                                        .append("nameSufix", nameSufix)
                                        .append("value", value)
                                        .append("type", type)
                                        .toString();
    }

}
