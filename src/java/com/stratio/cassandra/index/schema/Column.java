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
public class Column {

    /** The column's name. */
    private final String name;

    /** The column's name sufix used for maps. */
    private final String nameSufix;

    /** The column's value as {@link ByteBuffer}. */
    private final ByteBuffer value;

    /** The column's Cassandra type. */
    private final AbstractType<?> type;

    /**
     * Builds a new {@link Column} with the specified name, value, and type.
     *
     * @param name  The name of the column to be created.
     * @param value The value of the column to be created.
     * @param type  The type of the column to be created.
     */
    public Column(String name, ByteBuffer value, AbstractType<?> type) {
        this.name = name;
        this.nameSufix = null;
        this.value = value;
        this.type = type;
    }

    /**
     * Builds a new {@link Column} with the specified name, name sufix, value, and type.
     *
     * @param name      The name of the column to be created.
     * @param nameSufix The name sufix of the column to be created.
     * @param value     The value of the column to be created.
     * @param type      The type of the column to be created.
     */
    public Column(String name, String nameSufix, ByteBuffer value, AbstractType<?> type) {
        this.name = name;
        this.nameSufix = nameSufix;
        this.value = value;
        this.type = type;
    }

    /**
     * Returns the column name.
     *
     * @return the column name.
     */
    public String getName() {
        return name;
    }

    /**
     * Returns the Lucene field name, which is formed by the column name and sufix.
     *
     * @return The Lucene field name, which is formed by the column name and sufix.
     */
    public String getFieldName() {
        return nameSufix == null ? name : name + "." + nameSufix;
    }

    /**
     * Returns the {@link ByteBuffer} serialized value.
     *
     * @return the {@link ByteBuffer} serialized value.
     */
    public ByteBuffer getRawValue() {
        return value;
    }

    /**
     * Returns the Java column value.
     *
     * @return The Java column value.
     */
    public Object getValue() {
        return type.compose(value);
    }

    /**
     * Returns the Cassandra column type.
     *
     * @return The Cassandra column type.
     */
    public AbstractType<?> getType() {
        return type;
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return new ToStringBuilder(this).append("name", name)
                                        .append("nameSufix", nameSufix)
                                        .append("value", value)
                                        .append("type", type)
                                        .toString();
    }

}
