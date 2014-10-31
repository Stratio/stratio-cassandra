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

import org.apache.commons.lang3.builder.ToStringBuilder;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * A sorted list of CQL3 logic {@link Column}s.
 *
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class Columns implements Iterable<Column>
{

    /**
     * The wrapped columns
     */
    private List<Column> columns;

    /**
     * Constructs an empty {@link Column} list.
     */
    public Columns()
    {
        this.columns = new LinkedList<>();
    }

    /**
     * Adds the specified {@link Column} to the existing ones.
     *
     * @param column the {@link Column} to be added.
     * @return this
     */
    public Columns add(Column column)
    {
        columns.add(column);
        return this;
    }

    /**
     * Adds the specified {@link Column}s to the existing ones.
     *
     * @param columns the {@link Column}s to be added.
     * @return this
     */
    public Columns addAll(Collection<Column> columns)
    {
        this.columns.addAll(columns);
        return this;
    }

    /**
     * Adds the specified {@link Column}s to the existing ones.
     *
     * @param columns the {@link Column}s to be added.
     * @return this
     */
    public Columns addAll(Columns columns)
    {
        for (Column column : columns)
        {
            this.columns.add(column);
        }
        return this;
    }

    /**
     * Returns an iterator over the {@link Column}s in storage sequence.
     *
     * @return an iterator over the {@link Column}s storage sequence
     */
    public Iterator<Column> iterator()
    {
        return columns.iterator();
    }

    /**
     * Returns the number of {@link Column}s in this list. If this list contains more than <tt>Integer.MAX_VALUE</tt>
     * elements, returns <tt>Integer.MAX_VALUE</tt>.
     *
     * @return the number of {@link Column}s in this list
     */
    public int size()
    {
        return columns.size();
    }

    public Column getCell(String name)
    {
        for (Column column : columns)
        {
            if (column.getName().equals(name))
            {
                return column;
            }
        }
        return null;
    }

    @Override
    public String toString()
    {
        return new ToStringBuilder(this).append("columns", columns).toString();
    }
}
