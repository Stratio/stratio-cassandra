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
package org.apache.cassandra.db;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Class for merging range collected results.
 * 
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class Merger
{

    private final Integer limit;
    private final Comparator<Row> comparator;

    private List<Row> rows;
    private Lock lock;

    /**
     * Returns a new {@link Merger} that collects all the inserted {@link Row}s sorted according to insertion order.
     */
    public Merger()
    {
        this(null, null);
        lock = new ReentrantLock();
    }

    /**
     * Returns a new {@link Merger} that collects until {@code limit} {@link Row}s sorted according to insertion order.
     * 
     * @param limit
     *            The max number of {@link Row}s to be merged.
     */
    public Merger(Integer limit)
    {
        this(limit, null);
        lock = new ReentrantLock();
    }

    /**
     * Returns a new {@link Merger} that collects until {@code limit} {@link Row}s sorted according to
     * {@code comparator}.
     * 
     * @param limit
     *            The max number of {@link Row}s to be merged.
     * @param comparator
     *            The {@link Comparator} specifying the merged {@link Row}s order.
     */
    public Merger(Integer limit, Comparator<Row> comparator)
    {
        this.limit = limit;
        lock = new ReentrantLock();
        this.comparator = comparator;
        if (limit == null)
            rows = new LinkedList<>();
        else
            rows = new ArrayList<>(limit);
    }

    /**
     * Does the merge over the collected results.
     */
    public void merge()
    {
        lock.lock();
        try
        {
            if (comparator != null)
            {
                Collections.sort(rows, comparator);
            }
            if (limit != null && rows.size() > limit)
            {
                rows.subList(0, limit);
            }
        }
        finally
        {
            lock.unlock();
        }
    }

    /**
     * Adds a new {@link Row} to the collected ones.
     * 
     * @param row
     *            A {@link Row}.
     */
    public void add(Row row)
    {
        lock.lock();
        try
        {
            rows.add(row);
        }
        finally
        {
            lock.unlock();
        }
    }

    /**
     * Adds a collection of {@link Row}s to the collected ones.
     * 
     * @param rows
     *            A collection of {@link Row}s.
     */
    public void add(Collection<Row> rows)
    {
        lock.lock();
        try
        {
            this.rows.addAll(rows);
        }
        finally
        {
            lock.unlock();
        }
    }

    /**
     * Returns the number of collected {@link Row}s.
     * 
     * @return
     */
    public int size()
    {
        return rows.size();
    }

    /**
     * Returns the collected {@link Row}s.
     * 
     * @return The collected {@link Row}s.
     */
    public List<Row> getRows()
    {
        return rows;
    }

}
