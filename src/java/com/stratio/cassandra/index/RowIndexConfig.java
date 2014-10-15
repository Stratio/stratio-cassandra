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
package com.stratio.cassandra.index;

import com.stratio.cassandra.index.schema.Schema;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;

import java.io.File;
import java.util.Map;

/**
 * Class for building {@link RowService} instances.
 *
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class RowIndexConfig
{

    private static final String SCHEMA_OPTION = "schema";

    private static final String REFRESH_SECONDS_OPTION = "refresh_seconds";
    private static final double DEFAULT_REFESH_SECONDS = 60;

    private static final String FILTER_CACHE_SIZE_OPTION = "num_cached_filters";

    private static final String DEFAULT_PATH_PREFIX = "lucene_idx";

    private static final String RAM_BUFFER_MB_OPTION = "ram_buffer_mb";
    private static final int DEFAULT_RAM_BUFFER_MB = 64;

    private static final String MAX_MERGE_MB_OPTION = "max_merge_mb";
    private static final int DEFAULT_MAX_MERGE_MB = 5;

    private static final String MAX_CACHED_MB_OPTION = "max_cached_mb";
    private static final int DEFAULT_MAX_CACHED_MB = 30;

    private static final String INDEXING_THREADS_OPTION = "indexing_threads";
    private static final int DEFAULT_INDEXING_THREADS = Runtime.getRuntime().availableProcessors();

    private static final String INDEXING_QUEUES_SIZE_OPTION = "indexing_queues_size";
    private static final int DEFAULT_INDEXING_QUEUES_SIZE = 50;

    private final Schema schema;
    private final double refreshSeconds;
    private final FilterCache filterCache;
    private final String path;
    private final int ramBufferMB;
    private final int maxMergeMB;
    private final int maxCachedMB;
    private final int indexingThreads;
    private final int indexingQueuesSize;

    public RowIndexConfig(CFMetaData metadata, Map<String, String> options)
    {

        // Setup refresh seconds
        String refreshOption = options.get(REFRESH_SECONDS_OPTION);
        if (refreshOption != null)
        {
            try
            {
                refreshSeconds = Double.parseDouble(refreshOption);
            }
            catch (NumberFormatException e)
            {
                String msg = String.format("'%s' must be a strictly positive double", REFRESH_SECONDS_OPTION);
                throw new RuntimeException(msg);
            }
            if (refreshSeconds <= 0)
            {
                String msg = String.format("'%s' must be strictly positive", REFRESH_SECONDS_OPTION);
                throw new RuntimeException(msg);
            }
        }
        else
        {
            refreshSeconds = DEFAULT_REFESH_SECONDS;
        }

        // Setup filter cache size
        String filterCacheSizeOption = options.get(FILTER_CACHE_SIZE_OPTION);
        int filterCacheSize;
        if (filterCacheSizeOption != null)
        {
            try
            {
                filterCacheSize = Integer.parseInt(filterCacheSizeOption);
            }
            catch (NumberFormatException e)
            {
                String msg = String.format("'%s' must be a strictly positive integer", RAM_BUFFER_MB_OPTION);
                throw new RuntimeException(msg);
            }
        }
        else
        {
            filterCacheSize = DatabaseDescriptor.getNumTokens() + 1;
        }
        filterCache = filterCacheSize <= 0 ? null : new FilterCache(filterCacheSize);

        // Setup write buffer size
        String ramBufferSizeOption = options.get(RAM_BUFFER_MB_OPTION);
        if (ramBufferSizeOption != null)
        {
            try
            {
                ramBufferMB = Integer.parseInt(ramBufferSizeOption);
            }
            catch (NumberFormatException e)
            {
                String msg = String.format("'%s' must be a strictly positive integer", RAM_BUFFER_MB_OPTION);
                throw new RuntimeException(msg);
            }
            if (ramBufferMB <= 0)
            {
                String msg = String.format("'%s' must be strictly positive", RAM_BUFFER_MB_OPTION);
                throw new RuntimeException(msg);
            }
        }
        else
        {
            ramBufferMB = DEFAULT_RAM_BUFFER_MB;
        }

        // Setup max merge size
        String maxMergeSizeMBOption = options.get(MAX_MERGE_MB_OPTION);
        if (maxMergeSizeMBOption != null)
        {
            try
            {
                maxMergeMB = Integer.parseInt(maxMergeSizeMBOption);
            }
            catch (NumberFormatException e)
            {
                String msg = String.format("'%s' must be a strictly positive integer", MAX_MERGE_MB_OPTION);
                throw new RuntimeException(msg);
            }
            if (maxMergeMB <= 0)
            {
                String msg = String.format("'%s' must be strictly positive", MAX_MERGE_MB_OPTION);
                throw new RuntimeException(msg);
            }
        }
        else
        {
            maxMergeMB = DEFAULT_MAX_MERGE_MB;
        }

        // Setup max cached MB
        String maxCachedMBOption = options.get(MAX_CACHED_MB_OPTION);
        if (maxCachedMBOption != null)
        {
            try
            {
                maxCachedMB = Integer.parseInt(maxCachedMBOption);
            }
            catch (NumberFormatException e)
            {
                String msg = String.format("'%s'  must be a strictly positive integer", MAX_CACHED_MB_OPTION);
                throw new RuntimeException(msg);
            }
            if (maxCachedMB <= 0)
            {
                String msg = String.format("'%s'  must be strictly positive", MAX_CACHED_MB_OPTION);
                throw new RuntimeException(msg);
            }
        }
        else
        {
            maxCachedMB = DEFAULT_MAX_CACHED_MB;
        }

        // Setup queues in index pool
        String indexPoolNumQueuesOption = options.get(INDEXING_THREADS_OPTION);
        if (indexPoolNumQueuesOption != null)
        {
            try
            {
                indexingThreads = Integer.parseInt(indexPoolNumQueuesOption);
            }
            catch (NumberFormatException e)
            {
                String msg = String.format("'%s'  must be a strictly positive integer", INDEXING_THREADS_OPTION);
                throw new RuntimeException(msg);
            }
            if (indexingThreads <= 0)
            {
                String msg = String.format("'%s'  must be strictly positive", INDEXING_THREADS_OPTION);
                throw new RuntimeException(msg);
            }
        }
        else
        {
            indexingThreads = DEFAULT_INDEXING_THREADS;
        }

        // Setup queues in index pool
        String indexPoolQueuesSizeOption = options.get(INDEXING_QUEUES_SIZE_OPTION);
        if (indexPoolQueuesSizeOption != null)
        {
            try
            {
                indexingQueuesSize = Integer.parseInt(indexPoolQueuesSizeOption);
            }
            catch (NumberFormatException e)
            {
                String msg = String.format("'%s'  must be a strictly positive integer", INDEXING_QUEUES_SIZE_OPTION);
                throw new RuntimeException(msg);
            }
            if (indexingQueuesSize <= 0)
            {
                String msg = String.format("'%s'  must be strictly positive", INDEXING_QUEUES_SIZE_OPTION);
                throw new RuntimeException(msg);
            }
        }
        else
        {
            indexingQueuesSize = DEFAULT_INDEXING_QUEUES_SIZE;
        }

        // Get columns mapping schema
        String schemaOption = options.get(SCHEMA_OPTION);
        if (schemaOption != null && !schemaOption.trim().isEmpty())
        {
            try
            {
                schema = Schema.fromJson(schemaOption);
                schema.validate(metadata);
            }
            catch (Exception e)
            {
                String msg = String.format("'%s' is invalid : %s", SCHEMA_OPTION, e.getMessage());
                throw new RuntimeException(msg);
            }
        }
        else
        {
            String msg = String.format("'%s' required", SCHEMA_OPTION);
            throw new RuntimeException(msg);
        }

        // Get Lucene's directory path
        String[] dataFileLocations = DatabaseDescriptor.getAllDataFileLocations();
        StringBuilder directoryPathBuilder = new StringBuilder();
        directoryPathBuilder.append(dataFileLocations[0]);
        directoryPathBuilder.append(File.separatorChar);
        directoryPathBuilder.append(metadata.ksName);
        directoryPathBuilder.append(File.separatorChar);
        directoryPathBuilder.append(metadata.cfName);
        directoryPathBuilder.append(File.separatorChar);
        directoryPathBuilder.append(DEFAULT_PATH_PREFIX);
        path = directoryPathBuilder.toString();
    }

    public Schema getSchema()
    {
        return schema;
    }

    public double getRefreshSeconds()
    {
        return refreshSeconds;
    }

    public FilterCache getFilterCache()
    {
        return filterCache;
    }

    public String getPath()
    {
        return path;
    }

    public int getRamBufferMB()
    {
        return ramBufferMB;
    }

    public int getMaxMergeMB()
    {
        return maxMergeMB;
    }

    public int getMaxCachedMB()
    {
        return maxCachedMB;
    }

    public int getIndexingThreads()
    {
        return indexingThreads;
    }

    public int getIndexingQueuesSize()
    {
        return indexingQueuesSize;
    }

}
