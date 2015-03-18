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

import com.stratio.cassandra.util.Log;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.composites.CellName;
import org.apache.cassandra.db.index.PerRowSecondaryIndex;
import org.apache.cassandra.db.index.SecondaryIndexManager;
import org.apache.cassandra.db.index.SecondaryIndexSearcher;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.utils.concurrent.OpOrder;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * A {@link PerRowSecondaryIndex} that uses Apache Lucene as backend. It allows, among others, multi-column and
 * full-text search.
 *
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class RowIndex extends PerRowSecondaryIndex {

    private SecondaryIndexManager secondaryIndexManager;
    private ColumnDefinition columnDefinition;

    private String keyspaceName;
    private String tableName;
    private String indexName;
    private String columnName;
    private String logName;

    private RowService rowService;

    // Concurrency lock
    private ReadWriteLock lock = new ReentrantReadWriteLock();

    @Override
    public String getIndexName() {
        return indexName;
    }

    /**
     * Returns the indexed keyspace name.
     *
     * @return The indexed keyspace name.
     */
    public String getKeyspaceName() {
        return keyspaceName;
    }

    /**
     * Returns the indexed table name.
     *
     * @return The indexed table name.
     */
    public String getTableName() {
        return tableName;
    }

    /**
     * Returns the indexed column name.
     *
     * @return The indexed column name.
     */
    public String getColumnName() {
        return columnName;
    }

    /**
     * Returns the indexed column definition.
     *
     * @return The indexed column definition.
     */
    public ColumnDefinition getColumnDefinition() {
        return columnDefinition;
    }

    @Override
    public void init() {
        Log.info("Initializing index %s", logName);
        lock.writeLock().lock();
        try {
            setup();
            Log.info("Initialized index %s", logName);
        } catch (Exception e) {
            Log.error(e, "Error while initializing index %s", logName);
            throw new RuntimeException(e);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void validate(CFMetaData metadata, Map<String, String> indexOptions) {
        try {
            new RowIndexConfig(metadata, indexOptions);
        } catch (RuntimeException e) {
            Log.error(e, "Error validating index config");
            throw e;
        }
    }

    private void setup() {

        // Load column family info
        secondaryIndexManager = baseCfs.indexManager;
        columnDefinition = columnDefs.iterator().next();
        indexName = columnDefinition.getIndexName();
        keyspaceName = baseCfs.metadata.ksName;
        tableName = baseCfs.metadata.cfName;
        columnName = columnDefinition.name.toString();
        logName = String.format("%s.%s.%s", keyspaceName, tableName, indexName);

        // Build row mapper
        rowService = RowService.build(baseCfs, columnDefinition);
    }

    /**
     * Index the given row.
     *
     * @param key          The partition key.
     * @param columnFamily The column family data to be indexed
     */
    @Override
    public void index(ByteBuffer key, ColumnFamily columnFamily) {
        Log.debug("Indexing row %s in index %s ", key, logName);
        lock.readLock().lock();
        try {
            if (rowService != null) {
                long timestamp = System.currentTimeMillis();
                rowService.index(key, columnFamily, timestamp);
            }
        } catch (RuntimeException e) {
            Log.error("Error while indexing row %s", key);
            throw e;
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * cleans up deleted columns from cassandra cleanup compaction
     *
     * @param key The partition key of the physical row to be deleted.
     */
    @Override
    public void delete(DecoratedKey key, OpOrder.Group opGroup) {
        Log.debug("Removing row %s from index %s", key, logName);
        lock.writeLock().lock();
        try {
            rowService.delete(key);
            rowService = null;
        } catch (RuntimeException e) {
            Log.error(e, "Error deleting row %s", key);
            throw e;
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public boolean indexes(CellName cellName) {
        return true;
    }

    @Override
    public void validateOptions() throws ConfigurationException {
        Log.debug("Validating");
        try {
            ColumnDefinition columnDefinition = columnDefs.iterator().next();
            if (baseCfs != null) {
                new RowIndexConfig(baseCfs.metadata, columnDefinition.getIndexOptions());
                Log.debug("Index options are valid");
            } else {
                Log.debug("Validation skipped");
            }
        } catch (Exception e) {
            String message = "Error while validating index options: " + e.getMessage();
            Log.error(e, message);
            throw new ConfigurationException(message, e);
        }
    }

    @Override
    public long estimateResultRows() {
        try {
            return rowService.getIndexSize();
        } catch (Exception e) {
            Log.error(e, "Estimating row results for index %s", logName);
            throw new RuntimeException(e);
        }
    }

    @Override
    public ColumnFamilyStore getIndexCfs() {
        return null;
    }

    @Override
    public void removeIndex(ByteBuffer columnName) {
        Log.info("Removing index %s", logName);
        lock.writeLock().lock();
        try {
            if (rowService != null) {
                rowService.delete();
                rowService = null;
            }
            Log.info("Removed index %s", logName);
        } catch (Exception e) {
            Log.error(e, "Removing index %s", logName);
            throw new RuntimeException(e);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void invalidate() {
        Log.info("Invalidating index %s", logName);
        lock.writeLock().lock();
        try {
            if (rowService != null) {
                rowService.delete();
                rowService = null;
            }
            Log.info("Invalidated index %s", logName);
        } catch (Exception e) {
            Log.error(e, "Invalidating index %s", logName);
            throw new RuntimeException(e);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void truncateBlocking(long truncatedAt) {
        Log.info("Truncating index %s", logName);
        lock.writeLock().lock();
        try {
            if (rowService != null) {
                rowService.truncate();
            }
            Log.info("Truncated index %s", logName);
        } catch (Exception e) {
            Log.error(e, "Truncating index %s", logName);
            throw new RuntimeException(e);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void reload() {
        Log.info("Reloading index %s", logName);
    }

    @Override
    public void forceBlockingFlush() {
        Log.info("Flushing index %s", logName);
        lock.writeLock().lock();
        try {
            rowService.commit();
            Log.info("Flushed index %s", logName);
        } catch (RuntimeException e) {
            Log.error(e, "Flushing index %s", logName);
            throw e;
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    protected SecondaryIndexSearcher createSecondaryIndexSearcher(Set<ByteBuffer> columns) {
        Log.debug("Creating searcher for index %s", logName);
        return new RowIndexSearcher(secondaryIndexManager, this, columns, rowService);
    }

    @Override
    public void optimize() {
        Log.info("Compacting index %s", logName);
        try {
            rowService.optimize();
        } catch (Exception e) {
            Log.error(e, "Error while compacting index %s", logName);
            throw new RuntimeException(e);
        }
    }

    @Override
    public String toString() {
        return String.format("RowIndex [index=%s, keyspace=%s, table=%s, column=%s",
                             indexName,
                             keyspaceName,
                             tableName,
                             columnName);
    }

}
