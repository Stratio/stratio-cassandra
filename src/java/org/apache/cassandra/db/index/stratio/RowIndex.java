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
package org.apache.cassandra.db.index.stratio;

import java.nio.ByteBuffer;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.index.PerRowSecondaryIndex;
import org.apache.cassandra.db.index.SecondaryIndexManager;
import org.apache.cassandra.db.index.SecondaryIndexSearcher;
import org.apache.cassandra.db.index.stratio.util.Log;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.exceptions.ConfigurationException;

/**
 * A {@link PerRowSecondaryIndex} that uses Apache Lucene as backend. It allows, among others,
 * multi-comun and full-text search.
 * 
 * @author Andres de la Pena <adelapena@stratio.com>
 * 
 */
public class RowIndex extends PerRowSecondaryIndex {

	private SecondaryIndexManager secondaryIndexManager;
	private CFMetaData metadata;
	private ColumnDefinition columnDefinition;

	private String keyspaceName;
	private String tableName;
	private String indexName;
	private String columnName;
	private String logName;

	private RowIndexService rowIndexService;

	// Concurrency lock
	private ReadWriteLock lock = new ReentrantReadWriteLock();

	@Override
	public String getIndexName() {
		return indexName;
	}

	public String getKeyspaceName() {
		return keyspaceName;
	}

	public String getTableName() {
		return tableName;
	}

	public String getColumnName() {
		return columnName;
	}

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
		} finally {
			lock.writeLock().unlock();
		}
	}

	private void setup() {

		// Load column family info
		secondaryIndexManager = baseCfs.indexManager;
		metadata = baseCfs.metadata;
		columnDefinition = columnDefs.iterator().next();
		indexName = columnDefinition.getIndexName();
		keyspaceName = metadata.ksName;
		tableName = metadata.cfName;
		columnName = UTF8Type.instance.compose(columnDefinition.name);
		logName = String.format("%s.%s.%s", keyspaceName, tableName, indexName);

		// Build row mapper
		rowIndexService = new RowIndexService(baseCfs, columnDefinition);
	}

	/**
	 * Index the given row.
	 * 
	 * @param key
	 *            The partition key.
	 * @param columnFamily
	 *            The column family data to be indexed
	 */
	@Override
	public void index(ByteBuffer key, ColumnFamily columnFamily) {
		Log.debug("Indexing row %s in index %s", key, logName);
		lock.readLock().lock();
		try {
			if (rowIndexService != null) {
				long timestamp = System.currentTimeMillis();
				rowIndexService.index(key, columnFamily, timestamp);
			}
		} catch (Exception e) { // Ignore errors
			Log.error(e, "Ignoring error while indexing row %s", key);
		} finally {
			lock.readLock().unlock();
		}
	}

	/**
	 * cleans up deleted columns from cassandra cleanup compaction
	 * 
	 * @param key
	 */
	@Override
	public void delete(DecoratedKey key) {
		Log.debug("Removing row %s from index %s", key, logName);
		lock.writeLock().lock();
		try {
			rowIndexService.delete(key);
			rowIndexService = null;
		} finally {
			lock.writeLock().unlock();
		}
	}

	@Override
	public boolean indexes(ByteBuffer cellName) {
		return true;
	}

	@Override
	public void validateOptions() throws ConfigurationException {
		Log.debug("Validating");
		try {
			ColumnDefinition columnDefinition = columnDefs.iterator().next();
			if (baseCfs != null) {
				new RowIndexConfig(baseCfs.metadata,
				                   columnDefinition.getIndexName(),
				                   columnDefinition.getIndexOptions());
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
	public long getLiveSize() {
		return 0;
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
			if (rowIndexService != null) {
				rowIndexService.delete();
				rowIndexService = null;
			}
			Log.info("Removed index %s", logName);
		} catch (Exception e) {
			Log.error("Removing index %s", logName);
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
			if (rowIndexService != null) {
				rowIndexService.delete();
				rowIndexService = null;
			}
			Log.info("Invalidated index %s", logName);
		} catch (Exception e) {
			Log.error("Invalidating index %s", logName);
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
			if (rowIndexService != null) {
				rowIndexService.truncate();
			}
			Log.info("Truncated index %s", logName);
		} catch (Exception e) {
			Log.error("Truncating index %s", logName);
			throw new RuntimeException(e);
		} finally {
			lock.writeLock().unlock();
		}
	}

	@Override
	public void reload() {
		Log.info("Reloading index %s", logName);
		lock.writeLock().lock();
		try {
			if (rowIndexService == null) {
				setup();
			}
			Log.info("Reloaded index %s", logName);
		} catch (Exception e) {
			Log.error("Reloading index %s", logName);
			throw new RuntimeException(e);
		} finally {
			lock.writeLock().unlock();
		}
	}

	@Override
	public void forceBlockingFlush() {
		Log.info("Flushing index %s", logName);
		lock.writeLock().lock();
		try {
			rowIndexService.commit();
			Log.info("Flushed index %s", logName);
		} catch (Exception e) {
			Log.error("Flushing index %s", logName);
			throw new RuntimeException(e);
		} finally {
			lock.writeLock().unlock();
		}
	}

	@Override
	protected SecondaryIndexSearcher createSecondaryIndexSearcher(Set<ByteBuffer> columns) {
		Log.debug("Creating searcher for index %s", logName);
		return new RowIndexSearcher(secondaryIndexManager, this, columns, rowIndexService);
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("RowIndex [index=");
		builder.append(indexName);
		builder.append(", keyspace=");
		builder.append(keyspaceName);
		builder.append(", table=");
		builder.append(tableName);
		builder.append(", column=");
		builder.append(columnName);
		builder.append("]");
		return builder.toString();
	}

}
