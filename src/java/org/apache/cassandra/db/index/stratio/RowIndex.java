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
import org.apache.cassandra.exceptions.ConfigurationException;

/**
 * A {@link PerRowSecondaryIndex} that uses Apache Lucene as backend. It allows, among others,
 * multi-comun and full-text search.
 * 
 * @author adelapena
 * 
 */
public class RowIndex extends PerRowSecondaryIndex {

	protected SecondaryIndexManager secondaryIndexManager;
	protected CFMetaData metadata;
	protected ColumnDefinition columnDefinition;

	protected String ksName;
	protected String cfName;
	protected String indexName;

	private RowService rowService;

	// Concurrency lock
	private ReadWriteLock lock = new ReentrantReadWriteLock();

	@Override
	public void init() {
		Log.debug("Initializing index");
		lock.readLock().lock();
		try {
			setup();
		} finally {
			lock.readLock().unlock();
		}
	}

	private void setup() {
		Log.debug("Setup row mapper");

		// Load column family info
		secondaryIndexManager = baseCfs.indexManager;
		metadata = baseCfs.metadata;
		columnDefinition = columnDefs.iterator().next();
		ksName = metadata.ksName;
		cfName = metadata.cfName;
		indexName = columnDefinition.getIndexName();

		// Build row mapper
		rowService = RowService.build(baseCfs, columnDefinition);
	}

	/**
	 * Index the given row.
	 * 
	 * @param partitionKey
	 *            The partition key.
	 * @param columnFamily
	 *            The column family data to be indexed
	 */
	@Override
	public void index(ByteBuffer partitionKey, ColumnFamily columnFamily) {
		Log.debug("Indexing %s", partitionKey);
		lock.readLock().lock();
		try {
			rowService.index(partitionKey, columnFamily);
		} finally {
			lock.readLock().unlock();
		}
	}

	/**
	 * cleans up deleted columns from cassandra cleanup compaction
	 * 
	 * @param partitionKey
	 */
	@Override
	public void delete(DecoratedKey partitionKey) {
		Log.debug("Deleting %s", partitionKey);
		lock.writeLock().lock();
		try {
			rowService.delete(partitionKey);
			rowService = null;
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
	}

	@Override
	public String getIndexName() {
		return indexName;
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
		Log.info("Removing %s", columnName);
		lock.writeLock().lock();
		try {
			rowService.delete();
			rowService = null;
		} finally {
			lock.writeLock().unlock();
		}
	}

	@Override
	public void invalidate() {
		Log.info("Invalidating");
		rowService.delete();
		rowService = null;
	}

	@Override
	public void truncateBlocking(long truncatedAt) {
		Log.info("Truncating");
		rowService.truncate();
	}

	@Override
	public void reload() {
		Log.info("Reloading");
		lock.writeLock().lock();
		try {
			if (rowService == null) {
				setup();
			}
		} finally {
			lock.writeLock().unlock();
		}
	}

	@Override
	public void forceBlockingFlush() {
		Log.info("Flushing");
		lock.writeLock().lock();
		try {
			rowService.commit();
		} finally {
			lock.writeLock().unlock();
		}
	}

	@Override
	protected SecondaryIndexSearcher createSecondaryIndexSearcher(Set<ByteBuffer> columns) {
		Log.info("Creating searcher");
		return new RowIndexSearcher(secondaryIndexManager, this, columns, rowService);
	}

}
