package org.apache.cassandra.db.index.stratio;

import java.nio.ByteBuffer;
import java.util.Set;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.index.PerRowSecondaryIndex;
import org.apache.cassandra.db.index.SecondaryIndexManager;
import org.apache.cassandra.db.index.SecondaryIndexSearcher;
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

	@Override
	public void init() {

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

	private String format(String message, Object... options) {
		return String.format("Lucene index %s.%s.%s : %s", ksName, cfName, indexName, String.format(message, options));
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
		logger.debug(format("Indexing storage engine row %s", partitionKey));
		rowService.index(partitionKey, columnFamily);
	}

	/**
	 * cleans up deleted columns from cassandra cleanup compaction
	 * 
	 * @param partitionKey
	 */
	@Override
	public void delete(DecoratedKey partitionKey) {
		logger.debug(format("Deleting row %s", partitionKey));
		rowService.delete(partitionKey);
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
//		return rowService.getRAMSizeInBytes();
	}

	public ColumnFamilyStore getIndexCfs() {
		return null;
	}

	@Override
	public void removeIndex(ByteBuffer columnName) {
		logger.info(format("Removing"));
		rowService.delete();
	}

	@Override
	public void invalidate() {
		logger.info(format("Invalidating"));
		rowService.delete();
	}

	@Override
	public void truncateBlocking(long truncatedAt) {
		logger.info(format("Truncating"));
		rowService.truncate();
	}

	@Override
	public void reload() {
		logger.info(format("Reloading"));
		rowService.commit();
	}

	@Override
	public void forceBlockingFlush() {
		logger.info(format("Flushing"));
		rowService.commit();
	}

	@Override
	protected SecondaryIndexSearcher createSecondaryIndexSearcher(Set<ByteBuffer> columns) {
		logger.info(format("Creating searcher"));
		return new RowIndexSearcher(secondaryIndexManager, this, columns, rowService);
	}

}
