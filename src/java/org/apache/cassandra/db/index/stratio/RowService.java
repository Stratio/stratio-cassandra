package org.apache.cassandra.db.index.stratio;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.cql3.CFDefinition;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.Column;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionInfo;
import org.apache.cassandra.db.RangeTombstone;
import org.apache.cassandra.db.TreeMapBackedSortedColumns;
import org.apache.cassandra.db.filter.ExtendedFilter;
import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.db.index.SecondaryIndex;
import org.apache.cassandra.db.index.stratio.RowDirectory.ScoredDocument;
import org.apache.cassandra.db.index.stratio.util.ByteBufferUtils;
import org.apache.cassandra.db.index.stratio.util.ColumnFamilySerializer;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.thrift.IndexExpression;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.HeapAllocator;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.ChainedFilter;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.FilteredQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.util.BytesRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class for mapping rows between Cassandra and Lucene.
 * 
 * @author adelapena
 * 
 */
public abstract class RowService {

	protected static final Logger logger = LoggerFactory.getLogger(SecondaryIndex.class);

	protected final ColumnFamilyStore baseCfs;

	protected final CFMetaData metadata;
	protected final String cfName;
	protected final CompositeType nameType;
	protected final PartitionKeyMapper partitionKeyMapper;
	protected final TokenMapper tokenMapper;
	protected final CellsMapper cellsMapper;
	protected final Set<String> fieldsToLoad;
	protected final RowDirectory rowDirectory;
	protected final ColumnIdentifier indexedColumnName;
	protected final boolean storedRows;
	protected final int clusteringPosition;
	protected final CFDefinition cfDefinition;

	protected final FilterCache filterCache;

	public static final String SERIALIZED_ROW_NAME = "row";
	public static final FieldType SERIALIZED_ROW_TYPE = new FieldType();
	static {
		SERIALIZED_ROW_TYPE.setStored(true);
		SERIALIZED_ROW_TYPE.setIndexed(false);
		SERIALIZED_ROW_TYPE.setTokenized(false);
		SERIALIZED_ROW_TYPE.freeze();
	}

	/**
	 * 
	 * @param baseCfs
	 *            The base column family store.
	 * @param indexName
	 *            The index name.
	 * @param indexedColumnName
	 *            The indexed column name.
	 * @param cellsMapper
	 *            The user column mapping schema.
	 * @param refreshSeconds
	 *            The index readers refresh time in seconds.
	 * @param writeBufferSize
	 *            The index writer buffer size in MB.
	 * @param directoryPath
	 *            The path of the index files directory.
	 * @param filterCacheSize
	 *            The number of data range filters to be cached.
	 * @param storedRows
	 *            If the rows must be stored in a Lucene field.
	 * @param fieldsToLoad
	 *            The document fields to be loaded.
	 */
	protected RowService(ColumnFamilyStore baseCfs,
	                     String indexName,
	                     ColumnIdentifier indexedColumnName,
	                     CellsMapper cellsMapper,
	                     int refreshSeconds,
	                     int writeBufferSize,
	                     String directoryPath,
	                     int filterCacheSize,
	                     boolean storedRows,
	                     Set<String> fieldsToLoad) {

		this.baseCfs = baseCfs;

		metadata = baseCfs.metadata;
		cfName = metadata.cfName;
		nameType = (CompositeType) metadata.comparator;
		partitionKeyMapper = PartitionKeyMapper.instance();
		tokenMapper = TokenMapper.instance();

		this.cellsMapper = cellsMapper;
		this.indexedColumnName = indexedColumnName;
		this.fieldsToLoad = fieldsToLoad;
		this.fieldsToLoad.add(SERIALIZED_ROW_NAME);
		this.storedRows = storedRows;
		this.cfDefinition = metadata.getCfDef();
		this.clusteringPosition = cfDefinition.columns.size();

		rowDirectory = new RowDirectory(directoryPath, cellsMapper.analyzer(), refreshSeconds, writeBufferSize);

		filterCache = filterCacheSize > 0 ? new FilterCache(filterCacheSize) : null;
	}

	public static RowService build(ColumnFamilyStore baseCfs, ColumnDefinition columnDefinition) {
		return new RowServiceBuilder().build(baseCfs, columnDefinition);
	}

	/**
	 * Puts in the Lucene index the Cassandra's storage row identified by the specified partition
	 * key. Note that when using wide rows all the rows under the same partition key are indexed. It
	 * will be improved in the future.
	 * 
	 * @param partitionKey
	 * @param columnFamily
	 */
	public final void index(ByteBuffer key, ColumnFamily columnFamily) {
		/*
		System.out.println();
		System.out.println("INDEXING");
		for (Column column : columnFamily) {
			System.out.println("COLUMN " + ByteBufferUtil.bytesToHex(column.name())
			                   + " - "
			                   + ByteBufferUtil.bytesToHex(column.value()));
		}
		System.out.println("CLUSTERING POSITION " + clusteringPosition);
		System.out.println("DELETION INFO " + columnFamily.deletionInfo());
		System.out.println();
		*/
		DeletionInfo deletionInfo = columnFamily.deletionInfo();

		DecoratedKey partitionKey = partitionKeyMapper.decoratedKey(key);
		if (columnFamily.iterator().hasNext()) {
			//System.out.println("INSERTING ");
			for (Column column : columnFamily) {
				ByteBuffer name = column.name();
				ByteBuffer[] components = ByteBufferUtils.split(name, nameType);
				ByteBuffer lastComponent = components[clusteringPosition];
				if (lastComponent.equals(ByteBufferUtil.EMPTY_BYTE_BUFFER)) { // Is clustering cell
					Document document = document(partitionKey, name);
					//System.out.println(" -> INDEXED " + document);
					Term term = term(partitionKey, name);
					rowDirectory.updateDocument(term, document);
				}
			}
		} else if (deletionInfo != null) {
			//System.out.println("DELETING WITH KEY ");
			Iterator<RangeTombstone> deletionIterator = deletionInfo.rangeIterator();
			if (!deletionIterator.hasNext()) {
				Term term = partitionKeyMapper.term(partitionKey);
				rowDirectory.deleteDocuments(term);
			} else {
				//System.out.println("DELETING WITH RANGE "); // JUST FOR WIDE ROWS - SPECIALIZE !!!!
				while (deletionIterator.hasNext()) {
					RangeTombstone rangeTombstone = deletionIterator.next();
					ByteBuffer min = rangeTombstone.min;
					ByteBuffer max = rangeTombstone.max;
					ColumnsFilter columnsFilter = new ColumnsFilter(min, max, nameType);
					Query partitionKeyQuery = partitionKeyMapper.query(partitionKey);
					Query query = new FilteredQuery(partitionKeyQuery, columnsFilter);
					rowDirectory.deleteDocuments(query);
				}
			}
		}
	}

	/**
	 * Deletes the row identified by the specified partition key.
	 * 
	 * @param partitionKey
	 *            The partition key identifying the row to be deleted.
	 */
	public final void delete(DecoratedKey partitionKey) {
		Term term = partitionKeyMapper.term(partitionKey);
		rowDirectory.deleteDocuments(term);
	}

	/**
	 * Returns the total size of all index files currently cached in memory.
	 * 
	 * @return The total size of all index files currently cached in memory.
	 */
	public long getRAMSizeInBytes() {
		return rowDirectory.getRAMSizeInBytes();
	}

	/**
	 * Deletes all the {@link Document}s.
	 */
	public void truncate() {
		rowDirectory.deleteAll();
	}

	/**
	 * Closes and removes all the index files.
	 * 
	 * @return
	 */
	public void delete() {
		rowDirectory.removeIndex();
	}

	/**
	 * Commits the pending changes.
	 */
	public void commit() {
		rowDirectory.commit();
	}

	/**
	 * Adds to the specified {@link Documents} the {@link Field}s associated to the CQL3 row defined
	 * by the specified partition key and column family.
	 * 
	 * @param partitionKey
	 *            The partition key of the cell to be mapped.
	 * @param columnFamily
	 *            The columns of the cell to be mapped.
	 * @return A {@link Document} representing the cell defined by the specified partition key and
	 *         column family.
	 */
	protected abstract Document document(DecoratedKey key, ByteBuffer clusteringKey);

	/**
	 * Returns the Lucene's {@link Sort} to be used when querying.
	 * 
	 * @return The Lucene's {@link Sort} to be used when querying.
	 */
	protected abstract Sort sort();

	/**
	 * Returns a Lucene's {@link Filter} representing the specified Cassandra's {@link DataRange}.
	 * 
	 * @param dataRange
	 *            The Cassandra's {@link DataRange} to be mapped.
	 * @return A Lucene's {@link Filter} representing the specified Cassandra's {@link DataRange}.
	 */
	protected abstract Filter[] filters(DataRange dataRange);

	protected abstract QueryFilter queryFilter(Document document, long timestamp);

	protected abstract org.apache.cassandra.db.Column scoreCell(Document document, Float score);

	/**
	 * Returns the Cassandra rows satisfying {@code extendedFilter}. This rows are retrieved from
	 * the Cassandra storage engine.
	 * 
	 * @param extendedFilter
	 *            The filter to be satisfied.
	 * @return The Cassandra rows satisfying {@code extendedFilter}.
	 */
	public List<org.apache.cassandra.db.Row> search(ExtendedFilter extendedFilter) throws IOException {

		// Get filtering options
		int columns = extendedFilter.maxColumns();
		long timestamp = extendedFilter.timestamp;
		IndexExpression indexExpression = extendedFilter.getClause().get(0);
		ByteBuffer columnValue = indexExpression.value;
		String querySentence = UTF8Type.instance.compose(columnValue);
		DataRange dataRange = extendedFilter.dataRange;

		// Setup Lucene's query, filter and sort
		Query query = cellsMapper.query(querySentence);
		Filter filter = cachedFilter(dataRange);
		Sort sort = sort();

		// Search in Lucene's index
		long searchStart = System.currentTimeMillis();
		List<ScoredDocument> scoredDocuments = rowDirectory.search(query, filter, sort, columns, fieldsToLoad);
		long searchFinish = System.currentTimeMillis();
		System.out.println(" -> LUCENE SEARCH TIME " + (searchFinish - searchStart));

		// Collect matching rows
		long collectStart = System.currentTimeMillis();
		List<org.apache.cassandra.db.Row> rows = new ArrayList<>(scoredDocuments.size());
		for (ScoredDocument scoredDocument : scoredDocuments) {
			// System.out.println("FOUND " + scoredDocument);

			Document document = scoredDocument.getDocument();
			Float score = scoredDocument.getScore();

			// Get the decorated partition key
			DecoratedKey decoratedKey = partitionKeyMapper.decoratedKey(document);

			// Get the column family from Cassandra or Lucene
			ColumnFamily cf = null;
			if (storedRows) {
				BytesRef bytesRef = document.getBinaryValue(SERIALIZED_ROW_NAME);
				byte[] bytes = bytesRef.bytes;
				cf = ColumnFamilySerializer.columnFamily(bytes);
			} else {
				QueryFilter queryFilter = queryFilter(document, timestamp);
				cf = baseCfs.getColumnFamily(queryFilter);
			}

			// Create a new column family with the scoring cell and the stored ones
			org.apache.cassandra.db.Column scoringCell = scoreCell(document, score);
			ColumnFamily decoratedCf = TreeMapBackedSortedColumns.factory.create(baseCfs.metadata);
			decoratedCf.addColumn(scoringCell);
			decoratedCf.addAll(cf, HeapAllocator.instance);

			// Collect
			rows.add(new org.apache.cassandra.db.Row(decoratedKey, decoratedCf));
		}

		long collectFinish = System.currentTimeMillis();
		System.out.println(" -> ROW COLLECTION TIME " + (collectFinish - collectStart));
		return rows;
	}

	private Filter cachedFilter(DataRange dataRange) {
		if (filterCache == null) {
			return filter(dataRange);
		} else {
			Filter filter = filterCache.get(dataRange);
			if (filter == null) {
				logger.info(" -> Cache fails " + dataRange.keyRange());
				filter = filter(dataRange);
				filterCache.put(dataRange, filter);
			} else {
				logger.info(" -> Cache hits " + dataRange.keyRange());
			}
			return filter;
		}
	}

	private Filter filter(DataRange dataRange) {
		return new ChainedFilter(filters(dataRange), ChainedFilter.AND);
	}

	protected abstract Term term(DecoratedKey partitionKey, ByteBuffer clusteringKey);

}
