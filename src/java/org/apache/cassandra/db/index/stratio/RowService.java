package org.apache.cassandra.db.index.stratio;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.Column;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionInfo;
import org.apache.cassandra.db.RangeTombstone;
import org.apache.cassandra.db.Row;
import org.apache.cassandra.db.TreeMapBackedSortedColumns;
import org.apache.cassandra.db.filter.ExtendedFilter;
import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.db.filter.SliceQueryFilter;
import org.apache.cassandra.db.index.stratio.RowDirectory.ScoredDocument;
import org.apache.cassandra.db.index.stratio.schema.CellsMapper;
import org.apache.cassandra.db.index.stratio.util.ByteBufferUtils;
import org.apache.cassandra.db.index.stratio.util.Log;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.thrift.IndexExpression;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.HeapAllocator;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.ChainedFilter;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.FilteredQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;

/**
 * Class for mapping rows between Cassandra and Lucene.
 * 
 * @author adelapena
 * 
 */
public class RowService {

	private final ColumnFamilyStore baseCfs;
	private final CFMetaData metadata;
	private final CompositeType nameType;
	private final CellsMapper cellsMapper;
	private final Set<String> fieldsToLoad;
	private final RowDirectory rowDirectory;
	private final ColumnIdentifier columnIdentifier;
	private final int clusteringPosition;
	private final boolean isWide;

	private final FilterCache filterCache;

	private final TokenMapper tokenMapper;
	private final PartitionKeyMapper partitionKeyMapper;
	private final ClusteringKeyMapper clusteringKeyMapper;
	private final FullKeyMapper fullKeyMapper;

	/**
	 * Returns a new {@code RowService}
	 * 
	 * @param baseCfs
	 *            The base column family store.
	 * @param indexName
	 *            The index name.
	 * @param columnIdentifier
	 *            The indexed column name.
	 * @param cellsMapper
	 *            The user column mapping schema.
	 * @param rowDirectory
	 *            The Lucene's manager.
	 * @param filterCache
	 *            The filters cache, may be {@code null} meaning no caching.
	 */
	public RowService(ColumnFamilyStore baseCfs, ColumnDefinition columnDefinition) {

		this.baseCfs = baseCfs;
		metadata = baseCfs.metadata;
		nameType = (CompositeType) metadata.comparator;
		columnIdentifier = new ColumnIdentifier(columnDefinition.name, columnDefinition.getValidator());

		RowServiceConfig config = new RowServiceConfig(columnDefinition);

		filterCache = config.getFilterCache();

		cellsMapper = config.getCellsMapper();
		partitionKeyMapper = PartitionKeyMapper.instance(metadata);
		tokenMapper = TokenMapper.instance();
		clusteringKeyMapper = ClusteringKeyMapper.instance(metadata);
		fullKeyMapper = FullKeyMapper.instance(metadata);

		rowDirectory = new RowDirectory(config.getPath(),
		                                config.getRefreshSeconds(),
		                                config.getRamBufferMB(),
		                                config.getMaxMergeMB(),
		                                config.getMaxCachedMB(),
		                                cellsMapper.analyzer());

		clusteringPosition = metadata.clusteringKeyColumns().size();
		isWide = clusteringPosition > 0;

		this.fieldsToLoad = new HashSet<>();
		fieldsToLoad.add(PartitionKeyMapper.FIELD_NAME);
		if (isWide) {
			fieldsToLoad.add(ClusteringKeyMapper.FIELD_NAME);
		}
	}

	/**
	 * Puts in the Lucene index the Cassandra's storage row identified by the specified partition
	 * key. Note that when using wide rows all the rows under the same partition key are indexed. It
	 * will be improved in the future.
	 * 
	 * @param key
	 *            The partition key.
	 * @param columnFamily
	 *            The column family containing the clustering keys.
	 */
	public final void index(ByteBuffer key, ColumnFamily columnFamily) {

		DeletionInfo deletionInfo = columnFamily.deletionInfo();

		DecoratedKey partitionKey = partitionKeyMapper.decoratedKey(key);
		if (columnFamily.iterator().hasNext()) {
			for (Column column : columnFamily) {
				ByteBuffer name = column.name();
				ByteBuffer[] components = ByteBufferUtils.split(name, nameType);
				ByteBuffer lastComponent = components[clusteringPosition];
				if (lastComponent.equals(ByteBufferUtil.EMPTY_BYTE_BUFFER)) { // Is clustering cell

					QueryFilter queryFilter = queryFilter(partitionKey, name);
					ColumnFamily allColumns = baseCfs.getColumnFamily(queryFilter);

					Document document = new Document();
					partitionKeyMapper.addFields(document, partitionKey);
					tokenMapper.addFields(document, partitionKey);
					cellsMapper.addFields(document, metadata, partitionKey, allColumns);
					if (isWide) {
						clusteringKeyMapper.addFields(document, allColumns);
						fullKeyMapper.addFields(document, partitionKey, allColumns);
					}

					Term term = term(partitionKey, name);
					rowDirectory.updateDocument(term, document);
				}
			}
		} else if (deletionInfo != null) {
			Iterator<RangeTombstone> deletionIterator = deletionInfo.rangeIterator();
			if (!deletionIterator.hasNext()) { // Delete full storage row
				Term term = partitionKeyMapper.term(partitionKey);
				rowDirectory.deleteDocuments(term);
			} else { // Just for delete ranges of wide rows
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

	private QueryFilter queryFilter(DecoratedKey partitionKey, ByteBuffer clusteringKey) {
		long timestamp = System.currentTimeMillis();
		if (isWide) {
			ByteBuffer start = clusteringKeyMapper.start(clusteringKey);
			ByteBuffer stop = clusteringKeyMapper.stop(clusteringKey);
			SliceQueryFilter dataFilter = new SliceQueryFilter(start,
			                                                   stop,
			                                                   false,
			                                                   Integer.MAX_VALUE,
			                                                   clusteringPosition);
			return new QueryFilter(partitionKey, baseCfs.name, dataFilter, timestamp);
		} else {
			return QueryFilter.getIdentityFilter(partitionKey, metadata.cfName, timestamp);
		}
	}

	/**
	 * Returns the Cassandra rows satisfying {@code extendedFilter}. This rows are retrieved from
	 * the Cassandra storage engine.
	 * 
	 * @param extendedFilter
	 *            The filter to be satisfied.
	 * @return The Cassandra rows satisfying {@code extendedFilter}.
	 */
	public List<Row> search(ExtendedFilter extendedFilter) throws IOException, ParseException {

		// Get filtering options
		int columns = extendedFilter.maxColumns();
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
		Log.debug("Lucene search time " + (System.currentTimeMillis() - searchStart));

		// Collect matching rows
		long collectStart = System.currentTimeMillis();
		List<Row> rows = new ArrayList<>(scoredDocuments.size());
		for (ScoredDocument sc : scoredDocuments) {
			rows.add(row(sc.document, sc.score));
		}
		Log.debug("Cassandra collection time " + (System.currentTimeMillis() - collectStart));

		return rows;
	}

	/**
	 * Returns the Cassandra's {@link Row} identified by the specified Lucene's {@link Document}.
	 * The specified search score is added as a column to the row.
	 * 
	 * @param document
	 *            The Lucene's {@link Document} identifying the {@link Row} to be returned.
	 * @param score
	 *            The search score.
	 * @return The Cassandra's {@link Row} identified by the specified Lucene's {@link Document}.
	 */
	private Row row(Document document, Float score) {

		// Get the decorated partition key
		DecoratedKey decoratedKey = partitionKeyMapper.decoratedKey(document);

		// Get the column family from Cassandra or Lucene
		DecoratedKey partitionKey = partitionKeyMapper.decoratedKey(document);
		ByteBuffer clusteringKey = isWide ? clusteringKeyMapper.byteBuffer(document) : null;
		QueryFilter queryFilter = queryFilter(partitionKey, clusteringKey);
		ColumnFamily cf = baseCfs.getColumnFamily(queryFilter);

		// Create the score column
		ByteBuffer name = isWide ? clusteringKeyMapper.name(document, columnIdentifier)
		                        : partitionKeyMapper.name(document, columnIdentifier);
		ByteBuffer value = UTF8Type.instance.decompose(score.toString());
		Column column = new Column(name, value);

		// Create a new column family with the scoring cell and the stored ones
		ColumnFamily decoratedCf = TreeMapBackedSortedColumns.factory.create(baseCfs.metadata);
		decoratedCf.addColumn(column);
		decoratedCf.addAll(cf, HeapAllocator.instance);

		return new Row(decoratedKey, decoratedCf);
	}

	/**
	 * Returns the Lucene's {@link Sort} to be used when querying.
	 * 
	 * @return The Lucene's {@link Sort} to be used when querying.
	 */
	private Sort sort() {
		if (isWide) {
			SortField[] partitionKeySort = tokenMapper.sortFields();
			SortField[] clusteringKeySort = clusteringKeyMapper.sortFields();
			return new Sort(ArrayUtils.addAll(partitionKeySort, clusteringKeySort));
		} else {
			return new Sort(tokenMapper.sortFields());
		}
	}

	/**
	 * Returns a Lucene's {@link Filter} representing the specified Cassandra's {@link DataRange}.
	 * 
	 * @param dataRange
	 *            The Cassandra's {@link DataRange} to be mapped.
	 * @return A Lucene's {@link Filter} representing the specified Cassandra's {@link DataRange}.
	 */
	private Filter filter(DataRange dataRange) {
		if (isWide) {
			Filter tokenFilter = tokenMapper.filter(dataRange);
			Filter clusteringKeyFilter = clusteringKeyMapper.filter(dataRange);
			return new ChainedFilter(new Filter[] { tokenFilter, clusteringKeyFilter }, ChainedFilter.AND);
		} else {
			return tokenMapper.filter(dataRange);
		}
	}

	/**
	 * Returns a Lucene's {@link Filter} representing the specified Cassandra's {@link DataRange}
	 * using caching.
	 * 
	 * @param dataRange
	 *            The Cassandra's {@link DataRange} to be mapped.
	 * @return A Lucene's {@link Filter} representing the specified Cassandra's {@link DataRange}.
	 */
	private Filter cachedFilter(DataRange dataRange) {
		if (filterCache == null) {
			return filter(dataRange);
		}
		Filter filter = filterCache.get(dataRange);
		if (filter == null) {
			Log.debug(" -> Cache fails " + dataRange.keyRange());
			filter = filter(dataRange);
			filterCache.put(dataRange, filter);
		} else {
			Log.debug(" -> Cache hits " + dataRange.keyRange());
		}
		return filter;
	}

	/**
	 * Returns a Lucene's {@link Term} to be used as the unique identifier of a row.
	 * 
	 * @param partitionKey
	 *            The row's partition key.
	 * @param clusteringKey
	 *            The row's clustering key.
	 * @return A Lucene's {@link Term} to be used as the unique identifier of a row.
	 */
	private Term term(DecoratedKey partitionKey, ByteBuffer clusteringKey) {
		if (isWide) {
			return fullKeyMapper.term(partitionKey, clusteringKey);
		} else {
			return partitionKeyMapper.term(partitionKey);
		}
	}

}
