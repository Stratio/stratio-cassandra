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
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.Column;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionInfo;
import org.apache.cassandra.db.RangeTombstone;
import org.apache.cassandra.db.Row;
import org.apache.cassandra.db.TreeMapBackedSortedColumns;
import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.db.filter.SliceQueryFilter;
import org.apache.cassandra.db.index.stratio.RowDirectory.ScoredDocument;
import org.apache.cassandra.db.index.stratio.query.Search;
import org.apache.cassandra.db.index.stratio.schema.Cell;
import org.apache.cassandra.db.index.stratio.schema.Cells;
import org.apache.cassandra.db.index.stratio.schema.Schema;
import org.apache.cassandra.db.index.stratio.util.Log;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.thrift.IndexExpression;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.ChainedFilter;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.FilteredQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;

/**
 * Class for mapping rows between Cassandra and Lucene.
 * 
 * @author Andres de la Pena <adelapena@stratio.com>
 * 
 */
public class RowIndexService {

	private static final int PAGE_SIZE = 100;

	private final ColumnFamilyStore baseCfs;
	private final CFMetaData metadata;
	private final Schema schema;
	private final Set<String> fieldsToLoad;
	private final RowDirectory rowDirectory;
	private final int clusteringPosition;
	private final boolean isWide;

	private final FilterCache filterCache;

	private final TokenMapper tokenMapper;
	private final PartitionKeyMapper partitionKeyMapper;
	private final ClusteringKeyMapper clusteringKeyMapper;
	private final FullKeyMapper fullKeyMapper;

	/**
	 * Returns a new {@code RowService}.
	 * 
	 * @param baseCfs
	 *            The base column family store.
	 * @param columnDefinition
	 *            The indexed column definition.
	 */
	public RowIndexService(ColumnFamilyStore baseCfs, ColumnDefinition columnDefinition) {

		this.baseCfs = baseCfs;
		metadata = baseCfs.metadata;

		RowIndexConfig config = new RowIndexConfig(metadata,
		                                           columnDefinition.getIndexName(),
		                                           columnDefinition.getIndexOptions());

		filterCache = config.getFilterCache();

		schema = config.getSchema();
		partitionKeyMapper = PartitionKeyMapper.instance(metadata);
		tokenMapper = TokenMapper.instance();
		clusteringKeyMapper = ClusteringKeyMapper.instance(metadata);
		fullKeyMapper = FullKeyMapper.instance(metadata);

		rowDirectory = new RowDirectory(config.getPath(),
		                                config.getRefreshSeconds(),
		                                config.getRamBufferMB(),
		                                config.getMaxMergeMB(),
		                                config.getMaxCachedMB(),
		                                schema.analyzer());

		clusteringPosition = metadata.clusteringKeyColumns().size();
		isWide = clusteringPosition > 0;

		fieldsToLoad = new HashSet<>();
		fieldsToLoad.add(PartitionKeyMapper.FIELD_NAME);
		if (isWide) {
			fieldsToLoad.add(ClusteringKeyMapper.FIELD_NAME);
		}
	}

	/**
	 * Returns the used {@link Schema}.
	 * 
	 * @return The used {@link Schema}.
	 */
	public Schema getSchema() {
		return schema;
	}

	/**
	 * Returns the names of the document fields to be loaded when reading a Lucene's index.
	 * 
	 * @return The names of the document fields to be loaded.
	 */
	public Set<String> fieldsToLoad() {
		return fieldsToLoad;
	}

	/**
	 * Puts in the Lucene index the Cassandra's the row identified by the specified partition key
	 * and the clustering keys contained in the specified {@link ColumnFamily}.
	 * 
	 * @param key
	 *            The partition key.
	 * @param columnFamily
	 *            The column family containing the clustering keys.
	 * @param timestamp
	 *            The operation time stamp.
	 */
	public void index(ByteBuffer key, ColumnFamily columnFamily, long timestamp) {

		DeletionInfo deletionInfo = columnFamily.deletionInfo();
		DecoratedKey partitionKey = partitionKeyMapper.decoratedKey(key);

		if (isWide) {
			if (columnFamily.iterator().hasNext()) {
				System.out.println(" -> INDEXING : HAS COLUMNS ");
				for (ByteBuffer clusteringKey : clusteringKeyMapper.byteBuffers(columnFamily)) {
					System.out.println("\tCOLUMN");
					Row row = row(partitionKey, clusteringKey, timestamp);
					Document document = document(row);
					Term term = identifyingTerm(row);
					rowDirectory.updateDocument(term, document);
				}

			} else if (deletionInfo != null) {
				System.out.println(" -> INDEXING : HAS DELETION INFO ");
				Iterator<RangeTombstone> iterator = deletionInfo.rangeIterator();
				if (iterator.hasNext()) {
					while (iterator.hasNext()) {
						System.out.println("\tCOLUMN");
						RangeTombstone rangeTombstone = iterator.next();
						Filter filter = clusteringKeyMapper.filter(rangeTombstone);
						Query partitionKeyQuery = partitionKeyMapper.query(partitionKey);
						Query query = new FilteredQuery(partitionKeyQuery, filter);
						rowDirectory.deleteDocuments(query);
					}
				} else {
					System.out.println("\tDELETING FULL ROW");
					Term term = partitionKeyMapper.term(partitionKey);
					rowDirectory.deleteDocuments(term);
				}
			}
		} else {
			Row row = row(partitionKey, null, timestamp);
			if (row.cf.iterator().hasNext()) {
				Document document = document(row);
				Term term = identifyingTerm(row);
				rowDirectory.updateDocument(term, document);
			} else if (deletionInfo != null) {
				Term term = partitionKeyMapper.term(partitionKey);
				rowDirectory.deleteDocuments(term);
			}
		}
	}

	/**
	 * Returns the {@link Document} represented by the specified {@link Row}. It's assumed that the
	 * {@link Row} is a CQL3 one, so its {@link ColumnFamily} musts contain one and only one
	 * clustering key.
	 * 
	 * @param row
	 *            A {@link Row}.
	 * @return The Lucene {@link Document} representing the specified {@link Row}.
	 */
	public Document document(Row row) {
		DecoratedKey partitionKey = row.key;
		ColumnFamily columnFamily = row.cf;
		Document document = new Document();
		tokenMapper.addFields(document, partitionKey);
		partitionKeyMapper.addFields(document, partitionKey);
		schema.addFields(document, metadata, partitionKey, columnFamily);
		if (isWide) {
			ByteBuffer clusteringKey = clusteringKeyMapper.byteBuffer(columnFamily);
			clusteringKeyMapper.addFields(document, clusteringKey);
			fullKeyMapper.addFields(document, partitionKey, clusteringKey);
		}
		return document;
	}

	/**
	 * Deletes the partition identified by the specified partition key.
	 * 
	 * @param partitionKey
	 *            The partition key identifying the partition to be deleted.
	 */
	public void delete(DecoratedKey partitionKey) {
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

	/**
	 * Reads and returns the CQL3 {@link Row} identified by the specified key pair, using the
	 * specified time stamp to ignore deleted columns.
	 * 
	 * @param partitionKey
	 *            The partition key.
	 * @param clusteringKey
	 *            The clustering key, maybe {@code null}.
	 * @param timestamp
	 *            The time stamp to ignore deleted columns.
	 * @return The CQL3 {@link Row} identified by the specified key pair.
	 */
	private Row row(DecoratedKey partitionKey, ByteBuffer clusteringKey, long timestamp) {

		// Setup columns query
		QueryFilter queryFilter;
		if (isWide) {
			ByteBuffer start = clusteringKeyMapper.start(clusteringKey);
			ByteBuffer stop = clusteringKeyMapper.stop(clusteringKey);
			SliceQueryFilter dataFilter = new SliceQueryFilter(start,
			                                                   stop,
			                                                   false,
			                                                   Integer.MAX_VALUE,
			                                                   clusteringPosition);
			queryFilter = new QueryFilter(partitionKey, baseCfs.name, dataFilter, timestamp);
		} else {
			queryFilter = QueryFilter.getIdentityFilter(partitionKey, metadata.cfName, timestamp);
		}

		// Read columns from storage
		ColumnFamily columnFamily = baseCfs.getColumnFamily(queryFilter);

		// Remove deleted column families
		ColumnFamily cleanColumnFamily = TreeMapBackedSortedColumns.factory.create(baseCfs.metadata);
		for (Column column : columnFamily) {
			if (!column.isMarkedForDelete(timestamp)) {
				cleanColumnFamily.addColumn(column);
			}
		}

		// Return new row
		return new Row(partitionKey, cleanColumnFamily);
	}

	/**
	 * Returns the Cassandra rows satisfying {@code extendedFilter}. This rows are retrieved from
	 * the Cassandra storage engine.
	 * 
	 * @param extendedFilter
	 *            The filter to be satisfied.
	 * @return The Cassandra rows satisfying {@code extendedFilter}.
	 */
	public List<Row> search(Search search,
	                        List<IndexExpression> filteredExpressions,
	                        DataRange dataRange,
	                        int limit,
	                        long timestamp) {

		// Setup search arguments
		Filter rangefilter = cachedFilter(dataRange);
		Query query = search.query(schema, rangefilter);
		Sort sort = search.usesRelevance() ? null : sort();

		// Setup search pagination
		List<Row> rows = new LinkedList<>(); // The row list to be returned
		ScoreDoc lastDoc = null; // The last search result

		// Paginate search collecting documents
		List<ScoredDocument> scoredDocuments;
		do {

			// Search in Lucene
			scoredDocuments = rowDirectory.search(lastDoc, query, sort, PAGE_SIZE, fieldsToLoad);

			// Collect rows from Cassandra
			for (ScoredDocument sd : scoredDocuments) {
				lastDoc = sd.scoreDoc;
				Document document = sd.document;
				DecoratedKey partitionKey = partitionKeyMapper.decoratedKey(document);
				ByteBuffer clusteringKey = isWide ? clusteringKeyMapper.byteBuffer(document) : null;
				Row row = row(partitionKey, clusteringKey, timestamp);
				if (row != null && accepted(row, filteredExpressions)) {
					rows.add(row);
				}
				if (rows.size() >= limit) { // Break if we have enough rows
					return rows;
				}
			}
		} while (scoredDocuments.size() == PAGE_SIZE); // Repeat while there may be more rows

		Log.debug("Query time: " + (System.currentTimeMillis() - timestamp) + " ms");
		return rows;
	}

	private boolean accepted(Row row, List<IndexExpression> expressions) {
		if (!expressions.isEmpty()) {
			Cells cells = schema.cells(metadata, row.key, row.cf);
			for (IndexExpression expression : expressions) {
				if (!accepted(cells, expression)) {
					return false;
				}
			}
		}
		return true;
	}

	private boolean accepted(Cells cells, IndexExpression expression) {

		ByteBuffer expectedValue = expression.value;

		ColumnDefinition def = metadata.getColumnDefinition(expression.column_name);
		String name = UTF8Type.instance.compose(def.name);

		Cell cell = cells.getCell(name);
		if (cell == null) {
			return false;
		}

		ByteBuffer actualValue = cell.getRawValue();
		if (actualValue == null) {
			return false;
		}

		AbstractType<?> validator = def.getValidator();
		int comparison = validator.compare(actualValue, expectedValue);
		switch (expression.op) {
			case EQ:
				return comparison == 0;
			case GTE:
				return comparison >= 0;
			case GT:
				return comparison > 0;
			case LTE:
				return comparison <= 0;
			case LT:
				return comparison < 0;
			default:
				throw new IllegalStateException();
		}
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
	 * @param row
	 *            A {@link Row}.
	 * @return A Lucene's {@link Term} to be used as the unique identifier of a row.
	 */
	public Term identifyingTerm(Row row) {
		DecoratedKey partitionKey = row.key;
		if (isWide) {
			ByteBuffer clusteringKey = clusteringKeyMapper.byteBuffer(row.cf);
			return fullKeyMapper.term(partitionKey, clusteringKey);
		} else {
			return partitionKeyMapper.term(partitionKey);
		}
	}

	public ByteBuffer getUniqueId(Document document) {
		DecoratedKey partitionKey = partitionKeyMapper.decoratedKey(document);
		if (isWide) {
			ByteBuffer clusteringKey = clusteringKeyMapper.byteBuffer(document);
			return fullKeyMapper.byteBuffer(partitionKey, clusteringKey);
		} else {
			return partitionKey.key;
		}
	}

}
