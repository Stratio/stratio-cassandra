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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
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
import org.apache.cassandra.db.index.stratio.query.Search;
import org.apache.cassandra.db.index.stratio.schema.Cell;
import org.apache.cassandra.db.index.stratio.schema.Cells;
import org.apache.cassandra.db.index.stratio.schema.CellsMapper;
import org.apache.cassandra.db.index.stratio.util.Log;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.thrift.IndexExpression;
import org.apache.cassandra.utils.HeapAllocator;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.ChainedFilter;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.FilteredQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.Version;

/**
 * Class for mapping rows between Cassandra and Lucene.
 * 
 * @author adelapena
 * 
 */
public class RowService {

	private static final int MAX_PAGE_SIZE = 100;

	private final ColumnFamilyStore baseCfs;
	private final CFMetaData metadata;
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
		columnIdentifier = new ColumnIdentifier(columnDefinition.name, columnDefinition.getValidator());

		RowServiceConfig config = new RowServiceConfig(metadata,
		                                               columnDefinition.getIndexName(),
		                                               columnDefinition.getIndexOptions());

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

		fieldsToLoad = new HashSet<>();
		fieldsToLoad.add(PartitionKeyMapper.FIELD_NAME);
		if (isWide) {
			fieldsToLoad.add(ClusteringKeyMapper.FIELD_NAME);
			fieldsToLoad.add(FullKeyMapper.FIELD_NAME);
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
	 * @param timestamp
	 *            The operation time stamp.
	 */
	public final void index(ByteBuffer key, ColumnFamily columnFamily, long timestamp) {

		DeletionInfo deletionInfo = columnFamily.deletionInfo();
		DecoratedKey partitionKey = partitionKeyMapper.decoratedKey(key);

		if (columnFamily.iterator().hasNext()) {

			if (isWide) {
				ByteBuffer clusteringKey = clusteringKeyMapper.byteBuffer(columnFamily);
				QueryFilter queryFilter = queryFilter(partitionKey, clusteringKey, timestamp);
				ColumnFamily allColumns = baseCfs.getColumnFamily(queryFilter);
				Document document = document(partitionKey, allColumns, timestamp);
				Term term = term(partitionKey, clusteringKey);
				rowDirectory.updateDocument(term, document);
			} else {
				QueryFilter queryFilter = queryFilter(partitionKey, null, timestamp);
				ColumnFamily allColumns = baseCfs.getColumnFamily(queryFilter);
				Document document = document(partitionKey, allColumns, timestamp);
				Term term = term(partitionKey, null);
				rowDirectory.updateDocument(term, document);
			}

		} else if (deletionInfo != null) {
			Iterator<RangeTombstone> iterator = deletionInfo.rangeIterator();
			if (!iterator.hasNext()) { // Delete full storage row
				Term term = partitionKeyMapper.term(partitionKey);
				rowDirectory.deleteDocuments(term);
			} else { // Just for delete ranges of wide rows
				while (iterator.hasNext()) {
					RangeTombstone rangeTombstone = iterator.next();
					Filter filter = clusteringKeyMapper.filter(rangeTombstone);
					Query partitionKeyQuery = partitionKeyMapper.query(partitionKey);
					Query query = new FilteredQuery(partitionKeyQuery, filter);
					rowDirectory.deleteDocuments(query);
				}
			}
		}
	}

	public Document document(Row row, long timestamp) {
		DecoratedKey partitionKey = row.key;
		ColumnFamily columnFamily = row.cf;
		return document(partitionKey, columnFamily, timestamp);
	}

	public Document document(DecoratedKey partitionKey, ColumnFamily columnFamily, long timestamp) {
		Document document = new Document();
		if (isWide) {
			ByteBuffer clusteringKey = clusteringKeyMapper.byteBuffer(columnFamily);
			partitionKeyMapper.addFields(document, partitionKey);
			tokenMapper.addFields(document, partitionKey);
			cellsMapper.addFields(document, metadata, partitionKey, columnFamily, timestamp);
			clusteringKeyMapper.addFields(document, clusteringKey);
			fullKeyMapper.addFields(document, partitionKey, clusteringKey);
		} else {
			partitionKeyMapper.addFields(document, partitionKey);
			tokenMapper.addFields(document, partitionKey);
			cellsMapper.addFields(document, metadata, partitionKey, columnFamily, timestamp);
		}
		return document;
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

	private QueryFilter queryFilter(DecoratedKey partitionKey, ByteBuffer clusteringKey, long timestamp) {
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

	public boolean usesRelevance(List<IndexExpression> clause) {
		Search search = search(clause);
		return search == null ? false : search.relevance();
	}

	public Search search(List<IndexExpression> clause) {
		IndexExpression indexExpression = null;
		for (IndexExpression ie : clause) {
			ByteBuffer columnName = ie.column_name;
			if (columnName.equals(columnIdentifier.key)) {
				indexExpression = ie;
			}
		}
		if (indexExpression == null) {
			return null;
		}
		ByteBuffer columnValue = indexExpression.value;
		String querySentence = UTF8Type.instance.compose(columnValue);
		try {
			Search search = Search.fromJSON(querySentence);
			search.analyze(cellsMapper.analyzer());
			return search;
		} catch (Exception e) {
			Log.error(e, e.getMessage());
			return null;
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

		long timestamp = extendedFilter.timestamp;

		// Get filtering options
		int requestedRows = extendedFilter.maxColumns();
		IndexExpression indexExpression = null;
		List<IndexExpression> extraExpressions = new ArrayList<>();
		for (IndexExpression ie : extendedFilter.getClause()) {
			ByteBuffer columnName = ie.column_name;
			if (columnName.equals(columnIdentifier.key)) {
				indexExpression = ie;
			} else {
				extraExpressions.add(ie);
			}
		}
		ByteBuffer columnValue = indexExpression.value;

		String querySentence = UTF8Type.instance.compose(columnValue);
		DataRange dataRange = extendedFilter.dataRange;

		// Setup search arguments
		Filter filter = cachedFilter(dataRange);
		Sort sort;
		Query query;
		try { // Try with JSON syntax
			Search search = Search.fromJSON(querySentence);
			search.analyze(cellsMapper.analyzer());
			query = search.query(cellsMapper);
			sort = search.relevance() ? null : sort();
		} catch (IOException e) { // Try with Lucene syntax
			QueryParser queryParser = new RowQueryParser(Version.LUCENE_46, "lucene", cellsMapper);
			queryParser.setAllowLeadingWildcard(true);
			queryParser.setLowercaseExpandedTerms(false);
			query = queryParser.parse(querySentence);
			sort = sort();
		}

		// Setup search pagination
		List<Row> rows = new LinkedList<>(); // The row list to be returned
		int pageSize = Math.min(MAX_PAGE_SIZE, requestedRows); // The page size
		ScoreDoc lastDoc = null; // The last search result

		// Paginate search collecting documents
		List<ScoredDocument> scoredDocuments;
		do {

			// Search in Lucene
			scoredDocuments = rowDirectory.search(lastDoc, query, filter, sort, pageSize, fieldsToLoad);

			// Collect rows from Cassandra
			for (ScoredDocument sd : scoredDocuments) {
				lastDoc = sd.scoreDoc;
				Row row = row(sd.document, sd.score, extraExpressions, timestamp); // Collect
				if (row != null) { // May be null if not satisfies the filter expressions
					rows.add(row);
				}
				if (rows.size() >= requestedRows) { // Break if we have enough rows
					return rows;
				}
			}
		} while (scoredDocuments.size() == pageSize); // Repeat while there may be more rows

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
	private Row row(Document document, Float score, List<IndexExpression> expressions, long timestamp) {

		// Get the decorated partition key
		DecoratedKey decoratedKey = partitionKeyMapper.decoratedKey(document);

		// Get the column family from Cassandra or Lucene
		DecoratedKey partitionKey = partitionKeyMapper.decoratedKey(document);
		ByteBuffer clusteringKey = isWide ? clusteringKeyMapper.byteBuffer(document) : null;
		QueryFilter queryFilter = queryFilter(partitionKey, clusteringKey, timestamp);
		ColumnFamily cf = baseCfs.getColumnFamily(queryFilter);

		// Check filter
		if (!accepted(partitionKey, cf, expressions, timestamp)) {
			return null;
		}

		// Create the score column
		ByteBuffer name = scoreColumnName(clusteringKey);
		ByteBuffer value = UTF8Type.instance.decompose(score.toString());
		Column column = new Column(name, value);

		// Create a new column family with the scoring cell and the stored ones
		ColumnFamily decoratedCf = TreeMapBackedSortedColumns.factory.create(baseCfs.metadata);
		decoratedCf.addColumn(column);
		decoratedCf.addAll(cf, HeapAllocator.instance);

		return new Row(decoratedKey, decoratedCf);
	}

	public ByteBuffer scoreColumnName(ByteBuffer clusteringKey) {
		return isWide ? clusteringKeyMapper.name(columnIdentifier, clusteringKey)
		             : partitionKeyMapper.name(columnIdentifier);
	}

	public Float score(Row row) {
		ColumnFamily columnFamily = row.cf;
		ByteBuffer columnName;
		if (isWide) {
			ByteBuffer clusteringKey = clusteringKeyMapper.byteBuffer(columnFamily);
			columnName = clusteringKeyMapper.name(columnIdentifier, clusteringKey);
		} else {
			columnName = partitionKeyMapper.name(columnIdentifier);
		}
		Column column = columnFamily.getColumn(columnName);
		ByteBuffer columnValue = column.value();
		String string = UTF8Type.instance.compose(columnValue);
		Float score = Float.valueOf(string);
		return score;
	}

	private boolean accepted(DecoratedKey key, ColumnFamily cf, List<IndexExpression> expressions, long timestamp) {
		if (!expressions.isEmpty()) {
			Cells cells = cellsMapper.cells(metadata, key, cf, timestamp);
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

	public List<Row> sort(List<Row> rows, List<IndexExpression> clause, int limit, long timestamp) throws IOException {

		System.out.println(" ===> STARTING SORT ");
		Search search = search(clause);
		System.out.println(" ===> SEARCHIG " + search);
		Query query = search.query(cellsMapper);
		System.out.println(" ===> QUERYING " + query);

		Analyzer analyzer = cellsMapper.analyzer();
		Directory directory = new RAMDirectory();
		IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_46, analyzer);
		config.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
		config.setUseCompoundFile(false);
		IndexWriter indexWriter = new IndexWriter(directory, config);
		String docIdFieldName = isWide ? FullKeyMapper.FIELD_NAME : PartitionKeyMapper.FIELD_NAME;

		Map<String, Row> map = new HashMap<>(rows.size());
		for (Row row : rows) {
			Document document = document(row, timestamp);
			String docId = document.get(docIdFieldName);
			System.out.println("\tADDED " + docId + " - " + document + " - " + row);
			indexWriter.addDocument(document);
			map.put(docId, row);
		}
		indexWriter.commit();
		indexWriter.close();
		System.out.println(" ===> COMMITED ");

		IndexReader indexReader = DirectoryReader.open(directory);
		IndexSearcher indexSearcher = new IndexSearcher(indexReader);
		TopDocs topdocs = indexSearcher.search(query, limit);
		List<Row> result = new ArrayList<>(rows.size());
		for (ScoreDoc scoreDoc : topdocs.scoreDocs) {
			Document document = indexSearcher.doc(scoreDoc.doc, fieldsToLoad);
			String docId = document.get(docIdFieldName);
			Row row = map.get(docId);
			System.out.println("\tFOUND " + docId + " - " + document + " - " + row);
			result.add(row);
		}
		indexReader.close();

		System.out.println(" ===> RETURNING " + result);

		return result;
	}

}
