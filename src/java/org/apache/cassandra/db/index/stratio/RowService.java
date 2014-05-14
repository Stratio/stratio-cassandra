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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.Column;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Row;
import org.apache.cassandra.db.TreeMapBackedSortedColumns;
import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.db.index.stratio.LuceneIndex.ScoredDocument;
import org.apache.cassandra.db.index.stratio.query.Condition;
import org.apache.cassandra.db.index.stratio.query.Search;
import org.apache.cassandra.db.index.stratio.schema.Cell;
import org.apache.cassandra.db.index.stratio.schema.Cells;
import org.apache.cassandra.db.index.stratio.schema.Schema;
import org.apache.cassandra.db.index.stratio.util.Log;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.thrift.IndexExpression;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.Version;

/**
 * Class for mapping rows between Cassandra and Lucene.
 * 
 * @author Andres de la Pena <adelapena@stratio.com>
 * 
 */
public abstract class RowService {

	private static final int PAGE_SIZE = 100;

	protected final ColumnFamilyStore baseCfs;
	protected final CFMetaData metadata;
	protected final Schema schema;
	protected final LuceneIndex rowDirectory;
	protected final FilterCache filterCache;

	/**
	 * Returns a new {@code RowService}.
	 * 
	 * @param baseCfs
	 *            The base column family store.
	 * @param columnDefinition
	 *            The indexed column definition.
	 */
	protected RowService(ColumnFamilyStore baseCfs, ColumnDefinition columnDefinition) {

		this.baseCfs = baseCfs;
		metadata = baseCfs.metadata;

		RowIndexConfig config = new RowIndexConfig(metadata,
		                                           columnDefinition.getIndexName(),
		                                           columnDefinition.getIndexOptions());

		filterCache = config.getFilterCache();

		schema = config.getSchema();

		rowDirectory = new LuceneIndex(config.getPath(),
		                               config.getRefreshSeconds(),
		                               config.getRamBufferMB(),
		                               config.getMaxMergeMB(),
		                               config.getMaxCachedMB(),
		                               schema.analyzer());
	}

	public static RowService build(ColumnFamilyStore baseCfs, ColumnDefinition columnDefinition) {
		int clusteringPosition = baseCfs.metadata.clusteringKeyColumns().size();
		if (clusteringPosition > 0) {
			return new RowServiceWide(baseCfs, columnDefinition);
		} else {
			return new RowServiceSimple(baseCfs, columnDefinition);
		}
	}

	/**
	 * Returns the used {@link Schema}.
	 * 
	 * @return The used {@link Schema}.
	 */
	protected final Schema getSchema() {
		return schema;
	}

	/**
	 * Returns the names of the document fields to be loaded when reading a Lucene's index.
	 * 
	 * @return The names of the document fields to be loaded.
	 */
	protected abstract Set<String> fieldsToLoad();

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
	protected abstract void index(ByteBuffer key, ColumnFamily columnFamily, long timestamp);

	/**
	 * Returns the {@link Document} represented by the specified {@link Row}. It's assumed that the
	 * {@link Row} is a CQL3 one, so its {@link ColumnFamily} musts contain one and only one
	 * clustering key.
	 * 
	 * @param row
	 *            A {@link Row}.
	 * @return The Lucene {@link Document} representing the specified {@link Row}.
	 */
	protected abstract Document document(Row row);

	/**
	 * Deletes the partition identified by the specified partition key.
	 * 
	 * @param partitionKey
	 *            The partition key identifying the partition to be deleted.
	 */
	protected abstract void delete(DecoratedKey partitionKey);

	/**
	 * Deletes all the {@link Document}s.
	 */
	public final void truncate() {
		rowDirectory.truncate();
	}

	/**
	 * Closes and removes all the index files.
	 * 
	 * @return
	 */
	public final void delete() {
		rowDirectory.drop();
	}

	/**
	 * Commits the pending changes.
	 */
	public final void commit() {
		rowDirectory.commit();
	}

	/**
	 * Returns the Cassandra rows satisfying {@code extendedFilter}. This rows are retrieved from
	 * the Cassandra storage engine.
	 * 
	 * @param extendedFilter
	 *            The filter to be satisfied.
	 * @return The Cassandra rows satisfying {@code extendedFilter}.
	 */
	public final List<Row> search(Search search,
	                              List<IndexExpression> filteredExpressions,
	                              DataRange dataRange,
	                              int limit,
	                              long timestamp) {

		// Setup search arguments
		Filter filter = cachedFilter(dataRange);
		Query query = search.query(schema);
		Sort sort = search.usesRelevance() ? null : sort();

		// Setup search pagination
		List<Row> rows = new LinkedList<>(); // The row list to be returned
		ScoreDoc lastDoc = null; // The last search result

		// Paginate search collecting documents
		List<ScoredDocument> scoredDocuments;
		do {

			// Search in Lucene
			scoredDocuments = rowDirectory.search(lastDoc, query, filter, sort, PAGE_SIZE, fieldsToLoad());

			// Collect rows from Cassandra
			for (ScoredDocument sd : scoredDocuments) {
				lastDoc = sd.scoreDoc;
				Document document = sd.document;
				Row row = row(document, timestamp);
				if (row != null && accepted(row, filteredExpressions)) {
					rows.add(row);
				}
				if (rows.size() >= limit) { // Break if we have enough rows
					Log.debug("Query time: " + (System.currentTimeMillis() - timestamp) + " ms");
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
	 * Returns the {@link Row} identified by the specified {@link Document}, using the specified
	 * time stamp to ignore deleted columns. The {@link Row} is retrieved from the storage engine,
	 * so it involves IO operations.
	 * 
	 * @param document
	 *            A {@link Document}
	 * @param timestamp
	 *            The time stamp to ignore deleted columns.
	 * @return The {@link Row} identified by the specified {@link Document}
	 */
	protected abstract Row row(Document document, long timestamp);

	/**
	 * Returns the CQL3 {@link Row} identified by the specified {@link QueryFilter}, using the
	 * specified time stamp to ignore deleted columns. The {@link Row} is retrieved from the storage
	 * engine, so it involves IO operations.
	 * 
	 * @param queryFilter
	 *            A query filter
	 * @param timestamp
	 *            The time stamp to ignore deleted columns.
	 * @return The CQL3 {@link Row} identified by the specified {@link QueryFilter}
	 */
	protected final Row row(QueryFilter queryFilter, long timestamp) {

		// Read the column family from the storage engine
		ColumnFamily columnFamily = baseCfs.getColumnFamily(queryFilter);

		// Remove deleted column families
		ColumnFamily cleanColumnFamily = TreeMapBackedSortedColumns.factory.create(baseCfs.metadata);
		for (Column column : columnFamily) {
			if (!column.isMarkedForDelete(timestamp)) {
				cleanColumnFamily.addColumn(column);
			}
		}

		// Build and return the row
		DecoratedKey partitionKey = queryFilter.key;
		return new Row(partitionKey, cleanColumnFamily);
	}

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
	protected abstract Filter filter(DataRange dataRange);

	/**
	 * Returns a Lucene's {@link Filter} representing the specified Cassandra's {@link DataRange}
	 * using caching.
	 * 
	 * @param dataRange
	 *            The Cassandra's {@link DataRange} to be mapped.
	 * @return A Lucene's {@link Filter} representing the specified Cassandra's {@link DataRange}.
	 */
	protected final Filter cachedFilter(DataRange dataRange) {
		if (filterCache == null) {
			return filter(dataRange);
		}
		Filter filter = filterCache.get(dataRange);
		if (filter == null) {
			Log.debug(" -> Cache fails for range " + dataRange.keyRange());
			filter = filter(dataRange);
			filterCache.put(dataRange, filter);
		} else {
			Log.debug(" -> Cache hits for range " + dataRange.keyRange());
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
	protected abstract Term identifyingTerm(Row row);

	protected abstract ByteBuffer getUniqueId(Document document);

	/**
	 * Return the {@code limit} {@link Row}s of those in the specified {@link Row}s selected
	 * according to the specified {@link Search}.
	 * 
	 * @param search
	 *            A {@link Search}.
	 * @param rows
	 *            A list of {@link Row}s.
	 * @param count
	 *            The number of {@link Row}s to be returned.
	 * @return The {@code limit} {@link Row}s of those in the specified {@link Row}s selected
	 *         according to the specified {@link Search}.
	 */
	public List<Row> combine(Search search, List<Row> rows, int count) {

		// Skip trivia
		if (rows.isEmpty()) {
			return rows;
		}

		// If it is not a relevance search, simply trunk results
		if (!search.usesRelevance()) {
			return rows.size() > count ? rows.subList(0, count) : rows;
		}

		// Get limit and initialize result
		int limit = Math.min(count, rows.size());
		List<Row> result = new ArrayList<>(limit);

		// We only use the query condition because it is the only
		// restriction affecting the relevance score.
		Condition queryCondition = search.queryCondition();
		Query query = queryCondition.query(schema);

		try {

			// Setup RAM directory for index and query again partial results.
			Directory directory = new RAMDirectory();

			// Index partial results
			Analyzer analyzer = schema.analyzer();
			IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_46, analyzer);
			config.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
			config.setUseCompoundFile(false);
			IndexWriter indexWriter = new IndexWriter(directory, config);
			Map<ByteBuffer, Row> map = new HashMap<>(rows.size());
			for (Row row : rows) {
				Document document = document(row);
				Term term = identifyingTerm(row);
				indexWriter.updateDocument(term, document);
				ByteBuffer docId = getUniqueId(document);
				map.put(docId, row);
			}
			indexWriter.commit();
			indexWriter.close();

			// Search in partial results.
			IndexReader indexReader = DirectoryReader.open(directory);
			IndexSearcher indexSearcher = new IndexSearcher(indexReader);
			TopDocs topdocs = indexSearcher.search(query, limit);
			for (ScoreDoc scoreDoc : topdocs.scoreDocs) {
				Document document = indexSearcher.doc(scoreDoc.doc, fieldsToLoad());
				ByteBuffer docId = getUniqueId(document);
				Row row = map.get(docId);
				result.add(row);
			}
			indexReader.close();

			// Close RAM directory
			directory.close();

		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		return result;
	}
}
