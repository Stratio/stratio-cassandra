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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.cassandra.db.AbstractRangeCommand;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.Row;
import org.apache.cassandra.db.filter.ExtendedFilter;
import org.apache.cassandra.db.index.SecondaryIndexManager;
import org.apache.cassandra.db.index.SecondaryIndexSearcher;
import org.apache.cassandra.db.index.stratio.query.Search;
import org.apache.cassandra.db.index.stratio.schema.Schema;
import org.apache.cassandra.db.index.stratio.util.Log;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.thrift.IndexExpression;
import org.apache.cassandra.thrift.IndexOperator;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.Version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link SecondaryIndexSearcher} for {@link RowIndex}.
 * 
 * @author Andres de la Pena <adelapena@stratio.com>
 * 
 */
public class RowIndexSearcher extends SecondaryIndexSearcher {

	protected static final Logger logger = LoggerFactory.getLogger(SecondaryIndexSearcher.class);

	private final RowIndex index;
	private final RowIndexService rowIndexService;
	private final Schema schema;
	private final ByteBuffer indexedColumnName;

	/**
	 * Returns a new {@code RowIndexSearcher}.
	 * 
	 * @param indexManager
	 * @param index
	 * @param columns
	 * @param rowIndexService
	 */
	public RowIndexSearcher(SecondaryIndexManager indexManager,
	                        RowIndex index,
	                        Set<ByteBuffer> columns,
	                        RowIndexService rowIndexService) {
		super(indexManager, columns);
		this.index = index;
		this.rowIndexService = rowIndexService;
		schema = rowIndexService.getSchema();
		indexedColumnName = index.getColumnDefinition().name;
	}

	@Override
	public List<Row> search(ExtendedFilter extendedFilter) {
		Log.debug("Searching %s", extendedFilter);
		try {
			long timestamp = extendedFilter.timestamp;
			int limit = extendedFilter.maxColumns();
			DataRange dataRange = extendedFilter.dataRange;
			List<IndexExpression> clause = extendedFilter.getClause();
			List<IndexExpression> filteredExpressions = filteredExpressions(clause);
			Search search = search(clause);
			return rowIndexService.search(search, filteredExpressions, dataRange, limit, timestamp);
		} catch (Exception e) {
			Log.error(e, "Error while searching: %s", e.getMessage());
			return new ArrayList<>(0);
		}
	}

	@Override
	public boolean isIndexing(List<IndexExpression> clause) {
		for (IndexExpression expression : clause) {
			ByteBuffer columnName = expression.column_name;
			boolean sameName = indexedColumnName.equals(columnName);
			if (expression.op.equals(IndexOperator.EQ) && sameName) {
				return true;
			}
		}
		return false;
	}

	@Override
	public void validate(List<IndexExpression> clause) {
		Search search = search(clause);
		search.validate(schema);
	}

	@Override
	public boolean requiresFullScan(AbstractRangeCommand command) {
		Search search = search(command.rowFilter);
		return search.usesRelevance();
	}

	@Override
	public List<Row> combine(AbstractRangeCommand command, List<Row> rows) {
		try {

			if (rows.isEmpty()) {
				return rows;
			}

			Search search = search(command.rowFilter);
			if (!search.usesRelevance()) {
				return super.combine(command, rows);
			}

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
				Document document = rowIndexService.document(row);
				Term term = rowIndexService.identifyingTerm(row);
				indexWriter.updateDocument(term, document);
				ByteBuffer docId = rowIndexService.getUniqueId(document);
				map.put(docId, row);
			}
			indexWriter.commit();
			indexWriter.close();

			// Search in partial results. We only use the query condition because it is the only
			// restriction affecting the relevance score.
			IndexReader indexReader = DirectoryReader.open(directory);
			IndexSearcher indexSearcher = new IndexSearcher(indexReader);
			Query query = search.query(schema, null);
			int limit = Math.min(command.limit(), rows.size());
			Log.debug(" -> COMBINING %d ROWS WITH LIMIT %d", rows.size(), limit);
			TopDocs topdocs = indexSearcher.search(query, limit);
			List<Row> result = new ArrayList<>(limit);
			for (ScoreDoc scoreDoc : topdocs.scoreDocs) {
				Document document = indexSearcher.doc(scoreDoc.doc, rowIndexService.fieldsToLoad());
				ByteBuffer docId = rowIndexService.getUniqueId(document);
				Row row = map.get(docId);
				result.add(row);
			}
			indexReader.close();

			directory.close();

			return result;

		} catch (Exception e) {
			String message = String.format("Error while combining partial results: %s", e.getMessage());
			Log.error(e, message);
			throw new RuntimeException(message, e);
		}
	}

	private Search search(List<IndexExpression> clause) {
		IndexExpression indexedExpression = indexedExpression(clause);
		String json = UTF8Type.instance.compose(indexedExpression.value);
		return Search.fromJson(json);
	}

	private IndexExpression indexedExpression(List<IndexExpression> clause) {
		for (IndexExpression indexExpression : clause) {
			ByteBuffer columnName = indexExpression.column_name;
			if (indexedColumnName.equals(columnName)) {
				return indexExpression;
			}
		}
		return null;
	}

	private List<IndexExpression> filteredExpressions(List<IndexExpression> clause) {
		List<IndexExpression> filteredExpressions = new ArrayList<>(clause.size());
		for (IndexExpression ie : clause) {
			ByteBuffer columnName = ie.column_name;
			if (!indexedColumnName.equals(columnName)) {
				filteredExpressions.add(ie);
			}
		}
		return filteredExpressions;
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("RowIndexSearcher [index=");
		builder.append(index.getIndexName());
		builder.append(", keyspace=");
		builder.append(index.getKeyspaceName());
		builder.append(", table=");
		builder.append(index.getTableName());
		builder.append(", column=");
		builder.append(index.getColumnName());
		builder.append("]");
		return builder.toString();
	}

}