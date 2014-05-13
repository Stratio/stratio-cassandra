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

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.cassandra.db.index.stratio.util.Log;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.MurmurHash;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.NRTCachingDirectory;
import org.apache.lucene.util.Version;

/**
 * Class wrapping a Lucene's directory and its readers , writers and searchers for NRT.
 * 
 * @author Andres de la Pena <adelapena@stratio.com>
 * 
 */
public class RowDirectory {

	private Analyzer analyzer;

	private final File[] files;
	private final Directory[] shards;
	private final IndexWriter[] writers;

	private final int numShards;

	/**
	 * Builds a new {@code RowDirectory} using the specified directory path and analyzer.
	 * 
	 * @param path
	 *            The analyzer to be used. The path of the directory in where the Lucene's files
	 *            will be stored.
	 * @param refreshSeconds
	 *            The index readers refresh time in seconds. No guarantees that the writings are
	 *            visible until this time.
	 * @param ramBufferMB
	 *            The index writer buffer size in MB.
	 * @param maxMergeMB
	 *            NRTCachingDirectory max merge size in MB.
	 * @param maxCachedMB
	 *            NRTCachingDirectory max cached MB.
	 * @param numShards
	 * @param analyzer
	 */
	public RowDirectory(String path, Double refreshSeconds, Integer ramBufferMB, Integer maxMergeMB,
	        Integer maxCachedMB, Integer numShards, Analyzer analyzer) {
		try {

			// Set analyzer
			this.analyzer = analyzer;
			this.numShards = numShards;

			shards = new Directory[numShards];
			writers = new IndexWriter[numShards];
			files = new File[numShards];
			for (int i = 0; i < numShards; i++) {

				File file = new File(path + File.separatorChar + i);
				files[i] = file;

				FSDirectory fsDirectory = FSDirectory.open(file);
				Directory directory = new NRTCachingDirectory(fsDirectory, maxMergeMB, maxCachedMB);
				shards[i] = directory;

				IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_46, analyzer);
				config.setRAMBufferSizeMB(ramBufferMB);
				config.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
				config.setUseCompoundFile(false);

				writers[i] = new IndexWriter(directory, config);
			}

		} catch (IOException e) {
			Log.error(e, "Error initiating index");
			throw new RuntimeException(e);
		}
	}

	private IndexSearcher searcher() throws IOException {
		IndexReader[] readers = new IndexReader[numShards];
		for (int i = 0; i < numShards; i++) {
			Directory shard = shards[i];
			readers[i] = DirectoryReader.open(shard);
		}
		MultiReader multiReader = new MultiReader(readers);
		return new IndexSearcher(multiReader);
	}

	private long hash(Term term) {
		ByteBuffer bytes = ByteBuffer.wrap(term.bytes().bytes);
		long[] hash = new long[2];
		MurmurHash.hash3_x64_128(bytes, bytes.position(), bytes.remaining(), 0, hash);
		return hash[0];
	}

	private IndexWriter writer(Term term) {
		long hash = hash(term);
		int pos = (int) (Math.abs(hash) % numShards);
		return writers[pos];
	}

	/**
	 * Updates the specified {@link Document} by first deleting the documents containing
	 * {@code Term} and then adding the new document. The delete and then add are atomic as seen by
	 * a reader on the same index (flush may happen only after the add).
	 * 
	 * @param term
	 *            The {@link Term} to identify the document(s) to be deleted.
	 * @param document
	 *            The {@link Document} to be added.
	 */
	public void updateDocument(Term term, Document document) {
		// Log.debug("Updating document %s with term %s", document, term);
		try {
			writer(term).updateDocument(term, document);
		} catch (IOException e) {
			Log.error(e, "Error updating document");
			throw new RuntimeException(e);
		}
	}

	/**
	 * Updates the specified {@link Document}s by first deleting the documents containing
	 * {@code Term} and then adding the new documents. The delete and then adds are atomic as seen
	 * by a reader on the same index (flush may happen only after the add).
	 * 
	 * @param term
	 *            The {@link Term} to identify the document(s) to be deleted.
	 * @param documents
	 *            The {@link Document}s to be added.
	 */
	public void updateDocuments(Term term, Iterable<Document> documents) {
		// Log.debug("Updating documents %s with term %s", documents, term);
		try {
			writer(term).updateDocuments(term, documents);
		} catch (IOException e) {
			Log.error(e, "Error updating documents");
			throw new RuntimeException(e);
		}
	}

	/**
	 * Deletes all the {@link Document}s containing the specified {@link Term}.
	 * 
	 * @param term
	 *            The {@link Term} to identify the documents to be deleted.
	 */
	public void deleteDocuments(Term term) {
		// Log.debug(String.format("Deleting by term %s", term));
		try {
			writer(term).deleteDocuments(term);
		} catch (IOException e) {
			Log.error(e, "Error deleting documents by term");
			throw new RuntimeException(e);
		}
	}

	/**
	 * Deletes all the {@link Document}s satisfying the specified {@link Query}.
	 * 
	 * @param query
	 *            The {@link Query} to identify the documents to be deleted.
	 */
	public void deleteDocuments(Query query) {
		// Log.debug("Deleting by query %s", query);
		try {
			for (IndexWriter writer : writers) {
				writer.deleteDocuments(query);
			}
		} catch (IOException e) {
			Log.error(e, "Error deleting documents by query");
			throw new RuntimeException(e);
		}
	}

	/**
	 * Deletes all the {@link Document}s.
	 */
	public void deleteAll() {
		Log.info("Deleting all");
		try {
			for (IndexWriter writer : writers) {
				writer.deleteAll();
			}
		} catch (IOException e) {
			Log.error(e, "Error deleting all");
			throw new RuntimeException(e);
		}
	}

	/**
	 * Commits the pending changes.
	 */
	public void commit() {
		Log.info("Committing");
		try {
			for (IndexWriter writer : writers) {
				writer.commit();
			}
		} catch (IOException e) {
			Log.error(e, "Error committing");
			throw new RuntimeException(e);
		}
	}

	/**
	 * Commits all changes to an index, waits for pending merges to complete, and closes all
	 * associated resources.
	 */
	public void close() throws IOException {
		Log.info("Closing");
		for (IndexWriter writer : writers) {
			writer.close();
		}
		for (Directory shard : shards) {
			shard.close();
		}
		analyzer.close();
	}

	/**
	 * Closes and removes all the index files.
	 * 
	 * @return
	 */
	public void removeIndex() {
		Log.info("Removing");
		try {
			close();
			for (File file : files) {
				FileUtils.deleteRecursive(file);
			}
		} catch (IOException e) {
			Log.error(e, "Error removing");
			throw new RuntimeException(e);
		}
	}

	// /**
	// * Returns the total size of all index files currently cached in memory.
	// *
	// * @return The total size of all index files currently cached in memory.
	// */
	// public long getRAMSizeInBytes() {
	// return indexWriter == null ? 0 : indexWriter.ramSizeInBytes();
	// }

	/**
	 * Finds the top {@code count} hits for {@code query}, applying {@code filter} if non-null, and
	 * sorting the hits by the criteria in {@code sort}.
	 * 
	 * @param after
	 * @param query
	 *            The {@link Query} to search for.
	 * @param filter
	 *            The {@link Filter} to be applied.
	 * @param sort
	 *            The {@link Sort} to be applied.
	 * @param count
	 *            Return only the top {@code count} results.
	 * @param fieldsToLoad
	 *            The name of the fields to be loaded.
	 * @return The found documents, sorted according to the supplied {@link Sort} instance.
	 */
	public List<ScoredDocument> search(ScoreDoc after, Query query, Sort sort, Integer count, Set<String> fieldsToLoad) {
		// Log.debug("Searching with query %s ", query);
		// Log.debug("Searching with count %d", count);
		// Log.debug("Searching with sort %s", sort);

		// Validate
		if (query == null) {
			throw new IllegalArgumentException("Query required");
		}
		if (count == null || count < 0) {
			throw new IllegalArgumentException("Positive count required");
		}
		if (fieldsToLoad == null || fieldsToLoad.isEmpty()) {
			throw new IllegalArgumentException("Fields to load required");
		}

		try {
			IndexSearcher indexSearcher = searcher();

			// Search
			TopDocs topDocs;
			if (after == null) {
				if (sort == null) {
					topDocs = indexSearcher.search(query, count);
				} else {
					topDocs = indexSearcher.search(query, count, sort);
				}
			} else {
				if (sort == null) {
					topDocs = indexSearcher.searchAfter(after, query, count);
				} else {
					topDocs = indexSearcher.searchAfter(after, query, count, sort);
				}
			}
			ScoreDoc[] scoreDocs = topDocs.scoreDocs;

			// Collect the documents from query result
			List<ScoredDocument> scoredDocuments = new ArrayList<>(scoreDocs.length);
			for (ScoreDoc scoreDoc : scoreDocs) {
				Document document = indexSearcher.doc(scoreDoc.doc, fieldsToLoad);
				ScoredDocument scoredDocument = new ScoredDocument(scoreDoc, document);
				scoredDocuments.add(scoredDocument);
				// Log.debug("Found %s", scoredDocument);
			}
			indexSearcher.getIndexReader().close();

			return scoredDocuments;
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Tuple relating a {@link Document} to a search scoring.
	 * 
	 */
	public static class ScoredDocument {

		public final ScoreDoc scoreDoc;
		public final Document document;

		public ScoredDocument(ScoreDoc scoreDoc, Document document) {
			this.scoreDoc = scoreDoc;
			this.document = document;
		}

		@Override
		public String toString() {
			StringBuilder builder = new StringBuilder();
			builder.append("ScoredDocument [document=");
			builder.append(document);
			builder.append("]");
			return builder.toString();
		}

	}

}
