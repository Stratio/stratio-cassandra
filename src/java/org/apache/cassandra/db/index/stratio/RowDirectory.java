package org.apache.cassandra.db.index.stratio;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.cassandra.db.index.stratio.util.Log;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TrackingIndexWriter;
import org.apache.lucene.search.ControlledRealTimeReopenThread;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.NRTCachingDirectory;
import org.apache.lucene.util.Version;

/**
 * Class wrapping a Lucene's directory and its readers , writers and searchers for NRT.
 * 
 * @author adelapena
 * 
 */
public class RowDirectory {

	private File file;
	private Directory directory;
	private Analyzer analyzer;
	private IndexWriter indexWriter;
	private TrackingIndexWriter trackingIndexWriter;
	private SearcherManager searcherManager;
	private ControlledRealTimeReopenThread<IndexSearcher> indexSearcherReopenThread;

	/**
	 * Builds a new {@code RowDirectory} using the specified directory path and analyzer.
	 * 
	 * @param path
	 *            The analyzer to be used. The path of the directory in where the Lucene's files
	 *            will be stored.
	 * @param refreshSeconds
	 *            The index readers refresh time in seconds.
	 * @param ramBufferMB
	 *            The index writer buffer size in MB.
	 * @param maxMergeMB
	 *            NRTCachingDirectory max merge size in MB.
	 * @param maxCachedMB
	 *            NRTCachingDirectory max cached MB.
	 * @param analyzer
	 */
	public RowDirectory(String path,
	                    Integer refreshSeconds,
	                    Integer ramBufferMB,
	                    Integer maxMergeMB,
	                    Integer maxCachedMB,
	                    Analyzer analyzer) {
		try {

			// Get directory file
			file = new File(path);

			// Open or create directory
			FSDirectory fsDirectory = FSDirectory.open(file);
			directory = new NRTCachingDirectory(fsDirectory, maxMergeMB, maxCachedMB);

			// Set analyzer
			this.analyzer = analyzer;

			// Setup index writer
			IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_46, analyzer);
			config.setRAMBufferSizeMB(ramBufferMB);
			config.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
			config.setUseCompoundFile(false);
			indexWriter = new IndexWriter(directory, config);

			// Setup NRT search
			trackingIndexWriter = new TrackingIndexWriter(indexWriter);
			searcherManager = new SearcherManager(indexWriter, true, null);
			indexSearcherReopenThread = new ControlledRealTimeReopenThread<>(trackingIndexWriter,
			                                                                 searcherManager,
			                                                                 refreshSeconds,
			                                                                 refreshSeconds);
			indexSearcherReopenThread.start(); // Start the refresher thread

		} catch (IOException e) {
			Log.error(e, "Error initiating index");
			throw new RuntimeException(e);
		}
	}

	/**
	 * Inserts the specified {@link Document}.
	 * 
	 * @param document
	 *            the {@link Document} to be inserted.
	 */
	public void createDocument(Document document) {
		Log.debug("Inserting document %s", document);
		try {
			indexWriter.addDocument(document);
		} catch (IOException e) {
			Log.error(e, "Error creating document");
			throw new RuntimeException(e);
		}
	}

	/**
	 * Inserts the specified {@link Document}s.
	 * 
	 * @param document
	 *            the {@link Document} to be inserted.
	 */
	public void createDocuments(Iterable<Document> documents) {
		Log.debug("Inserting documents %s", documents);
		try {
			indexWriter.addDocuments(documents);
		} catch (IOException e) {
			Log.error(e, "Error creating documents");
			throw new RuntimeException(e);
		}
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
		Log.debug("Updating document %s with term %s", document, term);
		try {
			indexWriter.updateDocument(term, document);
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
		Log.debug("Updating documents %s with term %s", documents, term);
		try {
			indexWriter.updateDocuments(term, documents);
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
		Log.debug(String.format("Deleting by term %s", term));
		try {
			indexWriter.deleteDocuments(term);
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
		Log.debug("Deleting by query %s", query);
		try {
			indexWriter.deleteDocuments(query);
		} catch (IOException e) {
			Log.error(e, "Error deleting docuemnts by query");
			throw new RuntimeException(e);
		}
	}

	/**
	 * Deletes all the {@link Document}s.
	 */
	public void deleteAll() {
		Log.info("Deleting all");
		try {
			indexWriter.deleteAll();
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
			indexWriter.commit();
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
		indexSearcherReopenThread.interrupt();
		searcherManager.close();
		indexWriter.close();
		directory.close();
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
			FileUtils.deleteRecursive(file);
		} catch (IOException e) {
			Log.error(e, "Error removing");
			throw new RuntimeException(e);
		}
	}

	/**
	 * Returns the total size of all index files currently cached in memory.
	 * 
	 * @return The total size of all index files currently cached in memory.
	 */
	public long getRAMSizeInBytes() {
		return indexWriter == null ? 0 : indexWriter.ramSizeInBytes();
	}

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
	public List<ScoredDocument> search(ScoreDoc after,
	                                   Query query,
	                                   Filter filter,
	                                   Sort sort,
	                                   Integer count,
	                                   Set<String> fieldsToLoad) {
		Log.debug("Searching with query %s ", query);
		Log.debug("Searching with filter %s", filter);
		Log.debug("Searching with count %d", count);
		Log.debug("Searching with sort %s", sort);

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
			IndexSearcher indexSearcher = searcherManager.acquire();
			try {

				// Search
				TopDocs topDocs;
				if (after == null) {
					if (sort == null) {
						if (filter == null) {
							topDocs = indexSearcher.search(query, count);
						} else {
							topDocs = indexSearcher.search(query, filter, count);
						}
					} else {
						if (filter == null) {
							topDocs = indexSearcher.search(query, count, sort);
						} else {
							topDocs = indexSearcher.search(query, filter, count, sort, true, true);
						}
					}
				} else {
					if (sort == null) {
						if (filter == null) {
							topDocs = indexSearcher.searchAfter(after, query, count);
						} else {
							topDocs = indexSearcher.searchAfter(after, query, filter, count);
						}
					} else {
						if (filter == null) {
							topDocs = indexSearcher.searchAfter(after, query, count, sort);
						} else {
							topDocs = indexSearcher.searchAfter(after, query, filter, count, sort, true, true);
						}
					}
				}
				ScoreDoc[] scoreDocs = topDocs.scoreDocs;

				// Collect the documents from query result
				List<ScoredDocument> scoredDocuments = new ArrayList<>(scoreDocs.length);
				for (ScoreDoc scoreDoc : scoreDocs) {
					Document document = indexSearcher.doc(scoreDoc.doc, fieldsToLoad);
					Float score = scoreDoc.score;
					ScoredDocument scoredDocument = new ScoredDocument(scoreDoc, document, score);
					scoredDocuments.add(scoredDocument);
					// Log.debug("Found %s", scoredDocument);
				}

				return scoredDocuments;
			} finally {
				searcherManager.release(indexSearcher);
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Tuple relating a {@link Document} to a search scoring.
	 * 
	 * @author adelapena
	 * 
	 */
	public static class ScoredDocument {

		public final ScoreDoc scoreDoc;
		public final Document document;
		public final Float score;

		public ScoredDocument(ScoreDoc scoreDoc, Document document, Float score) {
			this.scoreDoc = scoreDoc;
			this.document = document;
			this.score = score;
		}

		@Override
		public String toString() {
			StringBuilder builder = new StringBuilder();
			builder.append("ScoredDocument [document=");
			builder.append(document);
			builder.append(", score=");
			builder.append(score);
			builder.append("]");
			return builder.toString();
		}

	}

}
