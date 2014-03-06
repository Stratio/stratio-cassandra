package org.apache.cassandra.db.index.stratio;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

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
import org.apache.lucene.util.Version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class wrapping a Lucene's directory and its readers , writers and searchers for NRT.
 * 
 * @author adelapena
 * 
 */
public class RowDirectory {

	protected static final Logger logger = LoggerFactory.getLogger(RowDirectory.class);

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
	 * @param analyzer
	 * @param refreshSeconds
	 *            The index readers refresh time in seconds.
	 * @param writeBufferSize
	 *            The index writer buffer size in MB.
	 */
	public RowDirectory(String path, Analyzer analyzer, Integer refreshSeconds, Integer writeBufferSize) {
		try {

			// Get directory file
			file = new File(path);

			// Open or create directory
			directory = FSDirectory.open(file);

			// Set analyzer
			this.analyzer = analyzer;

			// Setup index writer
			IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_46, analyzer);
			config.setRAMBufferSizeMB(writeBufferSize);
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
			indexSearcherReopenThread.start(); // start the refresher thread

			// DocComparator dc;
			// Sorter sorter;
			// SortingMergePolicy smp = new SortingMergePolicy(config.getMergePolicy(), sorter);

		} catch (IOException e) {
			logger.error("Error initiating index", e);
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
		logger.info("Inserting document " + document);
		try {
			indexWriter.addDocument(document);
		} catch (IOException e) {
			logger.error("Error creating document", e);
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
		// logger.info("Inserting document " + documents);
		try {
			indexWriter.addDocuments(documents);
		} catch (IOException e) {
			logger.error("Error creating documents", e);
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
		// logger.info(String.format("Updating document %s with term %s", document, term));
		try {
			indexWriter.updateDocument(term, document);
		} catch (IOException e) {
			logger.error("Error updating document", e);
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
		// logger.info(String.format("Updating documents %s with term %s", documents, term));
		try {
			indexWriter.updateDocuments(term, documents);
		} catch (IOException e) {
			logger.error("Error updating docuemnts", e);
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
		logger.info(String.format("Deleting by term %s", term));
		try {
			indexWriter.deleteDocuments(term);
		} catch (IOException e) {
			logger.error("Error deleting documents by term", e);
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
		logger.info("Deleting by query " + query);
		try {
			indexWriter.deleteDocuments(query);
		} catch (IOException e) {
			logger.error("Error deleting docuemnts by query", e);
			throw new RuntimeException(e);
		}
	}

	/**
	 * Deletes all the {@link Document}s.
	 */
	public void deleteAll() {
		logger.info("Deleting all");
		try {
			indexWriter.deleteAll();
		} catch (IOException e) {
			logger.error("Error deleting all", e);
			throw new RuntimeException(e);
		}
	}

	/**
	 * Commits the pending changes.
	 */
	public void commit() {
		logger.info("Committing");
		try {
			indexWriter.commit();
		} catch (IOException e) {
			logger.error("Error committing", e);
			throw new RuntimeException(e);
		}
	}

	/**
	 * Commits all changes to an index, waits for pending merges to complete, and closes all
	 * associated resources.
	 */
	public void close() throws IOException {
		logger.info("Closing");
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
		logger.info("Removing");
		try {
			close();
			FileUtils.deleteRecursive(file);
		} catch (IOException e) {
			logger.error("Error removing", e);
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
	public List<ScoredDocument> search(Query query, Filter filter, Sort sort, int count, Set<String> fieldsToLoad) {
		logger.debug(String.format("Searching %s %d %s", query, count, sort));
		try {
			IndexSearcher indexSearcher = searcherManager.acquire();
			try {

				// Search
				TopDocs topDocs = indexSearcher.search(query, filter, count, sort, true, true);
				ScoreDoc[] scoreDocs = topDocs.scoreDocs;

				// Get the document keys from query result
				List<ScoredDocument> scoredDocuments = new ArrayList<>(scoreDocs.length);
				for (ScoreDoc scoreDoc : scoreDocs) {
					Document document = indexSearcher.doc(scoreDoc.doc, fieldsToLoad);
					Float score = scoreDoc.score;
					ScoredDocument scoredDocument = new ScoredDocument(document, score);
					scoredDocuments.add(scoredDocument);
					logger.debug("Found " + scoredDocument);
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

		public final Document document;
		public final Float score;

		public ScoredDocument(Document document, Float score) {
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
