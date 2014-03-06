package org.apache.cassandra.db.index.stratio;

import java.io.File;
import java.util.Map;

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.ColumnFamilyStore;

/**
 * Class for building {@link RowService} instances.
 * 
 * @author adelapena
 * 
 */
public class RowServiceBuilder {

	private static final String SCHEMA_OPTION = "schema";

	private static final String REFRESH_SECONDS_OPTION = "refresh_seconds";
	private static final Integer DEFAULT_REFESH_SECONDS = 60;

	private static final String FILTER_CACHE_SIZE_OPTION = "filter_cache_size";
	private static final Integer DEFAULT_FILTER_CACHE_SIZE = 0;

	private static final String DIRECTORY_PATH_OPTION = "path";
	private static final String DEFAULT_PATH_PREFIX = "lucene";

	private static final String WRITE_BUFFER_SIZE_OPTION = "write_buffer_size";
	private static final Integer DEFAULT_WRITE_BUFFER_SIZE = 64;

	private static final String STORED_ROWS_OPTION = "stored_rows";
	private static final Boolean DEFAULT_STORED_ROWS = false;

	public RowService build(ColumnFamilyStore baseCfs, ColumnDefinition columnDefinition) {

		assert baseCfs != null;
		assert columnDefinition != null;

		ColumnIdentifier indexedColumnName = new ColumnIdentifier(columnDefinition.name,
		                                                          columnDefinition.getValidator());
		String indexName = columnDefinition.getIndexName();

		// Get index options
		Map<String, String> options = columnDefinition.getIndexOptions();

		// Get refresh time
		String refreshOption = options.get(REFRESH_SECONDS_OPTION);
		int refreshSeconds = refreshOption == null ? DEFAULT_REFESH_SECONDS : Integer.parseInt(refreshOption);

		// Get filter cache size
		String filterCacheSizeOption = options.get(FILTER_CACHE_SIZE_OPTION);
		int filterCacheSize = filterCacheSizeOption == null ? DEFAULT_FILTER_CACHE_SIZE
		                                                   : Integer.parseInt(filterCacheSizeOption);

		// Get filter cache size
		String writeBufferSizeOption = options.get(WRITE_BUFFER_SIZE_OPTION);
		int writeBufferSize = writeBufferSizeOption == null ? DEFAULT_WRITE_BUFFER_SIZE
		                                                   : Integer.parseInt(writeBufferSizeOption);

		// Get stored rows option
		String storedRowsOption = options.get(STORED_ROWS_OPTION);
		boolean storedRows = writeBufferSizeOption == null ? DEFAULT_STORED_ROWS
		                                                  : Boolean.parseBoolean(storedRowsOption);

		// Get columns mapping schema
		String schemaOption = options.get(SCHEMA_OPTION);
		assert schemaOption != null && !schemaOption.isEmpty();
		CellsMapper cellsMapper = CellsMapper.fromJson(schemaOption);

		// Get Lucene's directory path
		StringBuilder directoryPathBuilder = new StringBuilder();
		String directoryOption = options.get(DIRECTORY_PATH_OPTION);
		if (directoryOption == null) {
			String[] dataFileLocations = DatabaseDescriptor.getAllDataFileLocations();
			directoryPathBuilder.append(dataFileLocations[0]);
			directoryPathBuilder.append(File.separatorChar);
			directoryPathBuilder.append(DEFAULT_PATH_PREFIX);
			directoryPathBuilder.append(File.separatorChar);
		} else {
			directoryPathBuilder.append(directoryOption);
			if (!directoryOption.endsWith("" + File.separatorChar)) {
				directoryPathBuilder.append(File.separatorChar);
			}
		}
		directoryPathBuilder.append(indexName);
		String directoryPath = directoryPathBuilder.toString();

		return new RowService(baseCfs,
		                      indexName,
		                      indexedColumnName,
		                      cellsMapper,
		                      refreshSeconds,
		                      writeBufferSize,
		                      directoryPath,
		                      filterCacheSize,
		                      storedRows);
	}

}
