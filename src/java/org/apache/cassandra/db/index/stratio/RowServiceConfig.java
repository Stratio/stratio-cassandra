package org.apache.cassandra.db.index.stratio;

import java.io.File;
import java.util.Map;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.index.stratio.schema.CellsMapper;

/**
 * Class for building {@link RowService} instances.
 * 
 * @author adelapena
 * 
 */
public class RowServiceConfig {

	private static final String SCHEMA_OPTION = "schema";

	private static final String REFRESH_SECONDS_OPTION = "refresh_seconds";
	private static final Integer DEFAULT_REFESH_SECONDS = 60;

	private static final String FILTER_CACHE_SIZE_OPTION = "num_cached_filters";
	private static final Integer DEFAULT_FILTER_CACHE_SIZE = 0;

	private static final String PATH_OPTION = "path";
	private static final String DEFAULT_PATH_PREFIX = "lucene";

	private static final String RAM_BUFFER_MB_OPTION = "ram_buffer_mb";
	private static final Integer DEFAULT_RAM_BUFFER_MB = 64;

	private static final String MAX_MERGE_MB_OPTION = "max_merge_mb";
	private static final Integer DEFAULT_MAX_MERGE_MB = 5;

	private static final String MAX_CACHED_MB_OPTION = "max_cached_mb";
	private static final Integer DEFAULT_MAX_CACHED_MB = 30;

	private final CellsMapper cellsMapper;
	private final int refreshSeconds;
	private final FilterCache filterCache;
	private final String path;
	private final int ramBufferMB;
	private final int maxMergeMB;
	private final int maxCachedMB;

	public RowServiceConfig(CFMetaData metadata, String indexName, Map<String, String> options) {

		// Setup refresh seconds
		String refreshOption = options.get(REFRESH_SECONDS_OPTION);
		if (refreshOption != null) {
			try {
				refreshSeconds = Integer.parseInt(refreshOption);
			} catch (NumberFormatException e) {
				throw new RuntimeException(REFRESH_SECONDS_OPTION + " must be a strictly positive integer");
			}
			if (refreshSeconds <= 0) {
				throw new RuntimeException(REFRESH_SECONDS_OPTION + " must be strictly positive");
			}
		} else {
			refreshSeconds = DEFAULT_REFESH_SECONDS;
		}

		// Setup filter cache size
		String filterCacheSizeOption = options.get(FILTER_CACHE_SIZE_OPTION);
		int filterCacheSize;
		if (filterCacheSizeOption != null) {
			try {
				filterCacheSize = Integer.parseInt(filterCacheSizeOption);
			} catch (NumberFormatException e) {
				throw new RuntimeException(RAM_BUFFER_MB_OPTION + " must be a strictly positive integer");
			}
			if (filterCacheSize <= 0) {
				throw new RuntimeException(FILTER_CACHE_SIZE_OPTION + " must be strictly positive");
			}
		} else {
			filterCacheSize = DEFAULT_FILTER_CACHE_SIZE;
		}
		filterCache = new FilterCache(filterCacheSize);

		// Setup write buffer size
		String ramBufferSizeOption = options.get(RAM_BUFFER_MB_OPTION);
		if (ramBufferSizeOption != null) {
			try {
				ramBufferMB = Integer.parseInt(ramBufferSizeOption);
			} catch (NumberFormatException e) {
				throw new RuntimeException(RAM_BUFFER_MB_OPTION + " must be a strictly positive integer");
			}
			if (ramBufferMB <= 0) {
				throw new RuntimeException(RAM_BUFFER_MB_OPTION + " must be strictly positive");
			}
		} else {
			ramBufferMB = DEFAULT_RAM_BUFFER_MB;
		}

		// Setup max merge size
		String maxMergeSizeMBOption = options.get(MAX_MERGE_MB_OPTION);
		if (maxMergeSizeMBOption != null) {
			try {
				maxMergeMB = Integer.parseInt(maxMergeSizeMBOption);
			} catch (NumberFormatException e) {
				throw new RuntimeException(MAX_MERGE_MB_OPTION + " must be a strictly positive integer");
			}
			if (maxMergeMB <= 0) {
				throw new RuntimeException(MAX_MERGE_MB_OPTION + " must be strictly positive");
			}
		} else {
			maxMergeMB = DEFAULT_MAX_MERGE_MB;
		}

		// Setup max cached MB
		String maxCachedMBOption = options.get(MAX_CACHED_MB_OPTION);
		if (maxCachedMBOption != null) {
			try {
				maxCachedMB = Integer.parseInt(maxCachedMBOption);
			} catch (NumberFormatException e) {
				throw new RuntimeException(DEFAULT_MAX_CACHED_MB + " must be a strictly positive integer");
			}
			if (maxCachedMB <= 0) {
				throw new RuntimeException(DEFAULT_MAX_CACHED_MB + " must be strictly positive");
			}
		} else {
			maxCachedMB = DEFAULT_MAX_CACHED_MB;
		}

		// Get columns mapping schema
		String schemaOption = options.get(SCHEMA_OPTION);
		if (schemaOption != null && !schemaOption.trim().isEmpty()) {
			try {
				cellsMapper = CellsMapper.fromJson(schemaOption);
				cellsMapper.validate(metadata);
			} catch (Exception e) {
				throw new RuntimeException(SCHEMA_OPTION + " is invalid", e);
			}
		} else {
			throw new RuntimeException(SCHEMA_OPTION + " required");
		}

		// Get Lucene's directory path
		StringBuilder directoryPathBuilder = new StringBuilder();
		String directoryOption = options.get(PATH_OPTION);
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
		path = directoryPathBuilder.toString();
	}

	public CellsMapper getCellsMapper() {
		return cellsMapper;
	}

	public int getRefreshSeconds() {
		return refreshSeconds;
	}

	public FilterCache getFilterCache() {
		return filterCache;
	}

	public String getPath() {
		return path;
	}

	public int getRamBufferMB() {
		return ramBufferMB;
	}

	public int getMaxMergeMB() {
		return maxMergeMB;
	}

	public int getMaxCachedMB() {
		return maxCachedMB;
	}

}
