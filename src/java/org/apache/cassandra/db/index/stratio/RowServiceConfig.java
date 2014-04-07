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
				String msg = String.format("'%s' must be a strictly positive integer", REFRESH_SECONDS_OPTION);
				throw new RuntimeException(msg);
			}
			if (refreshSeconds <= 0) {
				String msg = String.format("'%s' must be strictly positive", REFRESH_SECONDS_OPTION);
				throw new RuntimeException(msg);
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
				String msg = String.format("'%s' must be a strictly positive integer", RAM_BUFFER_MB_OPTION);
				throw new RuntimeException(msg);
			}
		} else {
			filterCacheSize = DatabaseDescriptor.getNumTokens() + 1;
		}
		filterCache = filterCacheSize <= 0 ? null : new FilterCache(filterCacheSize);

		// Setup write buffer size
		String ramBufferSizeOption = options.get(RAM_BUFFER_MB_OPTION);
		if (ramBufferSizeOption != null) {
			try {
				ramBufferMB = Integer.parseInt(ramBufferSizeOption);
			} catch (NumberFormatException e) {
				String msg = String.format("'%s' must be a strictly positive integer", RAM_BUFFER_MB_OPTION);
				throw new RuntimeException(msg);
			}
			if (ramBufferMB <= 0) {
				String msg = String.format("'%s' must be strictly positive", RAM_BUFFER_MB_OPTION);
				throw new RuntimeException(msg);
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
				String msg = String.format("'%s' must be a strictly positive integer", MAX_MERGE_MB_OPTION);
				throw new RuntimeException(msg);
			}
			if (maxMergeMB <= 0) {
				String msg = String.format("'%s' must be strictly positive", MAX_MERGE_MB_OPTION);
				throw new RuntimeException(msg);
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
				String msg = String.format("'%s'  must be a strictly positive integer", DEFAULT_MAX_CACHED_MB);
				throw new RuntimeException(msg);
			}
			if (maxCachedMB <= 0) {
				String msg = String.format("'%s'  must be strictly positive", DEFAULT_MAX_CACHED_MB);
				throw new RuntimeException(msg);
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
				String msg = String.format("'%s' is invalid : %s", SCHEMA_OPTION, e.getMessage());
				throw new RuntimeException(msg);
			}
		} else {
			String msg = String.format("'%s' required", SCHEMA_OPTION);
			throw new RuntimeException(msg);
		}

		// Get Lucene's directory path
		String directoryOption = options.get(PATH_OPTION);
		if (directoryOption == null) {
			String[] dataFileLocations = DatabaseDescriptor.getAllDataFileLocations();
			StringBuilder directoryPathBuilder = new StringBuilder();
			directoryPathBuilder.append(dataFileLocations[0]);
			directoryPathBuilder.append(File.separatorChar);
			directoryPathBuilder.append(DEFAULT_PATH_PREFIX);
			directoryPathBuilder.append(File.separatorChar);
			directoryPathBuilder.append(metadata.ksName);
			directoryPathBuilder.append(File.separatorChar);
			directoryPathBuilder.append(metadata.cfName);
			directoryPathBuilder.append(File.separatorChar);
			directoryPathBuilder.append(indexName);
			path = directoryPathBuilder.toString();
		} else {
			path = directoryOption;
		}
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
