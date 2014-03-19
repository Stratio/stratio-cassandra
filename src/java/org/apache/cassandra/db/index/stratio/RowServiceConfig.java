package org.apache.cassandra.db.index.stratio;

import java.io.File;
import java.util.Map;

import org.apache.cassandra.config.ColumnDefinition;
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

	public RowServiceConfig(ColumnDefinition columnDefinition) {

		assert columnDefinition != null;

		// Get index options
		Map<String, String> options = columnDefinition.getIndexOptions();

		// Get refresh time
		String refreshOption = options.get(REFRESH_SECONDS_OPTION);
		refreshSeconds = refreshOption == null ? DEFAULT_REFESH_SECONDS : Integer.parseInt(refreshOption);

		// Get filter cache size
		String filterCacheSizeOption = options.get(FILTER_CACHE_SIZE_OPTION);
		int filterCacheSize = filterCacheSizeOption == null ? DEFAULT_FILTER_CACHE_SIZE
		                                                   : Integer.parseInt(filterCacheSizeOption);
		filterCache = filterCacheSize > 0 ? new FilterCache(filterCacheSize) : null;

		// Get write buffer size
		String ramBufferSizeOption = options.get(RAM_BUFFER_MB_OPTION);
		ramBufferMB = ramBufferSizeOption == null ? DEFAULT_RAM_BUFFER_MB : Integer.parseInt(ramBufferSizeOption);

		String maxMergeSizeMBOption = options.get(MAX_MERGE_MB_OPTION);
		maxMergeMB = maxMergeSizeMBOption == null ? DEFAULT_MAX_MERGE_MB : Integer.parseInt(maxMergeSizeMBOption);

		String maxCachedMBOption = options.get(MAX_CACHED_MB_OPTION);
		maxCachedMB = maxCachedMBOption == null ? DEFAULT_MAX_CACHED_MB : Integer.parseInt(maxCachedMBOption);

		// Get columns mapping schema
		String schemaOption = options.get(SCHEMA_OPTION);
		assert schemaOption != null && !schemaOption.isEmpty();
		cellsMapper = CellsMapper.fromJson(schemaOption);

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
		directoryPathBuilder.append(columnDefinition.getIndexName());
		path = directoryPathBuilder.toString();
	}

	public static void validate(ColumnDefinition columnDefinition) {
		new RowServiceConfig(columnDefinition);
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
