package org.apache.cassandra.db.index.stratio;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Set;

import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.db.filter.SliceQueryFilter;
import org.apache.cassandra.db.index.stratio.util.ColumnFamilySerializer;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;

/**
 * A {@link RowService} for indexing rows with clustering key (wide rows).
 * 
 * @author adelapena
 * 
 */
public class RowServiceWide extends RowService {

	/** The document fields to load when reading; just the partition and the clustering key. */
	public static Set<String> FIELDS_TO_LOAD;
	static {
		FIELDS_TO_LOAD = new HashSet<>();
		FIELDS_TO_LOAD.add(PartitionKeyMapper.FIELD_NAME);
		FIELDS_TO_LOAD.add(ClusteringKeyMapper.FIELD_NAME);
	}

	/** The clustering key mapper to be used. */
	private final ClusteringKeyMapper clusteringKeyMapper;

	/**
	 * Builds a new {@code WideRowMapper} for the specified column family store and index options.
	 * 
	 * @param baseCfs
	 *            The base column family store.
	 * @param indexName
	 *            The index name.
	 * @param indexedColumnName
	 *            The indexed column name.
	 * @param cellsMapper
	 *            The user column mapping schema.
	 * @param refreshSeconds
	 *            The index readers refresh time in seconds.
	 * @param writeBufferSize
	 *            The index writer buffer size in MB.
	 * @param directoryPath
	 *            The path of the index files directory.
	 * @param filterCacheSize
	 *            The number of data range filters to be cached.
	 * @param storedRows
	 *            If the rows must be stored in a Lucene field.
	 */
	public RowServiceWide(ColumnFamilyStore baseCfs,
	                      String indexName,
	                      ColumnIdentifier indexedColumnName,
	                      CellsMapper cellsMapper,
	                      int refreshSeconds,
	                      int writeBufferSize,
	                      String directoryPath,
	                      int filterCacheSize,
	                      boolean storedRows) {
		super(baseCfs,
		      indexName,
		      indexedColumnName,
		      cellsMapper,
		      refreshSeconds,
		      writeBufferSize,
		      directoryPath,
		      filterCacheSize,
		      storedRows,
		      FIELDS_TO_LOAD);
		this.clusteringKeyMapper = new ClusteringKeyMapper(metadata);
	}

	@Override
	public Document document(ByteBuffer partitionKey, ColumnFamily columnFamily) {
		Document document = new Document();
		partitionKeyMapper.document(document, partitionKey);
		clusteringKeyMapper.field(document, columnFamily);
		cellsMapper.fields(document, metadata, partitionKey, columnFamily);
		if (storedRows) {
			document.add(new Field(SERIALIZED_ROW_NAME, ColumnFamilySerializer.bytes(columnFamily), SERIALIZED_ROW_TYPE));
		}
		return document;
	}

	@Override
	public Sort sort() {
		SortField[] partitionKeySort = partitionKeyMapper.sortFields();
		SortField[] clusteringKeySort = clusteringKeyMapper.sortFields();
		return new Sort(ArrayUtils.addAll(partitionKeySort, clusteringKeySort));
	}

	@Override
	public Filter[] filters(DataRange dataRange) {
		Filter[] partitionKeyFilters = partitionKeyMapper.filters(dataRange);
		Filter[] clusteringKeyFilters = clusteringKeyMapper.filters(dataRange);
		return ArrayUtils.addAll(partitionKeyFilters, clusteringKeyFilters);
	}

	@Override
	protected QueryFilter queryFilter(Document document, long timestamp) {
		DecoratedKey decoratedKey = partitionKeyMapper.decoratedKey(document);
		ByteBuffer clusteringKey = clusteringKeyMapper.byteBuffer(document);
		ByteBuffer start = clusteringKeyMapper.start(clusteringKey);
		ByteBuffer stop = clusteringKeyMapper.stop(clusteringKey);
		SliceQueryFilter dataFilter = new SliceQueryFilter(start,
		                                                   stop,
		                                                   false,
		                                                   Integer.MAX_VALUE,
		                                                   baseCfs.metadata.clusteringKeyColumns().size());
		return new QueryFilter(decoratedKey, baseCfs.name, dataFilter, timestamp);
	}

	@Override
	protected org.apache.cassandra.db.Column scoreCell(Document document, Float score) {
		ByteBuffer clusteringKey = clusteringKeyMapper.byteBuffer(document);
		ByteBuffer columnName = clusteringKeyMapper.columnName(clusteringKey, indexedColumnName);
		ByteBuffer columnValue = UTF8Type.instance.decompose(score.toString());
		return new org.apache.cassandra.db.Column(columnName, columnValue);
	}

}
