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
import org.apache.cassandra.db.index.stratio.util.ColumnFamilySerializer;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.Sort;

/**
 * A {@link RowService} for indexing rows without clustering key.
 * 
 * @author adelapena
 * 
 */
public class RowServiceSimple extends RowService {

	/** The document fields to load when reading; just the partition key. */
	public static Set<String> FIELDS_TO_LOAD;
	static {
		FIELDS_TO_LOAD = new HashSet<>();
		FIELDS_TO_LOAD.add(PartitionKeyMapper.FIELD_NAME);
	}

	/**
	 * Builds a new {@code SimpleRowMapper} for the specified column family store and index options.
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
	public RowServiceSimple(ColumnFamilyStore baseCfs,
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
	}

	@Override
	protected Document document(ByteBuffer key, ColumnFamily columnFamily) {
		Document document = new Document();
		partitionKeyMapper.document(document, key);
		cellsMapper.fields(document, metadata, key, columnFamily);
		if (storedRows) {
			document.add(new Field(SERIALIZED_ROW_NAME, ColumnFamilySerializer.bytes(columnFamily), SERIALIZED_ROW_TYPE));
		}
		return document;
	}

	@Override
	public Sort sort() {
		return new Sort(partitionKeyMapper.sortFields());
	}

	@Override
	public Filter[] filters(DataRange dataRange) {
		return partitionKeyMapper.filters(dataRange);
	}

	@Override
	protected QueryFilter queryFilter(Document document, long timestamp) {
		DecoratedKey decoratedKey = partitionKeyMapper.decoratedKey(document);
		return QueryFilter.getIdentityFilter(decoratedKey, cfName, timestamp);
	}

	@Override
	protected org.apache.cassandra.db.Column scoreCell(Document document, Float score) {
		ByteBuffer columnName = nameType.builder().add(indexedColumnName.key).build();
		ByteBuffer columnValue = UTF8Type.instance.decompose(score.toString());
		return new org.apache.cassandra.db.Column(columnName, columnValue);
	}

}
