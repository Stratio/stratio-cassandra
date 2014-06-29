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
package com.stratio.cassandra.index;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.Column;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionInfo;
import org.apache.cassandra.db.RangeTombstone;
import org.apache.cassandra.db.Row;
import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.db.filter.SliceQueryFilter;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.ChainedFilter;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.FilteredQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;

/**
 * {@link RowService} that manages wide rows.
 * 
 * @author Andres de la Pena <adelapena@stratio.com>
 * 
 */
public class RowServiceWide extends RowService {

	private static final Set<String> FIELDS_TO_LOAD;
	static {
		FIELDS_TO_LOAD = new HashSet<>();
		FIELDS_TO_LOAD.add(PartitionKeyMapper.FIELD_NAME);
		FIELDS_TO_LOAD.add(ClusteringKeyMapper.FIELD_NAME);
	}

	private final int clusteringPosition;

	private final TokenMapper tokenMapper;
	private final PartitionKeyMapper partitionKeyMapper;
	private final ClusteringKeyMapper clusteringKeyMapper;
	private final FullKeyMapper fullKeyMapper;

	/**
	 * Returns a new {@code RowServiceWide} for manage wide rows.
	 * 
	 * @param baseCfs
	 *            The base column family store.
	 * @param columnDefinition
	 *            The indexed column definition.
	 */
	public RowServiceWide(ColumnFamilyStore baseCfs, ColumnDefinition columnDefinition) {
		super(baseCfs, columnDefinition);

		partitionKeyMapper = PartitionKeyMapper.instance(metadata);
		tokenMapper = TokenMapper.instance(baseCfs);
		clusteringKeyMapper = ClusteringKeyMapper.instance(metadata);
		fullKeyMapper = FullKeyMapper.instance(metadata);
		clusteringPosition = metadata.clusteringKeyColumns().size();

		luceneIndex.init(sort());
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Set<String> fieldsToLoad() {
		return FIELDS_TO_LOAD;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void indexInner(ByteBuffer key, ColumnFamily columnFamily, long timestamp) {

		DeletionInfo deletionInfo = columnFamily.deletionInfo();
		DecoratedKey partitionKey = partitionKeyMapper.decoratedKey(key);

		if (columnFamily.iterator().hasNext()) {
			for (ByteBuffer clusteringKey : clusteringKeyMapper.byteBuffers(columnFamily)) {
				Row row = row(partitionKey, clusteringKey, timestamp);
				Document document = document(row);
				Term term = identifyingTerm(row);
				luceneIndex.upsert(term, document);
			}
		} else if (deletionInfo != null) {
			Iterator<RangeTombstone> iterator = deletionInfo.rangeIterator();
			if (iterator.hasNext()) {
				while (iterator.hasNext()) {
					RangeTombstone rangeTombstone = iterator.next();
					Filter filter = clusteringKeyMapper.filter(rangeTombstone);
					Query partitionKeyQuery = partitionKeyMapper.query(partitionKey);
					Query query = new FilteredQuery(partitionKeyQuery, filter);
					luceneIndex.delete(query);
				}
			} else {
				Term term = partitionKeyMapper.term(partitionKey);
				luceneIndex.delete(term);
			}
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Document document(Row row) {

		DecoratedKey partitionKey = row.key;
		ColumnFamily columnFamily = row.cf;
		ByteBuffer clusteringKey = clusteringKeyMapper.byteBuffer(columnFamily);

		Document document = new Document();

		tokenMapper.addFields(document, partitionKey);
		partitionKeyMapper.addFields(document, partitionKey);
		schema.addFields(document, metadata, row);
		clusteringKeyMapper.addFields(document, clusteringKey);
		fullKeyMapper.addFields(document, partitionKey, clusteringKey);

		return document;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void deleteInner(DecoratedKey partitionKey) {
		Term term = partitionKeyMapper.term(partitionKey);
		luceneIndex.delete(term);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected Row row(Document document, long timestamp) {
		DecoratedKey partitionKey = partitionKeyMapper.decoratedKey(document);
		ByteBuffer clusteringKey = clusteringKeyMapper.byteBuffer(document);
		return row(partitionKey, clusteringKey, timestamp);
	}

	/**
	 * Returns the CQL3 {@link Row} identified by the specified key pair, using the specified time
	 * stamp to ignore deleted columns. The {@link Row} is retrieved from the storage engine, so it
	 * involves IO operations.
	 * 
	 * @param partitionKey
	 *            The partition key.
	 * @param clusteringKey
	 *            The clustering key, maybe {@code null}.
	 * @param timestamp
	 *            The time stamp to ignore deleted columns.
	 * @return The CQL3 {@link Row} identified by the specified key pair.
	 */
	private Row row(DecoratedKey partitionKey, ByteBuffer clusteringKey, long timestamp) {
		ByteBuffer start = clusteringKeyMapper.start(clusteringKey);
		ByteBuffer stop = clusteringKeyMapper.stop(clusteringKey);
		SliceQueryFilter dataFilter = new SliceQueryFilter(start, stop, false, Integer.MAX_VALUE, clusteringPosition);
		QueryFilter queryFilter = new QueryFilter(partitionKey, baseCfs.name, dataFilter, timestamp);
		return row(queryFilter, timestamp);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected Sort sort() {
		SortField[] partitionKeySort = tokenMapper.sortFields();
		SortField[] clusteringKeySort = clusteringKeyMapper.sortFields();
		return new Sort(ArrayUtils.addAll(partitionKeySort, clusteringKeySort));
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected Filter filter(DataRange dataRange) {
		Filter tokenFilter = tokenMapper.filter(dataRange);
		Filter clusteringKeyFilter = clusteringKeyMapper.filter(dataRange);
		if (tokenFilter == null && clusteringKeyFilter == null) {
			return null;
		} else if (tokenFilter != null && clusteringKeyFilter == null) {
			return tokenFilter;
		} else if (tokenFilter == null && clusteringKeyFilter != null) {
			return clusteringKeyFilter;
		} else {
			Filter[] filters = new Filter[] { tokenFilter, clusteringKeyFilter };
			return new ChainedFilter(filters, ChainedFilter.AND);
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Term identifyingTerm(Row row) {
		DecoratedKey partitionKey = row.key;
		ByteBuffer clusteringKey = clusteringKeyMapper.byteBuffer(row.cf);
		return fullKeyMapper.term(partitionKey, clusteringKey);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public ByteBuffer getUniqueId(Document document) {
		DecoratedKey partitionKey = partitionKeyMapper.decoratedKey(document);
		ByteBuffer clusteringKey = clusteringKeyMapper.byteBuffer(document);
		return fullKeyMapper.byteBuffer(partitionKey, clusteringKey);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public ByteBuffer getUniqueId(Row row) {
		DecoratedKey partitionKey = row.key;
		ByteBuffer clusteringKey = clusteringKeyMapper.byteBuffer(row.cf);
		return fullKeyMapper.byteBuffer(partitionKey, clusteringKey);
	}

	@Override
	protected Column scoreCell(Document document, Float score) {
		ByteBuffer clusteringKey = clusteringKeyMapper.byteBuffer(document);
		ByteBuffer columnName = clusteringKeyMapper.name(clusteringKey, indexedColumnName);
		ByteBuffer columnValue = UTF8Type.instance.decompose(score.toString());
		return new Column(columnName, columnValue);
	}

	@Override
	protected Float score(Row row) {
		ColumnFamily cf = row.cf;
		ByteBuffer clusteringKey = clusteringKeyMapper.byteBuffer(cf);
		ByteBuffer columnName = clusteringKeyMapper.name(clusteringKey, indexedColumnName);
		Column column = cf.getColumn(columnName);
		ByteBuffer columnValue = column.value();
		return Float.parseFloat(UTF8Type.instance.compose(columnValue));
	}

}
