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

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Set;

import org.apache.cassandra.db.Row;
import org.apache.cassandra.db.filter.ExtendedFilter;
import org.apache.cassandra.db.index.SecondaryIndexManager;
import org.apache.cassandra.db.index.SecondaryIndexSearcher;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.thrift.IndexExpression;
import org.apache.cassandra.thrift.IndexOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link SecondaryIndexSearcher} for {@link RowIndex}.
 * 
 * @author adelapena
 * 
 */
public class RowIndexSearcher extends SecondaryIndexSearcher {

	protected static final Logger logger = LoggerFactory.getLogger(SecondaryIndexSearcher.class);

	private final RowService rowService;

	private final String keyspaceName;
	private final String tableName;
	private final String indexName;
	private final String columnName;

	/**
	 * Returns a new {@code RowIndexSearcher}.
	 * 
	 * @param indexManager
	 * @param index
	 * @param columns
	 * @param rowService
	 */
	public RowIndexSearcher(SecondaryIndexManager indexManager,
	                        RowIndex index,
	                        Set<ByteBuffer> columns,
	                        RowService rowService) {
		super(indexManager, columns);
		this.rowService = rowService;
		this.indexName = index.getIndexName();
		this.keyspaceName = index.getIndexName();
		this.tableName = index.getTableName();
		this.columnName = index.getColumnName();
	}

	@Override
	public List<Row> search(ExtendedFilter extendedFilter) {
		logger.info("Searching " + extendedFilter);
		try {
			return rowService.search(extendedFilter);
		} catch (Exception e) {
			logger.error("Error while searching: '%s'", e.getMessage(), e);
			return null; // Force upper component NPE to allow fail by RPC timeout
		}
	}

	@Override
	public boolean isIndexing(List<IndexExpression> clause) {
		for (IndexExpression expression : clause) {
			String columnName = UTF8Type.instance.compose(expression.column_name);
			boolean sameName = columnName.equals(this.columnName);
			if (expression.op.equals(IndexOperator.EQ) && sameName) {
				return true;
			}
		}
		return false;
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("RowIndexSearcher [index=");
		builder.append(indexName);
		builder.append(", keyspace=");
		builder.append(keyspaceName);
		builder.append(", table=");
		builder.append(tableName);
		builder.append(", column=");
		builder.append(columnName);
		builder.append("]");
		return builder.toString();
	}

}