package org.apache.cassandra.db.index.stratio;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Set;

import org.apache.cassandra.db.Row;
import org.apache.cassandra.db.filter.ExtendedFilter;
import org.apache.cassandra.db.index.SecondaryIndex;
import org.apache.cassandra.db.index.SecondaryIndexManager;
import org.apache.cassandra.db.index.SecondaryIndexSearcher;
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

	private final RowIndex currentIndex;

	/**
	 * Returns a new {@code RowIndexSearcher}.
	 * 
	 * @param indexManager
	 * @param currentIndex
	 * @param columns
	 * @param rowService
	 */
	public RowIndexSearcher(SecondaryIndexManager indexManager,
	                        RowIndex currentIndex,
	                        Set<ByteBuffer> columns,
	                        RowService rowService) {
		super(indexManager, columns);
		this.currentIndex = currentIndex;
		this.rowService = rowService;
	}

	@Override
	public List<Row> search(ExtendedFilter extendedFilter) {
		logger.info("Searching " + extendedFilter);
		try {
			return rowService.search(extendedFilter);
		} catch (IOException e) {
			logger.error("Error while searching ", e);
			throw new RuntimeException(e);
		}
	}

	@Override
	public boolean isIndexing(List<IndexExpression> clause) {
		logger.info("isIndexing(" + clause + ")");
		return highestSelectivityPredicate(clause) != null;
	}

	@Override
	protected IndexExpression highestSelectivityPredicate(List<IndexExpression> clause) {
		logger.info("highestSelectivityPredicate(" + clause + ")");
		for (IndexExpression expression : clause) {
			SecondaryIndex index = indexManager.getIndexForColumn(expression.column_name);
			if (index != null && expression.op.equals(IndexOperator.EQ) && index == currentIndex) {
				return expression;
			} else {
				continue;
			}
		}
		return null;
	}

}