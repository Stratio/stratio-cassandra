package org.apache.cassandra.db.index.stratio;

import org.apache.lucene.search.Sort;

public class RowSortByToken extends RowSort {

	public Sort sort(TokenMapper tokenMapper, ClusteringKeyMapper clusteringKeyMapper) {
		return null;
	}
}
