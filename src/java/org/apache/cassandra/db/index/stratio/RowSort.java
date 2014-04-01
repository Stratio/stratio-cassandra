package org.apache.cassandra.db.index.stratio;

import org.apache.lucene.search.Sort;
import org.codehaus.jackson.annotate.JsonSubTypes;
import org.codehaus.jackson.annotate.JsonTypeInfo;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({ @JsonSubTypes.Type(value = RowSortByToken.class, name = "token"),
               @JsonSubTypes.Type(value = RowSortByRelevance.class, name = "relevance") })
public abstract class RowSort {

	public abstract Sort sort(TokenMapper tokenMapper, ClusteringKeyMapper clusteringKeyMapper);
}
