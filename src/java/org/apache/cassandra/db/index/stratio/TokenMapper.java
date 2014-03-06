package org.apache.cassandra.db.index.stratio;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.lucene.document.Document;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.SortField;

public abstract class TokenMapper {

	private static TokenMapper instance;

	public static TokenMapper instance() {
		if (instance == null) {
//			IPartitioner<?> partitioner = DatabaseDescriptor.getPartitioner();
//			if (partitioner instanceof Murmur3Partitioner) {
//				instance = new TokenMapperMurmur();
//			} else {
				instance = new TokenMapperGeneric();
//			}
		}
		return instance;
	}

	public abstract void addFields(Document document, DecoratedKey partitionKey);

	public abstract Filter[] filters(DataRange dataRange);

	public abstract SortField[] sortFields();

}
