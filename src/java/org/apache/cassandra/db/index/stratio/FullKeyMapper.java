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

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.index.stratio.util.ByteBufferUtils;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.Term;

/**
 * 
 * Class for several row full key mappings between Cassandra and Lucene. The full key includes both
 * the partitioning and the clustering keys.
 * 
 * @author Andres de la Pena <adelapen@stratio.com>
 * 
 */
public class FullKeyMapper {

	/** The Lucene's field name. */
	public static final String FIELD_NAME = "_full_key";

	/** The partition key type. */
	public AbstractType<?> partitionKeyType;

	/** The clustering key type. */
	public AbstractType<?> clusteringKeyType;

	/** The type of the full row key, which is composed by the partition and clustering key types. */
	public CompositeType type;

	/**
	 * Returns a new {@link FullKeyMapper} using the specified column family metadata.
	 * 
	 * @param metadata
	 *            The column family metadata to be used.
	 */
	private FullKeyMapper(CFMetaData metadata) {
		this.partitionKeyType = metadata.getKeyValidator();
		this.clusteringKeyType = metadata.comparator;
		type = CompositeType.getInstance(partitionKeyType, clusteringKeyType);
	}

	/**
	 * Returns a new {@link FullKeyMapper} using the specified column family metadata.
	 * 
	 * @param metadata
	 *            The column family metadata to be used.
	 * @return A new {@link FullKeyMapper} using the specified column family metadata.
	 */
	public static FullKeyMapper instance(CFMetaData metadata) {
		return metadata.clusteringKeyColumns().size() > 0 ? new FullKeyMapper(metadata) : null;
	}

	/**
	 * Returns the partition key type.
	 * 
	 * @return The partition key type.
	 */
	public AbstractType<?> getPartitionKeyType() {
		return partitionKeyType;
	}

	/**
	 * Returns the clustering key type.
	 * 
	 * @return The clustering key type.
	 */
	public AbstractType<?> getClusteringKeyType() {
		return clusteringKeyType;
	}

	/**
	 * Returns the type of the full row key, which is a {@link CompositeType} composed by the
	 * partition key and the clustering key.
	 * 
	 * @return The type of the full row key
	 */
	public CompositeType getType() {
		return type;
	}

	/**
	 * Returns the {@link ByteBuffer} representation of the full row key formed by the specified
	 * partition key and the clustering key.
	 * 
	 * @param partitionKey
	 *            A partition key.
	 * @param clusteringKey
	 *            A clustering key.
	 * @return The {@link ByteBuffer} representation of the full row key formed by the specified key
	 *         pair.
	 */
	public ByteBuffer byteBuffer(DecoratedKey partitionKey, ByteBuffer clusteringKey) {
		return type.builder().add(partitionKey.key).add(clusteringKey).build();
	}

	/**
	 * Adds to the specified Lucene's {@link Document} the full row key formed by the specified
	 * partition key and the clustering key.
	 * 
	 * @param document
	 *            A Lucene's {@link Document}.
	 * @param partitionKey
	 *            A partition key.
	 * @param clusteringKey
	 *            A clustering key.
	 */
	public void addFields(Document document, DecoratedKey partitionKey, ByteBuffer clusteringKey) {
		ByteBuffer fullKey = byteBuffer(partitionKey, clusteringKey);
		Field field = new StringField(FIELD_NAME, ByteBufferUtils.toString(fullKey), Store.NO);
		document.add(field);
	}

	/**
	 * Returns the Lucene's {@link Term} representing the full row key formed by the specified
	 * partition key and the clustering key.
	 * 
	 * @param partitionKey
	 *            A partition key.
	 * @param clusteringKey
	 *            A clustering key.
	 * @return The Lucene's {@link Term} representing the full row key formed by the specified key
	 *         pair.
	 */
	public Term term(DecoratedKey partitionKey, ByteBuffer clusteringKey) {
		ByteBuffer fullKey = type.builder().add(partitionKey.key).add(clusteringKey).build();
		return new Term(FIELD_NAME, ByteBufferUtils.toString(fullKey));
	}

}
