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

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.SortField;

/**
 * Class for several row partitioning token mappings between Cassandra and Lucene.
 * 
 * @author adelapena
 * 
 */
public abstract class TokenMapper {

	/** The lazily created singleton instance. */
	private static TokenMapper instance;

	/**
	 * Returns the {@link TokenMapper} instance for the current partitioner.
	 * 
	 * @return The {@link TokenMapper} instance for the current partitioner.
	 */
	public static TokenMapper instance() {
		if (instance == null) {
			IPartitioner<?> partitioner = DatabaseDescriptor.getPartitioner();
			if (partitioner instanceof Murmur3Partitioner) {
				instance = new TokenMapperMurmur();
			} else {
				instance = new TokenMapperGeneric();
			}
		}
		return instance;
	}

	/**
	 * Adds to the specified {@link Document} the {@link Field}s associated to the token of the
	 * specified row key.
	 * 
	 * @param document
	 *            A {@link Document}.
	 * @param partitionKey
	 *            The raw partition key to be converted.
	 */
	public abstract void addFields(Document document, DecoratedKey partitionKey);

	/**
	 * Returns a Lucene's {@link Filter} for filtering documents/rows according to the row token
	 * range specified in {@code dataRange}.
	 * 
	 * @param dataRange
	 *            The data range containing the row token range to be filtered.
	 * @return A Lucene's {@link Filter} for filtering documents/rows according to the row token
	 *         range specified in {@code dataRage}.
	 */
	public abstract Filter filter(DataRange dataRange);

	/**
	 * Returns a Lucene's {@link SortField} array for sorting documents/rows according to the
	 * current partitioner.
	 * 
	 * @return A Lucene's {@link SortField} array for sorting documents/rows according to the
	 *         current partitioner.
	 */
	public abstract SortField[] sortFields();

}
