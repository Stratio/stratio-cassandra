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

import java.util.Arrays;

import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.RowPosition;
import org.apache.cassandra.db.filter.ColumnSlice;
import org.apache.cassandra.db.filter.SliceQueryFilter;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.lucene.search.CachingWrapperFilter;
import org.apache.lucene.search.Filter;

import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;

/**
 * Cache for Lucene's {@link Filter}s associated to Cassandra's {@link DataRange}s.
 * 
 * @author adelapena
 * 
 */
public class FilterCache {

	private final ConcurrentLinkedHashMap<FilterKey, CachingWrapperFilter> map;

	public FilterCache(int capacity) {
		assert capacity > 0;
		map = new ConcurrentLinkedHashMap.Builder<FilterKey, CachingWrapperFilter>().maximumWeightedCapacity(capacity)
		                                                                            .build();
	}

	/**
	 * Associates the specified Lucene's {@link Filter}s with the specified Cassandra's
	 * {@link DataRange} in this cache. If the map previously contained a mapping for the key, the
	 * old value is replaced by the specified value. This operation may evict older entries.
	 * 
	 * @param dataRange
	 *            DataRange with which the specified value is to be associated.
	 * @param filter
	 *            Filter to be associated with the specified dataRange.
	 */
	public void put(DataRange dataRange, Filter filter) {
		FilterKey filterKey = new FilterKey(dataRange);
		CachingWrapperFilter cachingFilter = new CachingWrapperFilter(filter);
		map.put(filterKey, cachingFilter);
	}

	public CachingWrapperFilter get(DataRange dataRange) {
		FilterKey filterKey = new FilterKey(dataRange);
		return map.get(filterKey);
	}

	public int size() {
		return map.size();
	}

	public void clear() {
		map.clear();
	}

	public static class FilterKey {

		private final RowPosition left;
		private final RowPosition right;
		private final ColumnSlice[] slices;

		public FilterKey(DataRange dataRange) {
			left = dataRange.startKey();
			right = dataRange.stopKey();
			slices = ((SliceQueryFilter) dataRange.columnFilter(ByteBufferUtil.EMPTY_BYTE_BUFFER)).slices;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((left == null) ? 0 : left.hashCode());
			result = prime * result + ((right == null) ? 0 : right.hashCode());
			result = prime * result + Arrays.hashCode(slices);
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			FilterKey other = (FilterKey) obj;

			if (left == null) {
				if (other.left != null) {
					return false;
				}
			} else if (!left.equals(other.left)) {
				return false;
			}

			if (right == null) {
				if (other.right != null) {
					return false;
				}
			} else if (!right.equals(other.right)) {
				return false;
			}

			if (!Arrays.equals(slices, other.slices)) {
				return false;
			}
			return true;
		}

	}

}
