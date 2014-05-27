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
package org.apache.cassandra.db.index.stratio.query;

import java.util.Iterator;
import java.util.List;

import org.apache.cassandra.db.index.stratio.schema.Schema;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 * A sorting for a search.
 * 
 * @author Andres de la Pena <adelapena@stratio.com>
 * 
 */
public class Sorting implements Iterable<SortingField> {

	private final List<SortingField> sortingFields;

	@JsonCreator
	public Sorting(@JsonProperty("fields") List<SortingField> sortingFields) {
		this.sortingFields = sortingFields;
	}

	@Override
	public Iterator<SortingField> iterator() {
		return sortingFields.iterator();
	}

	public List<SortingField> getSortingFields() {
		return sortingFields;
	}

	public Sort sort(Schema schema) {
		SortField[] sortFields = new SortField[sortingFields.size()];
		for (int i = 0; i < sortingFields.size(); i++) {
			sortFields[i] = sortingFields.get(i).sortField(schema);
		}
		return new Sort(sortFields);
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("Sorting [sortingFields=");
		builder.append(sortingFields);
		builder.append("]");
		return builder.toString();
	}

}
