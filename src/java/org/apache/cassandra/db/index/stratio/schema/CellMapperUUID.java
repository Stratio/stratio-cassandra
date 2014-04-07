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
package org.apache.cassandra.db.index.stratio.schema;

import java.util.UUID;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.TimeUUIDType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.codehaus.jackson.annotate.JsonCreator;

/**
 * A {@link CellMapper} to map a UUID field.
 * 
 * @author adelapena
 */
public class CellMapperUUID extends CellMapper<String> {

	@JsonCreator
	public CellMapperUUID() {
		super(new AbstractType<?>[] { AsciiType.instance, UTF8Type.instance, UUIDType.instance, TimeUUIDType.instance });
	}

	@Override
	public Analyzer analyzer() {
		return EMPTY_ANALYZER;
	}

	@Override
	public String indexValue(Object value) {
		if (value == null) {
			return null;
		} else if (value instanceof UUID) {
			return value.toString();
		} else if (value instanceof String) {
			return java.util.UUID.fromString((String) value).toString();
		} else {
			throw new IllegalArgumentException();
		}
	}

	@Override
	public String queryValue(Object value) {
		if (value == null) {
			return null;
		} else {
			return value.toString();
		}
	}

	@Override
	public Field field(String name, Object value) {
		String uuid = indexValue(value);
		return new StringField(name, uuid, STORE);
	}

	@Override
	public Class<String> baseClass() {
		return String.class;
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append(getClass().getSimpleName());
		builder.append(" []");
		return builder.toString();
	}

}
