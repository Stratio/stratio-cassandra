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

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.InetAddressType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.codehaus.jackson.annotate.JsonCreator;

/**
 * A {@link CellMapper} to map a string, not tokenized field.
 * 
 * @author adelapena
 */
public class CellMapperInetAddress extends CellMapper<String> {

	@JsonCreator
	public CellMapperInetAddress() {
		super(new AbstractType<?>[]{AsciiType.instance,
		                             UTF8Type.instance,
		                             InetAddressType.instance});
	}

	@Override
	public Analyzer analyzer() {
		return EMPTY_ANALYZER;
	}

	@Override
	public String indexValue(Object value) {
		if (value == null) {
			return null;
		} else if (value instanceof InetAddress) {
			InetAddress inetAddress = (InetAddress) value;
			return inetAddress.getHostAddress();
		} else if (value instanceof String) {
			String svalue = (String) value;
			try {
				InetAddress inetAddress = InetAddress.getByName(svalue);
				return inetAddress.getHostAddress();
			} catch (UnknownHostException e) {
				throw new RuntimeException(e);
			}
		} else {
			throw new IllegalArgumentException(String.format("Value '%s' cannot be cast to InetAddress", value));
		}
	}

	@Override
	public String queryValue(Object value) {
		if (value == null) {
			return null;
		} else if (value instanceof String) {
			String svalue = (String) value;
			try {
				InetAddress inetAddress = InetAddress.getByName(svalue);
				System.out.println("IS INET " + inetAddress.getHostAddress());
				return inetAddress.getHostAddress();
			} catch (UnknownHostException e) {
				System.out.println("IS STRING " + svalue);
				return svalue;
			}
		} else {
			throw new IllegalArgumentException(String.format("Value '%s' cannot be cast to InetAddress", value));
		}
	}

	@Override
	public Field field(String name, Object value) {
		String string = indexValue(value);
		System.out.println("INDEXED " + string);
		return new StringField(name, string, STORE);
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
