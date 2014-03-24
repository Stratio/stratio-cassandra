package org.apache.cassandra.db.index.stratio.schema;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.codehaus.jackson.annotate.JsonCreator;

/**
 * A {@link CellMapper} to map a boolean field.
 * 
 * @author adelapena
 */
public class CellMapperBoolean extends CellMapper<String> {

	private static final String TRUE = "true";
	private static final String FALSE = "false";

	@JsonCreator
	public CellMapperBoolean() {
		super();
	}

	@Override
	public Analyzer analyzer() {
		return EMPTY_ANALYZER;
	}

	@Override
	public String indexValue(Object value) {
		if (value == null) {
			return null;
		} else if (value instanceof Boolean) {
			return (Boolean) value ? TRUE : FALSE;
		} else if (value instanceof String) {
			String s = (String) value;
			if (s.equalsIgnoreCase(TRUE)) {
				return TRUE;
			} else if (s.equalsIgnoreCase(FALSE)) {
				return FALSE;
			}
		}
		throw new IllegalArgumentException();
	}

	@Override
	public String queryValue(Object value) {
		if (value == null) {
			return null;
		} else if (value instanceof Boolean) {
			return (Boolean) value ? TRUE : FALSE;
		} else {
			return value.toString();
		}
	}

	@Override
	public Field field(String name, Object value) {
		return new StringField(name, indexValue(value), STORE);
	}

	@Override
	public Class<String> getBaseClass() {
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
