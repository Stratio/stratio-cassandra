package org.apache.cassandra.db.index.stratio.schema;

import java.nio.ByteBuffer;

import org.apache.cassandra.db.index.stratio.util.ByteBufferUtils;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.utils.Hex;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.codehaus.jackson.annotate.JsonCreator;

/**
 * A {@link CellMapper} to map a string, not tokenized field.
 * 
 * @author adelapena
 */
public class CellMapperBlob extends CellMapper<String> {

	@JsonCreator
	public CellMapperBlob() {
		super(new AbstractType<?>[] { AsciiType.instance, UTF8Type.instance, BytesType.instance });
	}

	@Override
	public Analyzer analyzer() {
		return EMPTY_ANALYZER;
	}

	@Override
	public String indexValue(Object value) {
		if (value == null) {
			return null;
		} else if (value instanceof ByteBuffer) {
			ByteBuffer bb = (ByteBuffer) value;
			return ByteBufferUtils.toHex(bb);
		} else if (value instanceof byte[]) {
			byte[] bytes = (byte[]) value;
			return ByteBufferUtils.toHex(bytes);
		} else if (value instanceof String) {
			String string = (String) value;
			byte[] bytes = Hex.hexToBytes(string);
			return Hex.bytesToHex(bytes);
		} else {
			throw new IllegalArgumentException(String.format("Value '%s' cannot be cast to byte array", value));
		}
	}

	@Override
	public String queryValue(Object value) {
		if (value == null) {
			return null;
		} else {
			return value.toString().toLowerCase();
		}
	}

	@Override
	public Field field(String name, Object value) {
		String string = indexValue(value);
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
