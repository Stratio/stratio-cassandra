package org.apache.cassandra.db.index.stratio.util;

import java.io.IOException;

import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;

public class JsonSerializer {

	/** The embedded JSON serializer. */
	private static final ObjectMapper jsonMapper = new ObjectMapper();
	static {
		// jsonMapper.configure(JsonGenerator.Feature.QUOTE_FIELD_NAMES, false);
		jsonMapper.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
		// jsonMapper.configure(SerializationConfig.Feature.INDENT_OUTPUT, true);
		jsonMapper.configure(SerializationConfig.Feature.AUTO_DETECT_IS_GETTERS, false);
		// jsonMapper.configure(SerializationConfig.Feature.WRITE_DATES_AS_TIMESTAMPS, true);
		// jsonMapper.configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);
	}

	/**
	 * Hidden constructor.
	 */
	private JsonSerializer() {
	}

	public static String toString(Object value) throws IOException {
		return jsonMapper.writeValueAsString(value);
	}

	public static byte[] toBytes(Object value) throws IOException {
		return jsonMapper.writeValueAsBytes(value);
	}

	public static <T> T fromString(String content, Class<T> valueType) throws IOException {
		return jsonMapper.readValue(content, valueType);
	}

	public static <T> T fromBytes(byte[] content, Class<T> valueType) throws IOException {
		return jsonMapper.readValue(content, valueType);
	}
}
