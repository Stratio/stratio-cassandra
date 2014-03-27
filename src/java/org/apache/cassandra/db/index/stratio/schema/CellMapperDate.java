package org.apache.cassandra.db.index.stratio.schema;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongField;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 * A {@link CellMapper} to map a date field.
 * 
 * @author adelapena
 */
public class CellMapperDate extends CellMapper<Long> {

	public static final String DEFAULT_PATTERN = "yyyy/MM/dd HH:mm:ss.SSS";

	/** The date and time pattern. */
	@JsonProperty("pattern")
	private final String pattern;

	/** The thread safe date format */
	@JsonIgnore
	private final ThreadLocal<DateFormat> concurrentDateFormat;

	@JsonCreator
	public CellMapperDate(@JsonProperty("pattern") String pattern) {
		this.pattern = pattern == null ? DEFAULT_PATTERN : pattern;
		concurrentDateFormat = new ThreadLocal<DateFormat>() {
			@Override
			protected DateFormat initialValue() {
				return new SimpleDateFormat(CellMapperDate.this.pattern);
			}
		};
	}

	@Override
	public Analyzer analyzer() {
		return EMPTY_ANALYZER;
	}

	@Override
	public Long indexValue(Object value) {
		if (value == null) {
			return null;
		} else if (value instanceof Date) {
			return ((Date) value).getTime();
		} else if (value instanceof Number) {
			return ((Number) value).longValue();
		} else if (value instanceof String) {
			try {
				return concurrentDateFormat.get().parse(value.toString()).getTime();
			} catch (ParseException e) {
				throw new IllegalArgumentException(e);
			}
		} else {
			throw new IllegalArgumentException();
		}
	}

	@Override
	public Long queryValue(Object value) {
		return indexValue(value);
	}

	@Override
	public Field field(String name, Object value) {
		return new LongField(name, indexValue(value), STORE);
	}

	@Override
	public Class<Long> baseClass() {
		return Long.class;
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append(getClass().getSimpleName());
		builder.append(" [pattern=");
		builder.append(pattern);
		builder.append("]");
		return builder.toString();
	}

}
