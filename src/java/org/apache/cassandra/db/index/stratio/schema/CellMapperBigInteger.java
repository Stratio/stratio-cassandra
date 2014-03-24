package org.apache.cassandra.db.index.stratio.schema;

import java.math.BigInteger;

import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 * A {@link CellMapper} to map a string, not tokenized field.
 * 
 * @author adelapena
 */
public class CellMapperBigInteger extends CellMapper<String> {

	@JsonProperty("padding")
	private final int padding;

	private final BigInteger complement;

	@JsonCreator
	public CellMapperBigInteger(@JsonProperty("padding") Integer padding) {
		super();

		assert padding != null : "Padding required";
		assert padding > 0 : "Padding must be positive";

		this.padding = padding + 1;
		complement = BigInteger.valueOf(10).pow(padding);
	}

	@Override
	public Analyzer analyzer() {
		return EMPTY_ANALYZER;
	}

	@Override
	public String indexValue(Object value) {
		if (value == null) {
			return null;
		} else {
			BigInteger bi = new BigInteger(value.toString());
			bi = bi.add(complement);
			return StringUtils.leftPad(bi.toString(), padding, '0');
		}
	}

	@Override
	public String queryValue(Object value) {
		return indexValue(value);
	}

	@Override
	public Field field(String name, Object value) {
		String string = indexValue(value);
		return new StringField(name, string, STORE);
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
