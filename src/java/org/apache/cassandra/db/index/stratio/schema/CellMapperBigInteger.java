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

	private final BigInteger minValue;

	@JsonCreator
	public CellMapperBigInteger(@JsonProperty("padding") Integer padding) {
		super();

		assert padding != null : "Padding required";
		assert padding > 0 : "Padding must be positive";

		this.padding = padding + 1;
		minValue = BigInteger.valueOf(10).pow(padding);
		System.out.println(" PADDING " + this.padding);
		System.out.println(" COMPLEMENT " + this.minValue);
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
			bi = bi.add(minValue);
			String result = bi.toString();
			result = StringUtils.leftPad(result, padding, '0');
			System.out.println("COMPLEMENT -> " + this.minValue);
			System.out.println("RESULT     -> " + result);
			return result;
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
