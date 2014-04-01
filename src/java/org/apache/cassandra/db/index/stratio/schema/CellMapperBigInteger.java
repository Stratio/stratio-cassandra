package org.apache.cassandra.db.index.stratio.schema;

import java.math.BigInteger;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.UTF8Type;
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

	public static final int DEFAULT_DIGITS = 32;

	private final int digits;
	private final BigInteger complement;
	private final int hexDigits;

	@JsonCreator
	public CellMapperBigInteger(@JsonProperty("digits") Integer digits) {
		super(new AbstractType<?>[]{AsciiType.instance,
		                             UTF8Type.instance,
		                             Int32Type.instance,
		                             LongType.instance,
		                             IntegerType.instance});

		if (digits != null && digits <= 0) {
			throw new IllegalArgumentException("Positive digits required");
		}

		this.digits = digits == null ? DEFAULT_DIGITS : digits;
		complement = BigInteger.valueOf(10).pow(this.digits).subtract(BigInteger.valueOf(1));
		BigInteger maxValue = complement.multiply(BigInteger.valueOf(2));
		hexDigits = encode(maxValue).length();
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

			if (bi.abs().toString().length() > digits) {
				throw new IllegalArgumentException("Value has more than " + digits + " digits");
			}

			bi = bi.add(complement);
			String bis = encode(bi);
			return StringUtils.leftPad(bis, hexDigits + 1, '0');
		}
	}

	private static String encode(BigInteger bi) {
		return bi.toString(Character.MAX_RADIX);
	}

	public int getDigits() {
		return digits;
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
