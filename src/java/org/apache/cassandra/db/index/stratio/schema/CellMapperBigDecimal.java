package org.apache.cassandra.db.index.stratio.schema;

import java.math.BigDecimal;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.DecimalType;
import org.apache.cassandra.db.marshal.DoubleType;
import org.apache.cassandra.db.marshal.FloatType;
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
public class CellMapperBigDecimal extends CellMapper<String> {

	public static final int DEFAULT_INTEGER_DIGITS = 32;
	public static final int DEFAULT_DECIMAL_DIGITS = 32;

	private final int integerDigits;
	private final int decimalDigits;

	private final BigDecimal complement;

	@JsonCreator
	public CellMapperBigDecimal(@JsonProperty("integer_digits") Integer integerDigits,
	                            @JsonProperty("decimal_digits") Integer decimalDigits) {
		super(new AbstractType<?>[] { AsciiType.instance,
		                             UTF8Type.instance,
		                             Int32Type.instance,
		                             LongType.instance,
		                             IntegerType.instance,
		                             FloatType.instance,
		                             DoubleType.instance,
		                             DecimalType.instance });

		// Setup integer part mapping
		if (integerDigits != null && integerDigits <= 0) {
			throw new IllegalArgumentException("Positive integer part digits required");
		}
		this.integerDigits = integerDigits == null ? DEFAULT_INTEGER_DIGITS : integerDigits;

		// Setup decimal part mapping
		if (decimalDigits != null && decimalDigits <= 0) {
			throw new IllegalArgumentException("Positive decimal part digits required");
		}
		this.decimalDigits = decimalDigits == null ? DEFAULT_DECIMAL_DIGITS : decimalDigits;

		int totalDigits = this.integerDigits + this.decimalDigits;
		BigDecimal divisor = BigDecimal.valueOf(10).pow(this.decimalDigits);
		BigDecimal dividend = BigDecimal.valueOf(10).pow(totalDigits).subtract(BigDecimal.valueOf(1));
		complement = dividend.divide(divisor);
	}

	public int getIntegerDigits() {
		return integerDigits;
	}

	public int getDecimalDigits() {
		return decimalDigits;
	}

	@Override
	public Analyzer analyzer() {
		return EMPTY_ANALYZER;
	}

	@Override
	public String indexValue(Object value) {
		if (value == null) {
			return null;
		}

		// Split integer and decimal part
		BigDecimal bi = new BigDecimal(value.toString());
		bi = bi.stripTrailingZeros();
		String[] parts = bi.toPlainString().split("\\.");
		String integerPart = parts[0];
		String decimalPart = parts.length == 1 ? "0" : parts[1];

		if (integerPart.replaceFirst("-", "").length() > integerDigits) {
			throw new IllegalArgumentException("Too much digits in integer part");
		}
		if (decimalPart.length() > decimalDigits) {
			throw new IllegalArgumentException("Too much digits in decimal part");
		}

		BigDecimal complemented = bi.add(complement);
		String bds[] = complemented.toString().split("\\.");
		integerPart = bds[0];
		decimalPart = bds.length == 2 ? bds[1] : "0";
		integerPart = StringUtils.leftPad(integerPart, integerDigits + 1, '0');

		return integerPart + "." + decimalPart;
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
