package org.apache.cassandra.db.index.stratio.schema;

import java.math.BigDecimal;

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

	@JsonProperty("left_padding")
	private final int leftPadding;

	@JsonProperty("right_padding")
	private final int rightPadding;

	@JsonCreator
	public CellMapperBigDecimal(@JsonProperty("left_padding") Integer leftPadding,
	                            @JsonProperty("right_padding") Integer rightPadding) {
		super();

		assert leftPadding != null : "Left padding required";
		assert rightPadding != null : "Right padding required";
		assert leftPadding > 0 : "Left padding must be positive";
		assert rightPadding > 0 : "Right padding must be positive";

		this.leftPadding = leftPadding;
		this.rightPadding = rightPadding;
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
			BigDecimal bi = new BigDecimal(value.toString());
			String[] bis = bi.toPlainString().split("\\.");
			String left = bis[0];
			String right = bis[1];

			left = left.replaceFirst("-", "");

			assert left.length() <= leftPadding;
			assert right.length() <= rightPadding;

			left = StringUtils.leftPad(left, rightPadding, '0');
			right = StringUtils.rightPad(right, rightPadding, '0');

			char prefix = bi.compareTo(BigDecimal.ZERO) > 0 ? 'p' : 'N';
			String result = prefix + left + "." + right;

//			System.out.println("RESULT -> " + result);
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
