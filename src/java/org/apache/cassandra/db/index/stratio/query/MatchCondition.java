package org.apache.cassandra.db.index.stratio.query;

import org.apache.cassandra.db.index.stratio.schema.CellMapper;
import org.apache.cassandra.db.index.stratio.schema.CellsMapper;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.annotate.JsonTypeName;

/**
 * A {@link Condition} implementation that matches documents containing a value for a field.
 * 
 * @version 0.1
 * @author adelapena
 */
@JsonTypeName("match")
public class MatchCondition extends Condition {

	/** The field name */
	@JsonProperty("field")
	private final String field;

	/** The field value */
	@JsonProperty("value")
	private Object value;

	/**
	 * Constructor using the field name and the value to be matched.
	 * 
	 * @param boost
	 *            The boost for this query clause. Documents matching this clause will (in addition
	 *            to the normal weightings) have their score multiplied by {@code boost}. If
	 *            {@code null}, then {@link DEFAULT_BOOST} is used as default.
	 * @param field
	 *            the field name.
	 * @param value
	 *            the field value.
	 */
	@JsonCreator
	public MatchCondition(@JsonProperty("boost") Float boost,
	                  @JsonProperty("field") String field,
	                  @JsonProperty("value") Object value) {
		super(boost);

		assert field != null : "Field name required";

		this.field = field.toLowerCase();
		this.value = value;
	}

	/**
	 * Returns the field name.
	 * 
	 * @return the field name.
	 */
	public String getField() {
		return field;
	}

	/**
	 * Returns the field value.
	 * 
	 * @return the field value.
	 */
	public Object getValue() {
		return value;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void analyze(Analyzer analyzer) {
		this.value = analyze(field, value, analyzer);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Query query(CellsMapper cellsMapper) {
		CellMapper<?> cellMapper = cellsMapper.getMapper(field);
		Class<?> clazz = cellMapper.baseClass();
		Query query;
		if (clazz == String.class) {
			String value = (String) cellMapper.queryValue(this.value);
			Term term = new Term(field, value);
			query = new TermQuery(term);
		} else if (clazz == Integer.class) {
			Integer value = (Integer) cellMapper.queryValue(this.value);
			query = NumericRangeQuery.newIntRange(field, value, value, true, true);
		} else if (clazz == Long.class) {
			Long value = (Long) cellMapper.queryValue(this.value);
			query = NumericRangeQuery.newLongRange(field, value, value, true, true);
		} else if (clazz == Float.class) {
			Float value = (Float) cellMapper.queryValue(this.value);
			query = NumericRangeQuery.newFloatRange(field, value, value, true, true);
		} else if (clazz == Double.class) {
			Double value = (Double) cellMapper.queryValue(this.value);
			query = NumericRangeQuery.newDoubleRange(field, value, value, true, true);
		} else {
			String message = String.format("Unsupported query %s for mapper %s", this, cellMapper);
			throw new UnsupportedOperationException(message);
		}
		query.setBoost(boost);
		return query;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("MatchQuery [boost=");
		builder.append(boost);
		builder.append(", field=");
		builder.append(field);
		builder.append(", value=");
		builder.append(value);
		builder.append("]");
		return builder.toString();
	}

}