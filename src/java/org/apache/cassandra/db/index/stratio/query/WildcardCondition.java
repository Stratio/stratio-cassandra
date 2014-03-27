package org.apache.cassandra.db.index.stratio.query;

import org.apache.cassandra.db.index.stratio.schema.CellMapper;
import org.apache.cassandra.db.index.stratio.schema.CellsMapper;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.annotate.JsonTypeName;

/**
 * 
 * Implements the wildcard search query. Supported wildcards are {@code *}, which matches any
 * character sequence (including the empty one), and {@code ?}, which matches any single character.
 * '\' is the escape character.
 * <p>
 * Note this query can be slow, as it needs to iterate over many terms. In order to prevent
 * extremely slow WildcardQueries, a Wildcard term should not start with the wildcard {@code *}.
 * 
 * @version 0.1
 * @author adelapena
 */
@JsonTypeName("wildcard")
public class WildcardCondition extends Condition {

	/** The field name */
	@JsonProperty("field")
	private final String field;

	/** The field value */
	@JsonProperty("value")
	private final Object value;

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
	public WildcardCondition(@JsonProperty("boost") Float boost,
	                     @JsonProperty("field") String field,
	                     @JsonProperty("value") Object value) {
		super(boost);

		assert field != null : "Field name required";

		this.field = field;
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
			query = new org.apache.lucene.search.WildcardQuery(term);
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
		builder.append("WildcardQuery [boost=");
		builder.append(boost);
		builder.append(", field=");
		builder.append(field);
		builder.append(", value=");
		builder.append(value);
		builder.append("]");
		return builder.toString();
	}

}
