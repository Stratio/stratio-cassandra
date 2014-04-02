package org.apache.cassandra.db.index.stratio.query;

import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.db.index.stratio.schema.CellMapper;
import org.apache.cassandra.db.index.stratio.schema.CellsMapper;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.annotate.JsonTypeName;

/**
 * A {@link Condition} implementation that matches documents containing a particular sequence of
 * terms.
 * 
 * @version 0.1
 * @author adelapena
 */
@JsonTypeName("match")
public class PhraseCondition extends Condition {

	public static final int DEFAULT_SLOP = 0;

	/** The field name */
	private final String field;

	/** The field values */
	private List<Object> values;

	/** The slop */
	private final int slop;

	/**
	 * Constructor using the field name and the value to be matched.
	 * 
	 * @param boost
	 *            The boost for this query clause. Documents matching this clause will (in addition
	 *            to the normal weightings) have their score multiplied by {@code boost}. If
	 *            {@code null}, then {@link DEFAULT_BOOST} is used as default.
	 * @param field
	 *            The field name.
	 * @param values
	 *            The field values.
	 * @param slop
	 *            The slop.
	 */
	@JsonCreator
	public PhraseCondition(@JsonProperty("boost") Float boost,
	                       @JsonProperty("field") String field,
	                       @JsonProperty("values") List<Object> values,
	                       @JsonProperty("slop") Integer slop) {
		super(boost);

		if (field == null || field.trim().isEmpty()) {
			throw new IllegalArgumentException("Field name required");
		}

		this.field = field;
		this.values = values;
		this.slop = slop == null ? DEFAULT_SLOP : slop;
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
	 * Returns the field values.
	 * 
	 * @return the field values.
	 */
	public List<Object> getValues() {
		return values;
	}

	/**
	 * Returns the slop.
	 * 
	 * @return the slop.
	 */
	public int getSlop() {
		return slop;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void analyze(Analyzer analyzer) {
		List<Object> values = new ArrayList<>(this.values.size());
		for (Object value : this.values) {
			Object analyzedValue = analyze(field, value, analyzer);
			values.add(analyzedValue);
		}
		this.values = values;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Query query(CellsMapper cellsMapper) {
		CellMapper<?> cellMapper = cellsMapper.getMapper(field);
		Class<?> clazz = cellMapper.baseClass();
		if (clazz == String.class) {
			PhraseQuery query = new PhraseQuery();
			query.setSlop(slop);
			query.setBoost(boost);
			int count = 0;
			for (Object o : values) {
				if (o != null) {
					String value = (String) cellMapper.queryValue(o);
					Term term = new Term(field, value);
					query.add(term, count);
				}
				count++;
			}
			return query;
		} else {
			String message = String.format("Unsupported query %s for mapper %s", this, cellMapper);
			throw new UnsupportedOperationException(message);
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append(getClass().getSimpleName());
		builder.append(" [boost=");
		builder.append(boost);
		builder.append(", field=");
		builder.append(field);
		builder.append(", values=");
		builder.append(values);
		builder.append(", slop=");
		builder.append(slop);
		builder.append("]");
		return builder.toString();
	}

}
