/*
 * Copyright 2014, Stratio.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.db.index.stratio.query;

import org.apache.cassandra.db.index.stratio.schema.CellMapper;
import org.apache.cassandra.db.index.stratio.schema.CellsMapper;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermRangeQuery;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.annotate.JsonTypeName;

/**
 * A {@link Condition} implementation that matches a field within an range of values.
 * 
 * @version 0.1
 * @author adelapena
 */
@JsonTypeName("range")
public class RangeCondition extends Condition {

	/** The field name. */
	@JsonProperty("field")
	private final String field;

	/** The lower field value included in the range. */
	@JsonProperty("lower")
	private Object lower;

	/** The upper field value included in the range. */
	@JsonProperty("upper")
	private Object upper;

	/** If the lower value is included in the range. */
	@JsonProperty("include_lower")
	private final boolean includeLower;

	/** If the upper value is included in the range. */
	@JsonProperty("include_upper")
	private final boolean includeUpper;

	/**
	 * Constructs a query selecting all fields greater/equal than {@code lowerValue} but less/equal
	 * than {@code upperValue}.
	 * 
	 * If an endpoint is null, it is said to be "open". Either or both endpoints may be open. Open
	 * endpoints may not be exclusive (you can't select all but the first or last term without
	 * explicitly specifying the term to exclude.)
	 * 
	 * @param boost
	 *            The boost for this query clause. Documents matching this clause will (in addition
	 *            to the normal weightings) have their score multiplied by {@code boost}. If
	 *            {@code null}, then {@link DEFAULT_BOOST} is used as default.
	 * @param field
	 *            the field name.
	 * @param lowerValue
	 *            the field value at the lower end of the range.
	 * @param upperValue
	 *            the field value at the upper end of the range.
	 * @param includeLower
	 *            if {@code true}, the {@code lowerValue} is included in the range.
	 * @param includeUpper
	 *            if {@code true}, the {@code upperValue} is included in the range.
	 */
	@JsonCreator
	public RangeCondition(@JsonProperty("boost") Float boost,
	                      @JsonProperty("field") String field,
	                      @JsonProperty("lower") Object lowerValue,
	                      @JsonProperty("upper") Object upperValue,
	                      @JsonProperty("include_lower") boolean includeLower,
	                      @JsonProperty("include_upper") boolean includeUpper) {
		super(boost);

		if (field == null || field.trim().isEmpty()) {
			throw new IllegalArgumentException("Field name required");
		}

		this.field = field;
		this.lower = lowerValue;
		this.upper = upperValue;
		this.includeLower = includeLower;
		this.includeUpper = includeUpper;
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
	 * Returns the field value at the lower end of the range.
	 * 
	 * @return the field value at the lower end of the range.
	 */
	public Object getLowerValue() {
		return lower;
	}

	/**
	 * Returns the field value at the upper end of the range.
	 * 
	 * @return the field value at the upper end of the range.
	 */
	public Object getUpperValue() {
		return upper;
	}

	/**
	 * Returns {@code true} if the {@code lowerValue} is included in the range, {@code false}
	 * otherwise.
	 * 
	 * @return {@code true} if the {@code lowerValue} is included in the range, {@code false}
	 *         otherwise.
	 */
	public boolean getIncludeLower() {
		return includeLower;
	}

	/**
	 * Returns {@code true} if the {@code includeUpper} is included in the range, {@code false}
	 * otherwise.
	 * 
	 * @return {@code true} if the {@code includeUpper} is included in the range, {@code false}
	 *         otherwise.
	 */
	public boolean getIncludeUpper() {
		return includeUpper;
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
			String lower = (String) cellMapper.queryValue(this.lower);
			String upper = (String) cellMapper.queryValue(this.upper);
			lower = analyze(field, lower, cellMapper.analyzer());
			upper = analyze(field, upper, cellMapper.analyzer());
			query = TermRangeQuery.newStringRange(field, lower, upper, includeLower, includeUpper);
		} else if (clazz == Integer.class) {
			Integer lower = (Integer) cellMapper.queryValue(this.lower);
			Integer upper = (Integer) cellMapper.queryValue(this.upper);
			query = NumericRangeQuery.newIntRange(field, lower, upper, includeLower, includeUpper);
		} else if (clazz == Long.class) {
			Long lower = (Long) cellMapper.queryValue(this.lower);
			Long upper = (Long) cellMapper.queryValue(this.upper);
			query = NumericRangeQuery.newLongRange(field, lower, upper, includeLower, includeUpper);
		} else if (clazz == Float.class) {
			Float lower = (Float) cellMapper.queryValue(this.lower);
			Float upper = (Float) cellMapper.queryValue(this.upper);
			query = NumericRangeQuery.newFloatRange(field, lower, upper, includeLower, includeUpper);
		} else if (clazz == Double.class) {
			Double lower = (Double) cellMapper.queryValue(this.lower);
			Double upper = (Double) cellMapper.queryValue(this.upper);
			query = NumericRangeQuery.newDoubleRange(field, lower, upper, includeLower, includeUpper);
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
		builder.append(getClass().getSimpleName());
		builder.append(" [boost=");
		builder.append(boost);
		builder.append(", field=");
		builder.append(field);
		builder.append(", lowerValue=");
		builder.append(lower);
		builder.append(", upperValue=");
		builder.append(upper);
		builder.append(", includeLower=");
		builder.append(includeLower);
		builder.append(", includeUpper=");
		builder.append(includeUpper);
		builder.append("]");
		return builder.toString();
	}

}