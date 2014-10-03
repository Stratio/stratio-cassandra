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
package com.stratio.cassandra.index.query;

import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermRangeQuery;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

import com.stratio.cassandra.index.schema.ColumnMapper;
import com.stratio.cassandra.index.schema.Schema;

/**
 * A {@link Condition} implementation that matches a field within an range of values.
 * 
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class RangeCondition extends Condition
{

    /** The field name. */
    private final String field;

    /** The lower field value included in the range. */
    private Object lower;

    /** The upper field value included in the range. */
    private Object upper;

    /** If the lower value is included in the range. */
    private final boolean includeLower;

    /** If the upper value is included in the range. */
    private final boolean includeUpper;

    /**
     * Constructs a query selecting all fields greater/equal than {@code lowerValue} but less/equal than
     * {@code upperValue}.
     * 
     * If an endpoint is null, it is said to be "open". Either or both endpoints may be open. Open endpoints may not be
     * exclusive (you can't select all but the first or last term without explicitly specifying the term to exclude.)
     * 
     * @param boost
     *            The boost for this query clause. Documents matching this clause will (in addition to the normal
     *            weightings) have their score multiplied by {@code boost}. If {@code null}, then {@link DEFAULT_BOOST}
     *            is used as default.
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
                          @JsonProperty("include_upper") boolean includeUpper)
    {
        super(boost);

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
    public String getField()
    {
        return field;
    }

    /**
     * Returns the field value at the lower end of the range.
     * 
     * @return the field value at the lower end of the range.
     */
    public Object getLowerValue()
    {
        return lower;
    }

    /**
     * Returns the field value at the upper end of the range.
     * 
     * @return the field value at the upper end of the range.
     */
    public Object getUpperValue()
    {
        return upper;
    }

    /**
     * Returns {@code true} if the {@code lowerValue} is included in the range, {@code false} otherwise.
     * 
     * @return {@code true} if the {@code lowerValue} is included in the range, {@code false} otherwise.
     */
    public boolean getIncludeLower()
    {
        return includeLower;
    }

    /**
     * Returns {@code true} if the {@code includeUpper} is included in the range, {@code false} otherwise.
     * 
     * @return {@code true} if the {@code includeUpper} is included in the range, {@code false} otherwise.
     */
    public boolean getIncludeUpper()
    {
        return includeUpper;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Query query(Schema schema)
    {

        if (field == null || field.trim().isEmpty())
        {
            throw new IllegalArgumentException("Field name required");
        }

        ColumnMapper<?> columnMapper = schema.getMapper(field);
        Class<?> clazz = columnMapper.baseClass();
        Query query;
        if (clazz == String.class)
        {
            String lower = (String) columnMapper.queryValue(field, this.lower);
            String upper = (String) columnMapper.queryValue(field, this.upper);
            if (lower != null)
            {
                lower = analyze(field, lower, schema.analyzer());
            }
            if (upper != null)
            {
                upper = analyze(field, upper, schema.analyzer());
            }
            query = TermRangeQuery.newStringRange(field, lower, upper, includeLower, includeUpper);
        }
        else if (clazz == Integer.class)
        {
            Integer lower = (Integer) columnMapper.queryValue(field, this.lower);
            Integer upper = (Integer) columnMapper.queryValue(field, this.upper);
            query = NumericRangeQuery.newIntRange(field, lower, upper, includeLower, includeUpper);
        }
        else if (clazz == Long.class)
        {
            Long lower = (Long) columnMapper.queryValue(field, this.lower);
            Long upper = (Long) columnMapper.queryValue(field, this.upper);
            query = NumericRangeQuery.newLongRange(field, lower, upper, includeLower, includeUpper);
        }
        else if (clazz == Float.class)
        {
            Float lower = (Float) columnMapper.queryValue(field, this.lower);
            Float upper = (Float) columnMapper.queryValue(field, this.upper);
            query = NumericRangeQuery.newFloatRange(field, lower, upper, includeLower, includeUpper);
        }
        else if (clazz == Double.class)
        {
            Double lower = (Double) columnMapper.queryValue(field, this.lower);
            Double upper = (Double) columnMapper.queryValue(field, this.upper);
            query = NumericRangeQuery.newDoubleRange(field, lower, upper, includeLower, includeUpper);
        }
        else
        {
            String message = String.format("Range queries are not supported by %s mapper", clazz.getSimpleName());
            throw new UnsupportedOperationException(message);
        }
        query.setBoost(boost);
        return query;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString()
    {
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