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
package com.stratio.cassandra.index.schema.mapping;

import com.google.common.base.Objects;
import org.apache.cassandra.db.marshal.*;
import org.apache.lucene.document.DoubleField;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.FloatField;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortField.Type;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * A {@link ColumnMapper} to map a double field.
 *
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class ColumnMapperDouble extends ColumnMapperSingle<Double> {

    /** The default boost. */
    public static final Float DEFAULT_BOOST = 1.0f;

    private static final FieldType FIELD_TYPE = new FieldType();
    static {
        FIELD_TYPE.setTokenized(true);
        FIELD_TYPE.setOmitNorms(true);
        FIELD_TYPE.setIndexOptions(IndexOptions.DOCS);
        FIELD_TYPE.setNumericType(FieldType.NumericType.DOUBLE);
        FIELD_TYPE.setDocValuesType(DocValuesType.NUMERIC);
        FIELD_TYPE.freeze();
    }

    /** The boost. */
    private final Float boost;

    /**
     * Builds a new {@link ColumnMapperDouble} using the specified boost.
     *
     * @param boost The boost to be used.
     */
    @JsonCreator
    public ColumnMapperDouble(@JsonProperty("boost") Float boost) {
        super(new AbstractType<?>[]{AsciiType.instance,
                                    UTF8Type.instance,
                                    Int32Type.instance,
                                    LongType.instance,
                                    IntegerType.instance,
                                    FloatType.instance,
                                    DoubleType.instance,
                                    DecimalType.instance}, new AbstractType[]{DoubleType.instance});
        this.boost = boost == null ? DEFAULT_BOOST : boost;
    }

    /** {@inheritDoc} */
    @Override
    public Double baseValue(String name, Object value, boolean checkValidity) {
        if (value == null) {
            return null;
        } else if (value instanceof Number) {
            return ((Number) value).doubleValue();
        } else if (value instanceof String) {
            String svalue = (String) value;
            try {
                return Double.valueOf(svalue);
            } catch (NumberFormatException e) {
                String message = String.format("Field %s requires a base 10 double, but found \"%s\"", name, svalue);
                throw new IllegalArgumentException(message);
            }
        } else {
            String message = String.format("Field %s requires a base 10 double, but found \"%s\"", name, value);
            throw new IllegalArgumentException(message);
        }
    }

    /** {@inheritDoc} */
    @Override
    public List<Field> fieldsFromBase(String name, Double value) {
        List<Field> fields = new ArrayList<>();
        fields.add(new DoubleField(name, value, STORE));
        fields.add(new NumericDocValuesField(name, Double.doubleToLongBits(value)));
        return fields;
    }

    /** {@inheritDoc} */
    @Override
    public SortField sortField(String field, boolean reverse) {
        return new SortField(field, Type.DOUBLE, reverse);
    }

    /** {@inheritDoc} */
    @Override
    public Class<Double> baseClass() {
        return Double.class;
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return Objects.toStringHelper(this).add("boost", boost).toString();
    }
}
