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

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortField.Type;
import org.apache.lucene.util.BytesRef;

import java.util.ArrayList;
import java.util.List;

/**
 * A {@link com.stratio.cassandra.index.schema.mapping.ColumnMapper} to map a string, not tokenized field.
 *
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public abstract class ColumnMapperKeyword extends ColumnMapperSingle<String> {

    protected ColumnMapperKeyword(AbstractType<?>[] supportedTypes, AbstractType<?>[] supportedClusteringTypes) {
        super(supportedTypes, supportedClusteringTypes);
    }

    /** {@inheritDoc} */
    @Override
    public final List<Field> fieldsFromBase(String name, String value) {
        List<Field> set = new ArrayList<>(2);
        BytesRef bytes = new BytesRef(value);
        set.add(new StringField(name, value, STORE));
        set.add(new SortedSetDocValuesField(name, bytes));
        return set;
    }

    /** {@inheritDoc} */
    @Override
    public final SortField sortField(String field, boolean reverse) {
        return new SortField(field, Type.STRING_VAL, reverse);
    }

    /** {@inheritDoc} */
    @Override
    public final Class<String> baseClass() {
        return String.class;
    }
}
