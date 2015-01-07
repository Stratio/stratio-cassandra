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
package com.stratio.cassandra.index.schema;

import com.spatial4j.core.context.SpatialContext;
import com.stratio.cassandra.index.spatial.Shape;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortField.Type;
import org.apache.lucene.spatial.SpatialStrategy;
import org.apache.lucene.spatial.prefix.RecursivePrefixTreeStrategy;
import org.apache.lucene.spatial.prefix.tree.GeohashPrefixTree;
import org.apache.lucene.spatial.prefix.tree.SpatialPrefixTree;
import org.apache.lucene.spatial.query.SpatialOperation;
import org.codehaus.jackson.annotate.JsonCreator;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * A {@link com.stratio.cassandra.index.schema.ColumnMapper} to map a long field.
 *
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class ColumnMapperShape extends ColumnMapper {

    private SpatialContext ctx = SpatialContext.GEO;
    private int maxLevels = 11;
    private SpatialPrefixTree grid = new GeohashPrefixTree(ctx, maxLevels);
    private Map<String, SpatialStrategy> strategies = new HashMap<>();

    /**
     * Builds a new {@link ColumnMapperShape}.
     */
    @JsonCreator
    public ColumnMapperShape() {
        super(new AbstractType<?>[]{AsciiType.instance, UTF8Type.instance});
    }

    /** {@inheritDoc} */
    @Override
    public Analyzer analyzer() {
        return EMPTY_ANALYZER;
    }

    public Set<IndexableField> fields(Column column) {

        String fieldName = column.getFieldName();
        SpatialStrategy strategy = getStrategy(fieldName);
        Set<IndexableField> fields = new HashSet<>();
        Shape shape = Shape.fromString((String) column.getValue());
        for (IndexableField field : strategy.createIndexableFields(shape.toSpatial4j(ctx))) {
            fields.add(field);
        }
        return fields;
    }

    public SpatialStrategy getStrategy(String fieldName) {
        SpatialStrategy strategy = strategies.get(fieldName);
        if (strategy == null) {
            strategy = new RecursivePrefixTreeStrategy(grid, fieldName);
        }
        return strategy;
    }

    public SpatialContext getSpatialContext() {
        return ctx;
    }

    /** {@inheritDoc} */
    @Override
    public SortField sortField(String field, boolean reverse) {
        return new SortField(field, Type.LONG, reverse);
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return new ToStringBuilder(this).toString();
    }
}
