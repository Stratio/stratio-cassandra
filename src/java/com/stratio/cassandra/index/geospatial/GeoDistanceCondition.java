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
package com.stratio.cassandra.index.geospatial;

import com.spatial4j.core.context.SpatialContext;
import com.spatial4j.core.distance.DistanceUtils;
import com.stratio.cassandra.index.query.Condition;
import com.stratio.cassandra.index.schema.ColumnMapper;
import com.stratio.cassandra.index.schema.Schema;
import org.apache.lucene.search.Query;
import org.apache.lucene.spatial.SpatialStrategy;
import org.apache.lucene.spatial.query.SpatialArgs;
import org.apache.lucene.spatial.query.SpatialOperation;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 * A {@link com.stratio.cassandra.index.query.Condition} implementation that matches documents containing a value for a
 * field.
 *
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class GeoDistanceCondition extends Condition {

    private final String field; // The name of the field to be matched.
    private final double longitude;
    private final double latitude;
    private final GeoDistance distance;

    /**
     * Constructor using the field name and the value to be matched.
     *
     * @param boost The boost for this query clause. Documents matching this clause will (in addition to the normal
     *              weightings) have their score multiplied by {@code boost}. If {@code null}, then {@link
     *              #DEFAULT_BOOST} is used as default.
     * @param field The name of the field to be matched.
     */
    @JsonCreator
    public GeoDistanceCondition(@JsonProperty("boost") Float boost,
                                @JsonProperty("field") String field,
                                @JsonProperty("longitude") double longitude,
                                @JsonProperty("latitude") double latitude,
                                @JsonProperty("distance") GeoDistance distance) {
        super(boost);
        this.field = field;
        this.longitude = longitude;
        this.latitude = latitude;
        this.distance = distance;
    }

    /** {@inheritDoc} */
    @Override
    public Query query(Schema schema) {
        GeoCircle circle = new GeoCircle(longitude, latitude, distance);
        return new GeoShapeCondition(boost, field, GeoOperator.Intersects, circle).query(schema);
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("GeoDistanceCondition{");
        sb.append("field='").append(field).append('\'');
        sb.append(", longitude=").append(longitude);
        sb.append(", latitude=").append(latitude);
        sb.append(", distance=").append(distance);
        sb.append('}');
        return sb.toString();
    }
}