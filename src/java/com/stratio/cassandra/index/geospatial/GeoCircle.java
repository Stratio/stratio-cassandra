/*
 * Copyright 2015, Stratio.
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
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 * Class representing a circle in geographical coordinates.
 *
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class GeoCircle extends GeoShape {

    private final double longitude;
    private final double latitude;
    private final GeoDistance distance;

    @JsonCreator
    public GeoCircle(@JsonProperty("longitude") double longitude,
                     @JsonProperty("latitude") double latitude,
                     @JsonProperty("distance") GeoDistance distance) {
        checkLongitude(longitude);
        checkLatitude(latitude);
        this.longitude = longitude;
        this.latitude = latitude;
        this.distance = distance;
    }

    @Override
    public com.spatial4j.core.shape.Shape toSpatial4j(SpatialContext spatialContext) {
        double kms = distance.getValue(GeoDistanceUnit.KILOMETRES);
        double d = DistanceUtils.dist2Degrees(kms, DistanceUtils.EARTH_MEAN_RADIUS_KM);
        return spatialContext.makeCircle(longitude, latitude, d);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Circle{");
        sb.append("longitude=").append(longitude);
        sb.append(", latitude=").append(latitude);
        sb.append(", distance=").append(distance);
        sb.append('}');
        return sb.toString();
    }
}
