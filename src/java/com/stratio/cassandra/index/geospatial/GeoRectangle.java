package com.stratio.cassandra.index.geospatial;

import com.spatial4j.core.context.SpatialContext;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class GeoRectangle extends GeoShape {

    private final double minLongitude;
    private final double maxLongitude;
    private final double minLatitude;
    private final double maxLatitude;

    @JsonCreator
    public GeoRectangle(@JsonProperty("min_longitude") double minLongitude,
                        @JsonProperty("max_longitude") double maxLongitude,
                        @JsonProperty("min_latitude") double minLatitude,
                        @JsonProperty("max_latitude") double maxLatitude) {
        checkLongitude(minLongitude);
        checkLongitude(maxLongitude);
        checkLatitude(minLatitude);
        checkLatitude(maxLatitude);
        this.minLongitude = minLongitude;
        this.maxLongitude = maxLongitude;
        this.minLatitude = minLatitude;
        this.maxLatitude = maxLatitude;
    }

    @Override
    public com.spatial4j.core.shape.Shape toSpatial4j(SpatialContext ctx) {
        return ctx.makeRectangle(minLongitude, maxLongitude, minLatitude, maxLatitude);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Rectangle{");
        sb.append("minLongitude=").append(minLongitude);
        sb.append(", maxLongitude=").append(maxLongitude);
        sb.append(", minLatitude=").append(minLatitude);
        sb.append(", maxLatitude=").append(maxLatitude);
        sb.append('}');
        return sb.toString();
    }
}
