package com.stratio.cassandra.index.geospatial;

import com.spatial4j.core.context.SpatialContext;
import com.spatial4j.core.distance.DistanceUtils;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

/**
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
    public com.spatial4j.core.shape.Shape toSpatial4j(SpatialContext ctx) {
        double kms = distance.getValue(GeoDistanceUnit.KILOMETRES);
        double d = DistanceUtils.dist2Degrees(kms, DistanceUtils.EARTH_MEAN_RADIUS_KM);
        return ctx.makeCircle(longitude, latitude, d);
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
