package com.stratio.cassandra.index.geospatial;

import com.spatial4j.core.context.SpatialContext;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class GeoPoint extends GeoShape {

    private double longitude;
    private double latitude;

    @JsonCreator
    public GeoPoint( @JsonProperty("longitude") double longitude, @JsonProperty("latitude") double latitude) {
        checkLongitude(longitude);
        checkLatitude(latitude);
        this.longitude = longitude;
        this.latitude = latitude;
    }

    public double getLongitude() {
        return longitude;
    }

    public double getLatitude() {
        return latitude;
    }

    public void reset(double longitude, double latitude) {
        this.longitude = longitude;
        this.latitude = latitude;
    }

    @Override
    public com.spatial4j.core.shape.Shape toSpatial4j(SpatialContext ctx) {
        return ctx.makePoint(longitude, latitude);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Point{");
        sb.append("longitude=").append(longitude);
        sb.append(", latitude=").append(latitude);
        sb.append('}');
        return sb.toString();
    }
}
