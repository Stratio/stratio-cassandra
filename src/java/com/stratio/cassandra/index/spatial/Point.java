package com.stratio.cassandra.index.spatial;

import com.spatial4j.core.context.SpatialContext;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class Point extends Shape {

    @JsonProperty("x")
    double x;
    @JsonProperty("y")
    double y;
    @JsonProperty("distance")
    double distance;

    @Override
    public com.spatial4j.core.shape.Shape toSpatial4j(SpatialContext ctx) {
        return ctx.makeCircle(x, y, distance);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Circle{");
        sb.append("x=").append(x);
        sb.append(", y=").append(y);
        sb.append(", distance=").append(distance);
        sb.append('}');
        return sb.toString();
    }
}
