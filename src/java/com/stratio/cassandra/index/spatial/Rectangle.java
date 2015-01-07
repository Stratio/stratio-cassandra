package com.stratio.cassandra.index.spatial;

import com.spatial4j.core.context.SpatialContext;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class Rectangle extends Shape {

    @JsonProperty("minX")
    double minX;
    @JsonProperty("maxX")
    double maxX;
    @JsonProperty("minY")
    double minY;
    @JsonProperty("maxY")
    double maxY;

    @Override
    public com.spatial4j.core.shape.Shape toSpatial4j(SpatialContext ctx) {
        return ctx.makeRectangle(minX, maxX, minY, maxY);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Rectangle{");
        sb.append("minX=").append(minX);
        sb.append(", maxX=").append(maxX);
        sb.append(", minY=").append(minY);
        sb.append(", maxY=").append(maxY);
        sb.append('}');
        return sb.toString();
    }
}
