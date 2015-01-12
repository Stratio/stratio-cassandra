package com.stratio.cassandra.index.geospatial;

import com.spatial4j.core.context.SpatialContext;
import com.stratio.cassandra.index.util.JsonSerializer;
import org.codehaus.jackson.annotate.JsonSubTypes;
import org.codehaus.jackson.annotate.JsonTypeInfo;

import java.io.IOException;

/**
 * @author Andres de la Pena <adelapena@stratio.com>
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({@JsonSubTypes.Type(value = GeoPoint.class, name = "point"),
               @JsonSubTypes.Type(value = GeoRectangle.class, name = "rectangle"),
               @JsonSubTypes.Type(value = GeoCircle.class, name = "circle"),})
public abstract class GeoShape {

    public abstract com.spatial4j.core.shape.Shape toSpatial4j(SpatialContext ctx);

    public static GeoShape fromString(String s) {
        try {
            return JsonSerializer.fromString(s, GeoShape.class);
        } catch (IOException e) {
            throw new IllegalArgumentException("Unparseable shape");
        }
    }

    public static void checkLongitude(Double longitude) {
        if (longitude == null) {
            throw new IllegalArgumentException("Not null longitude required");
        }
        if (longitude < -180.0 || longitude > 180) {
            throw new IllegalArgumentException("Longitude must be between -180.0 and 180.0");
        }
    }

    public static void checkLatitude(Double latitude) {
        if (latitude == null) {
            throw new IllegalArgumentException("Not null latitude required");
        }
        if (latitude < -90.0 || latitude > 90) {
            throw new IllegalArgumentException("Latitude must be between -90.0 and 90.0");
        }
    }
}
