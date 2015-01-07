package com.stratio.cassandra.index.spatial;

import com.spatial4j.core.context.SpatialContext;
import com.stratio.cassandra.index.util.JsonSerializer;
import org.codehaus.jackson.annotate.JsonSubTypes;
import org.codehaus.jackson.annotate.JsonTypeInfo;

import java.io.IOException;

/**
 * @author Andres de la Pena <adelapena@stratio.com>
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({@JsonSubTypes.Type(value = Point.class, name = "point"),
               @JsonSubTypes.Type(value = Rectangle.class, name = "rectangle"),
               @JsonSubTypes.Type(value = Circle.class, name = "circle"),})
public abstract class Shape {

    public abstract com.spatial4j.core.shape.Shape toSpatial4j(SpatialContext ctx);

    public static Shape fromString(String s) {
        try {
            return JsonSerializer.fromString(s, Shape.class);
        } catch (IOException e) {
            throw new IllegalArgumentException("Unparseable shape");
        }
    }
}
