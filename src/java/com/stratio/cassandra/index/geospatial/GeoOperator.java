package com.stratio.cassandra.index.geospatial;

import org.apache.lucene.spatial.query.SpatialOperation;
import org.codehaus.jackson.annotate.JsonCreator;

/**
 * @author Andres de la Pena <adelapena@stratio.com>
 */
enum GeoOperator {

    BBoxWithin("bbox_within", SpatialOperation.BBoxWithin),
    Contains("contains", SpatialOperation.Contains),
    Intersects("intersects", SpatialOperation.Intersects),
    IsEqualTo("is_equal_to", SpatialOperation.IsEqualTo),
    IsDisjointTo("is_disjoint_to", SpatialOperation.IsDisjointTo),
    IsWithin("is_within", SpatialOperation.IsWithin),
    Overlaps("overlaps", SpatialOperation.Overlaps);

    private String shortName;
    private SpatialOperation spatialOperation;

    GeoOperator(String shortName, SpatialOperation spatialOperation) {
        this.shortName = shortName;
        this.spatialOperation = spatialOperation;
    }

    public String getShortName() {
        return shortName;
    }

    public SpatialOperation getSpatialOperation() {
        return spatialOperation;
    }

    @JsonCreator
    public static GeoOperator create (String value) {
        if(value == null) {
            throw new IllegalArgumentException();
        }
        for(GeoOperator v : values()) {
            if(value.equals(v.getShortName())) {
                return v;
            }
        }
        throw new IllegalArgumentException();
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("HelloEnum{");
        sb.append("shortName='").append(shortName).append('\'');
        sb.append(", spatialOperation=").append(spatialOperation);
        sb.append('}');
        return sb.toString();
    }
}
