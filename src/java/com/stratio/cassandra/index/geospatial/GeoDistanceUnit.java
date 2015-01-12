package com.stratio.cassandra.index.geospatial;

import org.codehaus.jackson.annotate.JsonCreator;

/**
 * @author Andres de la Pena <adelapena@stratio.com>
 */
enum GeoDistanceUnit {

    MILLIMETRES(0.001, "mm", "millimetres"),
    CENTIMETRES(0.01, "cm", "centimetres"),
    DECIMETRES(0.1, "dm", "decimetres"),
    DECAMETRES(10, "dam", "decametres"),
    HECTOMETRES(100, "hm", "hectometres"),
    KILOMETRES(1000, "km", "kilometres"),
    FOOTS(0.3048, "ft", "foots"),
    YARDS(0.9144, "yd", "yards"),
    INCHES(0.0254, "in", "inches"),
    MILES(1609.344, "mi", "miles"),
    METRES(1, "m", "metres"),
    NAUTICAL_MILES(1850, "M", "NM", "mil", "nautical_miles");

    private String[] names;
    private Double metres;

    GeoDistanceUnit(double metres, String... names) {
        this.names = names;
        this.metres = metres;
    }

    public Double getMetres() {
        return metres;
    }

    public String[] getNames() {
        return names;
    }

    @JsonCreator
    public static GeoDistanceUnit create(String value) {
        if (value == null) {
            throw new IllegalArgumentException();
        }
        for (GeoDistanceUnit v : values()) {
            for (String s : v.names) {
                if (s.equals(value)) {
                    return v;
                }
            }
        }
        throw new IllegalArgumentException();
    }

}
