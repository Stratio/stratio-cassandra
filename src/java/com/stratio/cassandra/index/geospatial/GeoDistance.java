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

import org.codehaus.jackson.annotate.JsonCreator;

/**
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class GeoDistance {

    private double value;
    private GeoDistanceUnit unit;

    public GeoDistance(double value, GeoDistanceUnit unit) {
        this.value = value;
        this.unit = unit;
    }

    public double getMetres() {
        return unit.getMetres() * value;
    }

    public double getValue() {
        return value;
    }

    public double getValue(GeoDistanceUnit unit) {
        return this.unit.getMetres() * value / unit.getMetres();
    }

    @JsonCreator
    public static GeoDistance create(String s) {
        try {
            for (GeoDistanceUnit geoDistanceUnit : GeoDistanceUnit.values()) {
                for (String name : geoDistanceUnit.getNames()) {
                    if (s.endsWith(name)) {
                        double value = Double.parseDouble(s.substring(0, s.indexOf(name)));
                        return new GeoDistance(value, geoDistanceUnit);
                    }
                }
            }
            double value = Double.parseDouble(s);
            return new GeoDistance(value, GeoDistanceUnit.METRES);
        } catch (Exception e) {
            e.printStackTrace();
            throw new IllegalArgumentException("Unparseable distance: " + s);
        }
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("GeoDistance{");
        sb.append("value=").append(value);
        sb.append(", unit=").append(unit);
        sb.append('}');
        return sb.toString();
    }
}
