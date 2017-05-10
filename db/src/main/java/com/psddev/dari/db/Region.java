package com.psddev.dari.db;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.HashMap;
import java.util.Map;

import com.psddev.dari.util.ObjectUtils;

public class Region {

    public static final int EARTH_RADIUS_IN_MILES = 3959;
    public static final int EARTH_RADIUS_IN_KILOMETERS = 6371;
    public static final double METERS_IN_MILE = 1609.344;

    private MultiPolygon polygons;
    private List<Circle> circles;

    @Deprecated private Double x;
    @Deprecated private Double y;
    @Deprecated private Double radius;

    public static double milesToDegrees(double miles) {
        return Math.toDegrees(miles / EARTH_RADIUS_IN_MILES);
    }

    public static double metersToDegrees(double meters) {
        return Math.toDegrees(meters / METERS_IN_MILE / EARTH_RADIUS_IN_MILES);
    }

    public static double kilometersToDegrees(double kilometers) {
        return Math.toDegrees(kilometers / EARTH_RADIUS_IN_KILOMETERS);
    }

    public static double degreesToMiles(double degrees) {
        return EARTH_RADIUS_IN_MILES * Math.toRadians(degrees);
    }

    public static double degreesToMeters(double degrees) {
        return METERS_IN_MILE * EARTH_RADIUS_IN_MILES * Math.toRadians(degrees);
    }

    public static double degreesToKilometers(double degrees) {
        return EARTH_RADIUS_IN_KILOMETERS * Math.toRadians(degrees);
    }

    /** Creates an empty region. */
    public static Region empty() {
        return new Region();
    }

    /** Creates a circular region on a Cartesian coordinate system. */
    public static Region cartesianCircle(double x, double y, double radius, int sides) {
        return new Region(x, y, radius, sides);
    }

    /**
     * Creates a circular region on a spherical coordinate system, where x is
     * latitude and y is longitude.
     *
     * All arguments should be given as degrees.
     */
    public static Region sphericalCircle(double x, double y, double radius) {
        return new Region(x, y, radius, locationsLatLon(x, y, radius));
    }

    private Region(Double x, Double y, Double radius, MultiPolygon polygons) {
        this.x = x;
        this.y = y;
        this.radius = radius;
        this.polygons = polygons;
    }

    /**
     * Creates an empty region.
     * @deprecated Use {@link #empty()} instead.
     */
    @Deprecated
    public Region() {
        this.polygons = new MultiPolygon();
        this.circles = new ArrayList<Circle>();
    }

    /**
     * Creates a circular region on a Cartesian coordinate system.
     * @deprecated Use {@link #cartesianCircle(double, double, double, int)} instead.
     */
    @Deprecated
    public Region(double x, double y, double radius, int sides, double initialAngle) {
        // initialAngle has always been ignored. Keeping it here for API compatibility.
        this(x, y, radius, sides);
    }

    /**
     * Creates a circular region on a Cartesian coordinate system.
     * @deprecated Use {@link #cartesianCircle(double, double, double, int)} instead.
     */
    @Deprecated
    public Region(double x, double y, double radius, int sides) {
        this(x, y, radius, locationsFlat(x, y, radius, sides, false));
    }

    // Generates a polygon of locations on a Cartesian coordinate system.
    protected static MultiPolygon locationsFlat(double x, double y, double radius, int sides, boolean flip) {
        LinearRing ring = new LinearRing();

        Polygon polygon = new Polygon();
        polygon.add(ring);

        MultiPolygon polygons = new MultiPolygon();
        polygons.add(polygon);

        double d2r = Math.PI / 180.0; // degrees to radians
        double r2d = 180.0 / Math.PI; // radians to degrees

        double rlat = (Region.degreesToMiles(radius) / EARTH_RADIUS_IN_MILES) * r2d;
        double rlng = rlat / Math.cos(x * d2r);

        for (double i = 0.0; i < (sides + 1); i++) {
            double theta = Math.PI * (i / (sides / 2));
            double lng = y + (rlng * Math.cos(theta)); // center a + radius x * cos(theta)
            double lat = x + (rlat * Math.sin(theta)); // center b + radius y * sin(theta)

            Coordinate coordinate = new Coordinate(lat, lng);
            ring.add(coordinate);
        }

        return polygons;
    }

    // Generates rectangular boundary points on a spherical surface.
    private static MultiPolygon locationsLatLon(double x, double y, double radius) {
        double deltaY = radius * Math.cos(Math.toRadians(x));

        LinearRing ring = new LinearRing();

        Polygon polygon = new Polygon();
        polygon.add(ring);

        MultiPolygon polygons = new MultiPolygon();
        polygons.add(polygon);

        ring.add(new Coordinate(x + radius, y + deltaY));
        ring.add(new Coordinate(x - radius, y + deltaY));
        ring.add(new Coordinate(x - radius, y - deltaY));
        ring.add(new Coordinate(x + radius, y - deltaY));
        ring.add(new Coordinate(x + radius, y + deltaY));

        return polygons;
    }

    /** Returns an unmodifiable list of locations. */
    public List<Location> getLocations() {
        List<Location> locations = new ArrayList<Location>();

        if (polygons.size() > 0) {

            // Convert Polygon to List<Location> for backward compatibility.
            for (Polygon polygon : polygons) {
                for (LinearRing ring : polygon) {
                    for (Coordinate coordinate : ring) {
                        locations.add(coordinate.getLocation());
                    }
                }

                break;
            }
        }

        return Collections.unmodifiableList(locations);
    }

    public Double getX() {
        return x;
    }

    public Double getY() {
        return y;
    }

    public Double getRadius() {
        return radius;
    }

    public MultiPolygon getPolygons() {
        return polygons;
    }

    public void addPolygon(Polygon polygon) {
        polygons.add(polygon);
    }

    public List<Circle> getCircles() {
        return circles;
    }

    public void addCircle(Circle circle) {
        circles.add(circle);
    }

    /** Returns {@code true} if this region is a circle. */
    public boolean isCircle() {
        return getRadius() != null;
    }

    public String getGeoJson() {
        List<Map<String, Object>> features = new ArrayList<Map<String, Object>>();

        Map<String, Object> featureCollection = new HashMap<String, Object>();
        featureCollection.put("type", "GeometryCollection");
        featureCollection.put("geometries", features);

        if (circles != null && circles.size() > 0) {

            for (Circle circle : circles) {
                Map<String, Object> geometry = new HashMap<String, Object>();
                geometry.put("type", "Circle");
                geometry.put("coordinates", circle.getGeoJsonArray());
                geometry.put("radius", circle.getRadius());

                features.add(geometry);
            }
        }

        if (polygons != null && polygons.size() > 0) {
            Map<String, Object> geometry = new HashMap<String, Object>();
            geometry.put("type", "MultiPolygon");
            geometry.put("coordinates", polygons);

            features.add(geometry);
        }

        return ObjectUtils.toJson(featureCollection);
    }

    /**
     * Returns the well-known text representation of this region.
     *
     * @see <a href="https://en.wikipedia.org/wiki/Well-known_text">WKT</a>
     */
    public String toWkt() {
        StringBuilder mp = new StringBuilder();

        mp.append("MULTIPOLYGON(");

        List<Region.Polygon> polygons = getPolygons();

        if (polygons != null) {
            for (Region.Polygon polygon : polygons) {
                for (Region.LinearRing ring : polygon) {
                    mp.append("((");
                    for (Region.Coordinate coordinate : ring) {
                        mp.append(coordinate.getLatitude());
                        mp.append(' ');
                        mp.append(coordinate.getLongitude());
                        mp.append(", ");
                    }
                    mp.setLength(mp.length() - 2);
                    mp.append(")), ");
                }
            }
        }

        List<Region.Circle> circles = getCircles();

        if (circles != null) {
            for (Region.Circle circle : circles) {
                for (Region.Polygon polygon : circle.getPolygons()) {
                    for (Region.LinearRing ring : polygon) {
                        mp.append("((");
                        for (Region.Coordinate coordinate : ring) {
                            mp.append(coordinate.getLatitude());
                            mp.append(' ');
                            mp.append(coordinate.getLongitude());
                            mp.append(", ");
                        }
                        mp.setLength(mp.length() - 2);
                        mp.append(")), ");
                    }
                }
            }
        }

        mp.setLength(mp.length() - 2);
        mp.append(")");

        return mp.toString();
    }

    @SuppressWarnings("unchecked")
    public static Region fromGeoJson(String geoJson) {
        Map<String, Object> json = (Map<String, Object>) ObjectUtils.fromJson(geoJson);

        return Region.parseGeoJson(json);
    }

    protected static void parseCircles(Region region, List<List<Double>> geo) {
        for (List<Double> c : geo) {
            Double radius = ObjectUtils.to(Double.class, c.get(2));
            Double lat = ObjectUtils.to(Double.class, c.get(1));
            Double lng = ObjectUtils.to(Double.class, c.get(0));

            Circle circle = new Circle(lat, lng, radius);
            region.addCircle(circle);
        }
    }

    @SuppressWarnings("unchecked")
    protected static void parseCircle(Region region, Map<String, Object> geo) {
        Double radius = ObjectUtils.to(Double.class, geo.get("radius"));
        radius = Region.metersToDegrees(radius);

        List<List<Double>> coordinates = (List<List<Double>>) geo.get("coordinates");
        List<Double> coordinate = (List<Double>) coordinates.get(0);

        Circle circle = new Circle(
                ObjectUtils.to(Double.class, coordinate.get(1)),
                ObjectUtils.to(Double.class, coordinate.get(0)),
                radius);

        region.addCircle(circle);
    }

    protected static void parsePolygon(Region region, List<List<List<Double>>> geo) {
        Region.Polygon polygon = new Region.Polygon();

        for (List<List<Double>> r : geo) {
            Region.LinearRing ring = new Region.LinearRing();

            for (List<Double> c : r) {
                Region.Coordinate coordinate =
                    new Region.Coordinate(ObjectUtils.to(Double.class, c.get(1)),
                                          ObjectUtils.to(Double.class, c.get(0)));

                ring.add(coordinate);
            }

            polygon.add(ring);
        }

        region.addPolygon(polygon);
    }

    protected static void parseMultiPolygon(Region region, List<List<List<List<Double>>>> geo) {
        for (List<List<List<Double>>> p : geo) {
            parsePolygon(region, p);
        }
    }

    @SuppressWarnings("unchecked")
    protected static Region parseGeoJson(Map<String, Object> geoJSON) {
        if (geoJSON == null) {
            return null;
        }

        if (geoJSON.get("polygons") != null) {
            return parseDariGeoJson((List<List<List<List<Double>>>>) geoJSON.get("polygons"));
        }

        Region region = Region.empty();
        parseFullGeoJson(region, geoJSON);
        return region;
    }

    /**
     * Parses the partial GeoJSON that is stored by Dari in a Database.
     */
    private static Region parseDariGeoJson(List<List<List<List<Double>>>> geoJSON) {
        Region region = Region.empty();
        parseMultiPolygon(region, geoJSON);
        return region;
    }

    /**
     * Parses the a subset of the GeoJSON spec as returned by Leaflet.js.
     */
    @SuppressWarnings("unchecked")
    private static void parseFullGeoJson(Region region, Map<String, Object> geoJSON) {

        List<Map<String, Object>> features = (List<Map<String, Object>>) geoJSON.get("features");
        if (features == null) {
            features = (List<Map<String, Object>>) geoJSON.get("geometries");
            if (features == null) {
                return;
            }
        }

        for (Map<String, Object> feature : features) {
            Map<String, Object> geometry = (Map<String, Object>) feature.get("geometry");
            if (geometry == null) {
                geometry = feature;
            }

            String type = (String) geometry.get("type");

            if ("Circle".equals(type)) {
                parseCircle(region, geometry);
            } else if ("MultiPolygon".equals(type)) {
                parseMultiPolygon(region, (List<List<List<List<Double>>>>) geometry.get("coordinates"));
            } else if ("Polygon".equals(type)) {
                parsePolygon(region, (List<List<List<Double>>>) geometry.get("coordinates"));
            } else if ("FeatureCollection".equals(type)) {
                parseFullGeoJson(region, geometry);
            }
        }
    }

    public static class Circle extends Coordinate {
        private static final long serialVersionUID = 1L;
        public Double radius;

        public Circle(Double lat, Double lng, Double radius) {
            super(lat, lng);

            this.radius = radius;

            add(radius);
        }

        public List<List<Double>> getGeoJsonArray() {
            List<List<Double>> geo = new ArrayList<List<Double>>();
            geo.add(this.subList(0, 2));
            return geo;
        }

        /** Radius in meters. */
        public Double getRadius() {
            return Region.degreesToMeters(radius);
        }

        public MultiPolygon getPolygons() {
            return Region.locationsFlat(getLatitude(), getLongitude(), radius, 32, true);
        }
    }

    public static class Coordinate extends ArrayList<Double> {
        private static final long serialVersionUID = 1L;
        private Double lat;
        private Double lng;

        public Coordinate(Double lat, Double lng) {
            super.add(lng);
            super.add(lat);

            this.lat = lat;
            this.lng = lng;
        }

        public Location getLocation() {
            return new Location(lng, lat);
        }

        public Double getLatitude() {
            return lat;
        }

        public Double getLongitude() {
            return lng;
        }
    }

    public static class LinearRing extends ArrayList<Coordinate> {
        private static final long serialVersionUID = 1L;
    }

    public static class Polygon extends ArrayList<LinearRing> {
        private static final long serialVersionUID = 1L;
    }

    public static class MultiPolygon extends ArrayList<Polygon> {
        private static final long serialVersionUID = 1L;
    }

    @Override
    public String toString() {
        return getGeoJson();
    }
}
