package com.psddev.dari.db;

/** Location in a two-dimensional space. */
public class Location {

    public static final int MEAN_RADIUS_OF_EARTH_IN_MILES = 3959;
    public static final int MEAN_RADIUS_OF_EARTH_IN_KILOMETERS = 6371;
    public static final int MEAN_RADIUS_OF_EARTH_IN_NAUTICAL_MILES = 3440;

    private double x;
    private double y;

    /** Creates an instance with the given {@code x} and {@code y}. */
    public Location(double x, double y) {
        this.x = x;
        this.y = y;
    }

    /** Returns the x-coordinate. */
    public double getX() {
        return x;
    }

    /** Sets the x-coordinate. */
    public void setX(double x) {
        this.x = x;
    }

    /** Returns the y-coordinate. */
    public double getY() {
        return y;
    }

    /** Sets the y-coordinate. */
    public void setY(double y) {
        this.y = y;
    }

    /**
     * Returns the distance between this location and the given {@code other}.
     */
    public double getDistance(Location other) {
        return Math.hypot(getX() - other.getX(), getY() - other.getY());
    }

    /**
     * Returns the <a href="http://en.wikipedia.org/wiki/Great-circle_distance"
     * >great-circle distance</a> between this location and the given
     * {@code other}.
     */
    public double getGreatCircleDistance(Location other) {
        double x1 = rad(getX());
        double y1 = rad(getY());
        double x2 = rad(other.getX());
        double y2 = rad(other.getY());
        return Math.acos(
                Math.cos(x1) * Math.cos(x2) * Math.cos(y2 - y1)
                        + Math.sin(x1) * Math.sin(x2));
    }

    private double rad(double degree) {
        return degree * Math.PI / 180.0;
    }

    /**
     * Returns the well-known text representation of this location.
     *
     * @see <a href="https://en.wikipedia.org/wiki/Well-known_text">WKT</a>
     */
    public String toWkt() {
        return "POINT(" + getX() + " " + getY() + ")";
    }

    // --- Object support ---

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        } else if (other instanceof Location) {
            Location otherLocation = (Location) other;
            return getX() == otherLocation.getX() && getY() == otherLocation.getY();
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return (getX() + "\0" + getY()).hashCode();
    }

    @Override
    public String toString() {
        return "{x=" + getX() + ", y=" + getY() + "}";
    }
}
