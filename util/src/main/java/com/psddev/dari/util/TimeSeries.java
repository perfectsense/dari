package com.psddev.dari.util;

import com.google.common.base.Preconditions;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Bounded series of data points occurring at a predictable interval.
 * While the type argument is a non-primitive {@code Double}, no methods
 * in this class will accept or return a {@code null}.
 */
public class TimeSeries implements Iterable<Double> {

    private final long createTime;
    private final long interval;
    private final int capacity;
    private final double[] points;
    private int seriesIndex;
    private int pointIndex;
    private int size;

    /**
     * Creates an instance that will keep data occuring at the given
     * {@code interval} for the given {@code duration}.
     *
     * @param duration Must be positive.
     * @param interval Must be positive.
     * @throws IllegalArgumentException
     */
    public TimeSeries(long duration, long interval) {
        Preconditions.checkArgument(duration > 0);
        Preconditions.checkArgument(interval > 0);

        this.createTime = System.currentTimeMillis();
        this.interval = interval;
        this.capacity = (int) (duration / interval);
        this.points = new double[capacity];
    }

    /** Adds the given {@code number} to this series. */
    public void add(double number) {
        points[pointIndex] = number;
        ++ seriesIndex;
        ++ pointIndex;
        if (pointIndex >= capacity) {
            pointIndex = 0;
        }
        if (size < capacity) {
            ++ size;
        }
    }

    /**
     * Returns an iterable over the data points within the given
     * {@code fromTime} and {@code toTime}.
     */
    public Iterable<Double> subIterable(long fromTime, long toTime) {
        return new SnapshotIterable(fromTime, toTime);
    }

    private class SnapshotIterable implements Iterable<Double> {

        private final int iteratorStart;
        private final int iteratorSize;

        public SnapshotIterable(long fromTime, long toTime) {
            long fromDiff = fromTime - createTime;
            long fromIndex = fromDiff / interval;

            if (fromDiff % interval > 0) {
                ++ fromIndex;
            }
            if (fromIndex < 0) {
                fromIndex = 0;
            }

            long toDiff = toTime - createTime;
            long toIndex = toDiff / interval;

            if (toDiff % interval > 0) {
                ++ toIndex;
            }
            if (toIndex > seriesIndex) {
                toIndex = seriesIndex;
            }

            iteratorStart = (int) (((size == capacity ? pointIndex : 0) + fromIndex) % capacity);
            iteratorSize = (int) (toIndex - fromIndex) - 1;
        }

        @Override
        public Iterator<Double> iterator() {
            return new SnapshotIterator(iteratorStart, iteratorSize);
        }
    }

    // --- Iterable support ---

    @Override
    public Iterator<Double> iterator() {
        return new SnapshotIterator(size == capacity ? pointIndex : 0, size);
    }

    private class SnapshotIterator implements Iterator<Double> {

        private final int start;
        private final int size;
        private int startOffset;

        public SnapshotIterator(int start, int size) {
            this.start = start;
            this.size = size;
        }

        @Override
        public boolean hasNext() {
            return startOffset < size;
        }

        @Override
        public Double next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }

            int index = start + startOffset;
            if (index >= capacity) {
                index -= capacity;
            }

            double number = points[index];
            ++ startOffset;
            return number;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }
}
