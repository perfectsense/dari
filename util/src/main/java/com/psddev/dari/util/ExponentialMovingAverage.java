package com.psddev.dari.util;

import java.util.concurrent.atomic.AtomicLong;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.AtomicDouble;

/**
 * Exponential moving average.
 *
 * @see <a href="http://en.wikipedia.org/wiki/Moving_average#Exponential_moving_average">Wikipedia</a>
 */
public class ExponentialMovingAverage {

    private final TimeSeries averages;
    private final long tick;
    private final double alpha;

    private final AtomicLong tickOffset;
    private final AtomicDouble total;
    private volatile double currentAverage;

    /**
     * Creates an instance that calculates the average over the given
     * {@code averageInterval} every {@code measureInterval} and keeps
     * them for {@code keepDuration}.
     *
     * @param keepDuration In seconds. Must be positive.
     * @param measureInterval In seconds. Must be positive.
     * @param averageInterval In seconds. Must be positive.
     * @throws IllegalArgumentException
     */
    public ExponentialMovingAverage(double keepDuration, double measureInterval, double averageInterval) {
        Preconditions.checkArgument(keepDuration > 0);
        Preconditions.checkArgument(measureInterval > 0);
        Preconditions.checkArgument(averageInterval > 0);

        averages = new TimeSeries((long) (keepDuration * 1e3), (long) (measureInterval * 1e3));
        tick = (long) (measureInterval * 1e9);
        alpha = 1 - Math.exp(-tick / (averageInterval * 1e9));
        tickOffset = new AtomicLong(System.nanoTime() / tick);
        total = new AtomicDouble();
    }

    // Updates the average if it's past time to measure.
    private void tick(long time) {
        long oldOffset = tickOffset.get();
        long newOffset = time / tick;
        long offsetDiff = newOffset - oldOffset;

        if (offsetDiff > 0L
                && tickOffset.compareAndSet(oldOffset, newOffset)) {
            double oldAverage = total.getAndSet(0.0) / tick;

            for (long i = 0L, size = offsetDiff - 1L; i < size; ++ i) {
                currentAverage += alpha * (0L - currentAverage);
                averages.add(currentAverage * 1e9);
            }

            currentAverage += alpha * (oldAverage - currentAverage);
            averages.add(currentAverage * 1e9);
        }
    }

    /**
     * Measures the average before the given {@code time} and updates
     * it with the given {@code amount}.
     */
    public double updateAt(long time, double amount) {
        tick(time);
        total.addAndGet(amount);
        return currentAverage * 1e9;
    }

    /** Updates the average with the given {@code amount}. */
    public double update(double amount) {
        return updateAt(System.nanoTime(), amount);
    }

    /** Returns the current average. */
    public double getCurrentAverage() {
        tick(System.nanoTime());
        return currentAverage * 1e9;
    }

    /** Returns all averages. */
    public TimeSeries getAverages() {
        tick(System.nanoTime());
        return averages;
    }
}
