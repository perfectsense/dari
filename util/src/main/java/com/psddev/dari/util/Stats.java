package com.psddev.dari.util;

import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.AtomicDouble;

/**
 * Gathers statistics about arbitrary operations.
 *
 * <p>To use, first create an instance (usually as a {@code static}):
 *
 * <p><blockquote><pre>
 * private static final Stats STATS = new Stats("Stats Name");
 * </pre></blockquote></p>
 *
 * <p>Then wrap the code you want to measure with the timer methods:
 *
 * <p><blockquote><pre>
 * Stats.Timer timer = STATS.startTimer();
 * try {
 * &nbsp;   ...
 * } finally {
 * &nbsp;   timer.stop("Operation Name");
 * }
 * </pre></blockquote></p>
 *
 * <p>Measurements will automatically be displayed through {@link
 * StatsDebugServlet}, which is typically available at {@code /_debug/stats}.
 */
public class Stats {

    private static final List<WeakReference<Stats>> STATS_REFERENCES = new ArrayList<WeakReference<Stats>>();

    {
        STATS_REFERENCES.add(new WeakReference<Stats>(this));
    }

    private final String name;
    private final double keepDuration;
    private final double measureInterval;
    private final List<Double> averageIntervals;
    private final long start;
    private final Measurement totalMeasurement;

    private final LoadingCache<String, Measurement> measurements = CacheBuilder
            .newBuilder()
            .build(new CacheLoader<String, Measurement>() {

        @Override
        public Measurement load(String operation) {
            return new Measurement();
        }
    });

    /**
     * Creates an instance with the given {@code name} and {@linkplain
     * ExponentialMovingAverage moving averages} with the given
     * {@code averageIntervals}.
     *
     * @param name Must not be blank.
     * @param keepDuration In seconds. Must be positive.
     * @param measureInterval In seconds. Must be positive.
     * @param averageIntervals In seconds. Must not be blank.
     *        Must all be positive.
     * @throws IllegalArgumentException
     */
    public Stats(String name, double keepDuration, double measureInterval, double... averageIntervals) {
        Preconditions.checkArgument(!ObjectUtils.isBlank(name));
        Preconditions.checkArgument(keepDuration > 0);
        Preconditions.checkArgument(measureInterval > 0);
        Preconditions.checkNotNull(averageIntervals);
        Preconditions.checkArgument(averageIntervals.length > 0);

        List<Double> newAverageIntervals = new ArrayList<Double>();
        for (int i = 0, length = averageIntervals.length; i < length; ++ i) {
            double averageInterval = averageIntervals[i];
            Preconditions.checkArgument(averageInterval > 0);
            newAverageIntervals.add(averageInterval);
        }

        this.name = name;
        this.keepDuration = keepDuration;
        this.measureInterval = measureInterval;
        this.averageIntervals = Collections.unmodifiableList(newAverageIntervals);
        this.start = System.currentTimeMillis();
        this.totalMeasurement = getMeasurements().get("Total");
    }

    /**
     * Creates an instance with the given {@code name} with 2 moving averages
     * at 1 and 5 minute intervals.
     *
     * @see #Stats(String, double, double, double...)
     */
    public Stats(String name) {
        this(name, 3600.0, 5.0, 60.0, 600.0);
    }

    /**
     * Returns the name.
     *
     * @return Never blank.
     */
    public String getName() {
        return name;
    }

    /**
     * Returns how long the moving averages should be kept for.
     *
     * @return Always positive.
     */
    public double getKeepDuration() {
        return keepDuration;
    }

    /**
     * Returns how often the moving averages are measured.
     *
     * @return Always positive.
     */
    public double getMeasureInterval() {
        return measureInterval;
    }

    /**
     * Returns all moving average intervals.
     *
     * @return Never blank. All positives.
     */
    public List<Double> getAverageIntervals() {
        return averageIntervals;
    }

    /**
     * Returns how long this stats instance has been gathering data.
     *
     * @return In seconds.
     */
    public double getUptime() {
        return (System.currentTimeMillis() - start) / 1e3;
    }

    /**
     * Starts timing an operation.
     *
     * @return Never {@code null}.
     * @see Timer#stop
     */
    public Timer startTimer() {
        return new Timer();
    }

    /**
     * Returns the total measurement.
     *
     * @return Never {@code null}.
     */
    public Measurement getTotalMeasurement() {
        return totalMeasurement;
    }

    /**
     * Returns the map of all measurements, including the total.
     *
     * @return Never blank.
     */
    public Map<String, Measurement> getMeasurements() {
        return new LoadingCacheMap<String, Measurement>(String.class, measurements);
    }

    /** Timer for measuring the duration of an operation. */
    public class Timer {

        private final long start = System.nanoTime();

        /**
         * Stops timing and names the given {@code operation}.
         *
         * @return Duration in seconds.
         * @see Stats#startTimer
         */
        public double stop(String operation) {
            return stop(operation, 1L);
        }

        /**
         * Stops timing given {@code count} number of events and names the
         * given {@code operation}.
         *
         * @return Duration in seconds.
         * @see Stats#startTimer
         */
        public double stop(String operation, long count) {
            long end = System.nanoTime();
            double duration = (end - start) / 1e9;

            if (duration < 0.0) {
                return 0.0;

            } else {
                getTotalMeasurement().update(end, duration, count);
                getMeasurements().get(operation).update(end, duration, count);
                return duration;
            }
        }
    }

    /**
     * Specific measurement of an operation within {@link Stats}.
     * Most methods may return a {@link Double#NaN} when the measurement
     * isn't available.
     */
    public class Measurement {

        private final AtomicLong totalCount = new AtomicLong();
        private final AtomicDouble totalDuration = new AtomicDouble();
        private final List<ExponentialMovingAverage> countAverages = new ArrayList<ExponentialMovingAverage>();
        private final List<ExponentialMovingAverage> durationAverages = new ArrayList<ExponentialMovingAverage>();

        {
            double keepDuration = getKeepDuration();
            double measureInterval = getMeasureInterval();

            for (double averageInterval : getAverageIntervals()) {
                countAverages.add(new ExponentialMovingAverage(keepDuration, measureInterval, averageInterval));
                durationAverages.add(new ExponentialMovingAverage(keepDuration, measureInterval, averageInterval));
            }
        }

        /** Returns the overall total count. */
        public long getOverallTotalCount() {
            return totalCount.get();
        }

        /** Returns the overall count average. */
        public double getOverallCountAverage() {
            return getOverallTotalCount() / getUptime();
        }

        /** Returns the overall duration average. */
        public double getOverallDurationAverage() {
            return totalDuration.get() / getOverallTotalCount();
        }

        /**
         * Returns the current moving count average with the interval
         * identified by the given {@code intervalIndex}.
         */
        public double getCurrentCountAverage(int intervalIndex) {
            return countAverages.get(intervalIndex).getCurrentAverage();
        }

        /**
         * Returns the current moving duration average with the interval
         * identified by the given {@code intervalIndex}.
         */
        public double getCurrentDurationAverage(int intervalIndex) {
            return durationAverages.get(intervalIndex).getCurrentAverage() / getCurrentCountAverage(intervalIndex);
        }

        /**
         * Returns all count averages.
         *
         * @return Never {@code null}.
         */
        public Iterable<Double> getCountAverages(int intervalIndex, long begin, long end) {
            return countAverages.get(intervalIndex).getAverages().subIterable(begin, end);
        }

        /**
         * Returns all duration averages.
         *
         * @return Never {@code null}.
         */
        public Iterable<Double> getDurationAverages(int intervalIndex, long begin, long end) {
            return new DurationIterable(
                    durationAverages.get(intervalIndex).getAverages().subIterable(begin, end),
                    countAverages.get(intervalIndex).getAverages().subIterable(begin, end));
        }

        /**
         * Updates all count and duration averages based on the given
         * {@code end} and {@code duration}.
         */
        protected void update(long end, double duration) {
            update(end, duration, 1L);
        }

        /**
         * Updates all count and duration averages based on the given
         * {@code end}, {@code duration} and {@code count}.
         */
        protected void update(long end, double duration, long count) {
            totalCount.addAndGet(count);
            totalDuration.addAndGet(duration);

            for (ExponentialMovingAverage countAverage : countAverages) {
                countAverage.updateAt(end, count);
            }

            for (ExponentialMovingAverage durationAverage : durationAverages) {
                durationAverage.updateAt(end, duration);
            }
        }
    }

    private static class DurationIterable implements Iterable<Double> {

        private final Iterable<Double> durations;
        private final Iterable<Double> counts;

        public DurationIterable(Iterable<Double> durations, Iterable<Double> counts) {
            this.durations = durations;
            this.counts = counts;
        }

        @Override
        public Iterator<Double> iterator() {
            return new DurationIterator(durations.iterator(), counts.iterator());
        }
    }

    private static class DurationIterator implements Iterator<Double> {

        private final Iterator<Double> durations;
        private final Iterator<Double> counts;

        public DurationIterator(Iterator<Double> durations, Iterator<Double> counts) {
            this.durations = durations;
            this.counts = counts;
        }

        @Override
        public boolean hasNext() {
            return durations.hasNext();
        }

        @Override
        public Double next() {
            if (hasNext()) {
                return durations.next() / (counts.hasNext() ? counts.next() : 0.0);
            } else {
                throw new NoSuchElementException();
            }
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    /** {@link Stats} utility methods. */
    public static final class Static {

        /**
         * Returns all active stats instances.
         *
         * @return Never {@code null}. Mutable.
         */
        public static List<Stats> getAll() {
            List<Stats> statsInstances = new ArrayList<Stats>();

            for (Iterator<WeakReference<Stats>> i = STATS_REFERENCES.iterator(); i.hasNext();) {
                WeakReference<Stats> ref = i.next();
                Stats stats = ref.get();

                if (stats != null) {
                    statsInstances.add(stats);
                } else {
                    i.remove();
                }
            }

            return statsInstances;
        }
    }
}
