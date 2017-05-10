package com.psddev.dari.db;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.psddev.dari.util.ObjectUtils;
import com.psddev.dari.util.Task;

@Metric.Embedded
public class Metric extends Record {

    private static final Logger LOGGER = LoggerFactory.getLogger(Metric.class);

    private final transient State owner;
    private final transient ObjectField field;
    private transient MetricAccess metricAccess;

    /**
     * @param owner Can't be {@code null}.
     * @param field Can't be {@code null}.
     */
    public Metric(State owner, ObjectField field) {
        this.owner = owner;
        this.field = field;
    }

    /**
     * Returns the state that owns this metric instance.
     *
     * @return Never {@code null}.
     */
    public State getOwner() {
        return owner;
    }

    /**
     * Returns the field that this metric instance updates.
     *
     * @return Never {@code null}.
     */
    public ObjectField getField() {
        return field;
    }

    /**
     * Returns the MetricAccess or throw an exception if it could not find the SQL database.
     *
     */
    private MetricAccess getMetricAccess() {
        if (metricAccess == null) {
            if (getOwner() != null) {
                metricAccess = MetricAccess.Static.getMetricAccess(getOwner().getDatabase(), getOwner().getType(), field);
            }
        }
        if (metricAccess == null) {
            throw new RuntimeException(
                    "Metric field "
                            + field.getUniqueName()
                            + " cannot determine SQL database for database "
                            + owner.getDatabase().getName()
                            + " ("
                            + owner.getDatabase().getClass().getName()
                            + "), this Metric object is unusable!");
        }
        return metricAccess;
    }

    /**
     * Increases the metric value by the given {@code amount}.
     */
    public void increment(double amount) {
        incrementDimensionAt(amount, null, null);
    }

    /**
     * Increases the metric value by the given {@code amount} and associate it
     * with the given {@code dimension} and {@code time}.
     *
     * @param dimension May be {@code null}.
     * @param time If {@code null}, right now.
     */
    public void incrementDimensionAt(double amount, String dimension, DateTime time) {
        try {
            getMetricAccess().incrementMetric(owner.getId(), time, dimension, amount);
        } catch (SQLException e) {
            throw new DatabaseException(getMetricAccess().getDatabase(), "Error in MetricAccess.incrementMetric() : " + e.getLocalizedMessage());
        }
    }

    /**
     * Asynchronously (within the given {@code within} seconds) increases the
     * metric value by the given {@code amount} and associate it with the given
     * {@code dimension} and {@code time}.
     *
     * @param dimension May be {@code null}.
     * @param time May be {@code null}.
     * @param within In seconds. If less than or equal to {@code 0}, uses
     * {@code 1.0} instead.
     */
    public void incrementEventually(double amount, String dimension, DateTime time, double within) {
        if (within <= 0) {
            within = 1.0;
        }
        UUID dimensionId;
        try {
            dimensionId = getMetricAccess().getDimensionId(dimension);
        } catch (SQLException e) {
            throw new DatabaseException(getMetricAccess().getDatabase(), "Error in MetricAccess.getDimensionId() : " + e.getLocalizedMessage());
        }
        MetricIncrementQueue.queueIncrement(getOwner().getId(), dimensionId, time, getMetricAccess(), amount, within);
    }

    /**
     * Returns when the metric value associated with the given
     * {@code dimension} was last updated.
     *
     * @param dimension May be {@code null}.
     * @return May be {@code null}.
     */
    public DateTime getLastDimensionUpdate(String dimension) {
        try {
            Static.preFetchMetrics(getOwner(), getMetricAccess().getDimensionId(dimension), null, null);
            return getMetricAccess().getLastUpdate(getOwner().getId(), dimension);
        } catch (SQLException e) {
            throw new DatabaseException(getMetricAccess().getDatabase(), "Error in MetricAccess.getLastUpdate() : " + e.getLocalizedMessage());
        }
    }

    /**
     * Returns when the metric value was last updated.
     *
     * @return May be {@code null}.
     */
    public DateTime getLastUpdate() {
        return getLastDimensionUpdate(null);
    }

    /**
     * Returns when the metric value associated with the given
     * {@code dimension} was first updated.
     *
     * @param dimension May be {@code null}.
     * @return May be {@code null}.
     */
    public DateTime getFirstDimensionUpdate(String dimension) {
        try {
            Static.preFetchMetrics(getOwner(), getMetricAccess().getDimensionId(dimension), null, null);
            return getMetricAccess().getFirstUpdate(getOwner().getId(), dimension);
        } catch (SQLException e) {
            throw new DatabaseException(getMetricAccess().getDatabase(), "Error in MetricAccess.getLastUpdate() : " + e.getLocalizedMessage());
        }
    }

    /**
     * Returns when the metric value was first updated.
     *
     * @return May be {@code null}.
     */
    public DateTime getFirstUpdate() {
        return getFirstDimensionUpdate(null);
    }

    /**
     * Sets the metric value to the given {@code amount} and associates it with
     * the given {@code dimension} and {@code time}.
     *
     * @param dimension May be {@code null}.
     * @param time If {@code null}, right now.
     */
    public void setDimensionAt(double amount, String dimension, DateTime time) {
        try {
            getMetricAccess().setMetric(getOwner().getId(), time, dimension, amount);
        } catch (SQLException e) {
            throw new DatabaseException(getMetricAccess().getDatabase(), "Error in MetricAccess.setMetric() : " + e.getLocalizedMessage());
        }
    }

    /** Deletes all metric values. */
    public void deleteAll() {
        try {
            getMetricAccess().deleteMetric(getOwner().getId());
        } catch (SQLException e) {
            throw new DatabaseException(getMetricAccess().getDatabase(), "Error in MetricAccess.deleteMetric() : " + e.getLocalizedMessage());
        }
    }

    /**
     * Returns the metric value associated with the given {@code dimension}
     * between the given {@code start} and {@code end}.
     *
     * @param dimension May be {@code null}.
     * @param start If {@code null}, beginning of time.
     * @param end If {@code null}, end of time.
     */
    public double getByDimensionBetween(String dimension, DateTime start, DateTime end) {
        try {
            Long startTimestamp = (start == null ? null : start.getMillis());
            Long endTimestamp = (end == null ? null : end.getMillis());
            Static.preFetchMetrics(getOwner(), getMetricAccess().getDimensionId(dimension), startTimestamp, endTimestamp);
            Double metricValue = getMetricAccess().getMetric(getOwner().getId(), dimension, startTimestamp, endTimestamp);
            return metricValue == null ? 0.0 : metricValue;
        } catch (SQLException e) {
            throw new DatabaseException(getMetricAccess().getDatabase(), "Error in MetricAccess.getMetric() : " + e.getLocalizedMessage());
        }
    }

    /**
     * Returns the metric value associated with the given {@code dimension}.
     *
     * @param dimension May be {@code null}.
     */
    public double getByDimension(String dimension) {
        return getByDimensionBetween(dimension, null, null);
    }

    /**
     * Returns the sum of all metric values in each dimension between the given
     * {@code start} and {@code end}.
     *
     * @param start If {@code null}, beginning of time.
     * @param end If {@code null}, end of time.
     */
    public double getSumBetween(DateTime start, DateTime end) {
        return getByDimensionBetween(null, start, end);
    }

    /**
     * Returns the sum of all metric values in each dimension.
     */
    public double getSum() {
        return getSumBetween(null, null);
    }

    /**
     * Returns true if the Metric has no data for the given dimension
     * between the given {@code start} and {@code end}. Note that {@link #incrementDimensionAt}
     * does <b>not</b> insert a row where there is none, but {@link #setDimensionAt} does.
     *
     * @param dimension May be {@code null}.
     * @param start If {@code null}, beginning of time.
     * @param end If {@code null}, end of time.
     */
    public boolean isEmptyByDimensionBetween(String dimension, DateTime start, DateTime end) {
        try {
            Long startTimestamp = (start == null ? null : start.getMillis());
            Long endTimestamp = (end == null ? null : end.getMillis());
            Static.preFetchMetrics(getOwner(), getMetricAccess().getDimensionId(dimension), startTimestamp, endTimestamp);
            Double metricValue = getMetricAccess().getMetric(getOwner().getId(), dimension, startTimestamp, endTimestamp);
            return metricValue == null;
        } catch (SQLException e) {
            throw new DatabaseException(getMetricAccess().getDatabase(), "Error in MetricAccess.getMetric() : " + e.getLocalizedMessage());
        }
    }

    /**
     * Returns true if the Metric has no data for the default dimension over all time.
     */
    public boolean isEmpty() {
        return isEmptyByDimensionBetween(null, null, null);
    }

    /**
     * Groups the metric values between the given {@code start} and {@code end}
     * by each dimension.
     *
     * @param start If {@code null}, beginning of time.
     * @param end If {@code null}, end of time.
     * @return Never {@code null}.
     */
    public Map<String, Double> groupByDimensionBetween(DateTime start, DateTime end) {
        try {
            Long startTimestamp = (start == null ? null : start.getMillis());
            Long endTimestamp = (end == null ? null : end.getMillis());
            Map<String, Double> metricValues = getMetricAccess().getMetricValues(getOwner().getId(), startTimestamp, endTimestamp);
            return metricValues;
        } catch (SQLException e) {
            throw new DatabaseException(getMetricAccess().getDatabase(), "Error in MetricAccess.getMetricValues() : " + e.getLocalizedMessage());
        }
    }

    /**
     * Groups the metric values by each dimension.
     *
     * @return Never {@code null}.
     */
    public Map<String, Double> groupByDimension() {
        return groupByDimensionBetween(null, null);
    }

    /**
     * Groups the metric values associated with the given {@code dimension}
     * between the given {@code start} and {@code end} by the given
     * {@code interval}.
     *
     * @param dimension May be {@code null}.
     * @param start If {@code null}, beginning of time.
     * @param end If {@code null}, end of time.
     * @return Never {@code null}.
     */
    public Map<DateTime, Double> groupByDate(String dimension, MetricInterval interval, DateTime start, DateTime end) {
        try {
            Long startTimestamp = (start == null ? null : start.getMillis());
            Long endTimestamp = (end == null ? null : end.getMillis());
            Map<DateTime, Double> metricTimeline = getMetricAccess().getMetricTimeline(getOwner().getId(), dimension, startTimestamp, endTimestamp, interval);
            return metricTimeline;
        } catch (SQLException e) {
            throw new DatabaseException(getMetricAccess().getDatabase(), "Error in MetricAccess.getMetricTimeline() : " + e.getLocalizedMessage());
        }
    }

    /**
     * Groups the sum of all metric values between the given {@code start} and
     * {@code end} by the given {@code interval}.
     *
     * @param start If {@code null}, beginning of time.
     * @param end If {@code null}, end of time.
     * @return Never {@code null}.
     */
    public Map<DateTime, Double> groupSumByDate(MetricInterval interval, DateTime start, DateTime end) {
        return groupByDate(null, interval, start, end);
    }

    /**
     * Repairs the cumulativeAmount value for all rows since the beginning of
     * time. This method takes a long time to complete and should only be used
     * if something has gone wrong.
     *
     */
    public void repair() {
        try {
            getMetricAccess().reconstructCumulativeAmounts(getOwner().getId());
        } catch (SQLException e) {
            throw new DatabaseException(getMetricAccess().getDatabase(), "Error in MetricAccess.reconstructCumulativeAmounts() : " + e.getLocalizedMessage());
        }
    }

    /**
     * Resummarize the metric value to a new interval.
     *
     * @param interval Can't be {@code null}.
     * @param dimensionValue If {@code null}, default dimension.
     * @param start If {@code null}, beginning of time.
     * @param end If {@code null}, end of time.
     */
    public void resummarize(MetricInterval interval, String dimensionValue, DateTime start, DateTime end) {
        try {
            Long startTimestamp = (start == null ? null : start.getMillis());
            Long endTimestamp = (end == null ? null : end.getMillis());
            getMetricAccess().resummarize(getOwner().getId(), getMetricAccess().getDimensionId(dimensionValue), interval, startTimestamp, endTimestamp);
        } catch (SQLException e) {
            throw new DatabaseException(getMetricAccess().getDatabase(), "Error in MetricAccess.resummarize() : " + e.getLocalizedMessage());
        }
    }

    /**
     * Returns the metric value associated with the main ({@code null})
     * dimension between the given {@code start} and {@code end}.
     *
     * @deprecated Use {@link #getSumBetween} instead.
     */
    @Deprecated
    public double getValue(DateTime start, DateTime end) {
        return getByDimensionBetween(null, start, end);
    }

    /**
     * Returns the metric value associated with the main ({@code null})
     * dimension between the given {@code start} and {@code end}.
     * @deprecated Use {@link #getSumBetween} instead
     */
    @Deprecated
    public double getValueBetween(DateTime start, DateTime end) {
        return getByDimensionBetween(null, start, end);
    }

    /**
     * Returns the metric value associated with the main ({@code null})
     * dimension.
     * @deprecated Use {@link #getSum} instead.
     */
    @Deprecated
    public double getValue() {
        return getByDimensionBetween(null, null, null);
    }

    public static class FieldData extends Modification<ObjectField> {

        public MetricInterval getEventDateProcessor() {
            return getOriginalObject().as(MetricAccess.FieldData.class).getEventDateProcessor();
        }
    }

    public static class DistinctIds {
        public final UUID id;
        public final UUID typeId;
        public final UUID dimensionId;

        public DistinctIds(UUID id, UUID typeId, UUID dimensionId) {
            this.id = id;
            this.typeId = typeId;
            this.dimensionId = dimensionId;
        }
    }

    public static class Static {

        private static final String EXTRA_METRICS_FETCHED_PREFIX = "dari.metric.preFetched.";

        /**
         * Resummarize all metric values in the given field (all dimensions)
         * within a date range to a new interval. This submits a Task to be
         * executed asynchronously.
         *
         * @param database Can't be {@code null}.
         * @param type Can't be {@code null}.
         * @param field Can't be {@code null}.
         * @param interval Can't be {@code null}.
         * @param start If {@code null}, beginning of time.
         * @param end If {@code null}, end of time.
         * @param parallel Number of tasks to run in parallel. If {@code null}, 1.
         *
         */
        public static Task submitResummarizeAllBetweenTask(Database database, ObjectType type, ObjectField field, MetricInterval interval, DateTime start, DateTime end, Integer parallel, String executor, String name) {
            Long startTimestamp = (start == null ? null : start.getMillis());
            Long endTimestamp = (end == null ? null : end.getMillis());
            MetricAccess mdb = MetricAccess.Static.getMetricAccess(database, type, field);
            if (parallel == null || parallel < 1) {
                parallel = 1;
            }
            return mdb.submitResummarizeAllTask(interval, startTimestamp, endTimestamp, parallel, executor, name);
        }

        private static void preFetchMetrics(State state, UUID dimensionId, Long startTimestamp, Long endTimestamp) {
            if (state == null || state.getType() == null) {
                return;
            }
            String extraKey = EXTRA_METRICS_FETCHED_PREFIX + ObjectUtils.to(String.class, dimensionId) + '.' + ObjectUtils.to(String.class, startTimestamp) + '.' + ObjectUtils.to(String.class, endTimestamp);
            if (Boolean.TRUE.equals(state.getExtra(extraKey))) {
                return;
            }
            state.getExtras().put(extraKey, true);
            List<ObjectField> fields = new ArrayList<ObjectField>(state.getType().getMetricFields());
            fields.addAll(state.getDatabase().getEnvironment().getMetricFields());
            Set<MetricAccess> metricAccesses = new HashSet<MetricAccess>();
            for (ObjectField field : fields) {
                MetricAccess mdb = MetricAccess.Static.getMetricAccess(state.getDatabase(), state.getType(), field);
                if (mdb != null) {
                    metricAccesses.add(mdb);
                }
            }
            doDatabasePreFetch(state.getId(), dimensionId, startTimestamp, endTimestamp, metricAccesses);
        }

        private static void doDatabasePreFetch(UUID id, UUID dimensionId, Long startTimestamp, Long endTimestamp, Collection<MetricAccess> metricAccesses) {
            if (metricAccesses.isEmpty()) {
                return;
            }
            try {
                MetricAccess.Static.preFetchMetricSums(id, dimensionId, startTimestamp, endTimestamp, metricAccesses, false);
            } catch (SQLException ex) {
                LOGGER.warn("Exception when prefetching Metrics for object " + id + ": " + ex.getLocalizedMessage());
            }
        }

        public static Iterator<DistinctIds> getDistinctIdsBetween(Database database, ObjectType type, ObjectField field, DateTime start, DateTime end) {
            Long startTimestamp = (start == null ? null : start.getMillis());
            Long endTimestamp = (end == null ? null : end.getMillis());
            MetricAccess mdb = MetricAccess.Static.getMetricAccess(database, null, field);
            MetricAccessDatabase mdbDatabase = mdb.getDatabase();

            if (mdbDatabase == null) {
                return Collections.emptyIterator();
            }

            return MetricAccess.Static.getDistinctIds(mdbDatabase, type != null ? type.getId() : null, mdb.getSymbolId(), startTimestamp, endTimestamp);
        }
    }

    /**
     * @deprecated This constructor creates an invalid object and should never be used; it only exists for the benefit of TypeDefinition#newInstance()
     */
    @Deprecated
    public Metric() {
        this.field = null;
        this.owner = null;
    }

}
