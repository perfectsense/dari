package com.psddev.dari.db;

import com.psddev.dari.util.CompactMap;
import com.psddev.dari.util.Task;
import com.psddev.dari.util.UuidUtils;
import org.joda.time.DateTime;

import java.sql.SQLException;
import java.util.Map;
import java.util.UUID;

class NullMetricAccess extends MetricAccess {

    public NullMetricAccess(UUID typeId, ObjectField field, MetricInterval interval) {
        super(null, typeId, field, interval);
    }

    @Override
    public int getSymbolId() {
        return -1;
    }

    @Override
    public long getEventDate(DateTime time) {
        long now = Database.Static.getDefault().now();

        if (time == null) {
            time = new DateTime();

        } else if (time.getMillis() > now) {
            throw new RuntimeException("Metric.eventDate may not be a date in the future.");
        }

        return getEventDateProcessor().process(time);
    }

    @Override
    public DateTime getLastUpdate(UUID id, String dimensionValue) throws SQLException {
        return null;
    }

    @Override
    public DateTime getFirstUpdate(UUID id, String dimensionValue) throws SQLException {
        return null;
    }

    @Override
    public Double getMetric(UUID id, String dimensionValue, Long startTimestamp, Long endTimestamp) throws SQLException {
        return 0.0;
    }

    @Override
    public Double getMetricSum(UUID id, Long startTimestamp, Long endTimestamp) throws SQLException {
        return 0.0;
    }

    @Override
    public Map<String, Double> getMetricValues(UUID id, Long startTimestamp, Long endTimestamp) throws SQLException {
        return new CompactMap<>();
    }

    @Override
    public Map<DateTime, Double> getMetricTimeline(UUID id, String dimensionValue, Long startTimestamp, Long endTimestamp, MetricInterval metricInterval) throws SQLException {
        return new CompactMap<>();
    }

    @Override
    public void incrementMetric(UUID id, DateTime time, String dimensionValue, Double amount) throws SQLException {
    }

    @Override
    public void incrementMetricByDimensionId(UUID id, DateTime time, UUID dimensionId, Double amount) throws SQLException {
    }

    @Override
    public void setMetric(UUID id, DateTime time, String dimensionValue, Double amount) throws SQLException {
    }

    @Override
    public void setMetricByDimensionId(UUID id, DateTime time, UUID dimensionId, Double amount) throws SQLException {
    }

    @Override
    public void deleteMetric(UUID id) throws SQLException {
    }

    @Override
    public void reconstructCumulativeAmounts(UUID id) throws SQLException {
    }

    @Override
    public void resummarize(UUID id, UUID dimensionId, MetricInterval interval, Long startTimestamp, Long endTimestamp) throws SQLException {
    }

    @Override
    public Task submitResummarizeAllTask(MetricInterval interval, Long startTimestamp, Long endTimestamp, int numParallel, String executor, String name) {
        return null;
    }

    @Override
    public UUID getDimensionId(String dimensionValue) throws SQLException {
        return dimensionValue == null || dimensionValue.equals("")
                ? UuidUtils.ZERO_UUID
                : UuidUtils.createVersion3Uuid(dimensionValue);
    }
}
