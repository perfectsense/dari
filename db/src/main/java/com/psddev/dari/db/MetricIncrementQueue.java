package com.psddev.dari.db;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.psddev.dari.util.ObjectUtils;
import com.psddev.dari.util.Task;

final class MetricIncrementQueue {

    //private static final Logger LOGGER = LoggerFactory.getLogger(MetricIncrementQueue.class);

    private static final ConcurrentHashMap<Double, ConcurrentHashMap<String, QueuedMetricIncrement>> QUEUED_INCREMENTS = new ConcurrentHashMap<Double, ConcurrentHashMap<String, QueuedMetricIncrement>>();

    public static void queueIncrement(UUID id, UUID dimensionId, DateTime eventDate, MetricAccess metricAccess, double amount, double withinSeconds) {

        double waitSeconds = new BigDecimal(withinSeconds * .75d).setScale(2).doubleValue();
        double executeSeconds = new BigDecimal(withinSeconds * .25d).setScale(2).doubleValue();

        putInMap(id, dimensionId, eventDate, metricAccess, amount, waitSeconds);

        // If the task is already running or has been scheduled, this won't do anything.
        MetricIncrementQueueTask task = MetricIncrementQueueTask.getInstance(executeSeconds, waitSeconds, QUEUED_INCREMENTS.get(waitSeconds));
        task.schedule(waitSeconds);

    }

    private static void putInMap(UUID id, UUID dimensionId, DateTime eventDate, MetricAccess metricAccess, double amount, double waitSeconds) {

        ConcurrentHashMap<String, QueuedMetricIncrement> queue = QUEUED_INCREMENTS.get(waitSeconds);
        if (queue == null) {
            QUEUED_INCREMENTS.putIfAbsent(waitSeconds, new ConcurrentHashMap<String, QueuedMetricIncrement>());
            queue = QUEUED_INCREMENTS.get(waitSeconds);
        }

        String key = getKey(id, dimensionId, eventDate, metricAccess);
        QueuedMetricIncrement placeholder = new QueuedMetricIncrement(id, dimensionId, eventDate, metricAccess, 0d);
        while (true) {
            QueuedMetricIncrement current = queue.putIfAbsent(key, placeholder);
            if (current == null) {
                current = placeholder;
            }
            QueuedMetricIncrement next = new QueuedMetricIncrement(id, dimensionId, eventDate, metricAccess, current.amount + amount);
            if (queue.replace(key, current, next)) {
                return;
            } else {
                continue;
            }
        }

    }

    private static String getKey(UUID id, UUID dimensionId, DateTime eventDate, MetricAccess metricAccess) {
        StringBuilder str = new StringBuilder();
        str.append(id);
        str.append(':');
        str.append(metricAccess.getTypeId());
        str.append(':');
        str.append(metricAccess.getSymbolId());
        str.append(':');
        if (eventDate != null) {
            str.append(eventDate.getMillis());
        }
        str.append(':');
        str.append(dimensionId);
        return str.toString();
    }

}

class QueuedMetricIncrement {
    public final UUID id;
    public final UUID dimensionId;
    public final DateTime eventDate;
    public final MetricAccess metricAccess;
    public final double amount;

    public QueuedMetricIncrement(UUID id, UUID dimensionId, DateTime eventDate, MetricAccess metricAccess, double amount) {
        this.id = id;
        this.dimensionId = dimensionId;
        this.eventDate = eventDate;
        this.metricAccess = metricAccess;
        this.amount = amount;
    }

    @Override
    public boolean equals(Object other) {
        if (other == null || !(other instanceof QueuedMetricIncrement)) {
            return false;
        } else {
            QueuedMetricIncrement otherIncr = (QueuedMetricIncrement) other;
            return this.amount == otherIncr.amount
                    && this.metricAccess.equals(otherIncr.metricAccess)
                    && this.dimensionId.equals(otherIncr.dimensionId)
                    && ((this.eventDate == null && otherIncr.eventDate == null) || (this.eventDate != null && this.eventDate.equals(otherIncr.eventDate)))
                    && this.id.equals(otherIncr.id);
        }
    }

    @Override
    public int hashCode() {
        return ObjectUtils.hashCode(id, dimensionId, eventDate, metricAccess, amount);
    }
}

final class MetricIncrementQueueTask extends Task {
    private static final Logger LOGGER = LoggerFactory.getLogger(MetricIncrementQueue.class);
    //private static MetricIncrementQueueTask instance;
    private static final transient ConcurrentHashMap<Double, MetricIncrementQueueTask> INSTANCES = new ConcurrentHashMap<Double, MetricIncrementQueueTask>();

    private final transient ConcurrentHashMap<String, QueuedMetricIncrement> queuedIncrements;

    private final transient double executeSeconds;
    private final transient double waitSeconds;

    private MetricIncrementQueueTask(double executeSeconds, double waitSeconds, ConcurrentHashMap<String, QueuedMetricIncrement> queuedIncrements) {
        this.queuedIncrements = queuedIncrements;
        this.executeSeconds = executeSeconds;
        this.waitSeconds = waitSeconds;
    }

    public static MetricIncrementQueueTask getInstance(double executeSeconds, double waitSeconds, ConcurrentHashMap<String, QueuedMetricIncrement> queuedIncrements) {

        MetricIncrementQueueTask instance = INSTANCES.get(executeSeconds);
        if (instance == null) {
            INSTANCES.putIfAbsent(executeSeconds, new MetricIncrementQueueTask(executeSeconds, waitSeconds, queuedIncrements));
            instance = INSTANCES.get(executeSeconds);
        }

        return instance;
    }

    public void doTask() {

        long sleepMilliseconds = 0L;
        Iterator<String> iter = null;
        while (true) {
            if (queuedIncrements.isEmpty()) {
                break;
            }
            if (iter == null || !iter.hasNext()) {
                if (iter != null) {
                    long waitMilliseconds = (long) (1000 * waitSeconds);
                    try {
                        Thread.sleep(waitMilliseconds);
                    } catch (InterruptedException ex) {
                        getThread().interrupt();
                    }
                }
                sleepMilliseconds = (long) (1000 * (executeSeconds / (double) queuedIncrements.size())) - 10L;
                if (sleepMilliseconds <= 0) {
                    sleepMilliseconds = 10L;
                }
                //LOGGER.info("running MetricIncrementQueueTask within " + executeSeconds + " seconds, approx. size: " + queuedIncrements.size() + " sleeping for " + sleepMilliseconds + " milliseconds between executions");
                iter = queuedIncrements.keySet().iterator();
            }
            String key = iter.next();
            QueuedMetricIncrement queuedIncrement = queuedIncrements.remove(key);
            //LOGGER.info("Incrementing : " + queuedIncrement.metricAccess.getSymbolId() + " / " + queuedIncrement.id + " : " + queuedIncrement.dimensionId + " += " + queuedIncrement.amount );
            try {
                queuedIncrement.metricAccess.incrementMetricByDimensionId(queuedIncrement.id, queuedIncrement.eventDate, queuedIncrement.dimensionId, queuedIncrement.amount);
            } catch (SQLException ex) {
                LOGGER.error("SQLException during incrementMetricByDimensionId: " + ex.getLocalizedMessage());
                // TODO: log this somewhere so it can be recovered if the database fails catastrophically
                throw new DatabaseException(queuedIncrement.metricAccess.getDatabase(), "SQLException during MetricAccess.incrementMetricByDimensionId", ex);
            }
            try {
                Thread.sleep(sleepMilliseconds);
            } catch (InterruptedException ex) {
                getThread().interrupt();
            }
        }

    }

}
