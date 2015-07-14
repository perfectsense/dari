package com.psddev.dari.util;

import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Holds a value that will periodically update itself. */
public abstract class PeriodicValue<T> {

    private static final Logger LOGGER = LoggerFactory.getLogger(PeriodicValue.class);

    public static final double DEFAULT_INITIAL_DELAY = 0.0;
    public static final double DEFAULT_INTERVAL = 5.0;

    private volatile T value;
    private final Task task;
    private volatile Date updateDate = new Date(0);

    /**
     * Creates an instance that will update every given {@code interval}
     * after the given {@code initialDelay} (in seconds).
     */
    public PeriodicValue(double initialDelay, double interval) {

        if (initialDelay == 0.0) {
            refresh();
            initialDelay = interval;
        }

        task = new Task(PeriodicCache.TASK_EXECUTOR_NAME, getClass().getName()) {
            @Override
            public void doTask() {
                refresh();
            }
        };

        task.scheduleAtFixedRate(initialDelay, interval);
    }

    /**
     * Creates an instance that will update every given {@code interval}
     * (in seconds).
     */
    public PeriodicValue(double interval) {
        this(DEFAULT_INITIAL_DELAY, interval);
    }

    /**
     * Creates an instance that will update every {@link #DEFAULT_INTERVAL}
     * seconds.
     */
    public PeriodicValue() {
        this(DEFAULT_INITIAL_DELAY, DEFAULT_INTERVAL);
    }

    /** Returns the value. */
    public T get() {
        return value;
    }

    /** Returns the task used to update the value. */
    public Task getTask() {
        return task;
    }

    /** Returns the last time that this value was updated. */
    public Date getUpdateDate() {
        return updateDate;
    }

    /** Returns a value that will replace the existing value. */
    protected abstract T update();

    /** Refreshes the value immediately. */
    public synchronized void refresh() {

        T oldValue = value;
        value = update();
        updateDate = new Date();

        if (LOGGER.isDebugEnabled()
                && !ObjectUtils.equals(oldValue, value)) {
            LOGGER.debug("Changed from [{}] to [{}]", oldValue, value);
        }
    }
}
