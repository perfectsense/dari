package com.psddev.dari.util;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

/**
 * Skeletal runnable implementation that contains basic execution control
 * and status methods. Subclasses must implement:
 *
 * <ul>
 * <li>{@link #doTask}
 * </ul>
 */
public abstract class Task implements Comparable<Task>, Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(Task.class);

    private final TaskExecutor executor;
    private final String name;

    private final AtomicReference<Future<?>> future = new AtomicReference<Future<?>>();
    private final AtomicBoolean isRunning = new AtomicBoolean();
    private final AtomicBoolean isPauseRequested = new AtomicBoolean();
    private final AtomicBoolean isStopRequested = new AtomicBoolean();
    private final AtomicBoolean isSafeToStop = new AtomicBoolean();
    private final ThreadLocal<Boolean> isRunCounted = new ThreadLocal<Boolean>();
    private volatile Thread thread;
    private volatile long lastRunBegin = -1;
    private volatile long lastRunEnd = -1;
    private volatile String progress;
    private volatile long progressIndex;
    private volatile long progressTotal = -1;
    private volatile Throwable lastException;
    private final AtomicLong runCount = new AtomicLong();

    /**
     * Creates an instance that will run in the given
     * {@code initialExecutor} with the given {@code initialName}.
     *
     * @param initialExecutor If {@code null}, this task will run in the
     *        default executor.
     * @param initialName If blank, this task will be named based on its
     *        class name and the count of all instances created so far.
     */
    protected Task(String initialExecutor, String initialName) {
        executor = TaskExecutor.Static.getInstance(initialExecutor);
        if (ObjectUtils.isBlank(initialName)) {
            initialName = getClass().getName();
        }
        name = initialName + " #" + TASK_INDEXES.getUnchecked(initialName).incrementAndGet();
    }

    private static final LoadingCache<String, AtomicLong> TASK_INDEXES = CacheBuilder
            .newBuilder()
            .build(new CacheLoader<String, AtomicLong>() {

        @Override
        public AtomicLong load(String name) {
            return new AtomicLong();
        }
    });

    /**
     * Creates an instance that will run in the default executor
     * anonymously.
     */
    protected Task() {
        this(null);
    }

    /**
     * Returns the executor that will run this task.
     *
     * @return Never {@code null}.
     */
    public TaskExecutor getExecutor() {
        return executor;
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
     * Returns the future.
     *
     * @return {@code null} if this task hasn't been started or scheduled.
     */
    public Future<?> getFuture() {
        return future.get();
    }

    private boolean isSubmittable() {
        Future<?> future = getFuture();
        return future == null || future.isDone();
    }

    private static long nano(double seconds) {
        return (long) (seconds * 1e9);
    }

    /**
     * Submits this task to run immediately. Does nothing if this task is
     * already scheduled to run.
     */
    public void submit() {
        synchronized (future) {
            if (isSubmittable()) {
                future.set(getExecutor().submit(this));
            }
        }
    }

    /**
     * Schedule this task to run after the given {@code initialDelay}.
     * Does nothing if this task is already scheduled to run.
     *
     * @param initialDelay In seconds.
     */
    public void schedule(double initialDelay) {
        synchronized (future) {
            if (isSubmittable()) {
                future.set(getExecutor().schedule(this, nano(initialDelay), TimeUnit.NANOSECONDS));
            }
        }
    }

    /**
     * Schedule this task to run after the given {@code initialDelay} and
     * forever after. Each subsequent runs will be scheduled to execute after
     * the given {@code periodicDelay} once the current one finishes. Does
     * nothing if this task is already scheduled to run.
     *
     * @param initialDelay In seconds.
     * @param periodicDelay In seconds.
     */
    public void scheduleWithFixedDelay(double initialDelay, double periodicDelay) {
        synchronized (future) {
            if (isSubmittable()) {
                future.set(getExecutor().scheduleWithFixedDelay(this, nano(initialDelay), nano(periodicDelay), TimeUnit.NANOSECONDS));
            }
        }
    }

    /**
     * Schedule this task to run after the given {@code initialDelay} and
     * forever after every given {@code periodicDelay}. Does nothing if this
     * task is already scheduled to run.
     *
     * @param initialDelay In seconds.
     * @param periodicDelay In seconds.
     */
    public void scheduleAtFixedRate(double initialDelay, double periodicDelay) {
        synchronized (future) {
            if (isSubmittable()) {
                future.set(getExecutor().scheduleAtFixedRate(this, nano(initialDelay), nano(periodicDelay), TimeUnit.NANOSECONDS));
            }
        }
    }

    /**
     * Returns the thread where this task is running.
     *
     * @return {@code null} if this task isn't running.
     */
    public Thread getThread() {
        return thread;
    }

    /**
     * Returns {@code true} if the thread running this task has been
     * interrupted.
     *
     * @return Always {@code false} if this task isn't running.
     */
    public boolean isInterrupted() {
        Thread thread = getThread();
        return thread != null && thread.isInterrupted();
    }

    /** Returns {@code true} if this task is currently running. */
    public boolean isRunning() {
        return isRunning.get();
    }

    /**
     * Returns {@code true} if {@link #pause} has been called on this
     * task.
     */
    public boolean isPauseRequested() {
        return isPauseRequested.get();
    }

    /** Returns {@code true} if this execution will be included in the Run Count. */
    protected boolean isRunCounted() {
        return isRunCounted.get();
    }

    /** Do not include this run in the Run Count or Last Run Begin/End dates. */
    protected void skipRunCount() {
        LOGGER.debug("Not counting [{}]", getName());
        isRunCounted.set(false);
    }

    /** Tries to pause this task. */
    public void pause() {
        if (isPauseRequested.compareAndSet(false, true)) {
            LOGGER.debug("Pausing [{}]", getName());
        }
    }

    /** Resumes this task if it's paused. */
    public void resume() {
        if (isPauseRequested.compareAndSet(true, false)) {
            LOGGER.debug("Resuming [{}]", getName());

            synchronized (isPauseRequested) {
                isPauseRequested.notifyAll();
            }
        }
    }

    /**
     * Returns {@code true} if {@link #stop} has been called on this
     * task.
     */
    public boolean isStopRequested() {
        return isStopRequested.get();
    }

    /**
     * Returns {@code true} if this task is unimportant and safe to stop at any time.
     */
    public boolean isSafeToStop() {
        return isSafeToStop.get();
    }

    /**
     * Sets a flag to indicate the task is unimportant and safe to stop at any time.
     */
    protected void setSafeToStop(boolean safeToStop) {
        isSafeToStop.set(safeToStop);
    }

    /** Tries to stop this task. */
    public void stop() {
        if (isStopRequested.compareAndSet(false, true)) {
            LOGGER.debug("Stopping [{}]", getName());

            synchronized (future) {
                Future<?> f = getFuture();
                if (f != null && f.cancel(true)) {
                    return;
                }
            }

            Thread t = getThread();
            if (t != null) {
                t.interrupt();
            }
        }
    }

    /**
     * Returns the time when the last run of this task began.
     *
     * @return Milliseconds since the epoch. {@code -1} if this task
     *         never ran.
     */
    public long getLastRunBegin() {
        return lastRunBegin;
    }

    /**
     * Returns the time when the last run of this task ended.
     *
     * @return Milliseconds since the epoch. {@code -1} if this task
     *         is currenting running.
     */
    public long getLastRunEnd() {
        return lastRunEnd;
    }

    /**
     * Returns the run duration for this task. If it's not currently
     * running, this method returns the measurement from the last run.
     *
     * @return In milliseconds. {@code -1} if this task never ran.
     */
    public long getRunDuration() {
        long runBegin = getLastRunBegin();
        if (runBegin < 0) {
            return -1;
        } else {
            long lastRunEnd = getLastRunEnd();
            long runEnd = lastRunEnd > 0 ? lastRunEnd : System.currentTimeMillis();
            return runEnd - runBegin;
        }
    }

    /**
     * Returns the progress.
     *
     * @return {@code null} if the progress isn't available.
     */
    public String getProgress() {
        return progress;
    }

    /**
     * Sets the progress. For numeric progress, consider using
     * {@link #setProgressIndex} and {@link #setProgressTotal} instead.
     */
    public void setProgress(String newProgress) {
        progress = newProgress;
    }

    /**
     * Returns the progress index.
     *
     * @return {@code -1} if the progress index isn't available.
     */
    public long getProgressIndex() {
        return progressIndex;
    }

    /** Sets the progress index. Also updates the progress string. */
    public void setProgressIndex(long newProgressIndex) {
        progressIndex = newProgressIndex;
        setProgressAutomatically();
    }

    /**
     * Adds the given {@code amount} to the progress index. Also updates
     * the progress string.
     */
    public void addProgressIndex(long amount) {
        setProgressIndex(getProgressIndex() + amount);
    }

    /**
     * Returns the progress total.
     *
     * @return {@code -1} if the progress total isn't available.
     */
    public long getProgressTotal() {
        return progressTotal;
    }

    /** Sets the progress total. Also updates the progress string. */
    public void setProgressTotal(long newProgressTotal) {
        progressTotal = newProgressTotal;
        setProgressAutomatically();
    }

    private void setProgressAutomatically() {
        StringBuilder progress = new StringBuilder();

        long index = getProgressIndex();
        progress.append(index);
        progress.append('/');

        long total = getProgressTotal();
        if (total < 0) {
            progress.append('?');

        } else {
            progress.append(total);
            progress.append(" (");
            progress.append(index / (double) total * 100.0);
            progress.append(')');
        }

        setProgress(progress.toString());
    }

    /**
     * Returns the exception thrown from the last run of this task.
     *
     * @return {@code null} if there wasn't an error.
     */
    public Throwable getLastException() {
        return lastException;
    }

    /** Returns the number of times that this task ran. */
    public long getRunCount() {
        return runCount.get();
    }

    // --- Comparable support ---

    @Override
    public int compareTo(Task other) {
        return getName().compareTo(other.getName());
    }

    // --- Runnable support ---

    /** Returns {@code true} if this task should continue running. */
    protected boolean shouldContinue() {
        if (isInterrupted() || isStopRequested()) {
            return false;
        }

        synchronized (isPauseRequested) {
            while (isPauseRequested()) {
                try {
                    isPauseRequested.wait();
                } catch (InterruptedException ex) {
                    return false;
                }
            }
        }

        return true;
    }

    /** Called by {@link #run} to execute the actual task logic. */
    protected abstract void doTask() throws Exception;

    @Override
    public final void run() {
        if (!isRunning.compareAndSet(false, true)) {
            LOGGER.debug("[{}] already running!", getName());
            return;
        }

        // If running in Tomcat and this task belongs to an undeployed
        // application, stop running immediately.
        ClassLoader loader = getClass().getClassLoader();
        Class<?> loaderClass = loader.getClass();
        String loaderClassName = loaderClass.getName();

        if ("org.apache.catalina.loader.WebappClassLoader".equals(loaderClassName)) {
            try {
                Field startedField = loaderClass.getDeclaredField("started");
                startedField.setAccessible(true);
                if (Boolean.FALSE.equals(startedField.get(loader))) {
                    stop();
                    return;
                }

            } catch (IllegalAccessException error) {
                // This should never happen since #setAccessible is called
                // on the field.

            } catch (NoSuchFieldException error) {
                // In case this code is used on future versions of Tomcat that
                // doesn't use the field any more.
            }
        }

        long previousLastRunBegin = lastRunBegin;
        long previousLastRunEnd = lastRunEnd;
        String previousProgress = progress;
        long previousProgressIndex = progressIndex;
        long previousProgressTotal = progressTotal;
        Throwable previousLastException = lastException;

        try {
            LOGGER.debug("Begin running [{}]", getName());

            isRunCounted.set(true);
            thread = Thread.currentThread();
            lastRunBegin = System.currentTimeMillis();
            lastRunEnd = -1;
            progress = null;
            progressIndex = 0;
            progressTotal = -1;
            lastException = null;

            if (shouldContinue()) {
                doTask();
            }

        } catch (Throwable ex) {
            LOGGER.warn(String.format("Error running [%s]!", getName()), ex);
            lastException = ex;

        } finally {
            LOGGER.debug("End running [{}]", getName());

            thread = null;
            lastRunEnd = System.currentTimeMillis();
            isRunning.set(false);
            isPauseRequested.set(false);
            isStopRequested.set(false);

            if (isRunCounted()) {
                runCount.incrementAndGet();
            } else {
                lastRunBegin = previousLastRunBegin;
                lastRunEnd = previousLastRunEnd;
                progress = previousProgress;
                progressIndex = previousProgressIndex;
                progressTotal = previousProgressTotal;
                lastException = previousLastException;
            }
        }
    }

    // --- Deprecated ---

    /**
     * @deprecated Use {@link TaskExecutor.Static#getAll} and
     *             {@link TaskExecutor#getTasks} instead.
     */
    @Deprecated
    public static List<Task> getInstances() {
        List<Task> tasks = new ArrayList<Task>();
        for (TaskExecutor executor : TaskExecutor.Static.getAll()) {
            for (Object taskObject : executor.getTasks()) {
                if (taskObject instanceof Task) {
                    tasks.add((Task) taskObject);
                }
            }
        }
        return tasks;
    }

    /**
     * Creates an instance that will run in the default executor
     * with the given {@code name}.
     *
     * @param name If blank, this task will be named based on its
     *        class name and the count of all instances created so far.
     *
     * @deprecated Use {@link #Task(String, String)} instead.
     */
    @Deprecated
    protected Task(String name) {
        this(null, name);
    }

    /** @deprecated Use {@link #getLastRunBegin} instead. */
    @Deprecated
    public Date getStartTime() {
        return new Date(lastRunBegin);
    }

    /** @deprecated Use {@link #getLastRunEnd} instead. */
    @Deprecated
    public Date getStopTime() {
        return new Date(lastRunEnd);
    }

    /** @deprecated Use {@link #getRunDuration} instead. */
    @Deprecated
    public Long getDuration() {
        long runDuration = getRunDuration();
        return runDuration > 0 ? runDuration : null;
    }

    /** @deprecated Use {@link #getRunCount} instead. */
    @Deprecated
    public long getCount() {
        return runCount.get();
    }

    /** @deprecated Use {@link #submit} instead. */
    @Deprecated
    public void start() {
        submit();
    }

    /** @deprecated Use {@link #schedule(double)} instead. */
    @Deprecated
    public void scheduleOnce(double initialDelay) {
        schedule(initialDelay);
    }

    /** @deprecated Use {@link #scheduleWithFixedDelay} instead. */
    @Deprecated
    public void schedule(double initialDelay, double periodicDelay) {
        scheduleWithFixedDelay(initialDelay, periodicDelay);
    }
}
