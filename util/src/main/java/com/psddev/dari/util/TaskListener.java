package com.psddev.dari.util;

/**
 * For receiving {@link Task} events.
 */
public interface TaskListener {

    /**
     * Called before the task runs.
     *
     * @param task Nonnull.
     */
    default void before(Task task) {
    }

    /**
     * Called after the task runs.
     *
     * @param task Nonnull.
     * @param error Nullable.
     */
    default void after(Task task, Throwable error) {
    }
}
