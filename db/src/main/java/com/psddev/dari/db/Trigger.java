package com.psddev.dari.db;

/**
 * Code that's executed in response to some event, such as
 * {@link Record#beforeSave()}, within an instance of {@link State}.
 *
 * @see State#fireTrigger(Trigger)
 * @see State#fireTrigger(Trigger, boolean)
 */
public interface Trigger {

    /**
     * Executes this trigger on the given {@code object}.
     *
     * @param object
     *        Can't be {@code null}.
     */
    void execute(Object object);

    /**
     * Returns {@code true} if the trigger method is definitely missing on
     * the given {@code objectClass}.
     *
     * <p>It's always safe to return {@code false} if unsure.</p>
     */
    default boolean isMissing(Class<?> objectClass) {
        return false;
    }
}
