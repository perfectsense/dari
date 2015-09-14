package com.psddev.dari.util;

import com.google.common.base.Preconditions;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Keeps track of all events that occur within a specific time frame.
 *
 * <p>This class is NOT thread-safe.</p>
 */
public class Profiler {

    private final ArrayDeque<Event> eventQueue = new ArrayDeque<Event>();
    private final List<Event> rootEvents = new ArrayList<Event>();
    private final Map<String, EventStats> eventStats = new TreeMap<String, EventStats>();

    /**
     * Starts tracking an event and associates it with the given
     * {@code objects}.
     *
     * @param name Can't be blank.
     * @param objects {@code null} is equivalent to an empty array.
     * @return The started event.
     */
    public Event startEvent(String name, Object... objects) {
        Event parent = eventQueue.peekFirst();
        Event child = new Event(parent, name, objects);

        eventQueue.addFirst(child);

        if (parent == null) {
            rootEvents.add(child);
        }

        return child;
    }

    /**
     * Pauses tracking the current event.
     */
    public void pauseEvent() {
        Event current = eventQueue.peekFirst();

        if (current != null) {
            current.pause();
        }
    }

    /**
     * Resumes tracking the current event.
     */
    public void resumeEvent() {
        Event current = eventQueue.peekFirst();

        if (current != null) {
            current.resume();
        }
    }

    /**
     * Stops tracking the current event and associates it with the given
     * additional {@code objects}.
     *
     * @param objects {@code null} is equivalent to an empty array.
     * @return The stopped event.
     */
    public Event stopEvent(Object... objects) {
        Event current = eventQueue.removeFirst();

        current.associateObjects(objects);
        current.stop();

        String name = current.getName();
        EventStats stats = eventStats.get(name);

        if (stats == null) {
            stats = new EventStats();
            eventStats.put(name, stats);
        }

        stats.update(current);

        return current;
    }

    /**
     * Returns all events without parents.
     *
     * @return Never {@code null}. Immutable.
     */
    public List<Event> getRootEvents() {
        return Collections.unmodifiableList(rootEvents);
    }

    /**
     * Returns all aggregate event stats grouped by their names.
     *
     * @return Never {@code null}. Immutable.
     */
    public Map<String, EventStats> getEventStats() {
        return Collections.unmodifiableMap(eventStats);
    }

    /** {@link Profiler} utility methods. */
    public static final class Static {

        private static final ThreadLocal<Profiler> THREAD_PROFILER = new ThreadLocal<Profiler>();

        /**
         * Returns the profiler for the current thread.
         *
         * @return May be {@code null}.
         */
        public static Profiler getThreadProfiler() {
            return THREAD_PROFILER.get();
        }

        /**
         * Sets the profiler for the current thread.
         *
         * @param profiler Can be {@code null}.
         * */
        public static void setThreadProfiler(Profiler profiler) {
            THREAD_PROFILER.set(profiler);
        }

        /**
         * Starts tracking an event and associates it with the given
         * {@code objects} if there's a profiler set for the current
         * thread.
         *
         * @param name Can't be blank.
         * @param objects {@code null} is equivalent to an empty array.
         */
        public static void startThreadEvent(String name, Object... objects) {
            Profiler profiler = getThreadProfiler();
            if (profiler != null) {
                profiler.startEvent(name, objects);
            }
        }

        /**
         * Pauses tracking the event associated with the profiler in current
         * thread.
         */
        public static void pauseThreadEvent() {
            Profiler profiler = getThreadProfiler();
            if (profiler != null) {
                profiler.pauseEvent();
            }
        }

        /**
         * Resumes tracking the event associated with the profiler in current
         * thread.
         */
        public static void resumeThreadEvent() {
            Profiler profiler = getThreadProfiler();
            if (profiler != null) {
                profiler.resumeEvent();
            }
        }

        /**
         * Stops tracking the current event and associates it with the
         * given additional {@code objects} if there's a profielr set for
         * the curent thread.
         *
         * @param objects {@code null} is equivalent to an empty array.
         */
        public static void stopThreadEvent(Object... objects) {
            Profiler profiler = getThreadProfiler();
            if (profiler != null) {
                profiler.stopEvent(objects);
            }
        }
    }

    /** Aggregate event stats for {@link Profiler}. */
    public static class EventStats {

        private int count;
        private long ownDuration;

        /** Creates an instance. */
        protected EventStats() {
        }

        /**
         * Returns the number of times that the event was called.
         *
         * @return Always positive.
         */
        public int getCount() {
            return count;
        }

        /**
         * Returns how long the events took to run altogether.
         *
         * @return In nanoseconds. Never negative.
         */
        public long getOwnDuration() {
            return ownDuration;
        }

        /**
         * Updates this stats with the information from the given
         * {@code event}.
         */
        protected void update(Event event) {
            ++ count;
            ownDuration += event.getOwnDuration();
        }
    }

    /** Individual event details for {@link Profiler}. */
    public static class Event {

        private final String name;
        private final long start;
        private long pauseCount;
        private long pauseStart;
        private long pauseDuration;
        private long stop;
        private final List<Object> objects = new ArrayList<Object>();
        private final List<Event> children = new ArrayList<Event>();

        /**
         * Creates an instance as a child of the given {@code parent} event
         * with the given {@code name} and {@code objects}.
         *
         * @param parent Can be {@code null}.
         * @param name Can't be blank.
         * @param objects {@code null} is equivalent to an empty array.
         */
        protected Event(Event parent, String name, Object... objects) {
            Preconditions.checkArgument(!ObjectUtils.isBlank(name));

            this.name = name;
            this.start = System.nanoTime();

            associateObjects(objects);

            if (parent != null) {
                parent.children.add(this);
            }
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
         * Associates the given {@code objects} to this event.
         *
         * @param objects {@code null} is equivalent to an empty array.
         */
        protected void associateObjects(Object... objects) {
            if (objects != null) {
                for (Object object : objects) {
                    if (object != null) {
                        this.objects.add(object);
                    }
                }
            }
        }

        /**
         * Returns the objects associated with this event.
         *
         * @return Never {@code null}. Immutable.
         */
        public List<Object> getObjects() {
            return Collections.unmodifiableList(objects);
        }

        /**
         * Returns all sub-events.
         *
         * @return Never {@code null}. Immutable.
         */
        public List<Event> getChildren() {
            return Collections.unmodifiableList(children);
        }

        /**
         * Returns when this event started, using {@link System#nanoTime}.
         *
         * @return In nanoseconds.
         */
        public long getStart() {
            return start;
        }

        protected void pause() {
            ++ pauseCount;

            if (pauseCount == 1) {
                pauseStart = System.nanoTime();
            }
        }

        protected void resume() {
            if (pauseCount > 0) {
                -- pauseCount;

                if (pauseCount == 0) {
                    pauseDuration += (System.nanoTime() - pauseStart);
                }
            }
        }

        /** Stops tracking this event. */
        protected void stop() {
            this.stop = System.nanoTime();
        }

        /**
         * Returns how long this event took to run, including
         * all sub-events.
         *
         * @return In nanoseconds. Never negative.
         */
        public long getTotalDuration() {
            long duration = subtractChildrenPauseDuration(stop - start - pauseDuration);

            return duration < 0 ? 0 : duration;
        }

        private long subtractChildrenPauseDuration(long duration) {
            for (Event child : children) {
                duration = child.subtractChildrenPauseDuration(duration - child.pauseDuration);
            }

            return duration;
        }

        /**
         * Returns how long this event took to run, excluding
         * all sub-events.
         *
         * @return In nanoseconds. Never negative.
         */
        public long getOwnDuration() {
            long duration = getTotalDuration();

            for (Event child : children) {
                duration -= child.getTotalDuration();
            }

            return duration < 0 ? 0 : duration;
        }
    }
}
