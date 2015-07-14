package com.psddev.dari.db;

import java.util.Date;
import java.util.List;

import com.psddev.dari.util.ObjectUtils;
import com.psddev.dari.util.PaginatedResult;
import com.psddev.dari.util.Profiler;

/** Times all database operations using {@link Profiler}. */
public class ProfilingDatabase extends ForwardingDatabase {

    /** Event name used to mark {@link Database#readAll} calls. */
    public static final String READ_ALL_EVENT_NAME = "DB: Read All";

    /** Event name used to mark {@link Database#readAllGrouped} calls. */
    public static final String READ_ALL_GROUPED_EVENT_NAME = "DB: Read All Grouped";

    /** Event name used to mark {@link Database#readCount} calls. */
    public static final String READ_COUNT_EVENT_NAME = "DB: Read Count";

    /** Event name used to mark {@link Database#readFirst} calls. */
    public static final String READ_FIRST_EVENT_NAME = "DB: Read First";

    /** Event name used to mark {@link Database#readIterable} calls. */
    public static final String READ_ITERABLE_EVENT_NAME = "DB: Read Iterable";

    /** Event name used to mark {@link Database#readLastUpdate} calls. */
    public static final String READ_LAST_UPDATE_EVENT_NAME = "DB: Read Last Update";

    /** Event name used to mark {@link Database#readPartial} calls. */
    public static final String READ_PARTIAL_EVENT_NAME = "DB: Read Partial";

    /** Event name used to mark {@link Database#readPartialGrouped} calls. */
    public static final String READ_PARTIAL_GROUPED_EVENT_NAME = "DB: Read Partial Grouped";

    /** Event name used to mark {@link Database#save} calls. */
    public static final String SAVE_EVENT_NAME = "DB: Save";

    /** Event name used to mark {@link Database#saveUnsafely} calls. */
    public static final String SAVE_UNSAFELY_EVENT_NAME = "DB: Save Unsafely";

    /** Event name used to mark {@link Database#index} calls. */
    public static final String INDEX_EVENT_NAME = "DB: Index";

    /** Event name used to mark {@link Database#delete} calls. */
    public static final String DELETE_EVENT_NAME = "DB: Delete";

    /** Event name used to mark {@link Database#deleteByQuery} calls. */
    public static final String DELETE_BY_QUERY_EVENT_NAME = "DB: Delete By Query";

    // --- ForwardingDatabase support ---

    private void startQueryEvent(String event, Query<?> query) {
        StackTraceElement caller = null;

        Profiler.Static.pauseThreadEvent();

        try {
            StackTraceElement[] elements = new Throwable().getStackTrace();

            for (int i = 2, length = elements.length; i < length; ++ i) {
                StackTraceElement element = elements[i];
                String className = element.getClassName();
                Class<?> c = ObjectUtils.getClassByName(className);

                if (c == null
                        || !(Database.class.isAssignableFrom(c)
                        || Query.class.isAssignableFrom(c))) {
                    caller = element;
                    break;
                }
            }

        } finally {
            Profiler.Static.resumeThreadEvent();
        }

        Object resolving = query.getOptions().get(State.REFERENCE_RESOLVING_QUERY_OPTION);

        if (resolving != null) {
            Profiler.Static.startThreadEvent(event + " (Reference Resolving)", resolving, query.getOptions().get(State.REFERENCE_FIELD_QUERY_OPTION), caller);

        } else {
            Profiler.Static.startThreadEvent(event, caller, query);
        }
    }

    @Override
    public <T> List<T> readAll(Query<T> query) {
        List<T> result = null;

        try {
            startQueryEvent(READ_ALL_EVENT_NAME, query);
            result = super.readAll(query);
            return result;

        } finally {
            Profiler.Static.stopThreadEvent(result);
        }
    }

    @Override
    public <T> List<Grouping<T>> readAllGrouped(Query<T> query, String... fields) {
        List<Grouping<T>> result = null;

        try {
            startQueryEvent(READ_ALL_GROUPED_EVENT_NAME, query);
            result = super.readAllGrouped(query, fields);
            return result;

        } finally {
            Profiler.Static.stopThreadEvent(result);
        }
    }

    @Override
    public long readCount(Query<?> query) {
        long result = -1;

        try {
            startQueryEvent(READ_COUNT_EVENT_NAME, query);
            result = super.readCount(query);
            return result;

        } finally {
            Profiler.Static.stopThreadEvent(result);
        }
    }

    @Override
    public <T> T readFirst(Query<T> query) {
        T result = null;

        try {
            startQueryEvent(READ_FIRST_EVENT_NAME, query);
            result = super.readFirst(query);
            return result;

        } finally {
            Profiler.Static.stopThreadEvent(result);
        }
    }

    @Override
    public <T> Iterable<T> readIterable(Query<T> query, int fetchSize) {
        Iterable<T> result = null;

        try {
            startQueryEvent(READ_ITERABLE_EVENT_NAME, query);
            result = super.readIterable(query, fetchSize);
            return result;

        } finally {
            Profiler.Static.stopThreadEvent(result);
        }
    }

    @Override
    public Date readLastUpdate(Query<?> query) {
        Date result = null;

        try {
            startQueryEvent(READ_LAST_UPDATE_EVENT_NAME, query);
            result = super.readLastUpdate(query);
            return result;

        } finally {
            Profiler.Static.stopThreadEvent(result);
        }
    }

    @Override
    public <T> PaginatedResult<T> readPartial(Query<T> query, long offset, int limit) {
        PaginatedResult<T> result = null;

        try {
            startQueryEvent(READ_PARTIAL_EVENT_NAME, query);
            result = super.readPartial(query, offset, limit);
            return result;

        } finally {
            Profiler.Static.stopThreadEvent(result);
        }
    }

    @Override
    public <T> PaginatedResult<Grouping<T>> readPartialGrouped(Query<T> query, long offset, int limit, String... fields) {
        PaginatedResult<Grouping<T>> result = null;

        try {
            startQueryEvent(READ_PARTIAL_GROUPED_EVENT_NAME, query);
            result = super.readPartialGrouped(query, offset, limit, fields);
            return result;

        } finally {
            Profiler.Static.stopThreadEvent(result);
        }
    }

    @Override
    public void save(State state) {
        try {
            Profiler.Static.startThreadEvent(SAVE_EVENT_NAME, state);
            super.save(state);

        } finally {
            Profiler.Static.stopThreadEvent();
        }
    }

    @Override
    public void saveUnsafely(State state) {
        try {
            Profiler.Static.startThreadEvent(SAVE_UNSAFELY_EVENT_NAME, state);
            super.saveUnsafely(state);

        } finally {
            Profiler.Static.stopThreadEvent();
        }
    }

    @Override
    public void index(State state) {
        try {
            Profiler.Static.startThreadEvent(INDEX_EVENT_NAME, state);
            super.index(state);

        } finally {
            Profiler.Static.stopThreadEvent();
        }
    }

    @Override
    public void delete(State state) {
        try {
            Profiler.Static.startThreadEvent(DELETE_EVENT_NAME, state);
            super.delete(state);

        } finally {
            Profiler.Static.stopThreadEvent();
        }
    }

    @Override
    public void deleteByQuery(Query<?> query) {
        try {
            startQueryEvent(DELETE_BY_QUERY_EVENT_NAME, query);
            super.deleteByQuery(query);

        } finally {
            Profiler.Static.stopThreadEvent();
        }
    }
}
