package com.psddev.dari.db;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Date;
import java.util.Deque;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.UUID;

import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.psddev.dari.util.CompactMap;
import com.psddev.dari.util.ObjectUtils;
import com.psddev.dari.util.PaginatedResult;
import com.psddev.dari.util.Settings;
import com.psddev.dari.util.SettingsBackedObject;
import com.psddev.dari.util.SettingsException;

/** Database of objects. */
public interface Database extends AutoCloseable, SettingsBackedObject {

    public static final String CREATOR_EXTRA = "dari.creatorDatabase";
    public static final String DEFAULT_DATABASE_SETTING = "dari/defaultDatabase";
    public static final int MAXIMUM_LIMIT = Integer.MAX_VALUE - 1;
    public static final String SETTING_PREFIX = "dari/database";
    public static final String DISABLE_FUNNEL_CACHE_QUERY_OPTION = "db.disableFunnelCache";

    /** Returns the name. */
    public String getName();

    /** Sets the name. */
    public void setName(String name);

    /** Returns the environment. */
    public DatabaseEnvironment getEnvironment();

    /** Sets the environment. */
    public void setEnvironment(DatabaseEnvironment environment);

    /**
     * Returns a list of all objects matching the given {@code query}.
     *
     * <p>The default implementation uses {@link #readPartial(Query, long, int)}.
     * </p>
     *
     * @param query
     *        Can't be {@code null}.
     *
     * @return Never {@code null}.
     */
    default <T> List<T> readAll(Query<T> query) {
        return readPartial(query, 0L, MAXIMUM_LIMIT).getItems();
    }

    /**
     * Returns all objects matching the given {@code query} grouped by the
     * values of the given {@code fields}.
     *
     * @param query
     *        Can't be {@code null}.
     *
     * @param fields
     *        Can't be blank.
     *
     * @return Never {@code null}.
     */
    default <T> List<Grouping<T>> readAllGrouped(Query<T> query, String... fields) {
        return readPartialGrouped(query, 0L, MAXIMUM_LIMIT, fields).getItems();
    }

    /**
     * Returns a count of all objects matching the given {@code query}.
     *
     * <p>The default implementation uses {@link #readPartial(Query, long, int)}.
     * </p>
     *
     * @param query
     *        Can't be {@code null}.
     */
    default long readCount(Query<?> query) {
        return readPartial(query, 0L, 1).getCount();
    }

    /**
     * Returns the first object matching the given {@code query}.
     *
     * <p>The default implementation uses {@link #readPartial(Query, long, int)}.
     * </p>
     *
     * @param query
     *        Can't be {@code null}.
     *
     * @return May be {@code null}.
     */
    default <T> T readFirst(Query<T> query) {
        List<T> items = readPartial(query, 0L, 1).getItems();

        return items.isEmpty() ? null : items.get(0);
    }

    /**
     * Returns an iterable of all objects matching the given {@code query}
     * using the given {@code database}.
     *
     * @param fetchSize Maximum number of items to fetch at a time.
     */
    public <T> Iterable<T> readIterable(Query<T> query, int fetchSize);

    /**
     * Returns the date when the objects matching the given {@code query}
     * were last updated.
     */
    public Date readLastUpdate(Query<?> query);

    /**
     * Returns a partial list of all objects matching the given
     * {@code query} within the range of the given {@code offset} and
     * {@code limit}.
     */
    public <T> PaginatedResult<T> readPartial(Query<T> query, long offset, int limit);

    /**
     * Returns all objects matching the given {@code query} grouped by the
     * values of the given {@code fields}.
     */
    public <T> PaginatedResult<Grouping<T>> readPartialGrouped(Query<T> query, long offset, int limit, String... fields);

    /**
     * Begins a series of writes. Note that for every call of this method,
     * there must be a matching {@link #endWrites} call, so a typical use
     * would look like:
     *
     * <p><blockquote><pre>
     * try {
     * &nbsp;   database.beginWrites();
     * &nbsp;   ...
     * &nbsp;   database.commitWrites(); // Or commitWritesEventually();
     * } finally {
     * &nbsp;   database.endWrites();
     * }
     * </pre></blockquote>
     *
     * @return {@code false} if called within another nested transaction.
     */
    public boolean beginWrites();

    public void beginIsolatedWrites();

    /**
     * Commits all pending writes. The data will either be saved, or
     * discarded if there are any validation errors.
     *
     * @return {@code false} if called within another nested transaction.
     * @throws IllegalStateException If called without a preceding
     *         call to {@link #beginWrites}.
     */
    public boolean commitWrites();

    /**
     * Commits all pending writes to be available for read eventually.
     * If this method is used to write to the database, subsequent read
     * to the saved data may not be available immediately.
     *
     * @return {@code false} if called within another nested transaction.
     * @throws IllegalStateException If called without a preceding
     *         call to {@link #beginWrites}.
     */
    public boolean commitWritesEventually();

    /**
     * Ends the series of writes.
     *
     * @return {@code false} if called within another nested transaction.
     * @throws IllegalStateException If called without a preceding
     *         call to {@link #beginWrites}.
     */
    public boolean endWrites();

    /** Saves the given {@code state}. */
    public void save(State state);

    /** Saves the given {@code state} without validating the data. */
    public void saveUnsafely(State state);

    /** Ensures that the given {@code state}'s indexes are up-to-date. */
    public void index(State state);

    /** Updates the index for a single value of a state. */
    public default void recalculate(State state, ObjectIndex... indexes) {
        // no default implementation.
    }

    /** Returns the database's current time, falling back to the local current time. */
    public default long now() {
        return System.currentTimeMillis();
    }

    /** Ensures that given {@code index} is up-to-date across all states. */
    public void indexAll(ObjectIndex index);

    /** Deletes the given {@code state}. */
    public void delete(State state);

    /** Deletes all objects matching the given {@code query}. */
    public void deleteByQuery(Query<?> query);

    default void addUpdateNotifier(UpdateNotifier<?> notifier) {
        throw new UnsupportedOperationException();
    }

    default void removeUpdateNotifier(UpdateNotifier<?> notifier) {
        throw new UnsupportedOperationException();
    }

    /**
     * Closes the database.
     *
     * <p>The default implementation does nothing.</p>
     */
    default void close() throws Exception {
    }

    /** {@link Database} utility methods. */
    public static final class Static {

        private static final ThreadLocal<Deque<Database>> DEFAULT_OVERRIDES = new ThreadLocal<Deque<Database>>();
        private static final ThreadLocal<Boolean> IGNORE_READ_CONNECTION = new ThreadLocal<Boolean>();

        private static final LoadingCache<String, Database> INSTANCES = CacheBuilder.newBuilder()
                .build(new CacheLoader<String, Database>() {
                    @Override
                    public Database load(String name) {
                        Database database = Settings.newInstance(Database.class, SETTING_PREFIX + "/" + name);
                        database.setName(name);
                        return database;
                    }
                });

        /**
         * Returns a list of all databases.
         *
         * @return Never {@code null}. Mutable.
         */
        public static List<Database> getAll() {
            Object databases = Settings.get(Object.class, SETTING_PREFIX);

            if (databases instanceof Map) {
                for (Object name : ((Map<?, ?>) databases).keySet()) {
                    if (name != null) {
                        getInstance(name.toString());
                    }
                }
            }

            return new ArrayList<Database>(INSTANCES.asMap().values());
        }

        /**
         * Returns the database with the given {@code name}.
         *
         * @param name If blank, returns the default database.
         * @return Never {@code null}.
         */
        public static Database getInstance(String name) {
            return ObjectUtils.isBlank(name) ? getDefault() : INSTANCES.getUnchecked(name);
        }

        /**
         * Returns the default database.
         *
         * @return Never {@code null}.
         */
        public static Database getDefault() {
            Database override = getDefaultOverride();
            return override != null ? override : getDefaultOriginal();
        }

        /**
         * Returns the original default database without using the
         * overrides.
         *
         * @return Never {@code null}.
         * @throws SettingsException If a default database isn't defined.
         */
        public static Database getDefaultOriginal() {
            String name = Settings.getOrError(String.class, DEFAULT_DATABASE_SETTING, "No default database!");
            return getInstance(name);
        }

        /**
         * Returns the default database override in the current thread.
         *
         * @return May be {@code null}.
         */
        public static Database getDefaultOverride() {
            Deque<Database> overrides = DEFAULT_OVERRIDES.get();
            return overrides != null ? overrides.peekFirst() : null;
        }

        /**
         * Overrides the default database with the given {@code override}
         * in the current thread.
         *
         * @param override Can't be {@code null}.
         * @return Current default database before overriding.
         */
        public static Database overrideDefault(Database override) {
            Preconditions.checkNotNull(override);

            Database old = getDefaultOverride();

            if (old == null) {
                String name = Settings.get(String.class, DEFAULT_DATABASE_SETTING);

                if (name != null) {
                    old = getInstance(name);
                }
            }

            Deque<Database> overrides = DEFAULT_OVERRIDES.get();

            if (overrides == null) {
                overrides = new ArrayDeque<Database>();
                DEFAULT_OVERRIDES.set(overrides);
            }

            overrides.addFirst(override);

            return old;
        }

        /**
         * Restores the default database to the previous value before
         * {@link #overrideDefault} was called.
         *
         * @return Current default database before the restoration.
         * @throws NoSuchElementException If {@link #overrideDefault}
         * wasn't called before.
         */
        public static Database restoreDefault() {
            Deque<Database> overrides = DEFAULT_OVERRIDES.get();

            if (overrides != null) {
                return overrides.removeFirst();

            } else {
                throw new NoSuchElementException();
            }
        }

        /**
         * Returns {@code true} if the databases should ignore read-specific
         * connections.
         *
         * @deprecated Use {@link Query#isMaster} instead.
         */
        @Deprecated
        public static boolean isIgnoreReadConnection() {
            return Boolean.TRUE.equals(IGNORE_READ_CONNECTION.get());
        }

        /**
         * Sets whether the databases should ignore read-specific
         * connections.
         *
         * @deprecated Use {@link Query#setMaster} or {@link Query#master} instead.
         */
        @Deprecated
        public static void setIgnoreReadConnection(boolean isIgnoreReadConnection) {
            if (isIgnoreReadConnection) {
                IGNORE_READ_CONNECTION.set(Boolean.TRUE);
            } else {
                IGNORE_READ_CONNECTION.remove();
            }
        }

        /**
         * Returns a list of all databases of the given
         * {@code databaseClass}.
         *
         * @param databaseClass Can't be {@code null}.
         * @return Never {@code null}. Mutable.
         */
        public static <T extends Database> List<T> getByClass(Class<T> databaseClass) {
            List<T> databases = new ArrayList<T>();
            for (String name : getNames()) {
                try {
                    addByClass(databases, databaseClass, getInstance(name));
                } catch (SettingsException error) {
                    // Ignore misconfigured databases.
                }
            }
            return databases;
        }

        // Adds to the given {@code result} if the given {@code database}
        // is an instance of the given {@code databaseClas}.
        @SuppressWarnings("unchecked")
        private static <T extends Database> void addByClass(
                List<T> result,
                Class<T> databaseClass,
                Object database) {

            if (database == null) {
                // Nothing to add.

            } else if (databaseClass.isInstance(database)) {
                if (!result.contains(database)) {
                    result.add((T) database);
                }

            } else if (database instanceof Iterable) {
                for (Object subDatabase : (Iterable<?>) database) {
                    addByClass(result, databaseClass, subDatabase);
                }
            }
        }

        /**
         * Returns the first database of the given {@code databaseClass}.
         *
         * <p>The default database is always checked first.</p>
         *
         * @param databaseClass Can't be {@code null}.
         * @return May be {@code null}.
         */
        public static <T extends Database> T getFirst(Class<T> databaseClass) {
            Database defaultDatabase = getDefault();

            T found = findByClass(databaseClass, defaultDatabase);
            if (found != null) {
                return found;
            }

            for (Database database : getAll()) {
                if (!database.equals(defaultDatabase)) {
                    found = findByClass(databaseClass, database);
                    if (found != null) {
                        return found;
                    }
                }
            }

            return null;
        }

        @SuppressWarnings("unchecked")
        private static <T extends Database> T findByClass(Class<T> databaseClass, Object database) {
            if (database != null) {

                if (databaseClass.isInstance(database)) {
                    return (T) database;

                } else if (database instanceof Iterable) {
                    for (Object subDatabase : (Iterable<?>) database) {
                        T found = findByClass(databaseClass, subDatabase);
                        if (found != null) {
                            return found;
                        }
                    }
                }
            }

            return null;
        }

        /**
         * Finds an object of the given {@code type} matching the given
         * {@code id} using the given {@code database}.
         */
        public static <T> T findById(Database database, Class<T> type, UUID id) {
            return id == null ? null : database.readFirst(Query.from(type).where("_id = ?", id));
        }

        /**
         * Finds an unique object of the given {@code type} matching the
         * given {@code field} and {@code value} using the given
         * {@code database}.
         */
        public static <T> T findUnique(Database database, Class<T> type, String field, String value) {
            return value == null ? null : database.readFirst(Query.from(type).where(field + " = ?", value));
        }

        /**
         * Returns the database that caused the given {@code object}
         * to be created.
         *
         * @return May be {@code null} if the information isn't available.
         */
        public static Database getCreator(Object object) {
            return object != null ? (Database) State.getInstance(object).getExtras().get(CREATOR_EXTRA) : null;
        }

        // --- Deprecated ---

        /** @deprecated Use {@link #getAll} and {@link Database#getName} instead. */
        @Deprecated
        public static Set<String> getNames() {
            Set<String> names = new LinkedHashSet<String>();
            for (Database database : getAll()) {
                names.add(database.getName());
            }
            return names;
        }

        /** @deprecated Use {@link #overrideDefault} and/or {@link #restoreDefault} instead. */
        @Deprecated
        public static void setDefaultOverride(Database override) {
            if (override == null) {
                DEFAULT_OVERRIDES.set(null);

            } else {
                Deque<Database> overrides = new ArrayDeque<Database>();
                overrides.addFirst(override);
                DEFAULT_OVERRIDES.set(overrides);
            }
        }

        /** @deprecated Use {@link Database#readIterable} instead. */
        @Deprecated
        public static <T> Iterable<T> readIterable(Database database, Query<T> query, int fetchSize) {
            return database.readIterable(query, fetchSize);
        }
    }

    // --- Deprecated ---

    /**
     * @deprecated Use {@link #readAll} instead.
     */
    @Deprecated
    default <T> List<T> readList(Query<T> query) {
        return readAll(query);
    }

    /**
     * @deprecated Use {@link #readAllGrouped} or {@link #readPartialGrouped}
     * instead.
     */
    @Deprecated
    default Map<Object, Long> readGroupedCount(Query<?> query, String field) {
        Map<Object, Long> counts = new CompactMap<>();

        for (Grouping<?> grouping : readAllGrouped(query, field)) {
            counts.put(grouping.getKeys().get(0), grouping.getCount());
        }

        return counts;
    }
}
