package com.psddev.dari.db;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.psddev.dari.util.AggregateException;
import com.psddev.dari.util.CompactMap;
import com.psddev.dari.util.ObjectUtils;
import com.psddev.dari.util.PaginatedResult;
import com.psddev.dari.util.SettingsException;
import com.psddev.dari.util.SparseSet;

/** Group of databases that acts as one. */
public class AggregateDatabase implements Database, Iterable<Database> {

    public static final String DEFAULT_DELEGATE_SETTING = "defaultDelegate";
    public static final String DELEGATE_SETTING = "delegate";
    public static final String GROUPS_SETTING = "groups";
    public static final String READ_DELEGATE_SETTING = "readDelegate";

    private static final String FAKE_GROUP = UUID.randomUUID().toString();
    private static final Logger LOGGER = LoggerFactory.getLogger(AggregateDatabase.class);

    private volatile Database defaultDelegate;
    private volatile Database defaultReadDelegate;
    private final Map<Database, Set<String>> delegateGroupsMap = new CompactMap<Database, Set<String>>();
    private volatile Map<String, Database> delegates;
    private volatile Map<String, Database> readDelegates;

    private volatile String name;
    private volatile DatabaseEnvironment environment;

    /** Returns the default delegate database. */
    public Database getDefaultDelegate() {
        return defaultDelegate;
    }

    /** Sets the default delegate database. */
    public void setDefaultDelegate(Database defaultDelegate) {
        this.defaultDelegate = defaultDelegate;
    }

    /** Returns the default read delegate database. */
    public Database getDefaultReadDelegate() {
        return defaultReadDelegate != null ? defaultReadDelegate : getDefaultDelegate();
    }

    /** Sets the default read delegate database. */
    public void setDefaultReadDelegate(Database defaultReadDelegate) {
        this.defaultReadDelegate = defaultReadDelegate;
    }

    /** Returns the unmodifiable map of all delegate databases. */
    public Map<String, Database> getDelegates() {
        return delegates == null
                ? Collections.<String, Database>emptyMap()
                : delegates;
    }

    /**
     * Returns a list of all delegate databases of the given
     * {@code databaseClass}.
     */
    @SuppressWarnings("unchecked")
    public <T extends Database> List<T> getDelegatesByClass(Class<T> databaseClass) {
        List<T> matched = new ArrayList<T>();
        for (Database delegate : getDelegates().values()) {
            if (databaseClass.isInstance(delegate)) {
                matched.add((T) delegate);
            }
        }
        return matched;
    }

    /**
     * Returns the first delegate databases of the given
     * {@code databaseClass}.
     */
    @SuppressWarnings("unchecked")
    public <T extends Database> T getFirstDelegateByClass(Class<T> databaseClass) {
        for (Database delegate : getDelegates().values()) {
            if (databaseClass.isInstance(delegate)) {
                return (T) delegate;
            }
        }
        return null;
    }

    /** Sets the map of all delegate databases. */
    public void setDelegates(Map<String, Database> delegates) {
        this.delegates = Collections.unmodifiableMap(
                new CompactMap<String, Database>(delegates));
    }

    /**
     * Adds the given {@code delegate} and associates it with the given
     * {@code group}.
     *
     * @param delegate Can't be {@code null}.
     * @param groups Can't be {@code null}.
     */
    public void addDelegate(Database delegate, Set<String> groups) {
        delegate.setEnvironment(getEnvironment());

        Map<String, Database> newDelegates = delegates != null
                ? new CompactMap<String, Database>(delegates)
                : new CompactMap<String, Database>();

        newDelegates.put(delegate.getName(), delegate);
        delegateGroupsMap.put(delegate, groups);

        delegates = Collections.unmodifiableMap(newDelegates);
    }

    /** Returns the unmodifiable map of all delegate databases used for reading. */
    public Map<String, Database> getReadDelegates() {
        return readDelegates == null
                ? Collections.<String, Database>emptyMap()
                : readDelegates;
    }

    /** Sets the map of all delegate databases used for reading. */
    public void setReadDelegates(Map<String, Database> readDelegates) {
        this.readDelegates = Collections.unmodifiableMap(
                new CompactMap<String, Database>(readDelegates));
    }

    // --- Database support ---

    @Override
    public synchronized void initialize(
            String settingsKey,
            Map<String, Object> settings) {

        Object delegateSettings = settings.get(DELEGATE_SETTING);
        if (!(delegateSettings instanceof Map)) {
            throw new SettingsException(
                    settingsKey + "/" + DELEGATE_SETTING,
                    "Delegate settings must be a map!");
        }

        String namePrefix = settingsKey.substring(Database.SETTING_PREFIX.length() + 1);
        Map<String, Database> delegates = createDelegates(
                namePrefix + "/" + DELEGATE_SETTING,
                (Map<?, ?>) delegateSettings);

        setDelegates(delegates);

        Database defaultDelegate = delegates.get(settings.get(DEFAULT_DELEGATE_SETTING));
        if (defaultDelegate != null) {
            setDefaultDelegate(defaultDelegate);
        } else {
            throw new SettingsException(
                    settingsKey + "/" + DEFAULT_DELEGATE_SETTING,
                    "Default delegate can't be blank!");
        }

        Object readDelegateSettings = settings.get(READ_DELEGATE_SETTING);
        if (readDelegateSettings != null) {
            if (readDelegateSettings instanceof Map) {
                delegates.putAll(createDelegates(
                        namePrefix + "/" + READ_DELEGATE_SETTING,
                        (Map<?, ?>) readDelegateSettings));
            } else {
                throw new SettingsException(
                        settingsKey + "/" + READ_DELEGATE_SETTING,
                        "Read delegate settings must be blank or a map!");
            }
        }

        setReadDelegates(delegates);
        setDefaultReadDelegate(delegates.get(settings.get(DEFAULT_DELEGATE_SETTING)));
    }

    /** Creates database delegates based on the given {@code settings}. */
    private Map<String, Database> createDelegates(
            String namePrefix,
            Map<?, ?> settings) {

        Map<String, Database> delegates = new CompactMap<String, Database>();
        DatabaseEnvironment environment = getEnvironment();

        for (Map.Entry<?, ?> e : settings.entrySet()) {
            Object nameObject = e.getKey();
            if (nameObject != null) {

                String name = nameObject.toString();
                Database delegate = Database.Static.getInstance(namePrefix + "/" + name);
                delegate.setEnvironment(environment);
                delegates.put(name, delegate);

                String groups = ObjectUtils.to(String.class, ((Map<?, ?>) settings.get(name)).get(GROUPS_SETTING));
                delegateGroupsMap.put(delegate, new SparseSet(ObjectUtils.isBlank(groups) ? "+/" : groups));
            }
        }

        return delegates;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void setName(String name) {
        this.name = name;
    }

    private static final LoadingCache<Database, DatabaseEnvironment> DEFAULT_ENVIRONMENTS = CacheBuilder
            .newBuilder()
            .build(new CacheLoader<Database, DatabaseEnvironment>() {
                @Override
                public DatabaseEnvironment load(Database database) {
                    return new DatabaseEnvironment(database);
                }
            });

    @Override
    public DatabaseEnvironment getEnvironment() {
        if (environment == null) {
            environment = DEFAULT_ENVIRONMENTS.getUnchecked(this);
        }
        return environment;
    }

    @Override
    public void setEnvironment(DatabaseEnvironment environment) {
        this.environment = environment;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> List<T> readAll(Query<T> query) {
        return (List<T>) READ_ALL.execute(this, query);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> List<Grouping<T>> readAllGrouped(Query<T> query, String... fields) {
        return (List<Grouping<T>>) READ_ALL_GROUPED.execute(this, query, (Object) fields);
    }

    @Override
    public long readCount(Query<?> query) {
        return (Long) READ_COUNT.execute(this, query);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T readFirst(Query<T> query) {
        return (T) READ_FIRST.execute(this, query);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> Iterable<T> readIterable(Query<T> query, int fetchSize) {
        return (Iterable<T>) READ_ITERABLE.execute(this, query, fetchSize);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> PaginatedResult<T> readPartial(Query<T> query, long offset, int limit) {
        return (PaginatedResult<T>) READ_PARTIAL.execute(this, query, offset, limit);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> PaginatedResult<Grouping<T>> readPartialGrouped(Query<T> query, long offset, int limit, String... fields) {
        return (PaginatedResult<Grouping<T>>) READ_PARTIAL_GROUPED.execute(this, query, offset, limit, (Object) fields);
    }

    @Override
    public Date readLastUpdate(Query<?> query) {
        return (Date) READ_LAST_UPDATE.execute(this, query);
    }

    @Override
    public boolean beginWrites() {
        return BEGIN_WRITES.execute(this);
    }

    @Override
    public void beginIsolatedWrites() {
        BEGIN_ISOLATED_WRITES.execute(this);
    }

    @Override
    public boolean commitWrites() {
        return COMMIT_WRITES.execute(this);
    }

    @Override
    public boolean commitWritesEventually() {
        return COMMIT_WRITES_EVENTUALLY.execute(this);
    }

    @Override
    public boolean endWrites() {
        return END_WRITES.execute(this);
    }

    @Override
    public void save(State state) {
        getDefaultDelegate().save(state);

        for (Database delegate : findDelegatesByTypes(
                getDelegates().values(),
                Arrays.asList(state.getType()))) {
            try {
                delegate.saveUnsafely(state);
            } catch (Exception ex) {
                LOGGER.warn(String.format("Can't write to [%s]", delegate), ex);
            }
        }
    }

    @Override
    public void saveUnsafely(State state) {
        SAVE_UNSAFELY.execute(this, state);
    }

    @Override
    public void index(State state) {
        INDEX.execute(this, state);
    }

    @Override
    public void indexAll(ObjectIndex index) {
        Database defaultDelegate = getDefaultDelegate();
        defaultDelegate.indexAll(index);
        for (Database delegate : getDelegates().values()) {
            if (!delegate.equals(defaultDelegate)) {
                try {
                    delegate.indexAll(index);
                } catch (Exception ex) {
                    LOGGER.warn(String.format("Can't write to [%s]", delegate), ex);
                }
            }
        }
    }

    @Override
    public void recalculate(State state, ObjectIndex... indexes) {
        Database defaultDelegate = getDefaultDelegate();
        defaultDelegate.recalculate(state, indexes);
        for (Database delegate : getDelegates().values()) {
            if (!delegate.equals(defaultDelegate)) {
                try {
                    delegate.recalculate(state, indexes);
                } catch (Exception ex) {
                    LOGGER.warn(String.format("Can't write to [%s]", delegate), ex);
                }
            }
        }
    }

    @Override
    public long now() {
        return getDefaultDelegate().now();
    }

    @Override
    public void delete(State state) {
        DELETE.execute(this, state);
    }

    @Override
    public void deleteByQuery(Query<?> query) {
        Database defaultDelegate = getDefaultDelegate();
        defaultDelegate.deleteByQuery(query);
        for (Database delegate : getDelegates().values()) {
            if (!delegate.equals(defaultDelegate)) {
                try {
                    delegate.deleteByQuery(query);
                } catch (Exception ex) {
                    LOGGER.warn(String.format("Can't write to [%s]", delegate), ex);
                }
            }
        }
    }

    /**
     * Finds all non-default delegate databases that can be used
     * with the given {@code types} based on their groups.
     */
    public List<Database> findDelegatesByTypes(
            Collection<Database> delegates,
            Collection<ObjectType> types) {

        Database defaultDelegate = getDefaultDelegate();
        List<Database> found = new ArrayList<Database>();
        for (Database delegate : delegates) {

            if (delegate.equals(defaultDelegate)) {
                continue;
            }

            boolean isAllMatch = true;
            Set<String> delegateGroups = delegateGroupsMap.get(delegate);
            for (ObjectType type : types) {

                if (type != null && !type.isConcrete()) {
                    continue;
                }

                // If the database groups allows any of the type groups,
                // the delegate database can be used.
                boolean isMatch = false;
                if (type != null) {
                    Set<String> typeGroups = type.getGroups();
                    for (String typeGroup : typeGroups) {
                        if (delegateGroups.contains(typeGroup)) {
                            isMatch = true;
                            break;
                        }
                    }
                }

                // When the type is not available, check using a fake group
                // to see if the delegate database allows or denys all.
                if (!(isMatch || delegateGroups.contains(FAKE_GROUP))) {
                    isAllMatch = false;
                    break;
                }
            }

            if (isAllMatch) {
                found.add(delegate);
            }
        }

        return found;
    }

    /** Common logic for all read operations. */
    private abstract static class ReadOperation {

        protected abstract Object read(Database delegate, Query<?> query, Object... arguments);

        public final Object execute(
                AggregateDatabase database,
                Query<?> query,
                Object... arguments) {

            List<UnsupportedOperationException> exceptions = null;
            try {
                return read(database.getDefaultReadDelegate(), query, arguments);
            } catch (UnsupportedOperationException ex) {
                exceptions = addException(exceptions, ex);
            }

            for (Database delegate : database.findDelegatesByTypes(
                    database.getReadDelegates().values(),
                    database.getEnvironment().getTypesByGroup(query.getGroup()))) {
                try {
                    return read(delegate, query, arguments);
                } catch (UnsupportedOperationException ex) {
                    exceptions = addException(exceptions, ex);
                }
            }

            throw new AggregateException(exceptions);
        }

        private List<UnsupportedOperationException> addException(
                List<UnsupportedOperationException> exceptions,
                UnsupportedOperationException exception) {

            if (exceptions == null) {
                exceptions = new ArrayList<UnsupportedOperationException>();
            }
            exceptions.add(exception);
            return exceptions;
        }
    }

    private static final ReadOperation READ_COUNT = new ReadOperation() {
        @Override
        protected Object read(Database delegate, Query<?> query, Object... arguments) {
            return delegate.readCount(query);
        }
    };

    private static final ReadOperation READ_ALL = new ReadOperation() {
        @Override
        protected Object read(Database delegate, Query<?> query, Object... arguments) {
            return delegate.readAll(query);
        }
    };

    private static final ReadOperation READ_ALL_GROUPED = new ReadOperation() {
        @Override
        protected Object read(Database delegate, Query<?> query, Object... arguments) {
            return delegate.readAllGrouped(query, (String[]) arguments[0]);
        }
    };

    private static final ReadOperation READ_FIRST = new ReadOperation() {
        @Override
        protected Object read(Database delegate, Query<?> query, Object... arguments) {
            return delegate.readFirst(query);
        }
    };

    private static final ReadOperation READ_ITERABLE = new ReadOperation() {
        @Override
        protected Object read(Database delegate, Query<?> query, Object... arguments) {
            return delegate.readIterable(query, (Integer) arguments[0]);
        }
    };

    private static final ReadOperation READ_PARTIAL = new ReadOperation() {
        @Override
        protected Object read(Database delegate, Query<?> query, Object... arguments) {
            return delegate.readPartial(query, (Long) arguments[0], (Integer) arguments[1]);
        }
    };

    private static final ReadOperation READ_PARTIAL_GROUPED = new ReadOperation() {
        @Override
        protected Object read(Database delegate, Query<?> query, Object... arguments) {
            return delegate.readPartialGrouped(query, (Long) arguments[0], (Integer) arguments[1], (String[]) arguments[2]);
        }
    };

    private static final ReadOperation READ_LAST_UPDATE = new ReadOperation() {
        @Override
        protected Object read(Database delegate, Query<?> query, Object... arguments) {
            return delegate.readLastUpdate(query);
        }
    };

    /** Common logic for all batch operations. */
    private abstract static class BatchOperation {

        protected abstract boolean batch(Database delegate);

        public final boolean execute(AggregateDatabase database) {
            Database defaultDelegate = database.getDefaultDelegate();
            boolean result = batch(defaultDelegate);

            for (Database delegate : database.getDelegates().values()) {
                if (!delegate.equals(defaultDelegate)) {
                    try {
                        batch(delegate);
                    } catch (Exception ex) {
                        LOGGER.warn(String.format("Can't batch in [%s]", delegate), ex);
                    }
                }
            }

            return result;
        }
    }

    private static final BatchOperation BEGIN_WRITES = new BatchOperation() {
        @Override
        protected boolean batch(Database delegate) {
            return delegate.beginWrites();
        }
    };

    private static final BatchOperation BEGIN_ISOLATED_WRITES = new BatchOperation() {
        @Override
        protected boolean batch(Database delegate) {
            delegate.beginIsolatedWrites();
            return true;
        }
    };

    private static final BatchOperation COMMIT_WRITES = new BatchOperation() {
        @Override
        protected boolean batch(Database delegate) {
            return delegate.commitWrites();
        }
    };

    private static final BatchOperation COMMIT_WRITES_EVENTUALLY = new BatchOperation() {
        @Override
        protected boolean batch(Database delegate) {
            return delegate.commitWritesEventually();
        }
    };

    private static final BatchOperation END_WRITES = new BatchOperation() {
        @Override
        protected boolean batch(Database delegate) {
            return delegate.endWrites();
        }
    };

    /** Common logic for all write operations. */
    private abstract static class WriteOperation {

        protected abstract void write(Database delegate, State state);

        public final void execute(AggregateDatabase database, State state) {
            write(database.getDefaultDelegate(), state);

            for (Database delegate : database.findDelegatesByTypes(
                    database.getDelegates().values(),
                    Arrays.asList(state.getType()))) {
                try {
                    write(delegate, state);
                } catch (Exception ex) {
                    LOGGER.warn(String.format("Can't write to [%s]", delegate), ex);
                }
            }
        }
    }

    private static final WriteOperation SAVE_UNSAFELY = new WriteOperation() {
        @Override
        protected void write(Database delegate, State state) {
            delegate.saveUnsafely(state);
        }
    };

    private static final WriteOperation INDEX = new WriteOperation() {
        @Override
        protected void write(Database delegate, State state) {
            delegate.index(state);
        }
    };

    private static final WriteOperation DELETE = new WriteOperation() {
        @Override
        protected void write(Database delegate, State state) {
            delegate.delete(state);
        }
    };

    // --- Iterable support ---

    @Override
    public Iterator<Database> iterator() {
        return getDelegates().values().iterator();
    }

    // --- Object support ---

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(getName());

        Collection<Database> delegates = getDelegates().values();
        if (!delegates.isEmpty()) {
            sb.append('(');
            for (Database delegate : delegates) {
                sb.append(delegate.getName());
                sb.append(", ");
            }
            sb.setLength(sb.length() - 2);
            sb.append(')');
        }

        return sb.toString();
    }
}
