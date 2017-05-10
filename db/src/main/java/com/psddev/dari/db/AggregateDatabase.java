package com.psddev.dari.db;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
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

import javax.annotation.ParametersAreNonnullByDefault;

/**
 * Database implementation that aggregates multiple databases so that they
 * act as one.
 */
public class AggregateDatabase implements Database, Iterable<Database> {

    public static final String DEFAULT_DELEGATE_SETTING = "defaultDelegate";
    public static final String DELEGATE_SETTING = "delegate";
    public static final String GROUPS_SETTING = "groups";
    public static final String READ_DELEGATE_SETTING = "readDelegate";

    private static final String FAKE_GROUP = UUID.randomUUID().toString();
    private static final Logger LOGGER = LoggerFactory.getLogger(AggregateDatabase.class);

    private static final LoadingCache<Database, DatabaseEnvironment> ENVIRONMENTS = CacheBuilder
            .newBuilder()
            .build(new CacheLoader<Database, DatabaseEnvironment>() {

                @Override
                @ParametersAreNonnullByDefault
                public DatabaseEnvironment load(Database database) {
                    return new DatabaseEnvironment(database);
                }
            });

    private volatile Database defaultDelegate;
    private volatile Database defaultReadDelegate;
    private final Map<Database, Set<String>> delegateGroupsMap = new CompactMap<>();
    private volatile Map<String, Database> delegates;
    private volatile Map<String, Database> readDelegates;

    private volatile String name;
    private volatile DatabaseEnvironment environment;

    /**
     * Returns the default delegate.
     *
     * @return May be {@code null}.
     */
    public Database getDefaultDelegate() {
        return defaultDelegate;
    }

    /**
     * Sets the default delegate.
     *
     * @param defaultDelegate
     *        May be {@code null}.
     */
    public void setDefaultDelegate(Database defaultDelegate) {
        this.defaultDelegate = defaultDelegate;
    }

    /**
     * Returns the default delegate used by the read methods.
     *
     * @return May be {@code null}.
     */
    public Database getDefaultReadDelegate() {
        return defaultReadDelegate != null ? defaultReadDelegate : getDefaultDelegate();
    }

    /**
     * Sets the default delegate used by the read methods.
     *
     * @param defaultReadDelegate
     *        May be {@code null}.
     */
    public void setDefaultReadDelegate(Database defaultReadDelegate) {
        this.defaultReadDelegate = defaultReadDelegate;
    }

    /**
     * Returns the map of all delegates.
     *
     * @return Never {@code null}.
     */
    public Map<String, Database> getDelegates() {
        return delegates != null ? delegates : ImmutableMap.of();
    }

    /**
     * Sets the map of all delegates.
     *
     * @param delegates
     *        May be {@code null} to clear.
     */
    public void setDelegates(Map<String, Database> delegates) {
        this.delegates = delegates != null
                ? ImmutableMap.copyOf(delegates)
                : null;
    }

    /**
     * Returns the map of delegates used by the read methods.
     *
     * @return Never {@code null}.
     */
    public Map<String, Database> getReadDelegates() {
        return readDelegates != null ? readDelegates : ImmutableMap.of();
    }

    /**
     * Sets the map of all delegates used by the read methods.
     *
     * @param readDelegates
     *        May be {@code null} to clear.
     */
    public void setReadDelegates(Map<String, Database> readDelegates) {
        this.readDelegates = readDelegates != null
                ? ImmutableMap.copyOf(readDelegates)
                : null;
    }

    /**
     * Returns a list of all delegates that's an instance of the given
     * {@code databaseClass}.
     *
     * @param databaseClass
     *        Can't be {@code null}.
     *
     * @return Never {@code null}.
     */
    @SuppressWarnings("unchecked")
    public <T extends Database> List<T> getDelegatesByClass(Class<T> databaseClass) {
        Preconditions.checkNotNull(databaseClass);

        return (List<T>) getDelegates().values().stream()
                .filter(databaseClass::isInstance)
                .collect(Collectors.toList());
    }

    /**
     * Returns the first delegate that's an instance of the given
     * {@code databaseClass}.
     *
     * @param databaseClass
     *        Can't be {@code null}.
     */
    @SuppressWarnings("unchecked")
    public <T extends Database> T getFirstDelegateByClass(Class<T> databaseClass) {
        Preconditions.checkNotNull(databaseClass);

        return (T) getDelegates().values().stream()
                .filter(databaseClass::isInstance)
                .findFirst()
                .orElse(null);
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
                ? new CompactMap<>(delegates)
                : new CompactMap<>();

        newDelegates.put(delegate.getName(), delegate);
        delegateGroupsMap.put(delegate, groups);

        delegates = Collections.unmodifiableMap(newDelegates);
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

    // Creates database delegates based on the given settings.
    private Map<String, Database> createDelegates(
            String namePrefix,
            Map<?, ?> settings) {

        Map<String, Database> delegates = new CompactMap<>();
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

    @Override
    public DatabaseEnvironment getEnvironment() {
        if (environment == null) {
            environment = ENVIRONMENTS.getUnchecked(this);
        }

        return environment;
    }

    @Override
    public void setEnvironment(DatabaseEnvironment environment) {
        this.environment = environment;
    }

    private <T> T read(String group, Function<Database, T> function) {
        List<UnsupportedOperationException> errors;

        try {
            return function.apply(getDefaultReadDelegate());

        } catch (UnsupportedOperationException error) {
            errors = new ArrayList<>();

            errors.add(error);
        }

        for (Database delegate : findDelegatesByTypes(
                getReadDelegates().values(),
                getEnvironment().getTypesByGroup(group))) {

            try {
                return function.apply(delegate);

            } catch (UnsupportedOperationException error) {
                errors.add(error);
            }
        }

        throw new AggregateException(errors);
    }

    @Override
    public <T> List<T> readAll(Query<T> query) {
        return read(query.getGroup(), delegate -> delegate.readAll(query));
    }

    @Override
    public <T> List<Grouping<T>> readAllGrouped(Query<T> query, String... fields) {
        return read(query.getGroup(), delegate -> delegate.readAllGrouped(query, fields));
    }

    @Override
    public long readCount(Query<?> query) {
        return read(query.getGroup(), delegate -> delegate.readCount(query));
    }

    @Override
    public <T> T readFirst(Query<T> query) {
        return read(query.getGroup(), delegate -> delegate.readFirst(query));
    }

    @Override
    public <T> Iterable<T> readIterable(Query<T> query, int fetchSize) {
        return read(query.getGroup(), delegate -> delegate.readIterable(query, fetchSize));
    }

    @Override
    public <T> PaginatedResult<T> readPartial(Query<T> query, long offset, int limit) {
        return read(query.getGroup(), delegate -> delegate.readPartial(query, offset, limit));
    }

    @Override
    public <T> PaginatedResult<Grouping<T>> readPartialGrouped(Query<T> query, long offset, int limit, String... fields) {
        return read(query.getGroup(), delegate -> delegate.readPartialGrouped(query, offset, limit, fields));
    }

    @Override
    public Date readLastUpdate(Query<?> query) {
        return read(query.getGroup(), delegate -> delegate.readLastUpdate(query));
    }

    private boolean batch(java.util.function.Predicate<Database> predicate) {
        Database defaultDelegate = getDefaultDelegate();
        boolean result = predicate.test(defaultDelegate);

        getDelegates().values().stream()
                .filter(delegate -> !delegate.equals(defaultDelegate))
                .forEach(delegate -> {
                    try {
                        predicate.test(delegate);

                    } catch (Exception error) {
                        LOGGER.warn(String.format("Can't batch in [%s]", delegate), error);
                    }
                });

        return result;
    }

    @Override
    public boolean beginWrites() {
        return batch(Database::beginWrites);
    }

    @Override
    public void beginIsolatedWrites() {
        batch(delegate -> {
            delegate.beginIsolatedWrites();
            return true;
        });
    }

    @Override
    public boolean commitWrites() {
        return batch(Database::commitWrites);
    }

    @Override
    public boolean commitWritesEventually() {
        return batch(Database::commitWritesEventually);
    }

    @Override
    public boolean endWrites() {
        return batch(Database::endWrites);
    }

    @Override
    public void save(State state) {
        getDefaultDelegate().save(state);

        for (Database delegate : findDelegatesByTypes(
                getDelegates().values(),
                Collections.singletonList(state.getType()))) {

            try {
                delegate.saveUnsafely(state);

            } catch (Exception error) {
                LOGGER.warn(String.format("Can't write to [%s]", delegate), error);
            }
        }
    }

    private void write(Collection<ObjectType> types, Consumer<Database> consumer) {
        consumer.accept(getDefaultDelegate());

        for (Database delegate : findDelegatesByTypes(getDelegates().values(), types)) {
            try {
                consumer.accept(delegate);

            } catch (Exception error) {
                LOGGER.warn(String.format("Can't write to [%s]", delegate), error);
            }
        }
    }

    private void writeOne(State state, Consumer<Database> consumer) {
        write(Collections.singleton(state.getType()), consumer);
    }

    @Override
    public void saveUnsafely(State state) {
        writeOne(state, delegate -> delegate.saveUnsafely(state));
    }

    @Override
    public void index(State state) {
        writeOne(state, delegate -> delegate.index(state));
    }

    @Override
    public void recalculate(State state, ObjectIndex... indexes) {
        writeOne(state, delegate -> delegate.recalculate(state, indexes));
    }

    @Override
    public void delete(State state) {
        writeOne(state, delegate -> delegate.delete(state));
    }

    @Override
    public void indexAll(ObjectIndex index) {
        ObjectStruct parent = index.getParent();
        write(
                parent instanceof ObjectType ? Collections.singleton((ObjectType) parent) : null,
                delegate -> delegate.indexAll(index));
    }

    @Override
    public void deleteByQuery(Query<?> query) {
        String group = query.getGroup();
        write(
                group != null ? getEnvironment().getTypesByGroup(query.getGroup()) : null,
                delegate -> delegate.deleteByQuery(query));
    }

    /**
     * Finds all non-default delegates that should be used for the given
     * {@code types} based on their groups.
     *
     * <p>If the given {@code types} is {@code null}, returns all non-default
     * delegates.</p>
     *
     * @param delegates Nonnull.
     * @param types Nullable.
     * @return Nonnull.
     */
    public List<Database> findDelegatesByTypes(Collection<Database> delegates, Collection<ObjectType> types) {
        Database defaultDelegate = getDefaultDelegate();

        if (types == null) {
            return delegates.stream()
                    .filter(d -> !d.equals(defaultDelegate))
                    .collect(Collectors.toList());
        }

        List<Database> found = new ArrayList<>();

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

    @Override
    public long now() {
        return getDefaultDelegate().now();
    }

    @Override
    public void addUpdateNotifier(UpdateNotifier notifier) {
        getDefaultDelegate().addUpdateNotifier(notifier);
    }

    @Override
    public void removeUpdateNotifier(UpdateNotifier notifier) {
        getDefaultDelegate().removeUpdateNotifier(notifier);
    }

    @Override
    public Iterator<Database> iterator() {
        return getDelegates().values().iterator();
    }

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
