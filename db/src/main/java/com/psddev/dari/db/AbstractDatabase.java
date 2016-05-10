package com.psddev.dari.db;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Matcher;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.psddev.dari.util.CompactMap;
import com.psddev.dari.util.ObjectUtils;
import com.psddev.dari.util.PaginatedResult;
import com.psddev.dari.util.Settings;
import com.psddev.dari.util.SparseSet;

/**
 * Skeletal database implementation. A subclass must implement:
 *
 * <ul>
 * <li>{@link #openConnection}</li>
 * <li>{@link #closeConnection}</li>
 * <li>{@link #readLastUpdate}</li>
 * <li>{@link #readPartial}</li>
 * <li>{@link #doWrites}</li>
 * </ul>
 *
 * <p>If it supports read slaves, it should override:
 *
 * <ul>
 * <li>{@link #doOpenReadConnection}</li>
 * </ul>
 *
 * <p>It can override these to improve performance:
 *
 * <ul>
 * <li>{@link #readAll}</li>
 * <li>{@link #readAllGrouped}</li>
 * <li>{@link #readCount}</li>
 * <li>{@link #readFirst}</li>
 * <li>{@link #readIterable}</li>
 * <li>{@link #readPartialGrouped}</li>
 * <li>{@link #doIndexes}</li>
 * <li>{@link #deleteByQuery}</li>
 * </ul>
 *
 * <p>If it supports transactional writes, it should override:
 *
 * <ul>
 * <li>{@link #beginTransaction}</li>
 * <li>{@link #commitTransaction}</li>
 * <li>{@link #rollbackTransaction}</li>
 * <li>{@link #endTransaction}</li>
 * </ul>
 *
 * @param C Type of the implementation-specific connection object that's
 *        used for all database operations.
 */
public abstract class AbstractDatabase<C> implements Database {

    public static final double DEFAULT_READ_TIMEOUT = 3.0;
    public static final String NULL_TYPE_QUERY_OPTION = "db.nullType";
    public static final String GROUPS_SUB_SETTING = "groups";
    public static final String READ_TIMEOUT_SUB_SETTING = "readTimeout";
    public static final String TRIGGER_EXTRA_PREFIX = "db.trigger.";
    public static final String SAVING_UNSAFELY_EXTRA = "db.savingUnsafely";
    public static final String DATA_LENGTH_EXTRA = "dari.dataLength";

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractDatabase.class);

    private volatile String name;
    private transient volatile DatabaseEnvironment environment;
    private volatile Set<String> groups;
    private volatile double readTimeout = DEFAULT_READ_TIMEOUT;

    private final transient ThreadLocal<Deque<Writes>> writesQueueLocal = new ThreadLocal<Deque<Writes>>();

    private static class Writes {

        public int depth;
        public final List<State> validates = new ArrayList<State>();
        public final List<State> saves = new ArrayList<State>();
        public final List<State> indexes = new ArrayList<State>();
        public final List<State> deletes = new ArrayList<State>();
        public final Map<ObjectIndex, List<State>> recalculations = new CompactMap<ObjectIndex, List<State>>();

        public void addToValidates(State state) {
            validates.remove(state);
            validates.add(state);

            saveJunctions(state, state.getDatabase().getEnvironment());

            ObjectType type = state.getType();

            if (type != null) {
                saveJunctions(state, type);
            }
        }

        public void addToSaves(State state) {
            for (Iterator<State> i = saves.iterator(); i.hasNext();) {
                State s = i.next();

                if (s.equals(state)) {
                    i.remove();
                    state.getAtomicOperations().addAll(0, s.getAtomicOperations());
                }
            }

            saves.add(state);
        }

        private void saveJunctions(State state, ObjectStruct struct) {
            for (ObjectField field : struct.getFields()) {
                String junctionField = field.getJunctionField();

                if (!ObjectUtils.isBlank(junctionField)) {
                    String fieldName = field.getInternalName();
                    List<Object> oldItems = field.findJunctionItems(state);
                    Iterable<?> newItems = ObjectUtils.to(Iterable.class, state.get(fieldName));

                    if (newItems != null) {
                        Iterator<?> i = newItems.iterator();

                        if (i.hasNext()) {
                            Double lastPosition = null;
                            String positionField = field.getJunctionPositionField();

                            for (Object item = i.next(), next = null; item != null; item = next) {
                                next = i.hasNext() ? i.next() : null;
                                State itemState = State.getInstance(item);
                                Object junction = itemState.get(junctionField);
                                double position;
                                boolean save = false;

                                oldItems.remove(item);

                                if (junction == null
                                        || (junction instanceof Recordable
                                        && !State.getInstance(junction).equals(state))) {
                                    save = true;

                                    itemState.put(junctionField, state.getOriginalObject());

                                } else if (junction instanceof Collection) {
                                    save = true;
                                    @SuppressWarnings("unchecked")
                                    Collection<Object> junctionCollection = (Collection<Object>) junction;
                                    Object stateObject = state.getOriginalObject();

                                    if (!junctionCollection.contains(stateObject)) {
                                        junctionCollection.add(stateObject);
                                    }
                                }

                                if (!ObjectUtils.isBlank(positionField)) {
                                    position = ObjectUtils.to(double.class, itemState.get(positionField));

                                    if (position == 0.0) {
                                        position = 0.1;
                                        save = true;
                                    }

                                    if (lastPosition == null) {
                                        if (next != null) {
                                            double nextPosition = ObjectUtils.to(double.class, State.getInstance(next).get(positionField));

                                            if (nextPosition <= position) {
                                                position = nextPosition - 1.0;
                                                save = true;
                                            }
                                        }

                                    } else if (lastPosition >= position) {
                                        if (next == null) {
                                            position = lastPosition + 1.0;

                                        } else {
                                            double nextPosition = ObjectUtils.to(double.class, State.getInstance(next).get(positionField));
                                            position = (lastPosition + nextPosition) / 2.0;

                                            if (lastPosition >= position) {
                                                position = lastPosition + 1.0;
                                            }
                                        }

                                        save = true;
                                    }

                                    lastPosition = position;

                                    if (save) {
                                        itemState.put(positionField, position);
                                    }
                                }

                                if (save) {
                                    if (!saves.contains(itemState)) {
                                        if (Boolean.TRUE.equals(itemState.getExtra(SAVING_UNSAFELY_EXTRA))) {
                                            itemState.saveUnsafely();
                                        } else {
                                            itemState.save();
                                        }
                                    }
                                }
                            }
                        }
                    }

                    for (Object item : oldItems) {
                        State itemState = State.getInstance(item);

                        itemState.remove(junctionField);
                        if (!saves.contains(itemState)) {
                            if (Boolean.TRUE.equals(itemState.getExtra(SAVING_UNSAFELY_EXTRA))) {
                                itemState.saveUnsafely();
                            } else {
                                itemState.save();
                            }
                        }
                    }
                }
            }
        }

        public void addToIndexes(State state) {
            for (Iterator<State> i = indexes.iterator(); i.hasNext();) {
                State s = i.next();
                if (s.equals(state)) {
                    i.remove();
                }
            }
            indexes.add(state);
        }

        public void addToDeletes(State state) {
            deletes.remove(state);
            deletes.add(state);
        }

        public void addToRecalculations(State state, ObjectIndex index) {
            if (index == null || state == null) {
                return;
            }
            if (!recalculations.containsKey(index)) {
                recalculations.put(index, new ArrayList<State>());
            }
            recalculations.get(index).remove(state);
            recalculations.get(index).add(state);
        }

    }

    /**
     * Returns all {@linkplain ObjectType#getGroups groups} of types that
     * can be saved to this database.
     */
    public Set<String> getGroups() {
        if (groups == null) {
            groups = new SparseSet("+/");
        }
        return groups;
    }

    /**
     * Sets all {@linkplain ObjectType#getGroups groups} of types that
     * can be saved to this database.
     */
    public void setGroups(Set<String> groups) {
        this.groups = groups;
    }

    /**
     * Returns {@code true} if the given {@code types} represent all
     * types that can be saved to this database as indicated in
     * {@link #getGroups}.
     */
    public boolean isAllTypes(Collection<ObjectType> types) {
        Set<String> databaseGroups = getGroups();
        int allTypesCount = 0;
        for (ObjectType type : getEnvironment().getTypes()) {
            if (!(type.isAbstract() || type.isEmbedded())) {
                for (String typeGroup : type.getGroups()) {
                    if (databaseGroups.contains(typeGroup)) {
                        ++ allTypesCount;
                        break;
                    }
                }
            }
        }
        return types.size() == allTypesCount;
    }

    /**
     * Returns the read timeout, in seconds.
     *
     * @return May be less than or equal to {@code 0} to indicate
     *         no timeout.
     */
    public double getReadTimeout() {
        return readTimeout;
    }

    /**
     * Sets the read timeout, in seconds.
     *
     * @param readTimeout May be less than or equal to {@code 0} to
     *        indicate no timeout.
     */
    public void setReadTimeout(double readTimeout) {
        this.readTimeout = readTimeout;
    }

    /**
     * Opens an implementation-specific connection to the underlying
     * database. Once opened, the connection should be closed with
     * {@link #closeConnection}.
     *
     * @return Can't be {@code null}.
     */
    public abstract C openConnection();

    /**
     * Opens an implementation-specific connection to the underlying
     * database that can only be used for reading data. Once opened,
     * the connection should be closed with {@link #closeConnection}.
     *
     * @return Can't be {@code null}.
     */
    @SuppressWarnings("deprecation")
    public final C openReadConnection() {
        return Database.Static.isIgnoreReadConnection()
                ? openConnection()
                : doOpenReadConnection();
    }

    /**
     * Called when this database needs to open an implementation-specific
     * connection to the underlying database that can only be used for
     * reading data. The default implementation calls
     * {@link #openConnection}.
     *
     * @return Can't be {@code null}.
     */
    protected C doOpenReadConnection() {
        return openConnection();
    }

    /**
     * Opens a connection that should be used to execute the given
     * {@code query}.
     */
    public C openQueryConnection(Query<?> query) {
        return query != null && query.isMaster()
                ? openConnection()
                : openReadConnection();
    }

    /**
     * Closes the given implementation-specific {@code connection}
     * to the underlying database.
     */
    public abstract void closeConnection(C connection);

    /** Returns {@code true} if the given {@code error} is recoverable. */
    protected boolean isRecoverableError(Exception error) {
        return false;
    }

    // --- Database support ---

    @Override
    public final synchronized void initialize(String settingsKey, Map<String, Object> settings) {
        String groupsPattern = ObjectUtils.to(String.class, settings.get(GROUPS_SUB_SETTING));
        setGroups(new SparseSet(ObjectUtils.isBlank(groupsPattern) ? "+/" : groupsPattern));

        Double readTimeout = ObjectUtils.to(Double.class, settings.get(READ_TIMEOUT_SUB_SETTING));
        if (readTimeout != null) {
            setReadTimeout(readTimeout);
        }

        doInitialize(settingsKey, settings);
    }

    /**
     * Called to initialize this database using the given {@code settings}.
     */
    protected abstract void doInitialize(String settingsKey, Map<String, Object> settings);

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
            setEnvironment(new DatabaseEnvironment(this));
        }
        return environment;
    }

    @Override
    public void setEnvironment(DatabaseEnvironment environment) {
        this.environment = environment;
    }

    @Override
    public <T> Iterable<T> readIterable(Query<T> query, int fetchSize) {
        return new PaginatedIterable<T>(query, fetchSize);
    }

    private static class PaginatedIterable<T> implements Iterable<T> {

        private final Query<T> query;
        private final int fetchSize;

        public PaginatedIterable(Query<T> query, int fetchSize) {
            this.query = query;
            this.fetchSize = fetchSize;
        }

        @Override
        public Iterator<T> iterator() {
            return query.getSorters().isEmpty()
                    ? new ByIdIterator<T>(query, fetchSize)
                    : new PaginatedIterator<T>(query, fetchSize);
        }
    }

    private static class ByIdIterator<T> implements Iterator<T> {

        private final Query<T> query;
        private final int fetchSize;
        private UUID lastObjectId;
        private PaginatedResult<T> result;
        private int index;

        public ByIdIterator(Query<T> query, int fetchSize) {
            this.query = query.clone().sortAscending("_id");
            this.fetchSize = fetchSize > 0 ? fetchSize : 200;
        }

        @Override
        public boolean hasNext() {
            if (result != null && index >= result.getItems().size()) {
                if (result.hasNext()) {
                    result = null;
                } else {
                    return false;
                }
            }

            if (result == null) {
                Query<T> nextQuery = query.clone();
                if (lastObjectId != null) {
                    nextQuery.and("_id > ?", lastObjectId);
                }

                result = nextQuery.select(0, fetchSize);
                List<T> items = result.getItems();

                int size = items.size();
                if (size < 1) {
                    return false;
                }

                lastObjectId = State.getInstance(items.get(size - 1)).getId();
                index = 0;
            }

            return true;
        }

        @Override
        public T next() {
            if (hasNext()) {
                T object = result.getItems().get(index);
                ++ index;
                return object;

            } else {
                throw new NoSuchElementException();
            }
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    private static class PaginatedIterator<T> implements Iterator<T> {

        private final Query<T> query;
        private PaginatedResult<T> result;
        private long offset;
        private final int limit;
        private int index;

        public PaginatedIterator(Query<T> query, int limit) {
            this.query = query;
            this.limit = limit > 0 ? limit : 200;
        }

        @Override
        public boolean hasNext() {
            if (result != null && index >= result.getItems().size()) {
                if (result.hasNext()) {
                    result = null;
                } else {
                    return false;
                }
            }

            if (result == null) {
                result = query.select(offset, limit);
                List<T> items = result.getItems();

                int size = items.size();
                if (size < 1) {
                    return false;
                }

                offset += limit;
                index = 0;
            }

            return true;
        }

        @Override
        public T next() {
            if (hasNext()) {
                T object = result.getItems().get(index);
                ++ index;
                return object;

            } else {
                throw new NoSuchElementException();
            }
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    @Override
    public <T> PaginatedResult<Grouping<T>> readPartialGrouped(Query<T> query, long offset, int limit, String... fields) {
        Map<List<Object>, BasicGrouping<T>> groupingsMap = new CompactMap<List<Object>, BasicGrouping<T>>();
        for (Object item : readIterable(query, 0)) {

            State itemState = State.getInstance(item);
            List<Object> keys = new ArrayList<Object>();
            if (fields != null) {
                for (String field : fields) {
                    Matcher groupingMatcher = Query.RANGE_PATTERN.matcher(field);
                    if (groupingMatcher.find()) {
                        Double bucket = null;
                        String fieldName = groupingMatcher.group(1);

                        if (itemState.getByPath(fieldName) != null) {
                            Double start = ObjectUtils.to(Double.class, groupingMatcher.group(2).trim());
                            Double end   = ObjectUtils.to(Double.class, groupingMatcher.group(3).trim());
                            Double gap   = ObjectUtils.to(Double.class, groupingMatcher.group(4).trim());
                            Double value = ObjectUtils.to(Double.class, itemState.getByPath(fieldName));

                            for (double window = start; window <= end; window += gap) {
                                if (value < window) {
                                    bucket = window - gap;
                                    break;
                                }
                            }
                        }
                        keys.add(bucket);

                    } else {
                        keys.add(itemState.getByPath(field));
                    }
                }
            }
            BasicGrouping<T> grouping = groupingsMap.get(keys);
            if (grouping == null) {
                grouping = new BasicGrouping<T>(keys, query, fields);
                groupingsMap.put(keys, grouping);
            }

            grouping.count += 1;
        }

        List<Grouping<T>> groupings = new ArrayList<Grouping<T>>(groupingsMap.values());
        return new PaginatedResult<Grouping<T>>(offset, limit, groupings);
    }

    /** Basic implementation of {@link Grouping}. */
    private class BasicGrouping<T> extends AbstractGrouping<T> {

        private long count;

        public BasicGrouping(List<Object> keys, Query<T> query, String[] fields) {
            super(keys, query, fields);
        }

        // --- AbstractGrouping support ---

        @Override
        protected Aggregate createAggregate(String field) {
            Aggregate aggregate = new Aggregate();
            Query<?> aggregateQuery = Query.fromQuery(query);
            List<Object> keys = getKeys();

            ITEM: for (Object item : readIterable(aggregateQuery, 0)) {
                State itemState = State.getInstance(item);
                Object value = itemState.getByPath(field);
                if (value == null) {
                    continue;
                }

                for (int i = 0, length = fields.length; i < length; ++ i) {
                    if (!ObjectUtils.equals(keys.get(i), itemState.getByPath(fields[i]))) {
                        continue ITEM;
                    }
                }

                aggregate.setNonNullCount(aggregate.getNonNullCount() + 1);

                if (ObjectUtils.compare(aggregate.getMaximum(), value, false) < 0) {
                    aggregate.setMaximum(value);
                }

                if (ObjectUtils.compare(aggregate.getMinimum(), value, true) > 0) {
                    aggregate.setMinimum(value);
                }

                Double valueDouble = ObjectUtils.to(Double.class, value);
                if (valueDouble != null) {
                    aggregate.setSum(aggregate.getSum() + valueDouble);
                }
            }

            return aggregate;
        }

        @Override
        public long getCount() {
            return count;
        }
    }

    private final Deque<Writes> getOrCreateWritesQueue() {
        Deque<Writes> writesQueue = writesQueueLocal.get();
        if (writesQueue == null) {
            writesQueue = new ArrayDeque<Writes>();
            writesQueueLocal.set(writesQueue);
        }
        return writesQueue;
    }

    @Override
    public final boolean beginWrites() {
        Deque<Writes> writesQueue = getOrCreateWritesQueue();
        if (writesQueue.isEmpty()) {
            writesQueue.addLast(new Writes());
            return true;

        } else {
            ++ writesQueue.peekLast().depth;
            return false;
        }
    }

    @Override
    public final void beginIsolatedWrites() {
        getOrCreateWritesQueue().addLast(new Writes());
    }

    @Override
    public final boolean commitWrites() {
        return doCommitWrites(true);
    }

    @Override
    public final boolean commitWritesEventually() {
        return doCommitWrites(false);
    }

    private boolean doCommitWrites(boolean isImmediate) {
        Deque<Writes> writesQueue = writesQueueLocal.get();
        if (writesQueue == null || writesQueue.isEmpty()) {
            throw new IllegalStateException("Can't commit writes that never began!");
        }

        Writes writes = writesQueue.peekLast();
        if (writes.depth > 0) {
            return false;

        } else {
            try {
                write(writes.validates, writes.saves, writes.indexes, writes.deletes, writes.recalculations, isImmediate);
                return true;

            } finally {
                writes.validates.clear();
                writes.saves.clear();
                writes.indexes.clear();
                writes.deletes.clear();
                writes.recalculations.clear();
            }
        }
    }

    @Override
    public final boolean endWrites() {
        Deque<Writes> writesQueue = writesQueueLocal.get();
        if (writesQueue == null || writesQueue.isEmpty()) {
            throw new IllegalStateException("Can't end writes that never began!");
        }

        Writes writes = writesQueue.peekLast();
        if (writes.depth > 0) {
            -- writes.depth;
            return false;

        } else {
            writesQueue.removeLast();
            if (writesQueue.isEmpty()) {
                writesQueueLocal.remove();
            }
            return true;
        }
    }

    private final Writes getCurrentWrites() {
        Deque<Writes> writesQueue = writesQueueLocal.get();
        return writesQueue != null ? writesQueue.peekLast() : null;
    }

    private static class BeforeSaveTrigger extends TriggerOnce {

        @Override
        protected void executeOnce(Object object) {
            if (object instanceof Record) {
                ((Record) object).beforeSave();
            }
        }
    };

    private static class BeforeCommitTrigger extends TriggerOnce {

        @Override
        protected void executeOnce(Object object) {
            if (object instanceof Record) {
                ((Record) object).beforeCommit();
            }
        }
    }

    private static class AfterSaveTrigger extends TriggerOnce {

        @Override
        protected void executeOnce(Object object) {
            if (object instanceof Record) {
                Record record = (Record) object;

                try {
                    record.afterSave();

                } catch (RuntimeException error) {
                    LOGGER.warn(
                            String.format("Couldn't run afterSave on [%s]", record.getId()),
                            error);
                }
            }
        }
    };

    private static class BeforeDeleteTrigger extends TriggerOnce {

        @Override
        protected void executeOnce(Object object) {
            if (object instanceof Record) {
                ((Record) object).beforeDelete();
            }
        }
    };

    private static class AfterDeleteTrigger extends TriggerOnce {

        @Override
        protected void executeOnce(Object object) {
            if (object instanceof Record) {
                Record record = (Record) object;

                try {
                    record.afterDelete();

                } catch (RuntimeException error) {
                    LOGGER.warn("Couldn't run afterDelete on [{}]", record.getId());
                }
            }
        }
    };

    @Override
    public final void save(State state) {
        checkState(state);

        state.fireTrigger(new BeforeSaveTrigger());

        Writes writes = getCurrentWrites();

        if (writes != null) {
            writes.addToValidates(state);
            writes.addToSaves(state);

        } else {
            writes = new Writes();

            writes.addToValidates(state);
            writes.addToSaves(state);
            write(writes.validates, writes.saves, null, null, null, true);
        }
    }

    @Override
    public final void saveUnsafely(State state) {
        checkState(state);

        state.getExtras().put(SAVING_UNSAFELY_EXTRA, true);
        Writes writes = getCurrentWrites();
        if (writes != null) {
            writes.addToSaves(state);

        } else {
            write(null, Arrays.asList(state), null, null, null, true);
        }
        state.getExtras().remove(SAVING_UNSAFELY_EXTRA);
    }

    @Override
    public final void index(State state) {
        checkState(state);

        Writes writes = getCurrentWrites();
        if (writes != null) {
            writes.addToIndexes(state);

        } else {
            write(null, null, Arrays.asList(state), null, null, true);
        }
    }

    // Checks to make sure that the given {@code state} is savable.
    private void checkState(State state) {
        if (state == null) {
            throw new IllegalArgumentException("State is required!");
        }

        if (state.isReferenceOnly()) {
            throw new IllegalArgumentException(String.format(
                    "Can't write a reference-only object! (%s)",
                    state.getId()));
        }

        ObjectType type = state.getType();

        if (type != null && !type.isConcrete()) {
            throw new IllegalStateException(String.format(
                    "Can't save a non-concrete object! (%s)",
                    type.getLabel()));
        }
    }

    @Override
    public void indexAll(ObjectIndex index) {
    }

    @Override
    public void recalculate(State state, ObjectIndex... indexes) {
        checkState(state);
        Writes writes = getCurrentWrites();
        Map<ObjectIndex, List<State>> recalculations = writes == null ? new CompactMap<ObjectIndex, List<State>>() : null;
        for (ObjectIndex index : indexes) {
            if (index.isVisibility()) {
                throw new IllegalArgumentException("Updating single field index value is unsupported for visibility indexes!");
            }

            if (writes != null) {
                writes.addToRecalculations(state, index);

            } else {
                recalculations.put(index, Arrays.asList(state));
            }
        }
        if (recalculations != null) {
            write(null, null, null, null, recalculations, true);
        }
    }

    @Override
    public final void delete(State state) {
        state.fireTrigger(new BeforeDeleteTrigger());

        Writes writes = getCurrentWrites();

        if (writes != null) {
            writes.addToDeletes(state);

        } else {
            write(null, null, null, Arrays.asList(state), null, true);
        }
    }

    @Override
    public void deleteByQuery(Query<?> query) {
        int batchSize = 200;
        try {
            beginWrites();

            int i = 0;
            for (Object item : readIterable(query, batchSize)) {
                delete(State.getInstance(item));
                ++ i;
                if (i % batchSize == 0) {
                    commitWrites();
                }
            }

            commitWrites();
        } finally {
            endWrites();
        }
    }

    // Performs the given write operations.
    private void write(
            List<State> validates,
            List<State> saves,
            List<State> indexes,
            List<State> deletes,
            Map<ObjectIndex, List<State>> recalculations,
            boolean isImmediate) {

        boolean hasValidates = validates != null && !validates.isEmpty();
        boolean hasSaves = saves != null && !saves.isEmpty();
        boolean hasIndexes = indexes != null && !indexes.isEmpty();
        boolean hasDeletes = deletes != null && !deletes.isEmpty();
        boolean hasRecalculations = recalculations != null && !recalculations.isEmpty();

        if (!(hasValidates || hasSaves || hasIndexes || hasDeletes || hasRecalculations)) {
            return;
        }

        List<DistributedLock> locks = validate(validates, true);

        try {
            if (locks != null && !locks.isEmpty()) {
                for (DistributedLock lock : locks) {
                    lock.lock();
                }
                validate(validates, false);
            }

            if (validates != null) {
                for (State state : validates) {
                    state.fireTrigger(new BeforeCommitTrigger());
                }
            }

            boolean isCommitted = false;
            Exception lastError = null;

            for (int i = 0, limit = Settings.getOrDefault(int.class, "dari/databaseWriteRetryLimit", 10); i < limit; ++ i) {
                try {
                    C connection = openConnection();

                    try {
                        try {
                            beginTransaction(connection, isImmediate);
                            doWrites(connection, isImmediate, saves, indexes, deletes);
                            doWriteRecalculations(connection, isImmediate, recalculations);
                            commitTransaction(connection, isImmediate);
                            isCommitted = true;
                            break;

                        } finally {
                            try {
                                if (!isCommitted) {
                                    rollbackTransaction(connection, isImmediate);
                                }

                            } finally {
                                endTransaction(connection, isImmediate);
                            }
                        }

                    } finally {
                        closeConnection(connection);
                    }

                } catch (Retry error) {
                    -- i;

                } catch (Exception error) {
                    lastError = error;

                    if (error instanceof RecoverableDatabaseException
                            || isRecoverableError(error)) {
                        try {
                            long initialPause = Settings.getOrDefault(long.class, "dari/databaseWriteRetryInitialPause", 10L);
                            long finalPause = Settings.getOrDefault(long.class, "dari/databaseWriteRetryFinalPause", 1000L);
                            double pauseJitter = Settings.getOrDefault(double.class, "dari/databaseWriteRetryPauseJitter", 0.5);
                            long pause = ObjectUtils.jitter(initialPause + (finalPause - initialPause) * i / (limit - 1), pauseJitter);
                            Thread.sleep(pause);
                            continue;

                        } catch (InterruptedException ex2) {
                            // Ignore thread interruption and continue.
                        }
                    }

                    break;
                }
            }

            if (!isCommitted) {
                if (lastError instanceof DatabaseException) {
                    throw (DatabaseException) lastError;

                } else if (isRecoverableError(lastError)) {
                    throw new RecoverableDatabaseException(this, lastError);

                } else {
                    throw new DatabaseException(this, lastError);
                }
            }

        } finally {
            if (locks != null && !locks.isEmpty()) {
                for (DistributedLock lock : locks) {
                    try {
                        lock.unlock();
                    } catch (Throwable ex) {
                        if (LOGGER.isDebugEnabled()) {
                            LOGGER.debug("Can't unlock [" + lock + "]!", ex);
                        }
                    }
                }
            }
        }

        if (hasValidates) {
            for (State state : validates) {
                state.setStatus(StateStatus.SAVED);
                state.fireTrigger(new AfterSaveTrigger());
            }
        }

        if (hasDeletes) {
            for (State state : deletes) {
                state.setStatus(StateStatus.DELETED);
                state.fireTrigger(new AfterDeleteTrigger());
            }
        }
    }

    private static class OnDuplicateTrigger extends TriggerOnce {

        private final ObjectIndex index;
        private boolean corrected;

        public OnDuplicateTrigger(ObjectIndex index) {
            this.index = index;
        }

        @Override
        protected void executeOnce(Object object) {
            if (object instanceof Record) {
                corrected = ((Record) object).onDuplicate(index) || corrected;
            }
        }

        public boolean isCorrected() {
            return corrected;
        }
    }

    // Validates the given states and returns a list of locks that
    // should be used to enforce unique constraints.
    private List<DistributedLock> validate(List<State> states, boolean beforeLocks) {
        if (states == null || states.isEmpty()) {
            return null;
        }

        List<State> errors = null;
        Map<String, State> keys = null;
        DatabaseEnvironment environment = getEnvironment();

        for (State state : states) {
            boolean retry;

            RETRY: do {
                retry = false;

                if (beforeLocks) {
                    if (!state.validate()) {
                        if (errors == null) {
                            errors = new ArrayList<State>();
                        }
                        errors.add(state);
                    }

                } else {
                    state.clearAllErrors();
                }

                ObjectType type = state.getType();
                for (ObjectStruct struct : type != null
                        ? new ObjectStruct[] { type, environment }
                        : new ObjectStruct[] { environment }) {

                    for (ObjectIndex index : struct.getIndexes()) {
                        if (!index.isUnique()) {
                            continue;
                        }

                        Object[][] valuePermutations = index.getValuePermutations(state);
                        if (valuePermutations == null) {
                            continue;
                        }

                        String indexPrefix = index.getPrefix();
                        String indexName = index.getUniqueName();
                        List<String> fields = index.getFields();

                        for (int i = 0, ps = valuePermutations.length; i < ps; ++ i) {
                            Query<Object> duplicateQuery = Query
                                    .from(Object.class)
                                    .where("id != ?", state.getId())
                                    .using(state.getDatabase())
                                    .referenceOnly()
                                    .noCache()
                                    .master();

                            StringBuilder keyBuilder = new StringBuilder();
                            keyBuilder.append(indexName);

                            Object[] values = valuePermutations[i];
                            for (int j = 0, vs = values.length; j < vs; ++ j) {
                                Object value = values[j];
                                keyBuilder.append('\0');
                                keyBuilder.append(value);
                                duplicateQuery.and(indexPrefix + fields.get(j) + " = ?", values[j]);
                            }

                            Object duplicate = duplicateQuery.first();

                            if (duplicate == null) {
                                if (!beforeLocks) {
                                    continue;

                                } else {
                                    if (keys == null) {
                                        keys = new HashMap<String, State>();
                                    }

                                    String key = keyBuilder.toString();
                                    duplicate = keys.get(key);
                                    if (duplicate == null) {
                                        keys.put(key, state);
                                        continue;

                                    } else if (state.equals(State.getInstance(duplicate))) {
                                        continue;
                                    }
                                }
                            }

                            OnDuplicateTrigger trigger = new OnDuplicateTrigger(index);

                            state.fireTrigger(trigger);

                            if (trigger.isCorrected()) {
                                retry = true;
                                continue RETRY;
                            }

                            if (errors == null) {
                                errors = new ArrayList<State>();
                            }
                            errors.add(state);
                            state.addError(
                                    state.getField(index.getField()),
                                    "Must be unique but duplicate at "
                                            + (State.getInstance(duplicate).getId())
                                            + "!");
                        }
                    }
                }
            } while (retry);
        }

        if (errors != null && !errors.isEmpty()) {
            throw new ValidationException(errors);
        }

        if (keys == null || keys.isEmpty()) {
            return null;

        } else {
            List<DistributedLock> locks = new ArrayList<DistributedLock>();
            for (String key : keys.keySet()) {
                locks.add(DistributedLock.Static.getInstance(this, key));
            }
            return locks;
        }
    }

    /**
     * Called by the write methods to begin a transaction.
     *
     * @param connection {@link #openConnection Implementation-specific
     *        connection} to the underlying database.
     */
    protected void beginTransaction(C connection, boolean isImmediate) throws Exception {
    }

    /**
     * Called by the write methods to commit the transaction.
     *
     * @param connection {@link #openConnection Implementation-specific
     *        connection} to the underlying database.
     * @param isImmediate If {@code true}, the saved data must be
     *        available for read immediately.
     */
    protected void commitTransaction(C connection, boolean isImmediate) throws Exception {
    }

    /**
     * Called by the write methods to roll back the transaction.
     *
     * @param connection {@link #openConnection Implementation-specific
     *        connection} to the underlying database.
     */
    protected void rollbackTransaction(C connection, boolean isImmediate) throws Exception {
    }

    /**
     * Called by the write methods to end the transaction.
     *
     * @param connection {@link #openConnection Implementation-specific
     *        connection} to the underlying database.
     */
    protected void endTransaction(C connection, boolean isImmediate) throws Exception {
    }

    /**
     * Called by the write methods to save, index, or delete the given states.
     *
     * @param connection {@link #openConnection Implementation-specific
     *        connection} to the underlying database.
     */
    protected void doWrites(C connection, boolean isImmediate, List<State> saves, List<State> indexes, List<State> deletes) throws Exception {
        if (saves != null && !saves.isEmpty()) {
            doSaves(connection, isImmediate, saves);
        }
        if (indexes != null && !indexes.isEmpty()) {
            doIndexes(connection, isImmediate, indexes);
        }
        if (deletes != null && !deletes.isEmpty()) {
            doDeletes(connection, isImmediate, deletes);
        }
    }

    protected void doWriteRecalculations(C connection, boolean isImmediate, Map<ObjectIndex, List<State>> recalculations) throws Exception {
        if (recalculations != null) {
            for (Map.Entry<ObjectIndex, List<State>> entry : recalculations.entrySet()) {
                doRecalculations(connection, isImmediate, entry.getKey(), entry.getValue());
            }
        }
    }

    /**
     * Called by the write methods to save the given {@code states}.
     *
     * @param connection {@link #openConnection Implementation-specific
     *        connection} to the underlying database.
     */
    protected void doSaves(C connection, boolean isImmediate, List<State> states) throws Exception {
    }

    /**
     * Called by the write methods to index the given {@code states}.
     *
     * @param connection {@link #openConnection Implementation-specific
     *        connection} to the underlying database.
     */
    protected void doIndexes(C connection, boolean isImmediate, List<State> states) throws Exception {
    }

    /**
     * Called by the write methods to delete the given {@code states}.
     *
     * @param connection {@link #openConnection Implementation-specific
     *        connection} to the underlying database.
     */
    protected void doDeletes(C connection, boolean isImmediate, List<State> states) throws Exception {
    }

    /**
     * Called by the write methods to delete the given {@code states}.
     *
     * @param connection {@link #openConnection Implementation-specific
     *        connection} to the underlying database.
     */
    protected void doRecalculations(C connection, boolean isImmediate, ObjectIndex index, List<State> states) throws Exception {
    }

    @SuppressWarnings("serial")
    private static class Retry extends Error {

        @Override
        public Throwable fillInStackTrace() {
            return null;
        }
    }

    private static final Retry RETRY_INSTANCE = new Retry();

    /**
     * Retries the current writes. This method should only be called within
     * {@link #doWrites}, {@link #doSaves}, {@link #doIndexes}, or
     * {@link #doDeletes}.
     */
    protected void retryWrites() {
        throw RETRY_INSTANCE;
    }

    // --- Implementation helpers ---

    /**
     * Implementation helper method to create a previously saved object
     * with the given {@code id}, of the type represented by the given
     * {@code typeId}, and sets common state options based on the given
     * {@code query}. The ID parameters may be of any UUID-like object.
     */
    protected final <T> T createSavedObject(Object typeId, Object id, Query<T> query) {
        DatabaseEnvironment environment = getEnvironment();
        UUID typeIdUuid = ObjectUtils.to(UUID.class, typeId);
        UUID idUuid = ObjectUtils.to(UUID.class, id);

        if (typeIdUuid == null) {
            Object nullType = query.getOptions().get(NULL_TYPE_QUERY_OPTION);
            if (nullType != null) {
                if (nullType instanceof ObjectType) {
                    typeIdUuid = ((ObjectType) nullType).getId();
                } else if (nullType instanceof Class) {
                    typeIdUuid = environment.getTypeByClass((Class<?>) nullType).getId();
                } else if (nullType instanceof UUID) {
                    typeIdUuid = (UUID) nullType;
                } else if (nullType instanceof String) {
                    typeIdUuid = environment.getTypeByName((String) nullType).getId();
                } else {
                    throw new IllegalArgumentException(String.format(
                            "Can't interpret [%s] as a type!", nullType));
                }
            }
        }

        @SuppressWarnings("unchecked")
        T object = (T) environment.createObject(typeIdUuid, idUuid);
        State objectState = State.getInstance(object);

        if (idUuid != null) {
            objectState.setStatus(StateStatus.SAVED);
        }

        objectState.getExtras().put(Database.CREATOR_EXTRA, this);
        objectState.getExtras().put(Query.CREATOR_EXTRA, query);

        if (query != null) {
            objectState.setDatabase(query.getDatabase());
            objectState.setResolveToReferenceOnly(query.isResolveToReferenceOnly());
            objectState.setResolveUsingCache(query.isCache());
            objectState.setResolveUsingMaster(query.isMaster());
            objectState.setResolveInvisible(query.isResolveInvisible());

            if (query.isReferenceOnly()) {
                objectState.setStatus(StateStatus.REFERENCE_ONLY);
            }
        }

        return object;
    }

    @SuppressWarnings("unchecked")
    protected final <T> T swapObjectType(Query<T> query, T object) {
        DatabaseEnvironment environment = getEnvironment();
        State state = State.getInstance(object);
        ObjectType type = state.getType();

        if (type != null) {
            Class<?> objectClass = type.getObjectClass();

            if (objectClass != null && !objectClass.isInstance(object)) {
                State oldState = state;
                object = (T) environment.createObject(state.getTypeId(), state.getId());
                state = State.getInstance(object);

                state.setDatabase(oldState.getRealDatabase());
                state.setResolveToReferenceOnly(oldState.isResolveToReferenceOnly());
                state.setResolveUsingCache(oldState.isResolveUsingCache());
                state.setResolveUsingMaster(oldState.isResolveUsingMaster());
                state.setResolveInvisible(oldState.isResolveInvisible());
                state.setStatus(oldState.getStatus());
                state.setValues(oldState);
                state.getExtras().putAll(oldState.getExtras());
            }
        }

        populateJunctions(state, environment);

        if (type != null) {
            populateJunctions(state, type);
        }

        if (object instanceof ObjectType) {
            ObjectType asType = environment.getTypeById(((ObjectType) object).getId());

            if (asType != null && asType != object) {
                return (T) asType.clone();
            }
        }

        return object;
    }

    private void populateJunctions(State state, ObjectStruct struct) {
        for (ObjectField field : struct.getFields()) {
            String junctionField = field.getJunctionField();

            if (!ObjectUtils.isBlank(junctionField)) {
                state.put(field.getInternalName(), new JunctionList(state, field));
            }
        }
    }

    // --- Object support ---

    @Override
    public String toString() {
        return getName();
    }

    // --- Deprecated ---

    /** @deprecated Use {@link #TRIGGER_EXTRA_PREFIX} instead. */
    @Deprecated
    public static final String BEFORE_SAVES_EXTRA = "db.beforeSaves";

    /** @deprecated Use {@link #READ_TIMEOUT_SUB_SETTING} instead. */
    @Deprecated
    public static final String READ_TIMEOUT_SETTING = READ_TIMEOUT_SUB_SETTING;
}
