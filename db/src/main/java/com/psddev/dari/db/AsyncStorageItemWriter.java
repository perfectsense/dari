package com.psddev.dari.db;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.psddev.dari.util.AsyncConsumer;
import com.psddev.dari.util.AsyncQueue;
import com.psddev.dari.util.StorageItem;

/**
 * Background task that efficiently copies storage items from one
 * location to another, and optionally resaves the containing objects.
 */
public class AsyncStorageItemWriter<E> extends AsyncConsumer<E> {

    public static final int DEFAULT_COMMIT_SIZE = 100;

    private static final Logger LOGGER = LoggerFactory.getLogger(AsyncStorageItemWriter.class);

    private final Database database;
    private final WriteOperation operation;
    private final int commitSize;
    private final boolean isCommitEventually;
    private final List<State> states;
    private final String source;
    private final String destination;
    private final boolean saveObject;

    /**
     * Creates a new instance that runs in the given {@code executor},
     * consumes items from the given {@code input} queue, and writes
     * them to the given {@code database}.
     *
     * @param executor If {@code null}, uses the default executor.
     * @param input Can't be {@code null}.
     * @param database Can't be {@code null}.
     * @param operation Can't be {@code null}.
     * @param commitSize If less than or equal destination {@code 0}, it will be
     *        set destination {@value #DEFAULT_COMMIT_SIZE} instead.
     * @param isCommitEventually If {@code true},
     *        {@link Database#commitWritesEventually} is used instead of
     *        {@link Database#commitWrites}.
     * @throws IllegalArgumentException If the given {@code input}
     *         or {@code database} is {@code null}.
     */
    public AsyncStorageItemWriter(
            String executor,
            AsyncQueue<E> input,
            Database database,
            WriteOperation operation,
            int commitSize,
            boolean isCommitEventually,
            String source,
            String destination,
            boolean saveObject) {

        super(executor, input);

        if (database == null) {
            throw new IllegalArgumentException("Database can't be null!");
        }
        if (operation == null) {
            throw new IllegalArgumentException("Operation can't be null!");
        }

        this.database = database;
        this.operation = operation;
        this.commitSize = commitSize > 0 ? commitSize : DEFAULT_COMMIT_SIZE;
        this.isCommitEventually = isCommitEventually;
        this.source = source;
        this.destination = destination;
        this.states = new ArrayList<State>(this.commitSize);
        this.saveObject = saveObject;
    }

    // Commits all pending state writes.
    private void commit() {
        if (saveObject) {
            try {
                database.beginWrites();

                for (State state : states) {
                    operation.execute(database, state);
                }

                if (isCommitEventually) {
                    database.commitWritesEventually();
                } else {
                    database.commitWrites();
                }

            } finally {
                states.clear();
                database.endWrites();
            }
        }
    }

    @Override
    protected void consume(E item) {
        State state = State.getInstance(item);
        if (copyAny(state.getValues(), source, destination)) {
            states.add(State.getInstance(item));
            if (states.size() == commitSize) {
                commit();
            }
        }
    }

    @Override
    protected void finished() {
        super.finished();
        commit();
    }

    @SuppressWarnings("unchecked")
    private boolean copyAny(Object value, String source, String destination) {
        boolean isChanged = false;

        if (value instanceof List) {
            for (ListIterator<Object> i = ((List<Object>) value).listIterator(); i.hasNext();) {
                Object item = i.next();
                if (item instanceof StorageItem) {
                    StorageItem newItem = copyItem((StorageItem) item, source, destination);
                    if (newItem != null) {
                        i.set(newItem);
                        isChanged = true;
                    }
                } else {
                    if (copyAny(item, source, destination)) {
                        isChanged = true;
                    }
                }
            }

        } else if (value instanceof Map) {
            Map<String, Object> map = (Map<String, Object>) value;
            for (String key : new ArrayList<>(map.keySet())) {
                Object item = map.get(key);
                if (item instanceof StorageItem) {
                    StorageItem newItem = copyItem((StorageItem) item, source, destination);
                    if (newItem != null) {
                        map.put(key, newItem);
                        isChanged = true;
                    }
                } else {
                    if (copyAny(item, source, destination)) {
                        isChanged = true;
                    }
                }
            }

        } else if (value instanceof Recordable) {
            State state = ((Recordable) value).getState();
            if (state.isNew()) {
                if (copyAny(state.getValues(), source, destination)) {
                    isChanged = true;
                }
            }
        }

        return isChanged;
    }

    private StorageItem copyItem(StorageItem item, String source, String destination) {
        if (item.getStorage().equals(source)) {
            Throwable error;

            try {
                return StorageItem.Static.copy(item, destination);

            } catch (IOException e) {
                error = e;
            } catch (RuntimeException e) {
                error = e;
            }

            LOGGER.info(String.format(
                    "Can't copy from [%s] to [%s]!", source, destination),
                    error);
        }

        return null;
    }
}
