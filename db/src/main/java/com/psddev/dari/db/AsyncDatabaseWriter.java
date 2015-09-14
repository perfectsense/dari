package com.psddev.dari.db;

import java.util.ArrayList;
import java.util.List;

import com.psddev.dari.util.AsyncConsumer;
import com.psddev.dari.util.AsyncQueue;
import com.psddev.dari.util.ObjectUtils;

/** Background task that efficiently writes to a database from a queue. */
public class AsyncDatabaseWriter<E> extends AsyncConsumer<E> {

    public static final double DEFAULT_COMMIT_SIZE_JITTER = 0.2;

    private final Database database;
    private final WriteOperation operation;
    private final int commitSize;
    private final boolean isCommitEventually;

    private double commitSizeJitter = DEFAULT_COMMIT_SIZE_JITTER;
    private long maximumDataLength;

    private transient int nextCommitSize;
    private transient E lastItem;
    private transient long dataLength;
    private final transient List<E> toBeCommitted = new ArrayList<E>();

    /**
     * Creates a new instance that runs in the given {@code executor},
     * consumes items from the given {@code input} queue, and writes
     * them to the given {@code database}.
     *
     * @param executor If {@code null}, uses the default executor.
     * @param input Can't be {@code null}.
     * @param database Can't be {@code null}.
     * @param operation Can't be {@code null}.
     * @param commitSize Number of items to save in a single commit.
     * @param isCommitEventually If {@code true},
     *        {@link Database#commitWritesEventually} is used instead of
     *        {@link Database#commitWrites}.
     * @throws IllegalArgumentException If the given {@code input}
     *         or {@code database} is {@code null}.
     */
    public AsyncDatabaseWriter(
            String executor,
            AsyncQueue<E> input,
            Database database,
            WriteOperation operation,
            int commitSize,
            boolean isCommitEventually) {

        super(executor, input);

        if (database == null) {
            throw new IllegalArgumentException("Database can't be null!");
        }
        if (operation == null) {
            throw new IllegalArgumentException("Operation can't be null!");
        }

        this.database = database;
        this.operation = operation;
        this.commitSize = commitSize;
        this.isCommitEventually = isCommitEventually;
    }

    /**
     * Returns the {@linkplain ObjectUtils#jitter jitter scale} to apply
     * to the commit size.
     */
    public double getCommitSizeJitter() {
        return commitSizeJitter;
    }

    /**
     * Sets the {@linkplain ObjectUtils#jitter jitter scale} to apply
     * to the commit size.
     */
    public void setCommitSizeJitter(double commitSizeJitter) {
        this.commitSizeJitter = commitSizeJitter;
    }

    public long getMaximumDataLength() {
        return maximumDataLength;
    }

    public void setMaximumDataLength(long maximumDataLength) {
        this.maximumDataLength = maximumDataLength;
    }

    // Commits all pending writes.
    private void commit() {
        try {
            try {
                database.beginWrites();

                for (E item : toBeCommitted) {
                    operation.execute(database, State.getInstance(item));
                }

                if (isCommitEventually) {
                    database.commitWritesEventually();
                } else {
                    database.commitWrites();
                }

            } finally {
                database.endWrites();
            }

        // Can't write in batch so try one by one.
        } catch (RuntimeException error1) {
            for (E item : toBeCommitted) {
                try {
                    database.beginWrites();

                    operation.execute(database, State.getInstance(item));

                    if (isCommitEventually) {
                        database.commitWritesEventually();
                    } else {
                        database.commitWrites();
                    }

                } catch (RuntimeException error2) {
                    handleError(item, error2);

                } finally {
                    database.endWrites();
                }
            }

        } finally {
            toBeCommitted.clear();
        }
    }

    // --- AsyncConsumer support ---

    // Calculates the number of items to save in the next commit.
    private void calculateNextCommitSize() {
        nextCommitSize = (int) ObjectUtils.jitter(commitSize, getCommitSizeJitter());
    }

    @Override
    protected void beforeStart() {
        super.beforeStart();
        calculateNextCommitSize();
    }

    @Override
    protected void consume(E item) {
        lastItem = item;
        dataLength += ObjectUtils.to(long.class, State.getInstance(item).getExtras().get(AbstractDatabase.DATA_LENGTH_EXTRA));

        toBeCommitted.add(item);

        if (toBeCommitted.size() >= nextCommitSize
                || (maximumDataLength > 0
                && dataLength > maximumDataLength)) {
            calculateNextCommitSize();
            commit();
        }
    }

    @Override
    protected void finished() {
        super.finished();
        commit();

        if (lastItem != null) {
            database.beginWrites();
            try {
                operation.execute(database, State.getInstance(lastItem));
                database.commitWrites();
            } finally {
                database.endWrites();
            }
        }
    }

    // --- Deprecated ---

    /** @deprecated Use {@link #AsyncDatabaseWriter(String, AsyncQueue, Database, WriteOperation, int, boolean)} instead. */
    @Deprecated
    public AsyncDatabaseWriter(
            AsyncQueue<E> input,
            Database database,
            WriteOperation operation,
            int commitSize,
            boolean isCommitEventually) {

        this(
                null,
                input,
                database,
                operation,
                commitSize,
                isCommitEventually);
    }

    /** @deprecated Use {@link #AsyncDatabaseWriter(String, AsyncQueue, Database, WriteOperation, int, boolean)} instead. */
    @Deprecated
    public AsyncDatabaseWriter(
            AsyncQueue<E> input,
            Database database,
            int commitSize,
            boolean isCommitEventually,
            boolean isSaveUnsafely) {

        this(
                null,
                input,
                database,
                isSaveUnsafely ? WriteOperation.SAVE_UNSAFELY : WriteOperation.SAVE,
                commitSize,
                isCommitEventually);
    }
}
