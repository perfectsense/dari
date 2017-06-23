package com.psddev.dari.db;

import java.lang.ref.WeakReference;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.WeakHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.psddev.dari.util.ObjectUtils;
import com.psddev.dari.util.StringUtils;
import com.psddev.dari.util.UuidUtils;

/** Enforces mutual exclusion across multiple VMs using a {@link Database}. */
public class DistributedLock implements Lock {

    private static final Logger LOGGER = LoggerFactory.getLogger(DistributedLock.class);
    private static final long TIMEOUT = 10000;
    private static final long TRY_INTERVAL = 50L;

    private final String lockId = UUID.randomUUID().toString();
    private final Database database;
    private final String keyString;
    private final UUID keyId;
    private final AtomicReference<Thread> holderRef = new AtomicReference<Thread>();

    protected DistributedLock(Database database, String key) {
        this.database = database;
        this.keyString = key;
        this.keyId = UuidUtils.fromBytes(StringUtils.md5(key));
    }

    // --- Lock support ---

    /**
     * {@inheritDoc}
     *
     * @throws ReentrantException If this lock is already held by the
     *         current thread.
     */
    @Override
    public void lock() {
        if (!tryLock()) {
            LOGGER.debug("Waiting to acquire [{}]", this);
            do {
                try {
                    Thread.sleep(TRY_INTERVAL);
                } catch (InterruptedException ex) {
                    // Ignore thread interruption and keep trying to lock.
                }
            } while (!tryLock());
        }
    }

    /**
     * {@inheritDoc}
     *
     * @throws ReentrantException If this lock is already held by the
     *         current thread.
     */
    @Override
    public void lockInterruptibly() throws InterruptedException {
        if (!tryLock()) {
            LOGGER.debug("Waiting to acquire [{}] interruptibly", this);
            do {
                Thread.sleep(TRY_INTERVAL);
            } while (!tryLock());
        }
    }

    @Override
    public Condition newCondition() {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     *
     * @throws ReentrantException If this lock is already held by the
     *         current thread.
     */
    @Override
    public boolean tryLock() {
        if (Thread.currentThread().equals(holderRef.get())) {
            throw new ReentrantException();
        }

        synchronized (holderRef) {
            State key = State.getInstance(Query
                    .from(Object.class)
                    .where("_id = ?", keyId)
                    .using(database)
                    .noCache()
                    .master()
                    .first());

            if (key == null) {
                key = new State();
                key.setDatabase(database);
                key.setId(keyId);
                key.put("keyString", keyString);

            } else {
                if (ObjectUtils.to(long.class, key.get("lastPing")) + TIMEOUT < database.now()) {
                    LOGGER.debug("Timeout exceeded: [{}]", this);
                } else {
                    return false;
                }
            }

            try {
                key.replaceAtomically("lockId", lockId);
                key.replaceAtomically("lastPing", database.now());

                // saveImmediately (unsafely)
                database.beginIsolatedWrites();
                try {
                    database.saveUnsafely(key);
                    database.commitWrites();
                } finally {
                    database.endWrites();
                }

            } catch (DatabaseException ex) {
                Throwable cause = ex.getCause();
                if (cause instanceof AtomicOperation.ReplacementException) {
                    LOGGER.debug("Stolen by a different VM: [{}]", this);
                    return false;
                } else {
                    throw ex;
                }
            }

            holderRef.set(Thread.currentThread());
            LOGGER.debug("Acquired [{}]", this);
            return true;
        }
    }

    /**
     * {@inheritDoc}
     *
     * @throws ReentrantException If this lock is already held by the
     *         current thread.
     */
    @Override
    public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
        long end = database.now() + unit.toMillis(time);

        do {
            if (tryLock()) {
                return true;
            } else {
                Thread.sleep(TRY_INTERVAL);
            }
        } while (database.now() < end);

        return false;
    }

    /**
     * {@inheritDoc}
     *
     * @throws IllegalStateException If {@link #lock} hasn't been called yet.
     * @throws IllegalMonitorStateException If the current thread doesn't
     *         hold this lock.
     */
    @Override
    public void unlock() {
        Thread holder = holderRef.get();

        if (holder == null) {
            throw new IllegalStateException("Not locked yet!");

        } else if (!Thread.currentThread().equals(holder)) {
            throw new IllegalMonitorStateException("Not the lock owner!");
        }

        synchronized (holderRef) {
            try {
                LOGGER.debug("Releasing [{}]", this);

                State key = State.getInstance(Query
                        .from(Object.class)
                        .where("_id = ?", keyId)
                        .using(database)
                        .noCache()
                        .master()
                        .first());

                if (key != null && lockId.equals(key.get("lockId"))) {
                    key.deleteImmediately();
                }

            } finally {
                holderRef.set(null);
            }
        }
    }

    // --- Object support ---

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;

        } else if (other instanceof DistributedLock) {
            DistributedLock otherLock = (DistributedLock) other;
            return database.equals(otherLock.database)
                    && keyId.equals(otherLock.keyId);

        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return ObjectUtils.hashCode(database, keyId);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("{lockId=").append(lockId);
        sb.append(", database=").append(database);
        sb.append(", keyId=").append(keyId);
        sb.append('}');
        return sb.toString();
    }

    // ---

    /** Thrown when the thread tries to reacquire the same lock. */
    @SuppressWarnings("serial")
    public static class ReentrantException extends IllegalStateException {

        public ReentrantException() {
            super();
        }
    }

    /** {@link DistributedLock} utility methods. */
    public static final class Static {

        private static final Map<Database, Map<String, WeakReference<DistributedLock>>> INSTANCES = new WeakHashMap<Database, Map<String, WeakReference<DistributedLock>>>();

        /**
         * Returns an instance that locks around the given {@code key}
         * within the given {@code database}.
         */
        public static DistributedLock getInstance(Database database, String key) {
            if (database == null) {
                throw new IllegalArgumentException("Database can't be null!");
            }
            if (key == null) {
                throw new IllegalArgumentException("Key can't be null!");
            }

            synchronized (Static.class) {
                Map<String, WeakReference<DistributedLock>> byKey = INSTANCES.get(database);
                if (byKey == null) {
                    byKey = new HashMap<String, WeakReference<DistributedLock>>();
                    INSTANCES.put(database, byKey);
                }

                WeakReference<DistributedLock> lockRef = byKey.get(key);
                if (lockRef != null) {
                    DistributedLock lock = lockRef.get();
                    if (lock != null) {
                        return lock;
                    }
                }

                DistributedLock lock = new DistributedLock(database, key);
                byKey.put(key, new WeakReference<DistributedLock>(lock));
                return lock;
            }
        }
    }
}
