package com.psddev.dari.util;

import java.lang.ref.ReferenceQueue;
import java.lang.ref.SoftReference;
import java.util.AbstractMap;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// CHECKSTYLE:OFF
/**
 * Read-only map that can produce many values at once on demand.
 *
 * @deprecated Use {@link com.google.common.cache.CacheBuilder} instead.
 */
@Deprecated
public abstract class PullThroughManyCache<K, V> extends AbstractMap<K, V> {

    private static final Logger
            LOGGER = LoggerFactory.getLogger(PullThroughManyCache.class);

    // Stores actual cached values.
    private final ConcurrentMap<K, ValueReference>
            map = new ConcurrentHashMap<K, ValueReference>();

    // To clean up keys in the cache.
    private final ReferenceQueue<V> refQueue = new ReferenceQueue<V>();

    // Stores cache value production dates.
    private final ConcurrentMap<K, Date>
            dates = new ConcurrentHashMap<K, Date>();

    // Controls which thread can produce the value.
    private final ConcurrentMap<K, CountDownLatch>
            latches = new ConcurrentHashMap<K, CountDownLatch>();

    // For deadlock detection.
    private final ConcurrentMap<K, Thread>
            threads = new ConcurrentHashMap<K, Thread>();

    /**
     * Returns a map of values that should be associated with the given set
     * of {@code keys}.
     */
    protected abstract Map<K, V> produceMany(Set<K> keys) throws Exception;

    /**
     * Returns a map of values associated with the given {@code keys},
     * producing them if necessary.
     */
    @SuppressWarnings("unchecked")
    public Map<K, V> getMany(Set<Object> keys) {

        // Any GC'd values?
        ValueReference ref;
        while ((ref = (ValueReference) this.refQueue.poll()) != null) {
            if (this.map.remove(ref.key, ref)) {
                this.dates.remove(ref.key);
                this.latches.remove(ref.key);
                this.threads.remove(ref.key);
                LOGGER.debug(
                        "Removing [{}]; Cache size=[{}].",
                        ref.key, this.map.size());
            }
        }

        Set<K> toBeFetched = new LinkedHashSet<K>();
        Map<K, V> fetched = new CompactMap<K, V>();

        // Don't produce values if the key isn't the right type.
        for (Object key : keys) {
            K typedKey = null;
            if (key != null) {
                try {
                    typedKey = (K) key;
                } catch (ClassCastException ex) {
                    // This doesn't actually work, but leaving it as is,
                    // since this class is deprecated anyway.
                }
            }
            if (typedKey != null) {
                toBeFetched.add(typedKey);
            }
        }

        // Repeat until all values have been fetched, because production
        // may fail spuriously during the wait at [W].
        while (true) {

            // Figure out which keys to produce.
            Set<K> toBeProduced = new LinkedHashSet<K>();
            for (Iterator<K> i = toBeFetched.iterator(); i.hasNext();) {
                K key = i.next();

                // Already cached?
                Date date = this.dates.get(key);
                if (date != null && !isExpired(key, date)) {
                    ref = this.map.get(key);
                    if (ref != null && !ref.isEnqueued()) {
                        i.remove();
                        fetched.put(key, ref.get());
                        continue;
                    }
                }

                // [G] This guard guarantees that the produceMany is never
                // called by multiple threads at the same time.
                if (this.latches.putIfAbsent(
                        key, new CountDownLatch(1)) == null) {
                    toBeProduced.add(key);

                } else {

                    // [D] Detect when produceMany calls itself.
                    if (Thread.currentThread().equals(this.threads.get(key))) {
                        throw new PullThroughDeadlockException(key);
                    }

                    // [W] Wait on the guard using the latch at [G].
                    LOGGER.debug("Waiting on [{}].", key);
                    CountDownLatch latch = this.latches.get(key);
                    if (latch != null) {
                        try {
                            latch.await();
                        } catch (InterruptedException ex) {
                            // Ignore thread interruption and continue.
                        }
                    }
                }
            }

            if (!toBeProduced.isEmpty()) {
                LOGGER.debug("Producing {}.", toBeProduced);
                try {

                    // For deadlock detection at [D].
                    Thread currentThread = Thread.currentThread();
                    for (K key : toBeProduced) {
                        this.threads.put(key, currentThread);
                    }

                    Map<K, V> produced = produceMany(toBeProduced);

                    // Store all the values that were scheduled to be
                    // produced. May NOT be the same set of keys as ones
                    // to be fetched.
                    Date now = new Date();
                    if (produced != null) {
                        for (Map.Entry<K, V> e : produced.entrySet()) {
                            K key = e.getKey();
                            V value = e.getValue();
                            this.map.put(key, new ValueReference(key, value));
                            this.dates.put(key, now);
                        }
                        for (K key : toBeProduced) {
                            LOGGER.debug("Storing [{}].", key);
                            toBeFetched.remove(key);
                            fetched.put(key, produced.get(key));
                        }

                    } else {
                        LOGGER.debug("Setting {} to null.", toBeProduced);
                        for (K key : toBeProduced) {
                            this.map.put(key, new ValueReference(key, null));
                            this.dates.put(key, now);
                            toBeFetched.remove(key);
                            fetched.put(key, null);
                        }
                    }

                } catch (Exception ex) {
                    throw ex instanceof RuntimeException ?
                            (RuntimeException) ex :
                            new RuntimeException(String.format(
                                    "Unable to produce %s!", toBeProduced),
                                    ex);

                } finally {
                    for (K key : toBeProduced) {
                        this.threads.remove(key);

                        // Release the latch so other threads can continue.
                        CountDownLatch latch = this.latches.remove(key);
                        if (latch != null) {
                            latch.countDown();
                        }
                    }
                }
            }

            if (toBeFetched.isEmpty()) {
                return fetched;
            }
        }
    }

    /**
     * Returns the last time that the value at the given {@code key}
     * was produced.
     */
    public Date getLastProduceDate(K key) {
        return this.dates.get(key);
    }

    /**
     * Returns {@code true} if the given {@code key} has already been
     * produced.
     */
    public boolean isProduced(K key) {
        return getLastProduceDate(key) != null;
    }

    /** Invalidates all the values in this cache. */
    public synchronized void invalidate() {
        LOGGER.debug("Invalidating all cached values.");
        this.map.clear();
        this.dates.clear();
        this.latches.clear();
        this.threads.clear();
    }

    /** Invalidates the value at the given {@code key}. */
    public synchronized void invalidate(K key) {
        LOGGER.debug("Invalidating [{}].", key);
        this.map.remove(key);
        this.dates.remove(key);
        this.latches.remove(key);
        this.threads.remove(key);
    }

    /**
     * Returns {@code true} if the cached value at the given {@code key}
     * needs to be invalidated. Default implementation always returns
     * {@code false}, causing the cache to never invalidate.
     */
    protected boolean isExpired(K key, Date lastProduceDate) {
        return false;
    }

    // --- AbstractMap support ---

    /**
     * Returns {@code true} if a value for the given {@code key} can be
     * produced.
     */
    @Override
    public boolean containsKey(Object key) {
        return true;
    }

    /**
     * Returns the value associated with the given {@code key},
     * producing it if necessary.
     */
    @Override
    public V get(Object key) {
        return getMany(Collections.singleton(key)).get(key);
    }

    /** Returns a set view of all the values produced so far. */
    @Override
    public Set<Map.Entry<K, V>> entrySet() {
        Map<K, V> map = new CompactMap<K, V>();
        for (Map.Entry<K, ValueReference> e : this.map.entrySet()) {
            ValueReference ref = e.getValue();
            if (ref != null && !ref.isEnqueued()) {
                map.put(e.getKey(), ref.get());
            }
        }
        return Collections.unmodifiableMap(map).entrySet();
    }

    // Simple wrapper around SoftReference to clean up keys.
    private class ValueReference extends SoftReference<V> {

        public final K key;

        public ValueReference(K key, V value) {
            super(value, refQueue);
            this.key = key;
        }
    }
}
