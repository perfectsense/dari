package com.psddev.dari.util;

import java.lang.ref.ReferenceQueue;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// CHECKSTYLE:OFF
/**
 * Read-only map implementation that produces its values on demand.
 *
 * <p>Sub-classes must implement:
 *
 * <ul>
 * <li>{@link #produce}
 * </ul>
 *
 * <p>Optionally, they can further override:
 *
 * <ul>
 * <li>{@link #isExpired}
 * </ul>
 *
 * @deprecated Use {@link com.google.common.cache.CacheBuilder} instead.
 */
@Deprecated
public abstract class PullThroughCache<K, V> implements Map<K, V> {

    private static final Logger LOGGER = LoggerFactory.getLogger(PullThroughCache.class);

    private final Class<?> keyClass;
    private final PullThroughReferenceFactory referenceFactory;
    private final ReferenceQueue<V> garbages = new ReferenceQueue<V>();
    private final ConcurrentMap<K, PullThroughReference<K, V>> cache = new ConcurrentHashMap<K, PullThroughReference<K, V>>();
    private final ConcurrentMap<K, ThreadAwareLatch> latches = new ConcurrentHashMap<K, ThreadAwareLatch>();

    /**
     * Creates an instance that produces its values using instances
     * of the given {@code keyClass}.
     */
    public PullThroughCache(Class<?> keyClass, Class<? extends PullThroughReferenceFactory> factoryClass) {
        if (keyClass == null) {
            keyClass = Object.class;
            Type superClass = getClass().getGenericSuperclass();
            if (superClass instanceof ParameterizedType) {
                Type keyType = ((ParameterizedType) superClass).getActualTypeArguments()[0];
                while (keyType instanceof ParameterizedType) {
                    keyType = ((ParameterizedType) keyType).getRawType();
                }
                if (keyType instanceof Class) {
                    keyClass = (Class<?>) keyType;
                }
            }
        }

        this.keyClass = keyClass;
        this.referenceFactory = factoryClass != null ?
                TypeDefinition.getInstance(factoryClass).newInstance() :
                new PullThroughReferenceFactory.Soft();
    }

    public PullThroughCache(Class<?> keyClass) {
        this(keyClass, null);
    }

    public PullThroughCache() {
        this(null, null);
    }

    /**
     * Called to produce a value that should be associated with the
     * given {@code key}.
     */
    protected abstract V produce(K key) throws Exception;

    /**
     * Returns {@code true} if the cached value associated with the given
     * {@code key} needs to be invalidated. Default implementation always
     * returns {@code false}, causing the cache to never invalidate.
     */
    protected boolean isExpired(K key, Date lastProduceDate) {
        return false;
    }

    /**
     * Returns {@code true} if the value for the given {@code key}
     * has been produced already.
     */
    public boolean isProduced(K key) {
        PullThroughReference<K, V> value = cache.get(key);
        return value != null && value.isAvailable();
    }

    /**
     * Returns the last time that the value associated with the given
     * {@code key} was produced.
     *
     * @return {@code null} if the value associated with the given
     *         {@code key} was never produced.
     */
    public Date getLastProduce(K key) {
        PullThroughReference<K, V> value = cache.get(key);
        return value != null && value.isAvailable() ? value.getProduceDate() : null;
    }

    /** @deprecated Use {@link #getLastProduce} instead. */
    @Deprecated
    public Date getLastProduceDate(K key) {
        return getLastProduce(key);
    }

    /** Invalidates all values in this cache. */
    public synchronized void invalidate() {
        LOGGER.debug("Invalidating all cached values of [{}]", getClass().getName());
        cache.clear();
    }

    /** Invalidates the value associated with the given {@code key}. */
    public synchronized void invalidate(K key) {
        LOGGER.debug("Invalidating [{}] in [{}]", key, getClass().getName());
        cache.remove(key);
    }

    // --- Map support ---

    @Override
    public void clear() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean containsKey(Object key) {
        return keyClass.isInstance(key);
    }

    @Override
    public boolean containsValue(Object value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<Map.Entry<K, V>> entrySet() {
        Map<K, V> map = new CompactMap<K, V>();
        for (Map.Entry<K, PullThroughReference<K, V>> entry : cache.entrySet()) {
            PullThroughReference<K, V> value = entry.getValue();
            if (value != null && value.isAvailable()) {
                map.put(entry.getKey(), value.get());
            }
        }
        return Collections.unmodifiableMap(map).entrySet();
    }

    @Override
    @SuppressWarnings("unchecked")
    public V get(Object key) {

        // Any GC'd values?
        PullThroughReference<K, V> valueReference;
        while ((valueReference = (PullThroughReference<K, V>) garbages.poll()) != null) {
            K valueKey = valueReference.getKey();
            if (cache.remove(valueKey, valueReference)) {
                latches.remove(valueKey);
                LOGGER.debug(
                        "Removing [{}]; Cache size=[{}]",
                        valueKey, cache.size());
            }
        }

        if (!keyClass.isInstance(key)) {
            return null;
        }

        K typedKey = (K) key;
        V value = null;
        while (true) {

            // Already cached?
            valueReference = cache.get(typedKey);
            if (valueReference != null) {
                value = valueReference.get();
                if (value != null &&
                        !isExpired(typedKey, valueReference.getProduceDate())) {
                    return value;
                }
            }

            // [G] This guard guarantees that the produce is never
            // called by multiple threads at the same time.
            if (latches.putIfAbsent(typedKey, new ThreadAwareLatch()) == null) {
                LOGGER.debug("Producing [{}]", typedKey);

                try {
                    V produced = produce(typedKey);
                    valueReference = referenceFactory.create(garbages, typedKey, produced, new Date());
                    cache.put(typedKey, valueReference);
                    return produced;

                } catch (Exception ex) {
                    throw ex instanceof RuntimeException ?
                            (RuntimeException) ex :
                            new PullThroughProductionException(typedKey, ex);

                // Release the latch so other threads can continue.
                } finally {
                    ThreadAwareLatch latch = latches.remove(typedKey);
                    if (latch != null) {
                        latch.countDown();
                    }
                }

            } else {
                ThreadAwareLatch latch = latches.get(typedKey);
                if (latch != null) {

                    // [D] Detect when produce calls itself.
                    if (latch.isFromCurrentThread()) {
                        throw new PullThroughDeadlockException(typedKey);
                    }

                    // [W] Wait on the guard using the latch at [G].
                    LOGGER.debug("Waiting on [{}].", typedKey);
                    try {
                        latch.await();
                    } catch (InterruptedException ex) {
                        // Ignore thread interruption and continue.
                    }
                }

                valueReference = cache.get(typedKey);
                if (valueReference != null) {
                    value = valueReference.get();
                    if (value != null) {
                        return value;
                    }
                }
            }
        }
    }

    @Override
    public boolean isEmpty() {
        return false;
    }

    @Override
    public Set<K> keySet() {
        return cache.keySet();
    }

    @Override
    public V put(K key, V value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> map) {
        throw new UnsupportedOperationException();
    }

    @Override
    public V remove(Object key) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int size() {
        return Integer.MAX_VALUE;
    }

    @Override
    public Collection<V> values() {
        Collection<V> values = new ArrayList<V>();
        for (PullThroughReference<K, V> value : cache.values()) {
            if (value != null && value.isAvailable()) {
                values.add(value.get());
            }
        }
        return Collections.unmodifiableCollection(values);
    }

    // --- Object support ---

    @Override
    public String toString() {
        return entrySet().toString();
    }

    /** Latch that keeps track of the originating thread. */
    private class ThreadAwareLatch extends CountDownLatch {

        private final Thread thread;

        public ThreadAwareLatch() {
            super(1);
            thread = Thread.currentThread();
        }

        /**
         * Returns {@code true} if this latch originated from the same
         * thread as the current one.
         */
        public boolean isFromCurrentThread() {
            return thread.equals(Thread.currentThread());
        }
    }
}
