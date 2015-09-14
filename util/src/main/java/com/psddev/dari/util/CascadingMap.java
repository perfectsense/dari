package com.psddev.dari.util;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Combined and read-only view of multiple maps.
 *
 * <p>This class is thread-safe as long as the underlying source maps
 * are thread-safe.</p>
 */
public class CascadingMap<K, V> implements Map<K, V> {

    private final List<Map<K, V>> sources = new CopyOnWriteArrayList<Map<K, V>>();

    /** Creates an instance without any sources. */
    public CascadingMap() {
    }

    /**
     * Creates an instance with the given {@code sources}.
     *
     * @param sources If {@code null}, creates an instance without any sources.
     */
    public CascadingMap(Iterable<Map<K, V>> sources) {
        if (sources != null) {
            for (Map<K, V> source : sources) {
                this.sources.add(source);
            }
        }
    }

    /**
     * Creates an instance with the given array of {@code sources}.
     *
     * @param sources If {@code null}, creates an instance without any sources.
     */
    public CascadingMap(Map<K, V>... sources) {
        if (sources != null) {
            for (Map<K, V> source : sources) {
                this.sources.add(source);
            }
        }
    }

    /**
     * Returns the list of all sources that are used to look up the entries.
     *
     * <p>Maps earlier in the list are used first.</p>
     *
     * @return Never {@code null}. Mutable.
     */
    public List<Map<K, V>> getSources() {
        return sources;
    }

    // Combines all the sources, for when an unified view is required.
    private Map<K, V> combine() {

        // The listIterator method without index argument of sources.size() - 1
        // is used, because it's possible for the size to change between that
        // get and calling the listIterator.
        ListIterator<Map<K, V>> iterator = sources.listIterator();
        while (iterator.hasNext()) {
            iterator.next();
        }

        Map<K, V> combined = new HashMap<K, V>();
        while (iterator.hasPrevious()) {
            combined.putAll(iterator.previous());
        }
        return Collections.unmodifiableMap(combined);
    }

    // --- Map support ---

    @Override
    public void clear() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean containsKey(Object key) {
        for (Map<K, V> source : sources) {
            if (source.containsKey(key)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean containsValue(Object value) {
        return combine().containsValue(value);
    }

    @Override
    public Set<Map.Entry<K, V>> entrySet() {
        return combine().entrySet();
    }

    @Override
    public V get(Object key) {
        for (Map<K, V> source : sources) {
            if (source.containsKey(key)) {
                return source.get(key);
            }
        }
        return null;
    }

    @Override
    public boolean isEmpty() {
        for (Map<K, V> source : sources) {
            if (!source.isEmpty()) {
                return false;
            }
        }
        return true;
    }

    @Override
    public Set<K> keySet() {
        return combine().keySet();
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
        return combine().size();
    }

    @Override
    public Collection<V> values() {
        return combine().values();
    }

    // --- Object support ---

    @Override
    public boolean equals(Object object) {
        return this == object
                || (object instanceof Map && combine().equals(object));
    }

    @Override
    public int hashCode() {
        return combine().hashCode();
    }

    @Override
    public String toString() {
        return combine().toString();
    }
}
