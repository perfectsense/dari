package com.psddev.dari.util;

import com.google.common.base.Preconditions;

import java.util.AbstractSet;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * Set implementation that's optimized for a small number of entries.
 *
 * <p>Some of its behaviors are:</p>
 *
 * <ul>
 * <li>Most operations are {@code O(n)}.</li>
 * <li>Maintains insertion order during iterator.</li>
 * <li>Switches to using {@link LinkedHashSet} internally if the number of
 * entries exceed 8.</li>
 * </ul>
 */
@SuppressWarnings("unchecked")
public class CompactSet<E> extends AbstractSet<E> {

    private static final int ARRAY_SIZE = 8;

    private Object delegate;
    private int size;

    /**
     * Creates an empty instance.
     */
    public CompactSet() {
    }

    /**
     * Creates an instance initialized with the elements in the given
     * {@code collection}.
     *
     * @param collection Can't be {@code null}.
     */
    public CompactSet(Collection<? extends E> collection) {
        Preconditions.checkNotNull(collection, "[collection] can't be null!");

        int collectionSize = collection.size();

        if (collectionSize > ARRAY_SIZE) {
            delegate = new LinkedHashSet<>(collectionSize);
            size = -1;
        }

        addAll(collection);
    }

    /**
     * Creates an empty instance with the given {@code initialCapacity}.
     * If the initial capacity is greater than 8, switches to using
     * {@link LinkedHashSet} immediately.
     *
     * @param initialCapacity Must be greater than or equal to {@code 0}.
     */
    public CompactSet(int initialCapacity) {
        Preconditions.checkArgument(initialCapacity >= 0, "[initialCapacity] must be greater than or equal to 0!");

        if (initialCapacity > ARRAY_SIZE) {
            delegate = new LinkedHashSet<>(initialCapacity);
            size = -1;
        }
    }

    /**
     * Creates an empty instance with the given {@code initialCapacity}
     * and {@code loadFactor}. If the initial capacity is greater than 8,
     * switches to using {@link LinkedHashSet} immediately.
     *
     * @param initialCapacity Must be greater than or equal to {@code 0}.
     * @param loadFactor Must be greater than {@code 0.0f}. Only used if the
     * initial capacity is greater than 8.
     */
    public CompactSet(int initialCapacity, float loadFactor) {
        Preconditions.checkArgument(initialCapacity >= 0, "[initialCapacity] must be greater than or equal to 0!");
        Preconditions.checkArgument(loadFactor > 0.0f, "[loadFactor] must be greater than 0.0f!");

        if (initialCapacity > ARRAY_SIZE) {
            delegate = new LinkedHashSet<>(initialCapacity, loadFactor);
            size = -1;
        }
    }

    private int indexOfElement(Object element) {
        int elementHash = ObjectUtils.hashCode(element);

        for (int i = 0; i < size; ++ i) {
            E e = ((E[]) delegate)[i];

            if (elementHash == ObjectUtils.hashCode(e) && ObjectUtils.equals(element, e)) {
                return i;
            }
        }

        return -1;
    }

    @Override
    public boolean add(E element) {
        if (size < 0) {
            return ((Set<E>) delegate).add(element);

        } else {
            if (delegate == null) {
                delegate = new Object[ARRAY_SIZE];

            } else {
                int index = indexOfElement(element);

                if (index >= 0) {
                    return false;
                }
            }

            if (size >= ((Object[]) delegate).length) {
                delegate = new LinkedHashSet<>(this);
                size = -1;

                return add(element);

            } else {
                Object[] delegateArray = (Object[]) delegate;
                delegateArray[size] = element;
                ++ size;

                return true;
            }
        }
    }

    @Override
    public boolean addAll(Collection<? extends E> collection) {
        boolean changed = false;

        for (E element : collection) {
            if (add(element)) {
                changed = true;
            }
        }

        return changed;
    }

    @Override
    public void clear() {
        if (size < 0) {
            ((Set<?>) delegate).clear();

        } else {
            size = 0;
        }
    }

    @Override
    public boolean contains(Object element) {
        if (size < 0) {
            return ((Set<?>) delegate).contains(element);

        } else {
            return indexOfElement(element) >= 0;
        }
    }

    @Override
    public boolean containsAll(Collection<?> collection) {
        for (Object element : collection) {
            if (!contains(element)) {
                return false;
            }
        }

        return true;
    }

    @Override
    public boolean isEmpty() {
        if (size < 0) {
            return ((Set<?>) delegate).isEmpty();

        } else {
            return size == 0;
        }
    }

    @Override
    public Iterator<E> iterator() {
        return new IndexedIterator<>();
    }

    private void removeByIndex(int index) {
        -- size;

        System.arraycopy(delegate, index + 1, delegate, index, size - index);
    }

    @Override
    public boolean remove(Object element) {
        if (size < 0) {
            return ((Set<E>) delegate).remove(element);

        } else {
            int index = indexOfElement(element);

            if (index < 0) {
                return false;

            } else {
                removeByIndex(index);
                return true;
            }
        }
    }

    @Override
    public boolean removeAll(Collection<?> collection) {
        boolean changed = false;

        for (Object element : collection) {
            if (remove(element)) {
                changed = true;
            }
        }

        return changed;
    }

    @Override
    public boolean retainAll(Collection<?> collection) {
        boolean changed = false;

        for (Iterator<E> i = iterator(); i.hasNext();) {
            if (!collection.contains(i.next())) {
                i.remove();
                changed = true;
            }
        }

        return changed;
    }

    @Override
    public int size() {
        if (size < 0) {
            return ((Set<?>) delegate).size();

        } else {
            return size;
        }
    }

    private class IndexedIterator<E> implements Iterator<E> {

        private int index = 0;
        private boolean removeAvailable;

        @Override
        public boolean hasNext() {
            return index < size;
        }

        @Override
        public E next() {
            if (hasNext()) {
                E nextValue = ((E[]) delegate)[index];
                ++ index;
                removeAvailable = true;

                return nextValue;

            } else {
                throw new NoSuchElementException();
            }
        }

        @Override
        public void remove() {
            if (!removeAvailable) {
                throw new IllegalStateException();
            }

            -- index;
            removeByIndex(index);
            removeAvailable = false;
        }
    }
}
