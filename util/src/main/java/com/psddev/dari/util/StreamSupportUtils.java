package com.psddev.dari.util;

import java.util.Arrays;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;

import java8.util.function.BiConsumer;
import java8.util.function.Consumer;
import java8.util.function.Predicate;
import java8.util.stream.Stream;
import java8.util.stream.StreamSupport;

public class StreamSupportUtils {

    public static <T> void forEach(Iterable<? extends T> iterable, Consumer<? super T> action) {
        Objects.requireNonNull(action);
        for (T t : iterable) {
            action.accept(t);
        }
    }

    public static <K, V> void mapForEach(Map<K, V> map, BiConsumer<? super K, ? super V> action) {
        Objects.requireNonNull(action);
        for (Map.Entry<K, V> entry : map.entrySet()) {
            K k;
            V v;
            try {
                k = entry.getKey();
                v = entry.getValue();
            } catch (IllegalStateException ise) {
                // this usually means the entry is no longer in the map.
                throw new ConcurrentModificationException(ise);
            }
            action.accept(k, v);
        }
    }

    public static <E> boolean removeIf(Iterable<E> iterable, Predicate<? super E> filter) {
        Objects.requireNonNull(filter);
        boolean removed = false;
        final Iterator<E> each = iterable.iterator();
        while (each.hasNext()) {
            if (filter.test(each.next())) {
                each.remove();
                removed = true;
            }
        }
        return removed;
    }

    public static <T> Stream<T> of(T... values) {
        return StreamSupport.stream(Arrays.asList(values));
    }
}
