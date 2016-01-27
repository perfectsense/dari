package com.psddev.dari.util;

import java.lang.annotation.Annotation;
import java.util.Arrays;
import java.util.Comparator;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import java8.util.Spliterator;
import java8.util.Spliterators;
import java8.util.function.BiConsumer;
import java8.util.function.Consumer;
import java8.util.function.DoubleConsumer;
import java8.util.function.Function;
import java8.util.function.IntConsumer;
import java8.util.function.LongConsumer;
import java8.util.function.Predicate;
import java8.util.stream.BaseStream;
import java8.util.stream.Collectors;
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

    public static Stream<String> splitAsStream(Pattern parttern, CharSequence input) {
        class MatcherIterator implements Iterator<String> {
            private final Matcher matcher;
            // The start position of the next sub-sequence of input
            // when current == input.length there are no more elements
            private int current;
            // null if the next element, if any, needs to obtained
            private String nextElement;
            // > 0 if there are N next empty elements
            private int emptyElementCount;

            MatcherIterator() {
                this.matcher = parttern.matcher(input);
            }

            public String next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }

                if (emptyElementCount == 0) {
                    String n = nextElement;
                    nextElement = null;
                    return n;
                } else {
                    emptyElementCount--;
                    return "";
                }
            }

            public boolean hasNext() {
                if (nextElement != null || emptyElementCount > 0) {
                    return true;
                }

                if (current == input.length()) {
                    return false;
                }

                // Consume the next matching element
                // Count sequence of matching empty elements
                while (matcher.find()) {
                    nextElement = input.subSequence(current, matcher.start()).toString();
                    current = matcher.end();
                    if (!nextElement.isEmpty()) {
                        return true;
                    } else if (current > 0) { // no empty leading substring for zero-width
                                              // match at the beginning of the input
                        emptyElementCount++;
                    }
                }

                // Consume last matching element
                nextElement = input.subSequence(current, input.length()).toString();
                current = input.length();
                if (!nextElement.isEmpty()) {
                    return true;
                } else {
                    // Ignore a terminal sequence of matching empty elements
                    emptyElementCount = 0;
                    nextElement = null;
                    return false;
                }
            }
        }

        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(
                new MatcherIterator(), Spliterator.ORDERED | Spliterator.NONNULL), false);
    }

    /**
     * Returns a function that always returns its input argument.
     *
     * @param <T> the type of the input and output objects to the function
     * @return a function that always returns its input argument
     */
    public static <T> Function<T, T> identity() {
        return t -> t;
    }

    /**
     * Creates a lazily concatenated stream whose elements are all the
     * elements of the first stream followed by all the elements of the
     * second stream.  The resulting stream is ordered if both
     * of the input streams are ordered, and parallel if either of the input
     * streams is parallel.  When the resulting stream is closed, the close
     * handlers for both input streams are invoked.
     *
     * @implNote
     * Use caution when constructing streams from repeated concatenation.
     * Accessing an element of a deeply concatenated stream can result in deep
     * call chains, or even {@code StackOverflowException}.
     *
     * @param <T> The type of stream elements
     * @param a the first stream
     * @param b the second stream
     * @return the concatenation of the two input streams
     */
    public static <T> Stream<T> concat(Stream<? extends T> a, Stream<? extends T> b) {
        Objects.requireNonNull(a);
        Objects.requireNonNull(b);

        @SuppressWarnings("unchecked")
        Spliterator<T> split = new  ConcatSpliterator.OfRef<>(
                (Spliterator<T>) a.spliterator(), (Spliterator<T>) b.spliterator());
        Stream<T> stream = StreamSupport.stream(split, a.isParallel() || b.isParallel());
        return stream.onClose(composedClose(a, b));
    }

    /**
     * Given two streams, return a Runnable that
     * executes both of their {@link BaseStream#close} methods in sequence,
     * even if the first throws an exception, and if both throw exceptions, add
     * any exceptions thrown by the second as suppressed exceptions of the first.
     */
    private static Runnable composedClose(BaseStream<?, ?> a, BaseStream<?, ?> b) {
        return new Runnable() {
            @Override
            public void run() {
                try {
                    a.close();
                } catch (Throwable e1) {
                    try {
                        b.close();
                    } catch (Throwable e2) {
                        try {
                            e1.addSuppressed(e2);
                        } catch (Throwable ignore) {
                            //Do nothing
                        }
                    }
                    throw e1;
                }
                b.close();
            }
        };
    }

    abstract static class ConcatSpliterator<T, T_SPLITR extends Spliterator<T>> implements Spliterator<T> {
        protected final T_SPLITR aSpliterator;
        protected final T_SPLITR bSpliterator;
        // True when no split has occurred, otherwise false
        boolean beforeSplit;
        // Never read after splitting
        final boolean unsized;

        public ConcatSpliterator(T_SPLITR aSpliterator, T_SPLITR bSpliterator) {
            this.aSpliterator = aSpliterator;
            this.bSpliterator = bSpliterator;
            beforeSplit = true;
            // The spliterator is known to be unsized before splitting if the
            // sum of the estimates overflows.
            unsized = aSpliterator.estimateSize() + bSpliterator.estimateSize() < 0;
        }

        @Override
        public T_SPLITR trySplit() {
            @SuppressWarnings("unchecked")
            T_SPLITR ret = beforeSplit ? aSpliterator : (T_SPLITR) bSpliterator.trySplit();
            beforeSplit = false;
            return ret;
        }

        @Override
        public boolean tryAdvance(Consumer<? super T> consumer) {
            boolean hasNext;
            if (beforeSplit) {
                hasNext = aSpliterator.tryAdvance(consumer);
                if (!hasNext) {
                    beforeSplit = false;
                    hasNext = bSpliterator.tryAdvance(consumer);
                }
            } else {
                hasNext = bSpliterator.tryAdvance(consumer);
            }
            return hasNext;
        }

        @Override
        public void forEachRemaining(Consumer<? super T> consumer) {
            if (beforeSplit) {
                aSpliterator.forEachRemaining(consumer);
            }
            bSpliterator.forEachRemaining(consumer);
        }

        @Override
        public long estimateSize() {
            if (beforeSplit) {
                // If one or both estimates are Long.MAX_VALUE then the sum
                // will either be Long.MAX_VALUE or overflow to a negative value
                long size = aSpliterator.estimateSize() + bSpliterator.estimateSize();
                return (size >= 0) ? size : Long.MAX_VALUE;
            } else {
                return bSpliterator.estimateSize();
            }
        }

        @Override
        public int characteristics() {
            if (beforeSplit) {
                // Concatenation loses DISTINCT and SORTED characteristics
                return aSpliterator.characteristics() & bSpliterator.characteristics()
                       & ~(Spliterator.DISTINCT | Spliterator.SORTED
                           | (unsized ? Spliterator.SIZED | Spliterator.SUBSIZED : 0));
            } else {
                return bSpliterator.characteristics();
            }
        }

        @Override
        public Comparator<? super T> getComparator() {
            if (beforeSplit) {
                throw new IllegalStateException();
            }
            return bSpliterator.getComparator();
        }

        static class OfRef<T> extends ConcatSpliterator<T, Spliterator<T>> {
            OfRef(Spliterator<T> aSpliterator, Spliterator<T> bSpliterator) {
                super(aSpliterator, bSpliterator);
            }

            @Override
            public long getExactSizeIfKnown() {
                return 0;
            }

            @Override
            public boolean hasCharacteristics(int characteristics) {
                return false;
            }
        }

        private abstract static class OfPrimitive<T, T_CONS, T_SPLITR extends Spliterator.OfPrimitive<T, T_CONS, T_SPLITR>>
                extends ConcatSpliterator<T, T_SPLITR>
                implements Spliterator.OfPrimitive<T, T_CONS, T_SPLITR> {
            private OfPrimitive(T_SPLITR aSpliterator, T_SPLITR bSpliterator) {
                super(aSpliterator, bSpliterator);
            }

            @Override
            public boolean tryAdvance(T_CONS action) {
                boolean hasNext;
                if (beforeSplit) {
                    hasNext = aSpliterator.tryAdvance(action);
                    if (!hasNext) {
                        beforeSplit = false;
                        hasNext = bSpliterator.tryAdvance(action);
                    }
                } else {
                    hasNext = bSpliterator.tryAdvance(action);
                }
                return hasNext;
            }

            @Override
            public void forEachRemaining(T_CONS action) {
                if (beforeSplit) {
                    aSpliterator.forEachRemaining(action);
                }
                bSpliterator.forEachRemaining(action);
            }
        }

        static class OfInt extends ConcatSpliterator.OfPrimitive<Integer, IntConsumer, Spliterator.OfInt> implements Spliterator.OfInt {
            OfInt(Spliterator.OfInt aSpliterator, Spliterator.OfInt bSpliterator) {
                super(aSpliterator, bSpliterator);
            }

            @Override
            public long getExactSizeIfKnown() {
                return 0;
            }

            @Override
            public boolean hasCharacteristics(int characteristics) {
                return false;
            }
        }

        static class OfLong extends ConcatSpliterator.OfPrimitive<Long, LongConsumer, Spliterator.OfLong> implements Spliterator.OfLong {
            OfLong(Spliterator.OfLong aSpliterator, Spliterator.OfLong bSpliterator) {
                super(aSpliterator, bSpliterator);
            }

            @Override
            public long getExactSizeIfKnown() {
                return 0;
            }

            @Override
            public boolean hasCharacteristics(int characteristics) {
                return false;
            }
        }

        static class OfDouble extends ConcatSpliterator.OfPrimitive<Double, DoubleConsumer, Spliterator.OfDouble> implements Spliterator.OfDouble {
            OfDouble(Spliterator.OfDouble aSpliterator, Spliterator.OfDouble bSpliterator) {
                super(aSpliterator, bSpliterator);
            }

            @Override
            public long getExactSizeIfKnown() {
                return 0;
            }

            @Override
            public boolean hasCharacteristics(int characteristics) {
                return false;
            }
        }
    }

    public static <A extends Annotation> A[] getAnnotationsByType(Class annotatableClass, Class<A> annotationClass) {
        Objects.requireNonNull(annotationClass);

        List<Annotation> annotationList = StreamSupport.stream(Arrays.asList(annotatableClass.getAnnotations()))
                     .filter(annotation -> annotationClass.isInstance(annotation))
                     .collect(Collectors.toList());

        Annotation[] annotations = new Annotation[annotationList.size()];
        for (int i = 0; i < annotationList.size(); i++) {
            annotations[i] = annotationList.get(i);
        }

        return (A[]) annotations;
    }
}
