package com.psddev.dari.util;

import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

/**
 * Set of strings that can be represented concisely by a pattern.
 * Patterns:
 * - "+" alone matches blank string
 * - "+/" matches anything and includes it in the set
 * - "+a" the value "a" is included in the set
 * - "+a/b" the values "a/b" is included in the set
 * - "+a/" any children of the value "a" are included in the set ("a/b", "a/c", but NOT "a")
 * - "-a" the value "a" is not included in the set
 * - "-a/b" the value "a/b" is not included in the set
 * - "-a" any children of the value "a" are not included in the set ("a/b", "a/c", but NOT "a")
 *
 * <p>For example, given {@code +/ -foo/ +foo/bar}, {@code foo} and
 * {@code foo/bar} would considered to be in the set, while
 * {@code foo/qux} would not.
 *
 * <p>Another example
 * {@code + -/ +foo/ -foo/bar}
 * - the blank string is included in the set (due to the {@code +})
 * - {@code foo} is not included in the set ({@code +foo} is not in the pattern)
 * - {@code foo/bar} is not included in the set (due to the {@code -foo/bar})
 * - {@code foo/bar/ton} is included in the set (due to the {@code +foo/})
 * - {@code bar} is not included in the set (due to the {@code -/})
 *
 * <p>Because the instances of this set can contain an infinite number
 * of items, the {@link #iterator()}, {@link #size()}, {@link #toArray},
 * methods throw an {@link UnsupportedOperationException}.
 */
public class SparseSet implements Set<String> {

    private String pattern;

    /** Creates an instance based on the given {@code pattern}. */
    public SparseSet(String pattern) {
        setPattern(pattern);
    }

    /** Creates a blank instance. */
    public SparseSet() {
        clear();
    }

    /** Returns the pattern that represents this set concisely. */
    public String getPattern() {
        return pattern;
    }

    /** Sets the pattern that represents this set concisely. */
    public void setPattern(String pattern) {
        this.pattern = ObjectUtils.isBlank(pattern)
                ? ""
                : StringUtils.replaceAll(pattern.trim(), "\\s+", " ");
    }

    // --- Set support ---

    @Override
    public boolean add(String item) {
        if (contains(item)) {
            return true;
        } else {
            setPattern(getPattern() + " +" + item);
            return false;
        }
    }

    @Override
    public boolean addAll(Collection<? extends String> collection) {
        boolean isChanged = false;
        for (String e : collection) {
            if (add(e)) {
                isChanged = true;
            }
        }
        return isChanged;
    }

    @Override
    public void clear() {
        setPattern(null);
    }

    @Override
    public boolean contains(Object item) {

        if (!(item instanceof String)) {
            return false;
        }

        String itemString = (String) item;
        String paddedPattern = " " + getPattern() + " ";
        StringBuilder check = new StringBuilder(itemString.length() + 3);
        check.append("  ").append(itemString).append(' ');
        for (int slashAt;;) {

            // Check for negative match.
            check.setCharAt(1, '-');
            if (paddedPattern.contains(check)) {
                return false;

            // Check for positive match.
            } else {
                check.setCharAt(1, '+');
                if (paddedPattern.contains(check)) {
                    return true;
                }
            }

            // Check the next.
            slashAt = check.lastIndexOf("/", check.length() - 3);
            if (slashAt < 0) {
                break;

            // Remove the end and check again.
            } else {
                check.setLength(slashAt + 2);
                check.setCharAt(slashAt + 1, ' ');
            }
        }

        return paddedPattern.contains(" +/ ");
    }

    @Override
    public boolean containsAll(Collection<?> collection) {
        if (collection != null) {
            for (Object e : collection) {
                if (!contains(e)) {
                    return false;
                }
            }
        }
        return true;
    }

    @Override
    public boolean isEmpty() {
        return ObjectUtils.isBlank(getPattern());
    }

    @Override
    public Iterator<String> iterator() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean remove(Object item) {
        if (contains(item)) {
            setPattern(getPattern() + " -" + item);
            return true;
        } else {
            return false;
        }
    }

    @Override
    public boolean removeAll(Collection<?> collection) {
        boolean isChanged = false;
        if (collection != null) {
            for (Object e : collection) {
                if (remove(e)) {
                    isChanged = true;
                }
            }
        }
        return isChanged;
    }

    @Override
    public boolean retainAll(Collection<?> collection) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int size() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object[] toArray() {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> T[] toArray(T[] array) {
        throw new UnsupportedOperationException();
    }

    // --- Object support ---

    @Override
    public boolean equals(Object object) {
        return this == object
                || (object instanceof SparseSet
                && getPattern().equals(((SparseSet) object).getPattern()));
    }

    @Override
    public int hashCode() {
        return getPattern().hashCode();
    }

    @Override
    public String toString() {
        return getPattern();
    }
}
