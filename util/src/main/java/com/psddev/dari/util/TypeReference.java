package com.psddev.dari.util;

import javax.annotation.Nullable;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

/**
 * References a generic type.
 *
 * <p>This class only implements {@link Comparable} to force the subclass
 * to supply the type argument. It's not actually valid to compare an instance
 * of this class to another.</p>
 *
 * @see <a href="http://gafter.blogspot.com/2006/12/super-type-tokens.html">Super Type Tokens</a>
 */
public abstract class TypeReference<T> implements Comparable<TypeReference<T>> {

    private final Type type;

    protected TypeReference() {
        type = ((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments()[0];
    }

    /**
     * Returns the generic type.
     *
     * @return Never {@code null}.
     */
    public Type getType() {
        return type;
    }

    @Override
    public int compareTo(@Nullable TypeReference<T> object) {
        return 0;
    }

    @Override
    public int hashCode() {
        return getType().hashCode();
    }

    @Override
    public boolean equals(Object other) {
        return this == other
                || (other instanceof TypeReference
                && getType().equals(((TypeReference<?>) other).getType()));
    }

    @Override
    public String toString() {
        return getType().toString();
    }
}
