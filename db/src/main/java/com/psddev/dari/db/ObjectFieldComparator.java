package com.psddev.dari.db;

import java.io.Serializable;
import java.util.Comparator;

import com.google.common.base.Preconditions;
import com.psddev.dari.util.ObjectUtils;

/**
 * Comparator implementation that compares field values.
 */
public class ObjectFieldComparator implements Comparator<Object>, Serializable {

    private static final long serialVersionUID = 1L;

    private final String field;
    private final boolean isNullGreatest;

    /**
     * Creates an instance that compares values in the given {@code field}.
     *
     * @param field
     *        Can't be {@code null}.
     *
     * @param isNullGreatest
     *        {@code true} if {@code null} should be considered greater than
     *        any other value.
     *
     * @throws IllegalArgumentException
     *         If the given {@code field} is {@code null}.
     */
    public ObjectFieldComparator(String field, boolean isNullGreatest) {
        Preconditions.checkArgument(field != null);

        this.field = field;
        this.isNullGreatest = isNullGreatest;
    }

    @Override
    public int compare(Object x, Object y) {
        State xState = State.getInstance(x);
        State yState = State.getInstance(y);

        Object xValue = xState != null ? filter(xState.getByPath(field)) : null;
        Object yValue = yState != null ? filter(yState.getByPath(field)) : null;

        return ObjectUtils.compare(xValue, yValue, isNullGreatest);
    }

    /**
     * Filters the given field {@code value} to another before the comparison.
     *
     * <p>Default implementation returns the given {@code value} as is.</p>
     *
     * @param value May be {@code null}.
     * @return May be {@code null}.
     */
    protected Object filter(Object value) {
        return value;
    }
}
