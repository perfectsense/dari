package com.psddev.dari.db;

import java.util.List;

public interface Grouping<T> {

    public List<Object> getKeys();

    public Query<T> createItemsQuery();

    /** Use {@link #createItemsQuery} instead. */
    @Deprecated
    public default Query<T> getItemsQuery() {
	    return createItemsQuery();
	}

    public long getCount();

    public Object getMaximum(String field);

    public Object getMinimum(String field);

    public long getNonNullCount(String field);

    public double getSum(String field);
}
