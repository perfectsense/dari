package com.psddev.dari.sql;

import com.psddev.dari.db.AbstractGrouping;
import com.psddev.dari.db.Query;

import java.util.List;

class SqlGrouping<T> extends AbstractGrouping<T> {

    private final long count;

    public SqlGrouping(List<Object> keys, Query<T> query, String[] fields, long count) {
        super(keys, query, fields);
        this.count = count;
    }

    @Override
    protected Aggregate createAggregate(String field) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getCount() {
        return count;
    }
}
