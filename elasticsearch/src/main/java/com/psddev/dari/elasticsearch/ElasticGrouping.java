package com.psddev.dari.elasticsearch;

import com.psddev.dari.db.AbstractGrouping;
import com.psddev.dari.db.Query;

import java.util.List;

/**
 * Define the ElasticGrouping
 */
class ElasticGrouping<T> extends AbstractGrouping<T> {

    private final long count;

    public ElasticGrouping(List<Object> keys, Query<T> query, String[] fields, long count) {
        super(keys, query, fields);
        this.count = count;
    }

    // --- AbstractGrouping support ---

    @Override
    protected Aggregate createAggregate(String field) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getCount() {
        return count;
    }
}
