package com.psddev.dari.sql;

import com.psddev.dari.db.Query;
import com.psddev.dari.db.State;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.UUID;

final class ByIdIterator<T> implements Iterator<T> {

    private final Query<T> query;
    private final int fetchSize;
    private UUID lastTypeId;
    private UUID lastId;
    private List<T> items;
    private int index;

    public static <T> Iterable<T> iterable(Query<T> query, int fetchSize) {
        return () -> new ByIdIterator<>(query, fetchSize);
    }

    private ByIdIterator(Query<T> query, int fetchSize) {
        this.query = query.clone().sortAscending("_type").sortAscending("_id");
        this.fetchSize = fetchSize > 0 ? fetchSize : 200;
    }

    @Override
    public boolean hasNext() {
        if (items != null && items.isEmpty()) {
            return false;
        }

        if (items == null || index >= items.size()) {
            Query<T> nextQuery = query.clone();

            if (lastTypeId != null) {
                nextQuery.and("_type = ? and _id > ?", lastTypeId, lastId);
            }

            items = nextQuery.select(0, fetchSize).getItems();
            int size = items.size();

            if (size < 1) {
                if (lastTypeId == null) {
                    return false;

                } else {
                    nextQuery = query.clone().and("_type > ?", lastTypeId);
                    items = nextQuery.select(0, fetchSize).getItems();
                    size = items.size();

                    if (size < 1) {
                        return false;
                    }
                }
            }

            State lastState = State.getInstance(items.get(size - 1));
            lastTypeId = lastState.getVisibilityAwareTypeId();
            lastId = lastState.getId();
            index = 0;
        }

        return true;
    }

    @Override
    public T next() {
        if (hasNext()) {
            T object = items.get(index);
            ++ index;

            return object;

        } else {
            throw new NoSuchElementException();
        }
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }
}
