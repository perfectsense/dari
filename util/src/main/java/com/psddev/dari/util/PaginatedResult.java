package com.psddev.dari.util;

import java.util.ArrayList;
import java.util.List;

/** Convenience methods for working with a paginated result. */
public class PaginatedResult<E> {

    private final long offset;
    private final int limit;
    private final long count;
    private final List<E> items;

    /** Returns an empty instance. */
    public static <T> PaginatedResult<T> empty() {
        return new PaginatedResult<T>(0L, 0, 0L, null);
    }

    public PaginatedResult(long offset, int limit, long count, List<E> items) {
        this.offset = offset;
        this.limit = limit;
        this.items = items != null ? items : new ArrayList<E>();

        if (items == null) {
            this.count = count;

        } else {
            int itemsSize = items.size();
            this.count = itemsSize < limit ? offset + itemsSize : count;
        }
    }

    /**
     * Creates a view of the given {@code allItems} within the given
     * {@code offset} and {@code limit}.
     */
    public PaginatedResult(long offset, int limit, List<E> allItems) {
        this.offset = offset;
        this.limit = limit;
        this.count = allItems.size();
        long toIndex = offset + limit;
        this.items = offset >= count
                ? new ArrayList<E>()
                : toIndex >= count
                        ? allItems.subList((int) offset, (int) count)
                        : allItems.subList((int) offset, (int) toIndex);
    }

    public long getOffset() {
        return offset;
    }

    public int getLimit() {
        return limit;
    }

    public long getCount() {
        return count;
    }

    public List<E> getItems() {
        return items;
    }

    // --- Navigation ---

    public long getFirstOffset() {
        return 0;
    }

    public boolean hasPrevious() {
        return getOffset() > 0;
    }

    public long getPreviousOffset() {
        long offset = getOffset() - getLimit();
        return offset > 0 ? offset : 0;
    }

    public boolean hasNext() {
        return getCount() > getOffset() + getLimit();
    }

    public long getNextOffset() {
        return getOffset() + getLimit();
    }

    public long getLastOffset() {
        long count = getCount();
        return count - count % getLimit();
    }

    // --- Items ---

    public long getFirstItemIndex() {
        return getOffset() + 1;
    }

    /**
     * @deprecated Use {@code !instance.getItems().isEmpty()} instead}.
     * @see #hasPages
     */
    @Deprecated
    public boolean hasItems() {
        return getItems().size() > 0;
    }

    public long getLastItemIndex() {
        return getOffset() + getItems().size();
    }

    // --- Page ---

    public long getPageIndex() {
        return getOffset() / getLimit() + 1;
    }

    public long getPageCount() {
        return (long) Math.ceil((double) getCount() / getLimit());
    }

    public boolean hasPages() {
        return getOffset() > 0 || !getItems().isEmpty();
    }

    // --- Expression Language support ---

    /** @see #hasPrevious() */
    public boolean getHasPrevious() {
        return hasPrevious();
    }

    /** @see #hasNext() */
    public boolean getHasNext() {
        return hasNext();
    }

    /** @deprecated Use {@link #getHasPages} instead. */
    @Deprecated
    public boolean getHasItems() {
        return hasItems();
    }

    /** @see #hasPages */
    public boolean getHasPages() {
        return hasPages();
    }
}
