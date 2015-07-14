package com.psddev.dari.db;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.psddev.dari.util.ObjectUtils;
import com.psddev.dari.util.PaginatedResult;
import com.psddev.dari.util.Settings;

/**
 * Caches the results of read operations.
 *
 * <p>For example, given:</p>
 *
 * <blockquote><pre>{@literal
CachingDatabase caching = new CachingDatabase();
caching.setDelegate(Database.Static.getDefault());
PaginatedResult<Article> result = Query.from(Article.class).using(caching).select(0, 5);
 * }</pre></blockquote>
 *
 * <p>These are some of the queries that won't trigger additional
 * reads in the delegate database:</p>
 *
 * <ul>
 * <li>{@code Query.from(Article.class).using(caching).count()}</li>
 * <li>{@code Query.from(Article.class).using(caching).where("_id = ?", result.getItems().get(0));}</li>
 * </ul>
 *
 * <p>All methods are thread-safe.</p>
 */
public class CachingDatabase extends ForwardingDatabase {

    private static final String CACHE_SIZE_SETTING = "dari/cachingDatabaseMaximumSize";
    private static final long DEFAULT_CACHE_SIZE = 1000L;

    private static final Object MISSING = new Object();

    private final Cache<UUID, Object> objectCache = CacheBuilder.newBuilder().maximumSize(getCacheSize()).build();
    private final Cache<UUID, Object> referenceCache = CacheBuilder.newBuilder().maximumSize(getCacheSize()).build();
    private final Cache<Query<?>, List<?>> readAllCache = CacheBuilder.newBuilder().maximumSize(getCacheSize()).build();
    private final Cache<Query<?>, Long> readCountCache = CacheBuilder.newBuilder().maximumSize(getCacheSize()).build();
    private final Cache<Query<?>, Object> readFirstCache = CacheBuilder.newBuilder().maximumSize(getCacheSize()).build();
    private final LoadingCache<Query<?>, Map<Range, PaginatedResult<?>>> readPartialCache = CacheBuilder.newBuilder().maximumSize(getCacheSize()).build(
            new CacheLoader<Query<?>, Map<Range, PaginatedResult<?>>>() {
                @Override
                public Map<Range, PaginatedResult<?>> load(Query<?> key) throws Exception {
                    return new ConcurrentHashMap<>();
                }
            });
    private final Cache<UUID, Boolean> idOnlyQueryIds = CacheBuilder.newBuilder().maximumSize(getCacheSize()).build();

    private static class Range {

        public final long offset;
        public final int limit;

        public Range(long offset, int limit) {
            this.offset = offset;
            this.limit = limit;
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;

            } else if (other instanceof Range) {
                Range otherRange = (Range) other;
                return offset == otherRange.offset
                        && limit == otherRange.limit;

            } else {
                return false;
            }
        }

        @Override
        public int hashCode() {
            return ObjectUtils.hashCode(offset, limit);
        }
    }

    /**
     * Returns the set of all IDs which were results of ID-only queries.
     *
     * @return Never {@code null}. Mutable. Thread-safe.
     */
    public Set<UUID> getIdOnlyQueryIds() {
        return idOnlyQueryIds.asMap().keySet();
    }

    /**
     * Returns the map of all objects cached so far.
     *
     * @return Never {@code null}. Mutable. Thread-safe.
     */
    public Map<UUID, Object> getObjectCache() {
        return objectCache.asMap();
    }

    /**
     * Returns the map of all object references cached so far.
     *
     * @return Never {@code null}. Mutable. Thread-safe.
     */
    public Map<UUID, Object> getReferenceCache() {
        return referenceCache.asMap();
    }

    // --- ForwardingDatabase support ---

    private long getCacheSize() {
        return Settings.getOrDefault(long.class, CACHE_SIZE_SETTING, DEFAULT_CACHE_SIZE);
    }

    private boolean isCacheDisabled(Query<?> query) {
        if (query.isCache()) {
            return query.as(QueryOptions.class).isDisabled();
        } else {
            return true;
        }
    }

    private Object findCachedObject(UUID id, Query<?> query) {
        Object object = objectCache.getIfPresent(id);

        if (object == null && query.isReferenceOnly()) {
            object = referenceCache != null ? referenceCache.getIfPresent(id) : null;
        }

        if (object != null) {
            Class<?> objectClass = query.getObjectClass();

            if (objectClass != null && !objectClass.isInstance(object)) {
                object = null;
            }
        }

        return object;
    }

    private void cacheObject(Object object) {
        State state = ((Recordable) object).getState();
        UUID id = state.getId();

        if (state.isReferenceOnly()) {
            referenceCache.put(id, object);

        } else if (!state.isResolveToReferenceOnly()) {
            objectCache.put(id, object);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> List<T> readAll(Query<T> query) {
        if (isCacheDisabled(query)) {
            return super.readAll(query);
        }

        List<Object> all = new ArrayList<Object>();
        List<Object> values = query.findIdOnlyQueryValues();

        if (values != null) {
            List<Object> newValues = null;

            for (Object value : values) {
                UUID valueId = ObjectUtils.to(UUID.class, value);

                if (valueId != null) {
                    idOnlyQueryIds.put(valueId, true);
                    Object object = findCachedObject(valueId, query);
                    if (object != null) {
                        all.add(object);
                        continue;
                    }
                }

                if (newValues == null) {
                    newValues = new ArrayList<Object>();
                }
                newValues.add(value);
            }

            if (newValues == null) {
                return (List<T>) all;

            } else {
                query = query.clone();
                query.setPredicate(PredicateParser.Static.parse("_id = ?", newValues));
            }
        }

        List<?> list = readAllCache.getIfPresent(query);

        if (list == null) {
            list = super.readAll(query);
            readAllCache.put(query, list);

            for (Object item : list) {
                cacheObject(item);
            }
        }

        all.addAll(list);
        return (List<T>) all;
    }

    @Override
    public long readCount(Query<?> query) {
        if (isCacheDisabled(query)) {
            return super.readCount(query);
        }

        Long count = readCountCache.getIfPresent(query);

        if (count == null) {
            COUNT: {
                if (readAllCache != null) {
                    List<?> list = readAllCache.getIfPresent(query);

                    if (list != null) {
                        count = (long) list.size();
                        break COUNT;
                    }
                }

                if (readPartialCache != null) {
                    Map<Range, PaginatedResult<?>> subCache = readPartialCache.getIfPresent(query);

                    if (subCache != null && !subCache.isEmpty()) {
                        count = subCache.values().iterator().next().getCount();
                        break COUNT;
                    }
                }

                count = super.readCount(query);
            }

            readCountCache.put(query, count);
        }

        return count;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T readFirst(Query<T> query) {
        if (isCacheDisabled(query)) {
            return super.readFirst(query);
        }

        List<Object> values = query.findIdOnlyQueryValues();

        if (values != null) {
            for (Object value : values) {
                UUID valueId = ObjectUtils.to(UUID.class, value);

                if (valueId != null) {
                    idOnlyQueryIds.put(valueId, true);
                    Object object = findCachedObject(valueId, query);

                    if (object != null) {
                        return (T) object;
                    }
                }
            }
        }

        Object first = readFirstCache.getIfPresent(query);

        if (first == null) {
            first = super.readFirst(query);
            if (first == null) {
                first = MISSING;
            } else {
                cacheObject(first);
            }
            readFirstCache.put(query, first);
        }

        return first != MISSING ? (T) first : null;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> PaginatedResult<T> readPartial(Query<T> query, long offset, int limit) {
        if (isCacheDisabled(query)) {
            return super.readPartial(query, offset, limit);
        }

        Map<Range, PaginatedResult<?>> subCache = readPartialCache.getUnchecked(query);

        Range range = new Range(offset, limit);
        PaginatedResult<?> result = subCache.get(range);

        if (result == null) {
            result = super.readPartial(query, offset, limit);
            subCache.put(range, result);

            for (Object item : result.getItems()) {
                cacheObject(item);
            }
        }

        return (PaginatedResult<T>) result;
    }

    @Override
    public void save(State state) {
        super.save(state);
        flush();
    }

    /**
     * Flush the entire cache. This is executed after every .save() to avoid inconsistent results.
     */
    protected void flush() {
        objectCache.invalidateAll();
        referenceCache.invalidateAll();
        readAllCache.invalidateAll();
        readCountCache.invalidateAll();
        readFirstCache.invalidateAll();
        readPartialCache.invalidateAll();
    }

    /**
     * {@link Query} options for {@link CachingDatabase}.
     *
     * @deprecated Use {@link Query#isCache}, {@link Query#noCache}, or
     * {@link Query#setCache} instead.
     */
    @Deprecated
    @Modification.FieldInternalNamePrefix("caching.")
    public static class QueryOptions extends Modification<Query<?>> {

        private boolean disabled;

        /**
         * Returns {@code true} if the caching should be disabled when
         * running the query.
         *
         * @deprecated Use {@link Query#isCache} instead.
         */
        @Deprecated
        public boolean isDisabled() {
            Boolean old = ObjectUtils.to(Boolean.class, getOriginalObject().getOptions().get(IS_DISABLED_QUERY_OPTION));
            return old != null ? old : disabled;
        }

        /**
         * Sets whether the caching should be disabled when running
         * the query.
         *
         * @deprecated Use {@link Query#noCache} or {@link Query#setCache}
         * instead.
         */
        @Deprecated
        public void setDisabled(boolean disabled) {
            this.disabled = disabled;
        }
    }

    // --- Deprecated ---

    /** @deprecated Use {@link QueryOptions} instead. */
    @Deprecated
    public static final String IS_DISABLED_QUERY_OPTION = "caching.isDisabled";
}
