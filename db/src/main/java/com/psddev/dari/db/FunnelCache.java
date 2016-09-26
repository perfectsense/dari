package com.psddev.dari.db;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListenableFutureTask;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.psddev.dari.util.ObjectUtils;
import com.psddev.dari.util.Stats;

class FunnelCache<T extends Database> {

    private static final Stats STATS = new Stats("Funnel Cache");

    private static final String CACHE_EXPIRE_MILLISECONDS_SUB_SETTING = "funnelCacheExpireMillis";
    private static final String CACHE_REFRESH_MILLISECONDS_SUB_SETTING = "funnelCacheRefreshMillis";
    private static final String CONCURRENCY_LEVEL_SUB_SETTING = "funnelCacheConcurrencyLevel";
    private static final String CACHE_SIZE_SUB_SETTING = "funnelCacheSize";
    private static final long DEFAULT_CACHE_EXPIRE_MILLISECONDS = 1500;
    private static final long DEFAULT_CACHE_REFRESH_MILLISECONDS = 1000;
    private static final int DEFAULT_CONCURRENCY_LEVEL = 20;
    private static final long DEFAULT_CACHE_SIZE = 10000;

    private final T database;
    private final LoadingCache<CachedObjectProducer<T>, List<CachedObject>> objectCache;

    public FunnelCache(T db, Map<String, Object> settings) {
        this.database = db;

        Long cacheSize = ObjectUtils.to(Long.class, settings.get(CACHE_SIZE_SUB_SETTING));
        Integer concurrencyLevel = ObjectUtils.to(Integer.class, settings.get(CONCURRENCY_LEVEL_SUB_SETTING));
        Long expireMilliseconds = ObjectUtils.to(Long.class, settings.get(CACHE_EXPIRE_MILLISECONDS_SUB_SETTING));
        Long refreshMilliseconds = ObjectUtils.to(Long.class, settings.get(CACHE_REFRESH_MILLISECONDS_SUB_SETTING));

        objectCache = CacheBuilder.
            newBuilder().
            maximumSize(cacheSize != null ? cacheSize : DEFAULT_CACHE_SIZE).
            concurrencyLevel(concurrencyLevel != null ? concurrencyLevel : DEFAULT_CONCURRENCY_LEVEL).
            expireAfterWrite(expireMilliseconds != null ? expireMilliseconds : DEFAULT_CACHE_EXPIRE_MILLISECONDS, TimeUnit.MILLISECONDS).
            refreshAfterWrite(refreshMilliseconds != null ? refreshMilliseconds : DEFAULT_CACHE_REFRESH_MILLISECONDS, TimeUnit.MILLISECONDS).
            build(new FunnelCacheLoader());
    }

    public final List<CachedObject> get(final CachedObjectProducer<T> producer) {
        Stats.Timer timer = STATS.startTimer();
        try {
            return objectCache.getUnchecked(producer);
        } catch (UncheckedExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof RuntimeException) {
                throw (RuntimeException) cause;
            } else {
                throw new DatabaseException(database, cause);
            }
        } finally {
            timer.stop("Get");
        }
    }

    protected static final class CachedObject {

        private final UUID id;
        private final UUID typeId;
        private final Map<String, Object> values;
        private final Map<String, Object> extras;

        CachedObject(UUID id, UUID typeId, Map<String, Object> values, Map<String, Object> extras) {
            this.id = id;
            this.typeId = typeId;
            this.values = values;
            this.extras = extras;
        }

        public UUID getId() {
            return id;
        }

        public UUID getTypeId() {
            return typeId;
        }

        public Map<String, Object> getValues() {
            return values;
        }

        public Map<String, Object> getExtras() {
            return extras;
        }

        @Override
        public String toString() {
            return String.format("ID: %s, Type: %s", id, typeId);
        }
    }

    private final class FunnelCacheLoader extends CacheLoader<CachedObjectProducer<T>, List<CachedObject>> {

        @Override
        public List<CachedObject> load(CachedObjectProducer<T> producer) throws Exception {
            Stats.Timer timer = STATS.startTimer();
            try {
                return producer.produce(database);
            } finally {
                timer.stop("Load");
            }
        }

        @Override
        public ListenableFuture<List<CachedObject>> reload(final CachedObjectProducer<T> producer, List<CachedObject> previousResult) {
            ListenableFutureTask<List<CachedObject>> task = ListenableFutureTask.create(new Callable<List<CachedObject>>() {
                @Override
                public List<CachedObject> call() {
                    Stats.Timer timer = STATS.startTimer();
                    try {
                        return producer.produce(database);
                    } finally {
                        timer.stop("Reload");
                    }
                }
            });
            MoreExecutors.sameThreadExecutor().execute(task);
            return task;
        }
    }

    protected interface CachedObjectProducer<T extends Database> {
        List<CachedObject> produce(T database);
    }
}
