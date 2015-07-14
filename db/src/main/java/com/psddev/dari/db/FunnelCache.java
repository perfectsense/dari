package com.psddev.dari.db;

import java.util.List;
import java.util.Map;
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

/**
 * Provides a global (per-Database) short-lived cache for database read operations.
 */
public class FunnelCache<T extends Database> {

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
    private final LoadingCache<FunnelCachedObjectProducer<T>, List<FunnelCachedObject>> objectCache;

    public FunnelCache(T db, Map<String, Object> settings) {
        this.database = db;

        Long cacheSize = ObjectUtils.to(Long.class, settings.get(CACHE_SIZE_SUB_SETTING));
        Integer concurrencyLevel = ObjectUtils.to(Integer.class, settings.get(CONCURRENCY_LEVEL_SUB_SETTING));
        Long expireMilliseconds = ObjectUtils.to(Long.class, settings.get(CACHE_EXPIRE_MILLISECONDS_SUB_SETTING));
        Long refreshMilliseconds = ObjectUtils.to(Long.class, settings.get(CACHE_REFRESH_MILLISECONDS_SUB_SETTING));

        objectCache = CacheBuilder
                .newBuilder()
                .maximumSize(cacheSize != null ? cacheSize : DEFAULT_CACHE_SIZE)
                .concurrencyLevel(concurrencyLevel != null ? concurrencyLevel : DEFAULT_CONCURRENCY_LEVEL)
                .expireAfterWrite(expireMilliseconds != null ? expireMilliseconds : DEFAULT_CACHE_EXPIRE_MILLISECONDS, TimeUnit.MILLISECONDS)
                .refreshAfterWrite(refreshMilliseconds != null ? refreshMilliseconds : DEFAULT_CACHE_REFRESH_MILLISECONDS, TimeUnit.MILLISECONDS)
                .build(new FunnelCacheLoader());
    }

    public final List<FunnelCachedObject> get(final FunnelCachedObjectProducer<T> producer) {
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

    private final class FunnelCacheLoader extends CacheLoader<FunnelCachedObjectProducer<T>, List<FunnelCachedObject>> {

        @Override
        public List<FunnelCachedObject> load(FunnelCachedObjectProducer<T> producer) throws Exception {
            Stats.Timer timer = STATS.startTimer();
            try {
                return producer.produce(database);
            } finally {
                timer.stop("Load");
            }
        }

        @Override
        public ListenableFuture<List<FunnelCachedObject>> reload(final FunnelCachedObjectProducer<T> producer, List<FunnelCachedObject> previousResult) {
            ListenableFutureTask<List<FunnelCachedObject>> task = ListenableFutureTask.create(new Callable<List<FunnelCachedObject>>() {
                @Override
                public List<FunnelCachedObject> call() {
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
}
