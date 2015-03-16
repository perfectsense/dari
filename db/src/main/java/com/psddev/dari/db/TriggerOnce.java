package com.psddev.dari.db;

import java.util.IdentityHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

/**
 * Skeletal {@link Trigger} implementation that will only fire once.
 */
public abstract class TriggerOnce implements Trigger {

    private static final Logger LOGGER = LoggerFactory.getLogger(TriggerOnce.class);

    private static final ConcurrentHashMap<String, LoadingCache<Class<?>, Boolean>> HAS_METHOD = new ConcurrentHashMap<String, LoadingCache<Class<?>, Boolean>>();

    private final Map<Object, Void> executed = new IdentityHashMap<Object, Void>();

    /**
     * Executes this trigger only once on the given {@code object}.
     *
     * @param object Can't be {@code null}.
     */
    protected abstract void executeOnce(Object object);

    @Override
    public final void execute(Object object) {
        if (executed.containsKey(object)) {
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace(
                        "Already fired trigger [{}] from [{}] on [{}]",
                        new Object[] { this, object.getClass().getName(), State.getInstance(object).getId() });
            }

        } else {
            executed.put(object, null);
            executeOnce(object);
        }
    }

    protected boolean hasMethod(Class<?> cls, final String method) {
        if (!HAS_METHOD.containsKey(method)) {
            synchronized (HAS_METHOD) {
                if (!HAS_METHOD.containsKey(method)) {
                    HAS_METHOD.put(method, CacheBuilder.newBuilder().refreshAfterWrite(5L, TimeUnit.SECONDS).build(new CacheLoader<Class<?>, Boolean>() {
                        @Override
                        public Boolean load(Class<?> key) throws Exception {
                            try {
                                return key.getDeclaredMethod(method) != null;
                            } catch (NoClassDefFoundError | NoSuchMethodException error) {
                                // Class isn't available, or no method available to run.
                            }
                            return false;
                        }
                    }));
                }
            }
        }
        return HAS_METHOD.get(method).getUnchecked(cls);
    }
}
