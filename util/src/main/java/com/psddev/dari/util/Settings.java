package com.psddev.dari.util;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Type;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import javax.naming.Binding;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

/**
 * Global map of settings.
 */
public final class Settings {

    /**
     * Sub-key used to identify the implementation class for
     * {@link #newInstance}.
     */
    public static final String CLASS_SUB_SETTING = "class";

    /**
     * Key used to toggle debug mode.
     */
    public static final String DEBUG_SETTING = "DEBUG";

    /**
     * Key used to indicate when running in a production environment.
     */
    public static final String PRODUCTION_SETTING = "PRODUCTION";

    /**
     * Key used to specify a shared secret.
     */
    public static final String SECRET_SETTING = "SECRET";

    /**
     * Default properties file that contains all settings.
     */
    public static final String SETTINGS_FILE = "/settings.properties";

    private static final String JNDI_PREFIX = "java:comp/env";
    private static final Logger LOGGER = LoggerFactory.getLogger(Settings.class);
    private static final Map<String, Map<String, Object>> PERMANENT_OVERRIDES_MAP = new LinkedHashMap<String, Map<String, Object>>();
    private static final ThreadLocal<Map<String, Object>> THREAD_OVERRIDES = new ThreadLocal<Map<String, Object>>();
    private static final String RANDOM_SECRET = UUID.randomUUID().toString();

    private static final LoadingCache<Class<?>, Constructor<?>> CONSTRUCTOR_CACHE = CacheBuilder
            .newBuilder()
            .build(new CacheLoader<Class<?>, Constructor<?>>() {

                @Override
                public Constructor<?> load(Class<?> instanceClass) throws NoSuchMethodException {
                    Constructor<?> constructor = instanceClass.getDeclaredConstructor();

                    constructor.setAccessible(true);
                    return constructor;
                }
            });

    private static final Lazy<PeriodicCache<String, Object>> SETTINGS = new Lazy<PeriodicCache<String, Object>>() {

        @Override
        public PeriodicCache<String, Object> create() {
            return new PeriodicCache<String, Object>(0.0, 10.0) {

                private boolean jndiErrorLogged;

                @Override
                protected Map<String, Object> update() {

                    // Optional file.
                    Map<String, Object> settings = new TreeMap<String, Object>();
                    InputStream input = Settings.class.getResourceAsStream(SETTINGS_FILE);

                    if (input != null) {
                        try {
                            try {
                                Properties properties = new Properties();

                                properties.load(input);
                                putAllMap(settings, properties);

                            } finally {
                                input.close();
                            }

                        } catch (IOException error) {
                            LOGGER.warn(String.format(
                                    "Can't read from [%s]!", SETTINGS_FILE),
                                    error);
                        }
                    }

                    // Environment.
                    putAllMap(settings, System.getenv());
                    putAllMap(settings, System.getProperties());

                    // JNDI.
                    try {
                        putAllContext(settings, new InitialContext(), JNDI_PREFIX);

                    } catch (Throwable error) {
                        if (!jndiErrorLogged) {
                            jndiErrorLogged = true;

                            LOGGER.info(
                                    String.format("Can't read from JNDI! [%s: %s]",
                                            error.getClass().getName(),
                                            error.getMessage()),
                                    error);
                        }
                    }

                    // Permanent overrides.
                    synchronized (PERMANENT_OVERRIDES_MAP) {
                        for (Map<String, Object> overrides : PERMANENT_OVERRIDES_MAP.values()) {
                            CollectionUtils.putAllRecursively(settings, overrides);
                        }
                    }

                    return Collections.unmodifiableMap(settings);
                }

                private void putAllMap(Map<String, Object> map, Map<?, ?> other) {
                    for (Map.Entry<?, ?> entry : other.entrySet()) {
                        Object key = entry.getKey();

                        if (key != null) {
                            CollectionUtils.putByPath(map, key.toString(), entry.getValue());
                        }
                    }
                }

                private void putAllContext(Map<String, Object> map, Context context, String path) throws NamingException {
                    String pathWithSlash;

                    if (path.endsWith("/")) {
                        pathWithSlash = path;
                        path = path.substring(0, path.length() - 1);

                    } else {
                        pathWithSlash = path + "/";
                    }

                    for (Enumeration<Binding> e = context.listBindings(path); e.hasMoreElements();) {
                        Binding binding;

                        try {
                            binding = e.nextElement();

                        } catch (Throwable error) {
                            continue;
                        }

                        String name = binding.getName();

                        if (name.startsWith(pathWithSlash)) {
                            name = name.substring(pathWithSlash.length());
                        }

                        if (!ObjectUtils.isBlank(name)) {
                            String fullName = pathWithSlash + name;
                            Object value = binding.getObject();

                            if (value instanceof Context) {
                                putAllContext(map, context, fullName);

                            } else {
                                CollectionUtils.putByPath(map, fullName.substring(JNDI_PREFIX.length() + 1), value);
                            }
                        }
                    }
                }
            };
        }
    };

    /**
     * Returns the value associated with the given {@code key}, or if not
     * found or is blank, the given {@code defaultValue}.
     *
     * @param key Can be {@code null}.
     * @param defaultValue Can be {@code null}.
     */
    public static Object getOrDefault(String key, Object defaultValue) {
        Object value = null;
        Map<String, Object> overrides = THREAD_OVERRIDES.get();

        if (overrides != null) {
            value = CollectionUtils.getByPath(overrides, key);
        }

        if (ObjectUtils.isBlank(value)) {
            value = CollectionUtils.getByPath(SETTINGS.get(), key);
        }

        if (ObjectUtils.isBlank(value)) {
            value = defaultValue;
        }

        return value;
    }

    /**
     * Returns the value associated with the given {@code key} as an instance
     * of the given {@code returnType}, or if not found or is blank, the given
     * {@code defaultValue}.
     *
     * @param returnType Can't be {@code null}.
     * @param key Can be {@code null}.
     * @param defaultValue Can be {@code null}.
     */
    public static Object getOrDefault(Type returnType, String key, Object defaultValue) {
        return ObjectUtils.to(returnType, getOrDefault(key, defaultValue));
    }

    /**
     * Returns the value associated with the given {@code key} as an instance
     * of the given {@code returnClass}, or if not found or is blank, the given
     * {@code defaultValue}.
     *
     * @param returnClass Can't be {@code null}.
     * @param key Can be {@code null}.
     * @param defaultValue Can be {@code null}.
     */
    public static <T> T getOrDefault(Class<T> returnClass, String key, T defaultValue) {
        return ObjectUtils.to(returnClass, getOrDefault(key, defaultValue));
    }

    /**
     * Returns the value associated with the given {@code key} as an instance
     * of the type referenced by the given {@code returnTypeReference},
     * or if not found or is blank, the given {@code defaultValue}.
     *
     * @param returnTypeReference Can't be {@code null}.
     * @param key Can be {@code null}.
     * @param defaultValue Can be {@code null}.
     */
    public static <T> T getOrDefault(TypeReference<T> returnTypeReference, String key, T defaultValue) {
        return ObjectUtils.to(returnTypeReference, getOrDefault(key, defaultValue));
    }

    /**
     * Returns the value associated with the given {@code key}, or if not
     * found or is blank, the given {@code defaultValue}.
     *
     * @param key Can be {@code null}.
     */
    public static Object get(String key) {
        return get(key, null);
    }

    /**
     * Returns the value, as an instance of the given {@code returnType},
     * associated with the given {@code key}, or if not found or is blank,
     * the given {@code defaultValue}.
     *
     * @param returnType Can't be {@code null}.
     * @param key Can be {@code null}.
     */
    public static Object get(Type returnType, String key) {
        return get(returnType, key, null);
    }

    /**
     * Returns the value, as an instance of the given {@code returnClass},
     * associated with the given {@code key}, or if not found or is blank,
     * the given {@code defaultValue}.
     *
     * @param returnClass Can't be {@code null}.
     * @param key Can be {@code null}.
     */
    public static <T> T get(Class<T> returnClass, String key) {
        return get(returnClass, key, null);
    }

    /**
     * Returns the value, as an instance of the type referenced by the given
     * {@code returnTypeReference}, associated with the given {@code key},
     * or if not found or is blank, the given {@code defaultValue}.
     *
     * @param returnTypeReference Can't be {@code null}.
     * @param key Can be {@code null}.
     */
    public static <T> T get(TypeReference<T> returnTypeReference, String key) {
        return get(returnTypeReference, key, null);
    }

    private static <T> T checkValue(T value, String key, String message) {
        if (ObjectUtils.isBlank(value)) {
            throw new SettingsException(key, message != null ? message : "[" + key + "] can't be blank!");

        } else {
            return value;
        }
    }

    /**
     * Returns the value associated with the given {@code key}, or throws
     * a {@link SettingsException} with the given {@code message} if not
     * found or the value is blank.
     *
     * @param key Can be {@code null}.
     * @param message Can be {@code null}.
     * @throws SettingsException If the value associated with the given
     * {@code key} isn't found or is blank.
     */
    public static Object getOrError(String key, String message) {
        return checkValue(get(key), key, message);
    }

    /**
     * Returns the value associated with the given {@code key} as an instance
     * of the given {@code returnType}, or throws a {@link SettingsException}
     * with the given {@code message} if not found or the value is blank.
     *
     * @param returnType Can't be {@code null}.
     * @param key Can be {@code null}.
     * @param message Can be {@code null}.
     * @throws SettingsException If the value associated with the given
     * {@code key} isn't found or is blank.
     */
    public static Object getOrError(Type returnType, String key, String message) {
        return checkValue(get(returnType, key), key, message);
    }

    /**
     * Returns the value associated with the given {@code key} as an instance
     * of the given {@code returnClass}, or throws a {@link SettingsException}
     * with the given {@code message} if not found or the value is blank.
     *
     * @param returnClass Can't be {@code null}.
     * @param key Can be {@code null}.
     * @param message Can be {@code null}.
     * @throws SettingsException If the value associated with the given
     * {@code key} isn't found or is blank.
     */
    public static <T> T getOrError(Class<T> returnClass, String key, String message) {
        return checkValue(get(returnClass, key), key, message);
    }

    /**
     * Returns the value associated with the given {@code key} as an instance
     * of the type referenced by the given {@code returnTypeReference},
     * or throws a {@link SettingsException} with the given {@code message}
     * if not found or the value is blank.
     *
     * @param returnTypeReference Can't be {@code null}.
     * @param key Can be {@code null}.
     * @param message Can be {@code null}.
     * @throws SettingsException If the value associated with the given
     * {@code key} isn't found or is blank.
     */
    public static <T> T getOrError(TypeReference<T> returnTypeReference, String key, String message) {
        return checkValue(get(returnTypeReference, key), key, message);
    }

    /**
     * Puts all entries from the given {@code overrides} permanently to the
     * global settings and associates them with the given {@code name}.
     * They can be later removed with {@link #removePermanentOverrides}
     * using the given {@code name}. Any subsequent changes in the given
     * {@code overrides} will be automatically synchronized to the global
     * settings.
     *
     * @param name Can't be {@code null}.
     * @param overrides Does nothing if {@code null}.
     */
    public static void putPermanentOverrides(String name, Map<String, Object> overrides) {
        Preconditions.checkNotNull(name);

        if (overrides != null) {
            synchronized (PERMANENT_OVERRIDES_MAP) {
                PERMANENT_OVERRIDES_MAP.put(name, overrides);
                SETTINGS.get().refresh();
            }
        }
    }

    /**
     * Removes the permanent overrides previously associated with the given
     * {@code name} using {@link #putPermanentOverrides}.
     *
     * @param name Can't be {@code null}.
     */
    public static void removePermanentOverrides(String name) {
        Preconditions.checkNotNull(name);

        synchronized (PERMANENT_OVERRIDES_MAP) {
            PERMANENT_OVERRIDES_MAP.remove(name);
            SETTINGS.get().refresh();
        }
    }

    /**
     * Temporarily overrides the value associated with the given
     * {@code key} in the current thread.
     */
    public static void setOverride(String key, Object value) {
        Map<String, Object> overrides = THREAD_OVERRIDES.get();

        if (value != null) {
            if (overrides == null) {
                overrides = new HashMap<String, Object>();
                THREAD_OVERRIDES.set(overrides);
            }

            CollectionUtils.putByPath(overrides, key, value);

        } else {
            if (overrides != null) {
                CollectionUtils.putByPath(overrides, key, null);
            }
        }
    }

    /**
     * Returns {@code true} if running in debug mode.
     */
    public static boolean isDebug() {
        return get(boolean.class, DEBUG_SETTING);
    }

    /**
     * Returns {@code true} if running in a production environment.
     */
    public static boolean isProduction() {
        return get(boolean.class, PRODUCTION_SETTING);
    }

    /**
     * Returns a shared secret.
     */
    public static String getSecret() {
        String secret = get(String.class, SECRET_SETTING);

        if (ObjectUtils.isBlank(secret)) {
            secret = get(String.class, "cookieSecret");
        }

        if (ObjectUtils.isBlank(secret)) {
            secret = RANDOM_SECRET;
        }

        return secret;
    }

    /**
     * Returns a view of all settings as a map.
     *
     * @return Never {@code null}. Immutable.
     */
    public static Map<String, Object> asMap() {
        return SETTINGS.get();
    }

    /**
     * Creates an instance of the given {@code interfaceClass} based on the
     * values associated with the given {@code key}.
     *
     * @param interfaceClass Can't be {@code null}.
     * @param key Can't be blank.
     * @return Never {@code null}.
     * @throws SettingsException If the values associated with the given
     * {@code key} can't be used to create the instance.
     */
    @SuppressWarnings("unchecked")
    public static <T extends SettingsBackedObject> T newInstance(Class<T> interfaceClass, String key) {
        Object instanceSettings = get(key);

        if (!(instanceSettings instanceof Map)) {
            throw new SettingsException(key, String.format(
                    "[%s] settings must be a map!",
                    interfaceClass.getName()));
        }

        String classKey = key + "/" + CLASS_SUB_SETTING;
        String instanceClassName = ObjectUtils.to(String.class, get(classKey));

        if (ObjectUtils.isBlank(instanceClassName)) {
            throw new SettingsException(classKey, String.format(
                    "Implementation class for [%s] is missing!",
                    interfaceClass.getName()));
        }

        Class<?> instanceClass = ObjectUtils.getClassByName(instanceClassName);

        if (instanceClass == null) {
            throw new SettingsException(classKey, String.format(
                    "[%s] isn't a valid class!",
                    instanceClassName));
        }

        for (Class<?> requiredClass : new Class<?>[] {
                interfaceClass,
                SettingsBackedObject.class }) {
            if (!requiredClass.isAssignableFrom(instanceClass)) {
                throw new SettingsException(classKey, String.format(
                        "[%s] doesn't implement [%s]!",
                        instanceClass.getName(),
                        requiredClass.getName()));
            }
        }

        Constructor<?> constructor = null;

        try {
            constructor = CONSTRUCTOR_CACHE.get(instanceClass);

        } catch (ExecutionException error) {
            throw new SettingsException(classKey, String.format(
                    "[%s] doesn't have a nullary constructor!",
                    instanceClass.getName()));
        }

        T object;

        try {
            object = (T) constructor.newInstance();

        } catch (IllegalAccessException ex) {
            throw new IllegalStateException(ex);

        } catch (InstantiationException ex) {
            throw new IllegalStateException(ex);

        } catch (InvocationTargetException ex) {
            Throwable cause = ex.getCause();

            throw cause instanceof RuntimeException
                    ? (RuntimeException) cause
                    : new RuntimeException(String.format(
                            "Unexpected error trying to create [%s]!",
                            instanceClassName), cause);
        }

        object.initialize(key, (Map<String, Object>) instanceSettings);
        return object;
    }

    /**
     * @deprecated Use {@link #getOrDefault(String, Object)} instead.
     */
    @Deprecated
    public static Object get(String key, Object defaultValue) {
        return getOrDefault(key, defaultValue);
    }

    /**
     * @deprecated Use {@link #getOrDefault(Type, String, Object)} instead.
     */
    @Deprecated
    public static Object get(Type returnType, String key, Object defaultValue) {
        return getOrDefault(returnType, key, defaultValue);
    }

    /**
     * @deprecated Use {@link #getOrDefault(Class, String, Object)} instead.
     */
    @Deprecated
    public static <T> T get(Class<T> returnClass, String key, T defaultValue) {
        return getOrDefault(returnClass, key, defaultValue);
    }

    /**
     * @deprecated Use {@link #getOrDefault(TypeReference, String, Object)} instead.
     */
    @Deprecated
    public static <T> T get(TypeReference<T> returnTypeReference, String key, T defaultValue) {
        return getOrDefault(returnTypeReference, key, defaultValue);
    }
}
