package com.psddev.dari.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;

import com.google.common.base.Optional;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

/** Object utility methods. */
public abstract class ObjectUtils {

    private static final JsonProcessor JSON_PROCESSOR = new JsonProcessor();

    private static final Map<String, Class<?>> PRIMITIVE_CLASSES; static {
        Map<String, Class<?>> m = new HashMap<String, Class<?>>();
        for (Class<?> c : new Class<?>[] {
                byte.class, short.class, int.class, long.class,
                float.class, double.class, boolean.class, char.class }) {
            m.put(c.getName(), c);
        }
        PRIMITIVE_CLASSES = m;
    }

    // Because Class.forName is pretty slow.
    private static final LoadingCache<Optional<ClassLoader>, LoadingCache<String, Optional<Class<?>>>>
            CLASSES_BY_LOADER = CacheBuilder.newBuilder().build(new CacheLoader<Optional<ClassLoader>, LoadingCache<String, Optional<Class<?>>>>() {

                @Override
                public LoadingCache<String, Optional<Class<?>>> load(final Optional<ClassLoader> loader) {
                    return CacheBuilder.newBuilder().build(new CacheLoader<String, Optional<Class<?>>>() {

                        @Override
                        @SuppressWarnings({ "rawtypes", "unchecked" })
                        public Optional<Class<?>> load(String className) {
                            try {
                                return (Optional) Optional.of((Class) Class.forName(className, false, loader.orNull()));

                            } catch (ClassNotFoundException error) {
                                // Falls through to return absent below.

                            } catch (NoClassDefFoundError error) {
                                // Falls through to return absent below.
                            }

                            return Optional.absent();
                        }
                    });
                }
            });

    /**
     * Returns {@code true} if the given {@code object1} and {@code object2},
     * either of which may be {@code null}, are equal.
     *
     * @see Object#equals(Object)
     */
    public static boolean equals(Object object1, Object object2) {
        if (object1 == object2) {
            return true;

        } else if (object1 == null || object2 == null) {
            return false;

        } else if (object1 instanceof Enum && object2 instanceof String) {
            return ((Enum<?>) object1).name().equals(object2);

        } else if (object1 instanceof String && object2 instanceof Enum) {
            return object1.equals(((Enum<?>) object2).name());

        } else {
            return object1.equals(object2);
        }
    }

    /**
     * Returns the hash code value for the given {@code objects},
     * or 0 if it's {@code null}.
     */
    public static int hashCode(Object... objects) {
        if (objects == null) {
            return 0;

        } else if (objects.length == 1) {
            Object object = objects[0];
            return object == null ? 0 : object.hashCode();

        } else {
            return Arrays.hashCode(objects);
        }
    }

    /**
     * Returns the first non-{@code null} value among the given
     * {@code values}.
     *
     * @param values If {@code null}, returns {@code null}.
     * @return May be {@code null}.
     */
    public static <T> T firstNonNull(T... values) {
        if (values != null) {
            for (T value : values) {
                if (value != null) {
                    return value;
                }
            }
        }

        return null;
    }

    /**
     * Returns the first non-{@link #isBlank blank} value among the given
     * {@code values}.
     *
     * @param values If {@code null}, returns {@code null}.
     * @return May be {@code null}.
     */
    public static <T> T firstNonBlank(T... values) {
        if (values != null) {
            for (T value : values) {
                if (!ObjectUtils.isBlank(value)) {
                    return value;
                }
            }
        }

        return null;
    }

    /**
     * Returns the first non-{@code null} value among the given
     * {@code values}.
     *
     * @deprecated Use {@link #firstNonNull} instead.
     */
    @Deprecated
    public static <T> T coalesce(T... values) {
        return firstNonNull(values);
    }

    /**
     * Returns either the {@linkplain Thread#getContextClassLoader
     * context class loader} from the current thread or the one
     * that loaded this class.
     */
    public static ClassLoader getCurrentClassLoader() {
        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        return loader != null ? loader : ObjectUtils.class.getClassLoader();
    }

    /**
     * Returns the class with the given {@code name} from the given
     * class {@code loader}.
     *
     * @return {@code null} if the class with the given {@code name}
     * doesn't exist.
     */
    public static Class<?> getClassFromLoader(ClassLoader loader, String name) {
        if (ObjectUtils.isBlank(name)) {
            return null;

        } else {
            return CLASSES_BY_LOADER
                    .getUnchecked(Optional.fromNullable(loader))
                    .getUnchecked(name)
                    .orNull();
        }
    }

    /**
     * Returns the class with the given {@code name} from the
     * {@linkplain #getCurrentClassLoader current class loader}.
     *
     * @return {@code null} if the class with the given {@code name}
     * doesn't exist.
     */
    public static Class<?> getClassByName(String name) {
        Class<?> c = PRIMITIVE_CLASSES.get(name);
        return c != null ? c : getClassFromLoader(getCurrentClassLoader(), name);
    }

    /**
     * Compares the given {@code object1} and {@code object2} without
     * throwing any errors.
     *
     * @param isNullGreatest If {@code null} is considered to be greater
     *        than any other value.
     * @return A negative integer, zero, or a positive integer, depending
     *         on whether the given {@code object1} is less than, equal to,
     *         or greater than the given {@code object2}. If the parameters
     *         aren't {@linkplain Comparable comparable}, this method
     *         always returns {@code -1}.
     */
    public static int compare(Object object1, Object object2, boolean isNullGreatest) {
        if (object1 == null) {
            return object2 == null ? 0 : (isNullGreatest ? 1 : -1);

        } else if (object2 == null) {
            return isNullGreatest ? -1 : 1;

        } else {
            Class<?> object1Class = object1.getClass();
            Class<?> object2Class = object2.getClass();

            if (Comparable.class.isAssignableFrom(object1Class)
                    && object1Class.isAssignableFrom(object2Class)) {
                @SuppressWarnings("unchecked")
                Comparable<Object> object1Comparable = (Comparable<Object>) object1;
                return object1Comparable.compareTo(object2);

            } else if (Comparable.class.isAssignableFrom(object2Class)
                    && object2Class.isAssignableFrom(object1Class)) {
                @SuppressWarnings("unchecked")
                Comparable<Object> object2Comparable = (Comparable<Object>) object2;
                return 0 - object2Comparable.compareTo(object1);

            } else {
                return -1;
            }
        }
    }

    /** Sorts the given {@code list} using {@link #compare}. */
    public static void sort(List<?> list, boolean isNullGreatest) {
        Collections.sort(list, isNullGreatest
                ? NULL_GREATEST_COMPARATOR
                : NULL_LEAST_COMPARATOR);
    }

    private static final Comparator<Object> NULL_GREATEST_COMPARATOR = new Comparator<Object>() {
        @Override
        public int compare(Object object1, Object object2) {
            return ObjectUtils.compare(object1, object2, true);
        }
    };

    private static final Comparator<Object> NULL_LEAST_COMPARATOR = new Comparator<Object>() {
        @Override
        public int compare(Object object1, Object object2) {
            return ObjectUtils.compare(object1, object2, false);
        }
    };

    /**
     * Returns {@code true} if the given object is {@code null}, an empty
     * string, a string that consists of only whitespaces, an empty
     * collection, an empty map, an empty array, or an iterable without
     * any elements.
     */
    public static boolean isBlank(Object object) {
        if (object == null) {
            return true;

        } else if (object instanceof String) {
            String string = (String) object;

            for (int i = 0, length = string.length(); i < length; ++ i) {
                if (!Character.isWhitespace(string.charAt(i))) {
                    return false;
                }
            }

            return true;

        } else if (object instanceof Collection) {
            return ((Collection<?>) object).isEmpty();

        } else if (object instanceof Map) {
            return ((Map<?, ?>) object).isEmpty();

        } else if (object.getClass().isArray()) {
            return Array.getLength(object) == 0;

        } else if (object instanceof Iterable) {
            return !((Iterable<?>) object).iterator().hasNext();

        } else {
            return false;
        }
    }

    // --- ClassFinder bridge ---

    /**
     * Finds all classes that are compatible with the given {@code baseClass}
     * within the given class {@code loader}.
     *
     * @param loader If {@code null}, uses the current class loader.
     * @param baseClass Can't be {@code null}.
     * @return Never {@code null}.
     * @deprecated Use {@link ClassFinder.Static#findClassesFromLoader} instead.
     */
    @Deprecated
    public static <T> Set<Class<? extends T>> findClassesFromLoader(ClassLoader loader, Class<T> baseClass) {
        return ClassFinder.Static.findClassesFromLoader(loader, baseClass);
    }

    /**
     * Finds all classes that are compatible with the given {@code baseClass}
     * within the current class loader.
     *
     * @param baseClass Can't be {@code null}.
     * @return Never {@code null}.
     * @deprecated Use {@link ClassFinder.Static#findClasses} instead.
     */
    @Deprecated
    public static <T> Set<Class<? extends T>> findClasses(Class<T> baseClass) {
        return ClassFinder.Static.findClasses(baseClass);
    }

    // --- Converter bridge ---

    private static final Converter CONVERTER;
    private static final Converter ERRORING_CONVERTER;

    static {
        CONVERTER = new Converter();
        CONVERTER.putAllStandardFunctions();

        ERRORING_CONVERTER = new Converter();
        ERRORING_CONVERTER.setThrowError(true);
        ERRORING_CONVERTER.putAllStandardFunctions();
    }

    /**
     * Converts the given {@code object} into an instance of the given
     * {@code returnType}.
     *
     * @see Converter#convert(Type, Object)
     */
    public static Object to(Type returnType, Object object) {
        return CONVERTER.convert(returnType, object);
    }

    /**
     * Converts the given {@code object} into an instance of the given
     * {@code returnClass}.
     *
     * @see Converter#convert(Class, Object)
     */
    public static <T> T to(Class<T> returnClass, Object object) {
        return CONVERTER.convert(returnClass, object);
    }

    /**
     * Converts the given {@code object} into an instance of the type
     * referenced by the given {@code returnTypeReference}.
     *
     * @see Converter#convert(TypeReference, Object)
     */
    public static <T> T to(TypeReference<T> returnTypeReference, Object object) {
        return CONVERTER.convert(returnTypeReference, object);
    }

    /**
     * Converts the given {@code object} into an instance of the given
     * {@code returnType}.
     *
     * @throws ConversionException If the conversion fails.
     * @see Converter#convert(Type, Object)
     */
    public static Object toOrError(Type returnType, Object object) {
        return ERRORING_CONVERTER.convert(returnType, object);
    }

    /**
     * Converts the given {@code object} into an instance of the given
     * {@code returnClass}.
     *
     * @throws ConversionException If the conversion fails.
     * @see Converter#convert(Class, Object)
     */
    public static <T> T toOrError(Class<T> returnClass, Object object) {
        return ERRORING_CONVERTER.convert(returnClass, object);
    }

    /**
     * Converts the given {@code object} into an instance of the type
     * referenced by the given {@code returnTypeReference}.
     *
     * @throws ConversionException If the conversion fails.
     * @see Converter#convert(TypeReference, Object)
     */
    public static <T> T toOrError(TypeReference<T> returnTypeReference, Object object) {
        return ERRORING_CONVERTER.convert(returnTypeReference, object);
    }

    // --- JsonProcessor bridge ---

    /**
     * Parses the given JSON {@code string} into an object.
     *
     * @see JsonProcessor#parse(String)
     */
    public static Object fromJson(String string) {
        return JSON_PROCESSOR.parse(string);
    }

    /**
     * Parses the given JSON {@code bytes} into an object.
     *
     * @param bytes If {@code null}, returns {@code null}.
     * @see JsonProcessor#parse(byte[])
     */
    public static Object fromJson(byte[] bytes) {
        return JSON_PROCESSOR.parse(bytes);
    }

    /**
     * Generates a JSON string based on the given {@code object}.
     *
     * @see JsonProcessor#generate(Object)
     */
    public static String toJson(Object object) {
        return JSON_PROCESSOR.generate(object);
    }

    /**
     * Generates a JSON string, indented if the given {@code isIndentOutput}
     * is {@code true}, based on the given {@code object}.
     *
     * @see JsonProcessor#setIndentOutput(boolean)
     * @see JsonProcessor#generate(Object)
     */
    public static String toJson(Object object, boolean isIndentOutput) {
        JsonProcessor processor = new JsonProcessor();
        processor.setIndentOutput(isIndentOutput);
        return processor.generate(object);
    }

    /**
     * Generates a JSON string, indented if the given {@code isIndentOutput}
     * is {@code true}, based on the given {@code object}, after transforming
     * it using the given {@code transformer}.
     *
     * @see JsonProcessor#setIndentOutput(boolean)
     * @see JsonProcessor#setTransformer(Transformer)
     * @see JsonProcessor#generate(Object)
     *
     * @deprecated No replacement.
     */
    @Deprecated
    public static String toJson(Object object, boolean isIndentOutput, Transformer transformer) {
        JsonProcessor processor = new JsonProcessor();
        processor.setIndentOutput(isIndentOutput);
        processor.setTransformer(transformer);
        return processor.generate(object);
    }

    // --- Content type ---

    /**
     * The default content type returned by {@link #getContentType} when
     * a file extension isn't mapped to a known content type.
     */
    public static final String DEFAULT_CONTENT_TYPE = "application/octet-stream";

    // File name extensions to content types cache from:
    // http://svn.apache.org/repos/asf/httpd/httpd/trunk/docs/conf/mime.types
    private static final Lazy<Map<String, String>> CONTENT_TYPES = new Lazy<Map<String, String>>() {

        @Override
        protected Map<String, String> create() throws IOException {
            Map<String, String> contentTypes = new HashMap<String, String>();
            InputStream mimeInput = getClass().getResourceAsStream("mime.types");
            BufferedReader mimeInputReader = new BufferedReader(new InputStreamReader(mimeInput, StandardCharsets.UTF_8));

            try {
                for (String line; (line = mimeInputReader.readLine()) != null;) {
                    if (!line.startsWith("#")) {
                        String[] items = StringUtils.split(line.trim(), "\\s+");
                        String contentType = items[0].toLowerCase(Locale.ENGLISH);

                        for (int j = 1, length = items.length; j < length; ++ j) {
                            contentTypes.put(items[j].toLowerCase(Locale.ENGLISH), contentType);
                        }
                    }
                }

            } finally {
                mimeInputReader.close();
            }

            return contentTypes;
        }
    };

    /**
     * Returns the content type associated with the given {@code fileName}
     * extension, or {@value DEFAULT_CONTENT_TYPE} if not found.
     *
     * @param fileName If {@code null}, returns {@value DEFAULT_CONTENT_TYPE}.
     * @return Never {@code null}.
     */
    public static String getContentType(String fileName) {
        int dotAt = fileName.lastIndexOf('.');

        if (dotAt > -1) {
            String type = CONTENT_TYPES.get().get(fileName.substring(dotAt + 1).toLowerCase(Locale.ENGLISH));

            if (type != null) {
                return type;
            }
        }

        return DEFAULT_CONTENT_TYPE;
    }

    // --- Randomization ---

    private static final Random JITTER_RANDOM = new Random();

    /**
     * Returns a random number between {@code number - amount} and
     * {@code number + amount}.
     */
    public static double jitterAbsolutely(double number, double amount) {
        return number - amount + (JITTER_RANDOM.nextDouble() * amount * 2);
    }

    /**
     * Returns a random number between {@code number - amount} and
     * {@code number + amount}.
     */
    public static long jitterAbsolutely(long number, long amount) {
        return (number - amount + ((long) (JITTER_RANDOM.nextDouble() * (amount * 2 + 1))));
    }

    /**
     * Returns a random number between {@code number - number * scale}
     * and {@code number + number * scale}.
     */
    public static double jitter(double number, double scale) {
        return jitterAbsolutely(number, number * scale);
    }

    /**
     * Returns a random number between {@code number - number * scale}
     * and {@code number + number * scale}.
     */
    public static long jitter(long number, double scale) {
        return jitterAbsolutely(number, (long) (number * scale));
    }

    // --- Deprecated ---

    /**
     * Compares the given {@code object1} and {@code object2}.
     *
     * @return A negative integer, zero, or a positive integer, depending
     *         on whether the given {@code object1} is less than, equal to,
     *         or greater than the given {@code object2}.
     * @throws IllegalArgumentException If the parameters aren't comparable
     *         or compatible with each other.
     * @throws NullPointerException If one or more parameters are
     *         {@code null}.
     *
     * @deprecated Use {@link #compare} instead.
     */
    @Deprecated
    public static int compareTo(Object object1, Object object2) {
        if (object1 == null || object2 == null) {
            throw new NullPointerException(
                    "The [object1] and [object2] parameters cannot be null!");

        } else {
            Class<?> o1c = object1.getClass();
            Class<?> o2c = object2.getClass();

            if (Comparable.class.isAssignableFrom(o1c)
                    && o1c.isAssignableFrom(o2c)) {
                @SuppressWarnings("unchecked")
                Comparable<Object> object1Comparable = (Comparable<Object>) object1;
                return object1Comparable.compareTo(object2);

            } else if (Comparable.class.isAssignableFrom(o2c)
                    && o2c.isAssignableFrom(o1c)) {
                @SuppressWarnings("unchecked")
                Comparable<Object> object2Comparable = (Comparable<Object>) object2;
                return 0 - object2Comparable.compareTo(object1);

            } else {
                throw new IllegalArgumentException(
                        "The [object1] and [object2] parameters must be"
                                + " compatible and comparable!");
            }
        }
    }

    /** @deprecated Use {@link TypeDefinition#newInstance} instead. */
    @Deprecated
    public static <T> T newInstance(Class<T> objectClass) {
        return TypeDefinition.getInstance(objectClass).newInstance();
    }

    /**
     * Returns a value using any fields or methods with the given
     * {@code name} from the given {@code object}.
     *
     * The order of priority for values returned (if more than one
     * are defined) is:
     * - actual method {@code name}
     * - getter method "get{@code name}"
     * - field {@code name}
     * - underscore "_{@code name}" field name
     *
     * Can return value from private methods, but not private fields
     *
     * @deprecated No replacement.
     */
    @Deprecated
    public static Object getValue(Object object, String name) {
        Class<?> objectClass = object.getClass();

        for (Class<?> c = objectClass; c != null; c = c.getSuperclass()) {
            Method getter = null;

            try {
                getter = c.getDeclaredMethod(name);
            } catch (NoSuchMethodException error) {
                // Try the next getter check.
            }

            if (getter == null) {
                try {
                    getter = c.getDeclaredMethod("get" + capitalize(name));
                } catch (NoSuchMethodException error) {
                    // Getter doesn't exist. Oh well.
                }
            }

            if (getter != null) {
                getter.setAccessible(true);

                try {
                    return getter.invoke(object);

                } catch (IllegalAccessException ex) {
                    throw new IllegalStateException(ex);

                } catch (InvocationTargetException ex) {
                    Throwable cause = ex.getCause();
                    throw cause instanceof RuntimeException
                            ? (RuntimeException) cause
                            : new RuntimeException(cause);
                }

            } else {
                Field field = null;

                try {
                    field = c.getDeclaredField(name);
                } catch (NoSuchFieldException error) {
                    // Try the next field check.
                }

                if (field == null) {
                    try {
                        field = c.getDeclaredField("_" + name);
                    } catch (NoSuchFieldException error) {
                        // Field doesn't exist. Oh well.
                    }
                }

                if (field != null) {
                    try {
                        return field.get(object);
                    } catch (IllegalAccessException ex) {
                        throw new IllegalStateException(ex);
                    }
                }
            }
        }

        throw new IllegalArgumentException(String.format(
                "[%s] class does not contain a getter named [%s]!",
                objectClass.getName(), name));
    }

    // Captializes the given string.
    private static String capitalize(String string) {
        return string == null || string.length() == 0
                ? string
                : string.substring(0, 1).toUpperCase(Locale.ENGLISH) + string.substring(1);
    }

    /** @deprecated Use {@link #findClassesFromLoader} instead. */
    @Deprecated
    @SuppressWarnings("all")
    public static Set<Class<?>> findClasses(ClassLoader loader, Class<?> baseClass) {
        return (Set) findClassesFromLoader(loader, baseClass);
    }

    /** @deprecated Use {@link #to} instead. */
    @Deprecated
    public static Byte asByte(Object byteLike) {
        return to(Byte.class, byteLike);
    }

    /** @deprecated Use {@link #to} instead. */
    @Deprecated
    public static Date asDate(Object dateLike) {
        return to(Date.class, dateLike);
    }

    /** @deprecated Use {@link #to} instead. */
    @Deprecated
    public static <T extends Enum<T>> T asEnum(Class<T> type, Object enumLike) {
        return to(type, enumLike);
    }

    /** @deprecated Use {@link #to} instead. */
    @Deprecated
    public static Double asDouble(Object doubleLike) {
        return to(Double.class, doubleLike);
    }

    /** @deprecated Use {@link #to} instead. */
    @Deprecated
    public static Float asFloat(Object floatLike) {
        return to(Float.class, floatLike);
    }

    /** @deprecated Use {@link #to} instead. */
    @Deprecated
    public static Integer asInteger(Object integerLike) {
        return to(Integer.class, integerLike);
    }

    /** @deprecated Use {@link #to} instead. */
    @Deprecated
    public static Long asLong(Object longLike) {
        return to(Long.class, longLike);
    }

    /** @deprecated Use {@link #to} instead. */
    @Deprecated
    public static Short asShort(Object shortLike) {
        return to(Short.class, shortLike);
    }

    /** @deprecated Use {@link #to} instead. */
    @Deprecated
    public static UUID asUuid(Object uuidLike) {
        return to(UUID.class, uuidLike);
    }

    /** @deprecated Use {@link ErrorUtils#errorIf} instead. */
    @Deprecated
    public static void errorIf(boolean condition, String parameterName, String message) {
        ErrorUtils.errorIf(condition, parameterName, message);
    }

    /** @deprecated Use {@link ErrorUtils#errorIfNull} instead. */
    @Deprecated
    public static void errorIfNull(Object object, String parameterName) {
        ErrorUtils.errorIfNull(object, parameterName);
    }

    /** @deprecated Use {@link ErrorUtils#errorIfBlank} instead. */
    @Deprecated
    public static void errorIfBlank(Object object, String parameterName) {
        ErrorUtils.errorIfBlank(object, parameterName);
    }

    /** @deprecated Use {@link CollectionUtils#getByPath} instead. */
    @Deprecated
    public static Object getByPath(Object object, String path) {
        return CollectionUtils.getByPath(object, path);
    }
}
