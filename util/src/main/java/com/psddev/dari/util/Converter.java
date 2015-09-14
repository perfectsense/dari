package com.psddev.dari.util;

import java.lang.reflect.Array;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.GenericArrayType;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

/** Converts an arbitrary object into an instance of another class. */
public class Converter {

    private static final Map<Type, Object> NULL_TO_PRIMITIVE; static {
        Map<Type, Object> m = new HashMap<Type, Object>();
        m.put(boolean.class, Boolean.FALSE);
        m.put(byte.class, Byte.valueOf((byte) 0));
        m.put(char.class, Character.valueOf('\0'));
        m.put(double.class, Double.valueOf(0.0));
        m.put(float.class, Float.valueOf(0.0f));
        m.put(int.class, Integer.valueOf(0));
        m.put(long.class, Long.valueOf(0L));
        m.put(short.class, Short.valueOf((short) 0));
        NULL_TO_PRIMITIVE = m;
    }

    private final Map<Class<?>, Map<Type, ConversionFunction<?, ?>>> directFunctions = new HashMap<Class<?>, Map<Type, ConversionFunction<?, ?>>>();
    private final Map<Class<?>, Map<Class<?>, ConversionFunction<?, ?>>> inheritableFunctions = new HashMap<Class<?>, Map<Class<?>, ConversionFunction<?, ?>>>();
    private boolean isThrowError;
    private Map<String, Exception> errors;

    /**
     * Returns the function used to convert an instance of the given
     * {@code fromClass} to an instance of the {@code toType}.
     * - If a direct conversion function is available, it is returned. Otherwise
     * - If inheritable conversion functions are available, the one with the least
     *   inheritance distance is returned. Otherwise,
     * - null is returned
     * inheritance distance is the number of Parent->Child relationships between
     *       the {@code fromClass} asked for and the {@code fromClass} of the conversion
     *       function.
     */
    @SuppressWarnings("unchecked")
    public <F> ConversionFunction<F, Object> getFunction(Class<F> fromClass, Type toType) {
        if (fromClass == null || toType == null) {
            return null;
        } else {
            return (ConversionFunction<F, Object>) functionCache.getUnchecked(new FunctionCacheKey(fromClass, toType));
        }
    }

    private static class FunctionCacheKey {

        public final Class<?> fromClass;
        public final Type toType;

        public FunctionCacheKey(Class<?> fromClass, Type toType) {
            this.fromClass = fromClass;
            this.toType = toType;
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            } else if (other instanceof FunctionCacheKey) {
                FunctionCacheKey otherKey = (FunctionCacheKey) other;
                return fromClass.equals(otherKey.fromClass) && toType.equals(otherKey.toType);
            } else {
                return false;
            }
        }

        @Override
        public int hashCode() {
            return ObjectUtils.hashCode(fromClass, toType);
        }
    }

    private final LoadingCache<FunctionCacheKey, ConversionFunction<?, ?>> functionCache = CacheBuilder
            .newBuilder()
            .build(new CacheLoader<FunctionCacheKey, ConversionFunction<?, ?>>() {

        @Override
        @SuppressWarnings("all")
        public ConversionFunction<?, ?> load(FunctionCacheKey cacheKey) {
            List<Class<?>> fromAssignables = cacheKey.fromClass != null
                    ? (List) TypeDefinition.getInstance(cacheKey.fromClass).getAssignableClasses()
                    : Collections.<Class<?>>singletonList(null);

            Map<? extends Type, ConversionFunction<?, ?>> functions;
            ConversionFunction<?, ?> function;

            for (Class<?> assignable : fromAssignables) {
                functions = directFunctions.get(assignable);
                if (functions != null) {
                    function = functions.get(cacheKey.toType);
                    if (function != null) {
                        return function;
                    }
                }
            }

            for (Class<?> assignable : fromAssignables) {
                functions = inheritableFunctions.get(assignable);
                if (functions != null) {

                    List<Class<?>> keys = new ArrayList<Class<?>>((Collection<Class<?>>) functions.keySet());
                    Collections.sort(keys, new Comparator<Class<?>>() {

                        @Override
                        public int compare(Class<?> x, Class<?> y) {
                            return getDepth(y).compareTo(getDepth(x));
                        }

                        private Integer getDepth(Class<?> objectClass) {
                            if (objectClass == Object.class) {
                                return Integer.valueOf(-1);
                            } else {
                                int depth = 0;
                                for (Class<?> parent = objectClass; (parent = parent.getSuperclass()) != null;) {
                                    ++ depth;
                                }
                                return Integer.valueOf(depth);
                            }
                        }
                    });

                    Class<?> toClass = TypeDefinition.getInstance(cacheKey.toType).getObjectClass();
                    for (Class<?> key : keys) {
                        if (key.isAssignableFrom(toClass)) {
                            return functions.get(key);
                        }
                    }
                }
            }

            return null;
        }
    });

    /**
     * Puts the function used to convert an instance of the given
     * {@code fromClass} to an instance of the {@code toClass}.
     */
    public <F> void putDirectFunction(
            Class<F> fromClass,
            Type toType,
            ConversionFunction<F, ?> function) {

        Map<Type, ConversionFunction<?, ?>> functions = directFunctions.get(fromClass);
        if (functions == null) {
            functions = new HashMap<Type, ConversionFunction<?, ?>>();
            directFunctions.put(fromClass, functions);
        }
        functions.put(toType, function);
        functionCache.invalidateAll();
    }

    public <F, T> void putDirectFunction(
            Class<F> fromClass,
            Class<T> toClass,
            ConversionFunction<F, T> function) {

        putDirectFunction(fromClass, (Type) toClass, function);
    }

    public <F, T> void putDirectFunction(
            Class<F> fromClass,
            TypeReference<T> toType,
            ConversionFunction<F, T> function) {

        putDirectFunction(fromClass, toType.getType(), function);
    }

    /**
     * Puts the function used to convert an instance of the given
     * {@code fromClass} to an instance of the {@code toClass}.
     */
    public <F, T> void putInheritableFunction(
            Class<F> fromClass,
            Class<T> toClass,
            ConversionFunction<F, ?> function) {

        Map<Class<?>, ConversionFunction<?, ?>> functions = inheritableFunctions.get(fromClass);
        if (functions == null) {
            functions = new HashMap<Class<?>, ConversionFunction<?, ?>>();
            inheritableFunctions.put(fromClass, functions);
        }
        functions.put(toClass, function);
        functionCache.invalidateAll();
    }

    public void putAllStandardFunctions() {
        ObjectToBoolean toBool = new ObjectToBoolean();
        putDirectFunction(Object.class, boolean.class, toBool);
        putDirectFunction(Object.class, Boolean.class, toBool);

        ObjectToByte toByte = new ObjectToByte();
        putDirectFunction(Object.class, byte.class, toByte);
        putDirectFunction(Object.class, Byte.class, toByte);

        ObjectToCharacter toChar = new ObjectToCharacter();
        putDirectFunction(Object.class, char.class, toChar);
        putDirectFunction(Object.class, Character.class, toChar);

        ObjectToDouble toDouble = new ObjectToDouble();
        putDirectFunction(Object.class, double.class, toDouble);
        putDirectFunction(Object.class, Double.class, toDouble);

        ObjectToFloat toFloat = new ObjectToFloat();
        putDirectFunction(Object.class, float.class, toFloat);
        putDirectFunction(Object.class, Float.class, toFloat);

        ObjectToInteger toInt = new ObjectToInteger();
        putDirectFunction(Object.class, int.class, toInt);
        putDirectFunction(Object.class, Integer.class, toInt);

        ObjectToLong toLong = new ObjectToLong();
        putDirectFunction(Object.class, long.class, toLong);
        putDirectFunction(Object.class, Long.class, toLong);

        ObjectToShort toShort = new ObjectToShort();
        putDirectFunction(Object.class, short.class, toShort);
        putDirectFunction(Object.class, Short.class, toShort);

        putDirectFunction(Object.class, Date.class, new ObjectToDate());
        putDirectFunction(Object.class, DateTime.class, new ObjectToDateTime());
        putDirectFunction(Object.class, Iterable.class, new ObjectToIterable());
        putDirectFunction(Object.class, String.class, new ObjectToString());
        putDirectFunction(Object.class, UUID.class, new ObjectToUuid());
        putDirectFunction(Object.class, Locale.class, new ObjectToLocale());

        putInheritableFunction(Object.class, Collection.class, new ObjectToAnyCollection());
        putInheritableFunction(Object.class, Enum.class, new ObjectToAnyEnum());
        putInheritableFunction(Object.class, Map.class, new ObjectToAnyMap());
        putInheritableFunction(Object.class, Object.class, new ObjectToAnyObject());
    }

    /**
     * Returns {@code true} if the {@link #convert} methods are allowed
     * to throw errors.
     */
    public boolean isThrowError() {
        return isThrowError;
    }

    /**
     * Sets whether the {@link #convert} methods are allowed to throw
     * errors.
     */
    public void setThrowError(boolean isThrowError) {
        this.isThrowError = isThrowError;
    }

    /** Returns a map of all the errors. */
    public Map<String, Exception> getErrors() {
        Map<String, Exception> copy = new TreeMap<String, Exception>();
        if (errors != null) {
            copy.putAll(errors);
        }
        return copy;
    }

    /**
     * Converts the given {@code object} into an instance of the given
     * {@code returnType}.
     */
    public Object convert(Type returnType, Object object) {
        if (object == null) {
            Object primitive = NULL_TO_PRIMITIVE.get(returnType);
            if (isThrowError() && primitive != null) {
                throw new ConversionException(
                        "Can't convert a null into a primitive!",
                        primitive);
            } else {
                return primitive;
            }
        }

        Class<?> returnClass;
        if (returnType instanceof Class) {
            returnClass = (Class<?>) returnType;
            if (returnClass.isInstance(object)) {
                return object;
            }
        } else {
            returnClass = null;
        }

        Type itemType;
        if (returnClass != null && returnClass.isArray()) {
            itemType = returnClass.getComponentType();

        } else if (returnType instanceof GenericArrayType) {
            itemType = ((GenericArrayType) returnType).getGenericComponentType();

        } else {
            Class<?> objectClass = object.getClass();
            @SuppressWarnings("unchecked")
            ConversionFunction<Object, Object> function = (ConversionFunction<Object, Object>) getFunction(objectClass, returnType);

            if (function != null) {
                try {
                    return function.convert(this, returnType, object);

                } catch (Exception ex) {
                    if (isThrowError()) {
                        throw new ConversionException(ex);

                    } else if (ex instanceof ConversionException) {
                        return ((ConversionException) ex).getDefaultValue();

                    } else {
                        return NULL_TO_PRIMITIVE.get(returnType);
                    }
                }
            }

            throw new ConversionException(String.format(
                    "Can't convert an instance of [%s] to an instance of [%s]!",
                    objectClass, returnType), null);
        }

        Collection<?> collection = convert(Collection.class, object);
        Object array = Array.newInstance(
                itemType instanceof Class ? (Class<?>) itemType : Object.class,
                collection.size());
        int index = 0;
        for (Object item : collection) {
            Array.set(array, index, convert(itemType, item));
            ++ index;
        }
        return array;
    }

    /**
     * Converts the given {@code object} into an instance of the given
     * {@code returnClass}.
     */
    @SuppressWarnings("unchecked")
    public <T> T convert(Class<T> returnClass, Object object) {
        return (T) convert((Type) returnClass, object);
    }

    /**
     * Converts the given {@code object} into an instance of the type
     * referenced by the given {@code returnTypeReference}.
     */
    @SuppressWarnings("unchecked")
    public <T> T convert(TypeReference<T> returnTypeReference, Object object) {
        return (T) convert(returnTypeReference.getType(), object);
    }

    // --- Default conversion functions ---

    private static class ObjectToBoolean implements ConversionFunction<Object, Boolean> {
        @Override
        public Boolean convert(Converter converter, Type returnType, Object object) {
            return Boolean.parseBoolean(object.toString());
        }
    }

    private static class ObjectToByte implements ConversionFunction<Object, Byte> {
        @Override
        public Byte convert(Converter converter, Type returnType, Object object) {
            return object instanceof Number
                    ? Byte.valueOf(((Number) object).byteValue())
                    : Byte.valueOf(object.toString().trim());
        }
    }

    private static class ObjectToCharacter implements ConversionFunction<Object, Character> {

        @Override
        public Character convert(Converter converter, Type returnType, Object object) {
            String string = object.toString();
            if (string.length() != 1) {
                throw new ConversionException(
                        "Can't convert a string with length other than 1 into a character!",
                        '\0');
            } else {
                return string.charAt(0);
            }
        }
    }

    private static class ObjectToDouble implements ConversionFunction<Object, Double> {
        @Override
        public Double convert(Converter converter, Type returnType, Object object) {
            return object instanceof Number
                    ? Double.valueOf(((Number) object).doubleValue())
                    : Double.valueOf(object.toString().trim());
        }
    }

    private static class ObjectToFloat implements ConversionFunction<Object, Float> {
        @Override
        public Float convert(Converter converter, Type returnType, Object object) {
            return object instanceof Number
                    ? Float.valueOf(((Number) object).floatValue())
                    : Float.valueOf(object.toString().trim());
        }
    }

    private static class ObjectToInteger implements ConversionFunction<Object, Integer> {
        @Override
        public Integer convert(Converter converter, Type returnType, Object object) {
            return object instanceof Number
                    ? Integer.valueOf(((Number) object).intValue())
                    : Integer.valueOf(object.toString().trim());
        }
    }

    private static class ObjectToLong implements ConversionFunction<Object, Long> {
        @Override
        public Long convert(Converter converter, Type returnType, Object object) {
            return object instanceof Number
                    ? Long.valueOf(((Number) object).longValue())
                    : Long.valueOf(object.toString().trim());
        }
    }

    private static class ObjectToShort implements ConversionFunction<Object, Short> {
        @Override
        public Short convert(Converter converter, Type returnType, Object object) {
            return object instanceof Number
                    ? Short.valueOf(((Number) object).shortValue())
                    : Short.valueOf(object.toString().trim());
        }
    }

    private static class ObjectToDate implements ConversionFunction<Object, Date> {

        private static final String[] STANDARD_FORMATS = {
                "(GMT)yyyy-MM-dd'T'HH:mm:ss'Z'",
                "(GMT)yyyy-MM-dd'T'HH:mm:ss.S'Z'",
                "EEE MMM dd HH:mm:ss z yyyy",
                "yyyy-MM-dd HH:mm:ss",
                "yyyy-MM-dd",
                "yyyy-MM-dd'T'HH:mm" };

        private static final Pattern TIME_ZONE_PATTERN = Pattern.compile("^\\(([^)]+)\\)\\s*(.+)$");

        private static final LoadingCache<String, DateTimeFormatter> FORMATTERS = CacheBuilder
                .newBuilder()
                .build(new CacheLoader<String, DateTimeFormatter>() {

            @Override
            public DateTimeFormatter load(String format) {
                Matcher timeZoneMatcher = TIME_ZONE_PATTERN.matcher(format);

                if (timeZoneMatcher.matches()) {
                    return DateTimeFormat
                            .forPattern(timeZoneMatcher.group(2))
                            .withZone(DateTimeZone.forID(timeZoneMatcher.group(1)));

                } else {
                    return DateTimeFormat.forPattern(format);
                }
            }
        });

        @Override
        public Date convert(Converter converter, Type returnType, Object object) {
            try {
                Long millis = converter.convert(Long.class, object);

                if (millis != null) {
                    return new Date(millis);
                }

            } catch (ConversionException error) {
                // Try a different conversion below.
            }

            String objectString = object.toString().trim();

            for (String format : STANDARD_FORMATS) {
                try {
                    return new Date(FORMATTERS.getUnchecked(format).parseMillis(objectString));

                } catch (IllegalArgumentException error) {
                    // Try the default Java conversion or the next format.
                }

                if (format.contains("z")) {
                    try {
                        return new SimpleDateFormat(format).parse(objectString);
                    } catch (ParseException error) {
                        // Try the next format.
                    }
                }
            }

            throw new ConversionException(String.format(
                    "Can't convert [%s] to Date instance!", objectString));
        }
    }

    private static class ObjectToDateTime implements ConversionFunction<Object, DateTime> {

        @Override
        public DateTime convert(Converter converter, Type returnType, Object object) {
            Date date = converter.convert(Date.class, object);

            if (date != null) {
                return new DateTime(date);

            } else {
                throw new ConversionException(String.format(
                        "Can't convert [%s] to DateTime instance!", object));
            }
        }
    }

    private static class ObjectToString implements ConversionFunction<Object, String> {

        @Override
        public String convert(Converter converter, Type returnType, Object object) {
            return object.toString();
        }
    }

    private static class ObjectToUuid implements ConversionFunction<Object, UUID> {

        @Override
        public UUID convert(Converter converter, Type returnType, Object object) {
            return object instanceof byte[]
                    ? UuidUtils.fromBytes((byte[]) object)
                    : UuidUtils.fromString(object.toString().trim());
        }
    }

    private static class ObjectToLocale implements ConversionFunction<Object, Locale> {

        @Override
        public Locale convert(Converter converter, Type returnType, Object object) {
            if (object instanceof Locale) {
                return (Locale) object;

            } else {
                return Locale.forLanguageTag(object.toString().trim());
            }
        }
    }

    private static class ObjectToAnyEnum implements ConversionFunction<Object, Enum<?>> {

        @Override
        public Enum<?> convert(Converter converter, Type returnType, Object object) {
            if (returnType instanceof Class) {
                @SuppressWarnings("unchecked")
                Enum<?>[] constants = ((Class<Enum<?>>) returnType).getEnumConstants();
                if (constants != null) {
                    String objectString = object.toString();
                    for (Enum<?> constant : constants) {
                        if (objectString.equalsIgnoreCase(constant.name())
                                || objectString.equalsIgnoreCase(constant.toString())) {
                            return constant;
                        }
                    }
                }
            }
            return null;
        }
    }

    private static class ObjectToAnyObject implements ConversionFunction<Object, Object> {

        @Override
        public Object convert(Converter converter, Type returnType, Object object) throws Exception {
            TypeDefinition<?> returnTypeDefinition = TypeDefinition.getInstance(returnType);
            Class<?> objectClass = object.getClass();

            Constructor<?> constructor = returnTypeDefinition.getConstructor(objectClass);
            if (constructor != null) {
                return constructor.newInstance(object);
            }

            Method method = getFactoryMethod(returnTypeDefinition, objectClass);
            if (method != null) {
                return method.invoke(null, object);
            }

            constructor = returnTypeDefinition.getConstructor(String.class);
            if (constructor != null) {
                return constructor.newInstance(object.toString());
            }

            method = getFactoryMethod(returnTypeDefinition, String.class);
            if (method != null) {
                return method.invoke(null, object.toString());
            }

            if (object instanceof Map) {
                Object converted = returnTypeDefinition.newInstance();
                for (Map.Entry<?, ?> entry : ((Map<?, ?>) object).entrySet()) {
                    Object key = entry.getKey();
                    if (key != null) {
                        Field field = returnTypeDefinition.getField(key.toString());
                        if (field != null) {
                            field.set(converted, converter.convert(field.getGenericType(), entry.getValue()));
                        }
                    }
                }
                return converted;
            }

            throw new ConversionException(String.format(
                    "Can't find a constructor or a factory method that"
                            + " takes [%s] class as a parameter to create an"
                            + " instance of [%s] type!",
                    objectClass, returnType), null);
        }

        private Method getFactoryMethod(TypeDefinition<?> typeDefinition, Class<?> parameterClass) {
            for (Method method : typeDefinition.getAllMethods()) {
                if (Modifier.isStatic(method.getModifiers())
                        && typeDefinition.getObjectClass().isAssignableFrom(method.getReturnType())) {
                    Class<?>[] parameterTypes = method.getParameterTypes();
                    if (parameterTypes.length == 1
                            && parameterTypes[0].isAssignableFrom(parameterClass)) {
                        return method;
                    }
                }
            }
            return null;
        }
    }
}
