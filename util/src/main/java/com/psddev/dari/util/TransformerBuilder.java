package com.psddev.dari.util;

import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;

/**
 * For building common types of transformers.
 *
 * <p>Typically, most objects will be converted to a map using a
 * {@linkplain Converter converter}, with the include, exclude, rename,
 * and transform methods used to control the transformation.
 *
 * <p>For example, given the following 2 classes:
 *
 * <p><blockquote><pre>
 * class Foo {
 * &nbsp;   String field1 = "foo1";
 * &nbsp;   String field2 = "foo2";
 * &nbsp;   transient String ignored = "foo3";
 * }
 *
 * class Bar extends Foo {
 * &nbsp;   public String getExtra() {
 * &nbsp;       return "bar";
 * &nbsp;   }
 * }
 * </pre><blockquote>
 *
 * <p>The following would return a map that contains
 * {@code {field1=foo1,field2=foo2,extra=bar}}:
 *
 * <p><blockquote><pre>
 * Object transformed = new TransformerBuilder()
 * &nbsp;   .putAllFields(Foo.class)
 * &nbsp;   .putGetters(Bar.class, "extra")
 * &nbsp;   .toTransformer()
 * &nbsp;   .transform(new Bar());
 * </pre></blockquote>
 *
 * @deprecated No replacement.
 */
@Deprecated
public class TransformerBuilder {

    private Type mapType;
    private final Map<Class<?>, TransformationFunction<?>> functions = new HashMap<Class<?>, TransformationFunction<?>>();
    private final ObjectToAnyMap toMap = new ObjectToAnyMap();

    /** Returns the type used to create the map when transforming objects. */
    public Type getMapType() {
        if (mapType == null) {
            mapType = Map.class;
        }
        return mapType;
    }

    /** Sets the type used to create the map when transforming objects. */
    public TransformerBuilder setMapType(Type mapType) {
        this.mapType = mapType;
        return this;
    }

    /** Sets the type used to create the map when transforming objects. */
    @SuppressWarnings("rawtypes")
    public TransformerBuilder setMapType(Class<? extends Map> mapType) {
        return setMapType((Type) mapType);
    }

    /** Sets the type used to create the map when transforming objects. */
    @SuppressWarnings("rawtypes")
    public TransformerBuilder setMapType(TypeReference<? extends Map> mapType) {
        return setMapType(mapType.getType());
    }

    /**
     * Includes the return value from the given {@code function} into the
     * given {@code name} when transforming instances of the given
     * {@code objectClass} to a map.
     */
    public <F> TransformerBuilder includeFunction(Class<F> objectClass, String name, TransformationFunction<F> function) {
        functions.put(objectClass, null);
        toMap.includeFunction(objectClass, name, function);
        return this;
    }

    /**
     * Includes the given {@code constant} into the given {@code name}
     * when transforming instances of the given {@code objectClass}
     * to a map.
     */
    public TransformerBuilder includeConstant(Class<?> objectClass, String name, Object constant) {
        functions.put(objectClass, null);
        toMap.includeConstant(objectClass, name, constant);
        return this;
    }

    /**
     * Includes all the non-transient field values when transforming
     * instances of the given {@code objectClass} to a map.
     */
    public TransformerBuilder includeAllFields(Class<?> objectClass) {
        functions.put(objectClass, null);
        toMap.includeAllFields(objectClass);
        return this;
    }

    /**
     * Includes the field values with the given {@code names} when
     * transforming instances of the given {@code objectClass} to a map.
     */
    public TransformerBuilder includeFields(Class<?> objectClass, String... names) {
        functions.put(objectClass, null);
        toMap.includeFields(objectClass, names);
        return this;
    }

    /**
     * Includes all the public getter values when transforming instances
     * of the given {@code objectClass} to a map.
     */
    public TransformerBuilder includeAllGetters(Class<?> objectClass) {
        functions.put(objectClass, null);
        toMap.includeAllGetters(objectClass);
        return this;
    }

    /**
     * Includes the getter values with the given {@code names} when
     * transforming instances of the given {@code objectClass} to a map.
     */
    public TransformerBuilder includeGetters(Class<?> objectClass, String... names) {
        functions.put(objectClass, null);
        toMap.includeGetters(objectClass, names);
        return this;
    }

    /**
     * Excludes all the values associated with the given {@code names}
     * when transforming instances of the given {@code objectClass} to a map.
     */
    public TransformerBuilder exclude(Class<?> objectClass, String... names) {
        functions.put(objectClass, null);
        toMap.exclude(objectClass, names);
        return this;
    }

    /**
     * Excludes all the non-transient field values when transforming instances
     * of the given {@code objectClass} to a map.
     */
    public TransformerBuilder excludeAllFields(Class<?> objectClass) {
        functions.put(objectClass, null);
        toMap.excludeAllFields(objectClass);
        return this;
    }

    /**
     * Excludes all the public getter values when transforming instances of the
     * given {@code objectClass} to a map.
     */
    public TransformerBuilder excludeAllGetters(Class<?> objectClass) {
        functions.put(objectClass, null);
        toMap.excludeAllGetters(objectClass);
        return this;
    }

    /**
     * Renames the value associated with the given {@code oldName} to the
     * given {@code newName} when transforming instances of the given
     * {@code objectClass} to a map.
     */
    public TransformerBuilder rename(Class<?> objectClass, String oldName, String newName) {
        functions.put(objectClass, null);
        toMap.rename(objectClass, oldName, newName);
        return this;
    }

    /**
     * Transforms the instances of the given {@code objectClass} using the
     * given {@code function}.
     */
    public <F> TransformerBuilder transform(Class<F> objectClass, TransformationFunction<F> function) {
        functions.put(objectClass, function);
        return this;
    }

    /** Builds the transformer. */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public Transformer toTransformer() {

        Converter converter = new Converter();
        converter.putAllStandardFunctions();
        converter.putInheritableFunction(Object.class, Map.class, toMap);

        Transformer transformer = new Transformer();
        TransformationFunction mapFunction = new MapFunction(converter, getMapType());
        for (Map.Entry<Class<?>, TransformationFunction<?>> e : functions.entrySet()) {
            TransformationFunction function = e.getValue();
            transformer.putFunction((Class) e.getKey(), function != null ? function : mapFunction);
        }
        return transformer;
    }

    // --- Nested ---

    private static class MapFunction implements TransformationFunction<Object> {

        private final Converter converter;
        private final Type mapType;

        public MapFunction(Converter converter, Type mapType) {
            this.converter = converter;
            this.mapType = mapType;
        }

        @Override
        public Object transform(Object object) {
            return converter.convert(mapType, object);
        }
    }
}
