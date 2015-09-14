package com.psddev.dari.util;

import java.lang.reflect.Type;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import javax.annotation.ParametersAreNonnullByDefault;

/**
 * Converts an object into a constant.
 */
public class ObjectToConstant<T> implements ConversionFunction<Object, T> {

    private static final ObjectToConstant<Object> NULL_INSTANCE = new ObjectToConstant<>(null);

    private static final LoadingCache<Object, ObjectToConstant<?>> INSTANCES = CacheBuilder.newBuilder()
            .weakKeys()
            .build(new CacheLoader<Object, ObjectToConstant<?>>() {

                @Override
                @ParametersAreNonnullByDefault
                public ObjectToConstant<?> load(Object constant) {
                    return new ObjectToConstant<>(constant);
                }
            });

    private final T constant;

    /**
     * Returns an instance that will convert any object into the given
     * {@code constant}.
     *
     * @param constant
     *        May be {@code null}.
     *
     * @return Never {@code null}.
     */
    @SuppressWarnings("unchecked")
    public static <T> ObjectToConstant<T> getInstance(T constant) {
        return (ObjectToConstant<T>) (constant == null ? NULL_INSTANCE : INSTANCES.getUnchecked(constant));
    }

    /**
     * Creates a instance that will convert any object into the given
     * {@code constant}.
     *
     * @param constant
     *        May be {@code null}.
     */
    protected ObjectToConstant(T constant) {
        this.constant = constant;
    }

    @Override
    public T convert(Converter converter, Type returnType, Object object) {
        return constant;
    }
}
