package com.psddev.dari.util;

import java.lang.reflect.Type;

/**
 * For converting an arbitrary object into an instance of another type.
 *
 * @param <F>
 *        Type of the object to be converted.
 *
 * @param <T>
 *        Type of the object to convert into.
 */
@FunctionalInterface
public interface ConversionFunction<F, T> {

    /**
     * Converts the given {@code object} into an instance of the given
     * {@code returnType}.
     *
     * @param converter
     *        For when this method needs to convert other values. Can't
     *        be {@code null}.
     *
     * @param returnType
     *        Can't be {@code null}.
     *
     * @param object
     *        Can't be {@code null}.
     *
     * @throws NullPointerException
     *         If the given {@code returnType} or {@code object} is
     *         {@code null}.
     */
    T convert(Converter converter, Type returnType, F object) throws Exception;
}
