package com.psddev.dari.util;

/**
 * Represents a function used to {@linkplain Transformer transform} an
 * arbitrary object into another object.
 *
 * @deprecated No replacement.
 */
@Deprecated
public interface TransformationFunction<F> {

    /** Transforms the given {@code object} into another object. */
    public Object transform(F object);
}
