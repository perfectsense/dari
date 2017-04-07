package com.psddev.dari.db;

/**
 * Interface that defines a relationship between this class and the type
 * {@code <T>}.
 */
interface Relatable<T> extends Recordable {

    /** Returns the original object. */
    @SuppressWarnings("unchecked")
    default T getOriginalObject() {
        return (T) getState().getOriginalObject();
    }
}
