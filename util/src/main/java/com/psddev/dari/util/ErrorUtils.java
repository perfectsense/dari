package com.psddev.dari.util;

/**
 * Error utility methods.
 *
 * @deprecated Use {@link com.google.common.base.Preconditions} or
 *             {@link com.google.common.base.Throwables} instead.
 */
@Deprecated
public final class ErrorUtils {

    /**
     * Throws an {@link IllegalArgumentException} if the given
     * {@code condition} is {@code true}.
     */
    public static void errorIf(boolean condition, String parameterName, String message) {
        if (condition) {
            throw new IllegalArgumentException("[" + parameterName + "] " + message);
        }
    }

    /**
     * Throws an {@link IllegalArgumentException} if the given {@code object}
     * is {@code null}.
     */
    public static void errorIfNull(Object object, String parameterName) {
        errorIf(object == null, parameterName, "is required!");
    }

    /**
     * Throws an {@link IllegalArgumentException} if the given {@code object}
     * is {@link ObjectUtils#isBlank blank}.
     */
    public static void errorIfBlank(Object object, String parameterName) {
        errorIf(ObjectUtils.isBlank(object), parameterName, "can't be blank!");
    }

    /**
     * Rethrows the given {@code error} if it's an instance of the given
     * {@code errorClass}.
     */
    @SuppressWarnings("unchecked")
    public static <T extends Throwable> void rethrowIf(Throwable error, Class<T> errorClass) throws T {
        if (errorClass.isInstance(error)) {
            throw (T) error;
        }
    }

    /**
     * Rethrows the given {@code error} if it's an instance of
     * {@link RuntimeException} or {@link Error}, or if it's not, wraps
     * it first in {@link RuntimeException} and rethrows.
     */
    public static void rethrow(Throwable error) {
        if (error instanceof RuntimeException) {
            throw (RuntimeException) error;

        } else if (error instanceof Error) {
            throw (Error) error;

        } else {
            throw new RuntimeException(error);
        }
    }
}
