package com.psddev.dari.util;

import java.util.HashMap;
import java.util.Map;

/**
 * For pinging an arbitrary service to ensure that it's operational.
 */
public interface Ping {

    /**
     * Pings a service using an instance of the given {@code pingClass}.
     *
     * @return {@code null} if there wasn't an error.
     */
    static Throwable pingOne(Class<? extends Ping> pingClass) {
        try {
            TypeDefinition.getInstance(pingClass).newInstance().ping();
            return null;

        } catch (Throwable error) {
            return error;
        }
    }

    /**
     * Pings all available services.
     *
     * @return Map of all services that ran along with any errors.
     */
    static Map<Class<?>, Throwable> pingAll() {
        Map<Class<?>, Throwable> errors = new HashMap<>();

        for (Class<? extends Ping> pingClass : ClassFinder.findClasses(Ping.class)) {
            errors.put(pingClass, pingOne(pingClass));
        }

        return errors;
    }

    /**
     * Implementation should be quick to return and be careful not to throw
     * any unintentional errors that would mark the service non-operational.
     */
    void ping() throws Throwable;

    /**
     * {@link Ping} utility methods.
     *
     * @deprecated Use {@link Ping} instead. Deprecated 2015-07-23.
     */
    @Deprecated
    final class Static {

        /**
         * Pings a service using an instance of the given {@code pingClass}.
         *
         * @return {@code null} if there wasn't an error.
         *
         * @deprecated Use {@link Ping#pingOne(Class)} instead.
         *             Deprecated 2015-07-23.
         */
        @Deprecated
        public static Throwable ping(Class<? extends Ping> pingClass) {
            return Ping.pingOne(pingClass);
        }

        /**
         * Pings all available services.
         *
         * @return Map of all services that ran along with any errors.
         *
         * @deprecated Use {@link Ping#pingAll()} instead.
         *             Deprecated 2015-07-23.
         */
        @Deprecated
        public static Map<Class<?>, Throwable> pingAll() {
            return Ping.pingAll();
        }
    }
}
