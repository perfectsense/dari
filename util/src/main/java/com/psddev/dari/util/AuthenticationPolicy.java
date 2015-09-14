package com.psddev.dari.util;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

/** Policy for authenticating a user. */
public interface AuthenticationPolicy extends SettingsBackedObject {

    /** Setting key for all authentication policy configurations. */
    public static final String SETTING_PREFIX = "dari/authenticationPolicy";

    /**
     * Authenticates using the given {@code username} and {@code password}
     * and returns the user object.
     *
     * @param username Can be {@code null}.
     * @param password Can be {@code null}.
     * @throws AuthenticationException If anything's wrong authenticating
     * the user.
     */
    public Object authenticate(String username, String password) throws AuthenticationException;

    /**
     * {@link AuthenticationPolicy} utility methods.
     *
     * <p>The factory method, {@link #getInstance}, uses {@link Settings}
     * to construct instances.</p>
     */
    public static final class Static {

        private static final LoadingCache<String, AuthenticationPolicy> INSTANCES = CacheBuilder
                .newBuilder()
                .build(new CacheLoader<String, AuthenticationPolicy>() {

            @Override
            public AuthenticationPolicy load(String name) {
                return Settings.newInstance(AuthenticationPolicy.class, SETTING_PREFIX + "/" + name);
            }
        });

        /**
         * Returns the authentication policy associated with the given
         * {@code name}.
         *
         * @param name If blank, returns {@code null}.
         * @return May be {@code null}.
         */
        public static AuthenticationPolicy getInstance(String name) {
            return ObjectUtils.isBlank(name) ? null : INSTANCES.getUnchecked(name);
        }
    }
}
