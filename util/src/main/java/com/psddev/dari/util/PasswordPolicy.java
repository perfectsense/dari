package com.psddev.dari.util;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

/**
 * Policy for {@link Password} creation.
 * @deprecated Use {@link UserPasswordPolicy} instead.
 */
@Deprecated
public interface PasswordPolicy extends SettingsBackedObject {

    /** Setting key for all password policy configurations. */
    public static final String SETTING_PREFIX = "dari/passwordPolicy";

    /**
     * Validates the given {@code password}.
     *
     * @param password Can be {@code null}.
     * @throws PasswordException If anything's wrong with the given
     * {@code password}.
     */
    public void validate(String password) throws PasswordException;

    /**
     * {@link PasswordPolicy} utility methods.
     *
     * <p>The factory method, {@link #getInstance}, uses {@link Settings}
     * to construct instances.</p>
     */
    public static final class Static {

        private static final LoadingCache<String, PasswordPolicy> INSTANCES = CacheBuilder
                .newBuilder()
                .build(new CacheLoader<String, PasswordPolicy>() {

            @Override
            public PasswordPolicy load(String name) {
                return Settings.newInstance(PasswordPolicy.class, SETTING_PREFIX + "/" + name);
            }
        });

        /**
         * Returns the password policy associated with the given
         * {@code name}.
         *
         * @param name If blank, returns {@code null}.
         * @return May be {@code null}.
         */
        public static PasswordPolicy getInstance(String name) {
            return ObjectUtils.isBlank(name) ? null : INSTANCES.getUnchecked(name);
        }
    }
}
