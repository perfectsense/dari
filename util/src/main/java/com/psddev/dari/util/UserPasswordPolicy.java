package com.psddev.dari.util;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

/** Policy for {@link Password} creation and change. */
public interface UserPasswordPolicy extends SettingsBackedObject {

    /** Setting key for all password policy configurations. */
    public static final String SETTING_PREFIX = "dari/userPasswordPolicy";

    /**
     * Validates the given {@code password} for the given {@code user}.
     *
     * @param user Can be {@code null}.
     * @param password Can be {@code null}.
     * @throws PasswordException If anything's wrong with the given
     * {@code password}.
     */
    public void validate(Object user, String password) throws PasswordException;

    /**
     * {@link UserPasswordPolicy} utility methods.
     *
     * <p>The factory method, {@link #getInstance}, uses {@link Settings}
     * to construct instances.</p>
     */
    public static final class Static {

        private static final LoadingCache<String, UserPasswordPolicy> INSTANCES = CacheBuilder.newBuilder().build(new CacheLoader<String, UserPasswordPolicy>() {

            @Override
            public UserPasswordPolicy load(String name) {
                return Settings.newInstance(UserPasswordPolicy.class, SETTING_PREFIX + "/" + name);
            }
        });

        /**
         * Returns the password policy associated with the given
         * {@code name}.
         *
         * @param name If blank, returns {@code null}.
         * @return May be {@code null}.
         */
        public static UserPasswordPolicy getInstance(String name) {
            return ObjectUtils.isBlank(name) ? null : INSTANCES.getUnchecked(name);
        }
    }
}
