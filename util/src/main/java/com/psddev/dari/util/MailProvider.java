package com.psddev.dari.util;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

/** Interface for sending a {@link MailMessage} **/
public interface MailProvider extends SettingsBackedObject {

    /** Setting key for default mail provider name. */
    public static final String DEFAULT_MAIL_SETTING = "dari/defaultMailProvider";

    /** Setting key prefix for all mail provider configuration. */
    public static final String SETTING_PREFIX = "dari/mailProvider";

    /**
     * Sends mail given a {@code MailMessage}.
     */
    public void send(MailMessage message);

    /**
     * {@link MailProvider} utility methods.
     *
     * <p>The factory method, {@link #getInstance}, uses {@link Settings}
     * to construct instances.</p>
     */
    public static final class Static {

        private static final LoadingCache<String, MailProvider> INSTANCES = CacheBuilder
                .newBuilder()
                .build(new CacheLoader<String, MailProvider>() {

            @Override
            public MailProvider load(String name) {
                return Settings.newInstance(MailProvider.class, SETTING_PREFIX + "/" + name);
            }
        });

        /**
         * Returns the MailProvider instance associated with the given
         * {@code name}.
         *
         * @param name If blank, returns {@code null}.
         * @return May be {@code null}.
         */
        public static MailProvider getInstance(String name) {
            if (ObjectUtils.isBlank(name)) {
                name = Settings.get(String.class, DEFAULT_MAIL_SETTING);

                if (ObjectUtils.isBlank(name)) {
                    throw new IllegalStateException(
                        "No default mail provider setting found!");
                }
            }

            return INSTANCES.getUnchecked(name);
        }

        /**
         * Returns the default configured mail provider.
         */
        public static MailProvider getDefault() {
            return getInstance(null);
        }
    }
}
