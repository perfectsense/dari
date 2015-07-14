package com.psddev.dari.util;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

/** For sending SMS messages. */
public interface SmsProvider extends SettingsBackedObject {

    /** Setting key for default SMS provider name. */
    public static final String DEFAULT_SMS_PROVIDER_SETTING = "dari/defaultSmsProvider";

    /** Setting key prefix for all SMS provider configuration. */
    public static final String SETTING_PREFIX = "dari/smsProvider";

    /**
     * Sends the given SMS {@code message} from the given {@code fromNumber}
     * to the given {@code toNumber}.
     *
     * @param fromNumber Can't be blank.
     * @param toNumber Can't be blank.
     * @param message Can't be blank.
     */
    public void send(String fromNumber, String toNumber, String message);

    /**
     * {@link SmsProvider} utility methods.
     *
     * <p>The factory method, {@link #getInstance}, uses {@link Settings}
     * to construct instances.</p>
     */
    public static final class Static {

        private static final LoadingCache<String, SmsProvider> INSTANCES = CacheBuilder.newBuilder()
                .build(new CacheLoader<String, SmsProvider>() {

                    @Override
                    public SmsProvider load(String name) {
                        return Settings.newInstance(SmsProvider.class, SETTING_PREFIX + "/" + name);
                    }
                });

        /**
         * Returns the SMS provider associated with the given {@code name}.
         *
         * @param name If blank, returns the default SMS provider.
         * @return Never {@code null}, but throws an exception if the SMS
         * provider associated with the given {@code name} can't be found or
         * isn't configured.
         */
        public static SmsProvider getInstance(String name) {
            if (ObjectUtils.isBlank(name)) {
                name = Settings.get(String.class, DEFAULT_SMS_PROVIDER_SETTING);

                if (ObjectUtils.isBlank(name)) {
                    throw new IllegalStateException(
                            "No default SMS provider setting found!");
                }
            }

            return INSTANCES.getUnchecked(name);
        }

        /**
         * Returns the default SMS provider.
         *
         * @return Never {@code null}, but throws an exception if the default
         * SMS provider can't be found or isn't configured.
         */
        public static SmsProvider getDefault() {
            return getInstance(null);
        }
    }
}
