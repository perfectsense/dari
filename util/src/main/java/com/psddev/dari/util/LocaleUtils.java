package com.psddev.dari.util;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Locale;

import com.google.common.base.Throwables;

/**
 * Use {@link Locale} instead.
 */
@Deprecated
public class LocaleUtils {

    private static final Method FOR_LANGUAGE_TAG_METHOD = getLocaleMethod("forLanguageTag", String.class);
    private static final Method TO_LANGUAGE_TAG_METHOD = getLocaleMethod("toLanguageTag");

    private static Method getLocaleMethod(String name, Class<?>... parameterTypes) {
        try {
            return Locale.class.getMethod(name, parameterTypes);

        } catch (NoSuchMethodException error) {
            return null;
        }
    }

    /**
     * @deprecated Use {@link Locale#forLanguageTag(String)} instead.
     */
    @Deprecated
    public static Locale forLanguageTag(String languageTag) {
        if (FOR_LANGUAGE_TAG_METHOD != null) {
            try {
                return (Locale) FOR_LANGUAGE_TAG_METHOD.invoke(null, languageTag);

            } catch (IllegalAccessException error) {
                throw new IllegalStateException(error);

            } catch (InvocationTargetException error) {
                throw Throwables.propagate(error.getCause());
            }

        } else {
            int dashAt = languageTag.indexOf('-');

            if (dashAt > -1) {
                return new Locale(languageTag.substring(0, dashAt), languageTag.substring(dashAt + 1));

            } else {
                return new Locale(languageTag);
            }
        }
    }

    /**
     * @deprecated Use {@link Locale#toLanguageTag()} instead.
     */
    @Deprecated
    public static String toLanguageTag(Locale locale) {
        if (TO_LANGUAGE_TAG_METHOD != null) {
            try {
                return (String) TO_LANGUAGE_TAG_METHOD.invoke(locale);

            } catch (IllegalAccessException error) {
                throw new IllegalStateException(error);

            } catch (InvocationTargetException error) {
                throw Throwables.propagate(error.getCause());
            }

        } else {
            if (!ObjectUtils.isBlank(locale.getVariant())) {
                throw new IllegalArgumentException();
            }

            return locale.getLanguage() + "-" + locale.getCountry();
        }
    }
}
