package com.psddev.dari.db;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimap;
import com.psddev.dari.util.ClassFinder;
import com.psddev.dari.util.CodeUtils;
import com.psddev.dari.util.Lazy;
import com.psddev.dari.util.TypeDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

class SubstitutionUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(SubstitutionUtils.class);

    private static final Lazy<Cache> CACHE = new Lazy<Cache>() {

        @Override
        protected Cache create() {
            return new Cache();
        }
    };

    static {
        CodeUtils.addRedefineClassesListener(classes -> CACHE.reset());
    }

    public static <T> T newInstance(Class<T> objectClass) {
        Class<?> substitutionClass = CACHE.get().substitutionClasses.get(objectClass);
        return (T) TypeDefinition.getInstance(substitutionClass != null ? substitutionClass : objectClass).newInstance();
    }

    public static String getOriginalName(String substitutionName) {
        String originalName = CACHE.get().originalNames.get(substitutionName);
        return originalName != null ? originalName : substitutionName;
    }

    public static Class<?> getOriginalClass(Class<?> substitutionClass) {
        Class<?> originalClass = CACHE.get().originalClasses.get(substitutionClass);
        return originalClass != null ? originalClass : substitutionClass;
    }

    private static class Cache {

        public final Map<String, String> originalNames;
        public final Map<Class<?>, Class<?>> originalClasses;
        public final Map<Class<?>, Class<?>> substitutionClasses;

        public Cache() {
            Multimap<Class<?>, Class<?>> substitutions = ArrayListMultimap.create();

            for (Class<?> c : ClassFinder.findConcreteClasses(Substitution.class)) {
                substitutions.put(c.getSuperclass(), c);
            }

            ImmutableMap.Builder<String, String> originalNamesBuilder = new ImmutableMap.Builder<>();
            ImmutableMap.Builder<Class<?>, Class<?>> originalClassesBuilder = new ImmutableMap.Builder<>();
            ImmutableMap.Builder<Class<?>, Class<?>> substitutionClassesBuilder = new ImmutableMap.Builder<>();

            for (Map.Entry<Class<?>, Collection<Class<?>>> entry : substitutions.asMap().entrySet()) {
                Class<?> s = entry.getKey();
                Collection<Class<?>> cs = entry.getValue();

                if (cs.size() != 1) {
                    LOGGER.warn(
                            "Can't substitute [{}] because there are multiple candidates!{}",
                            s.getName(),
                            cs.stream()
                                    .map(c -> "\n\t" + c.getName())
                                    .collect(Collectors.joining()));

                } else {
                    Class<?> c = cs.iterator().next();

                    originalNamesBuilder.put(c.getName().toLowerCase(Locale.ENGLISH), s.getName().toLowerCase(Locale.ENGLISH));
                    originalClassesBuilder.put(c, s);
                    substitutionClassesBuilder.put(s, c);
                }
            }

            originalNames = originalNamesBuilder.build();
            originalClasses = originalClassesBuilder.build();
            substitutionClasses = substitutionClassesBuilder.build();
        }
    }
}
