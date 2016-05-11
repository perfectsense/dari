package com.psddev.dari.util;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Modifier;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.TreeSet;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarInputStream;
import java.util.jar.Manifest;
import java.util.regex.Pattern;

import javax.annotation.ParametersAreNonnullByDefault;
import javax.servlet.ServletContext;
import javax.tools.JavaFileObject;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

/**
 * For finding sub-classes or implementations that are compatible with an
 * arbitrary class.
 */
public class ClassFinder {

    /**
     * Manifest attribute that should be set to {@code true} to indicate
     * that this class should scan the JAR file.
     */
    public static final String INCLUDE_ATTRIBUTE = "Dari-ClassFinder-Include";

    private static final String CLASS_FILE_SUFFIX = JavaFileObject.Kind.CLASS.extension;
    private static final Logger LOGGER = LoggerFactory.getLogger(ClassFinder.class);

    private static final ThreadLocalStack<ClassFinder> THREAD_DEFAULT = new ThreadLocalStack<>();
    private static final ClassFinder DEFAULT = new ClassFinder();

    private static final ThreadLocalStack<ServletContext> THREAD_DEFAULT_SERVLET_CONTEXT = new ThreadLocalStack<>();
    private static final String[] RESOURCE_PATHS = {
            "/WEB-INF/classes",
            "/WEB-INF/lib"
    };

    private static final LoadingCache<ClassFinder, LoadingCache<ClassLoader, LoadingCache<Class<?>, Set<?>>>> CLASSES_BY_BASE_CLASS_BY_LOADER_BY_FINDER = CacheBuilder.newBuilder()
            .weakKeys()
            .build(new CacheLoader<ClassFinder, LoadingCache<ClassLoader, LoadingCache<Class<?>, Set<?>>>>() {

                @Override
                @ParametersAreNonnullByDefault
                public LoadingCache<ClassLoader, LoadingCache<Class<?>, Set<?>>> load(ClassFinder finder) {
                    return CacheBuilder.newBuilder()
                            .weakKeys()
                            .build(new CacheLoader<ClassLoader, LoadingCache<Class<?>, Set<?>>>() {

                                @Override
                                @ParametersAreNonnullByDefault
                                public LoadingCache<Class<?>, Set<?>> load(final ClassLoader loader) {
                                    return CacheBuilder.newBuilder()
                                            .weakKeys()
                                            .build(new CacheLoader<Class<?>, Set<?>>() {

                                                @Override
                                                @ParametersAreNonnullByDefault
                                                public Set<?> load(Class<?> baseClass) {
                                                    return finder.find(loader, baseClass);
                                                }
                                            });
                                }
                            });
                }
            });

    static {
        CodeUtils.addRedefineClassesListener(classes -> CLASSES_BY_BASE_CLASS_BY_LOADER_BY_FINDER.invalidateAll());
    }

    private Set<String> classLoaderExclusions = new HashSet<>(Arrays.asList(
            "sun.misc.Launcher$ExtClassLoader",
            "sun.misc.Launcher$AppClassLoader",
            "org.apache.catalina.loader.StandardClassLoader",
            "org.apache.jasper.servlet.JasperLoader"));

    /**
     * Returns the thread local stack for overriding the default ServletContext.
     *
     * @return Never {@code null}.
     */
    public static ThreadLocalStack<ServletContext> getThreadDefaultServletContext() {
        return THREAD_DEFAULT_SERVLET_CONTEXT;
    }

    /**
     * Returns the thread local stack for overriding the default instance
     * used by the static methods.
     *
     * @return Never {@code null}.
     */
    public static ThreadLocalStack<ClassFinder> getThreadDefault() {
        return THREAD_DEFAULT;
    }

    /**
     * Finds all classes that are compatible with the given {@code baseClass}
     * within the given class {@code loader}.
     *
     * @param loader
     *        If {@code null}, uses the current class loader.
     * @param baseClass
     *        Can't be {@code null}.
     *
     * @return Never {@code null}.
     */
    @SuppressWarnings("unchecked")
    public static <T> Set<Class<? extends T>> findClassesFromLoader(ClassLoader loader, Class<T> baseClass) {
        Preconditions.checkNotNull(baseClass);

        if (loader == null) {
            loader = ObjectUtils.getCurrentClassLoader();
        }

        try {
            return new LinkedHashSet<>((Set<Class<? extends T>>) CLASSES_BY_BASE_CLASS_BY_LOADER_BY_FINDER
                    .getUnchecked(MoreObjects.firstNonNull(THREAD_DEFAULT.get(), DEFAULT))
                    .getUnchecked(loader)
                    .getUnchecked(baseClass));

        } catch (RuntimeException e) {
            return Collections.emptySet();
        }
    }

    /**
     * Finds all classes that are compatible with the given {@code baseClass}
     * within the current class loader.
     *
     * @param baseClass
     *        Can't be {@code null}.
     *
     * @return Never {@code null}.
     */
    public static <T> Set<Class<? extends T>> findClasses(Class<T> baseClass) {
        return findClassesFromLoader(null, baseClass);
    }

    /**
     * Finds all non-interface, non-abstract classes that are compatible with
     * the given {@code baseClass} within the current class loader.
     *
     * @param baseClass
     *        Can't be {@code null}.
     *
     * @return Never {@code null}.
     */
    public static <T> Set<Class<? extends T>> findConcreteClasses(Class<T> baseClass) {
        Set<Class<? extends T>> concreteClasses = findClasses(baseClass);

        concreteClasses.removeIf(c -> c.isInterface() || Modifier.isAbstract(c.getModifiers()));

        return concreteClasses;
    }

    /**
     * Returns the set of class loader exclusions.
     *
     * @return Never {@code null}.
     */
    public Set<String> getClassLoaderExclusions() {
        if (classLoaderExclusions == null) {
            classLoaderExclusions = new HashSet<>();
        }

        return classLoaderExclusions;
    }

    /**
     * Sets the set of class loader exclusions.
     *
     * @param classLoaderExclusions
     *        {@code null} to clear.
     */
    public void setClassLoaderExclusions(Set<String> classLoaderExclusions) {
        this.classLoaderExclusions = classLoaderExclusions;
    }

    /**
     * Finds all classes that are compatible with the given {@code baseClass}
     * within the given {@code loader}.
     *
     * @param loader
     *        Can't be {@code null}.
     *
     * @param baseClass
     *        Can't be {@code null}.
     *
     * @return Never {@code null}.
     */
    public <T> Set<Class<? extends T>> find(ClassLoader loader, Class<T> baseClass) {
        Preconditions.checkNotNull(loader);
        Preconditions.checkNotNull(baseClass);

        Set<String> classNames = new TreeSet<>();

        for (ClassLoader l = loader; l != null; l = l.getParent()) {
            if (l instanceof URLClassLoader
                    && !getClassLoaderExclusions().contains(l.getClass().getName())) {
                for (URL url : ((URLClassLoader) l).getURLs()) {
                    processUrl(classNames, url);
                }
            }
        }

        String classPath = System.getProperty("java.class.path");

        if (!ObjectUtils.isBlank(classPath)) {
            for (String path : StringUtils.split(classPath, Pattern.quote(File.pathSeparator))) {
                try {
                    processUrl(classNames, new File(path).toURI().toURL());

                } catch (MalformedURLException error) {
                    // Ignore JARs in the class path that can't be found.
                }
            }
        }

        if (classNames.isEmpty()) {
            ServletContext context = findServletContext();
            if (context != null) {
                for (String path : RESOURCE_PATHS) {
                    processResourcePath(classNames, context, path);
                }
            }

            if (classNames.isEmpty()) {
                throw new RuntimeException("No classes were found.");
            }
        }

        Set<Class<? extends T>> classes = new LinkedHashSet<>();

        for (String className : classNames) {
            try {
                Class<?> c = Class.forName(className, false, loader);

                if (!baseClass.equals(c) && baseClass.isAssignableFrom(c)) {
                    @SuppressWarnings("unchecked")
                    Class<? extends T> tc = (Class<? extends T>) c;

                    classes.add(tc);
                }

            } catch (ClassNotFoundException
                    | NoClassDefFoundError error) {

                // Ignore classes that can't be somehow resolved at runtime.
            }
        }

        return classes;
    }

    // Processes the given url and adds all associated class files to the
    // given classNames.
    private void processUrl(Set<String> classNames, URL url) {
        if (url.getPath().endsWith(".jar")) {
            try (InputStream urlInput = url.openStream()) {
                JarInputStream jarInput = new JarInputStream(urlInput);
                Manifest manifest = jarInput.getManifest();

                if (manifest != null) {
                    Attributes attributes = manifest.getMainAttributes();

                    if (attributes != null
                            && Boolean.parseBoolean(attributes.getValue(INCLUDE_ATTRIBUTE))) {

                        for (JarEntry entry; (entry = jarInput.getNextJarEntry()) != null;) {
                            String name = entry.getName();

                            if (name.endsWith(CLASS_FILE_SUFFIX)) {
                                String className = name.substring(0, name.length() - CLASS_FILE_SUFFIX.length());
                                className = className.replace('/', '.');

                                classNames.add(className);
                            }
                        }
                    }
                }

            } catch (IOException error) {
                LOGGER.debug(String.format(
                        "Can't read [%s] to scan its classes!", url),
                        error);
            }

        } else {
            File file = IoUtils.toFile(url, StandardCharsets.UTF_8);

            if (file != null && file.isDirectory()) {
                processFile(classNames, file, "");
            }
        }
    }

    // Processes the given path under the given root and adds all associated
    // class files to the given classNames.
    private void processFile(Set<String> classNames, File root, String path) {
        File file = new File(root, path);

        if (file.isDirectory()) {
            File[] children = file.listFiles();

            if (children != null) {
                for (File child : children) {
                    processFile(classNames, root, path.isEmpty()
                            ? child.getName()
                            : path + File.separator + child.getName());
                }
            }

        } else {
            if (path.endsWith(CLASS_FILE_SUFFIX)) {
                String className = path.substring(0, path.length() - CLASS_FILE_SUFFIX.length());
                className = StringUtils.replaceAll(className, Pattern.quote(File.separator), ".");

                classNames.add(className);
            }
        }
    }

    // Process a path within a given ServletContext and add all found class
    // files to the given classNames
    private void processResourcePath(Set<String> classNames, ServletContext context, String path) {
        if (path == null) {
            return;
        }
        URL url;
        try {
            url = context.getResource(path);
        } catch (MalformedURLException ignored) {
            // Ignore
            return;
        }

        processUrl(classNames, url);

        processFilename(classNames, path);

        Set<String> paths = context.getResourcePaths(path);
        if (paths != null) {
            for (String p : paths) {
                processResourcePath(classNames, context, p);
            }
        }
    }

    // Processes a String filename and add the matching class name to the given classNames.
    private void processFilename(Set<String> classNames, String filename) {
        if (filename.endsWith(CLASS_FILE_SUFFIX)) {
            for (String resourcePath : RESOURCE_PATHS) {
                int chr = filename.lastIndexOf(resourcePath);
                if (chr > -1) {
                    String className = filename.substring(chr + resourcePath.length() + 1, filename.length() - CLASS_FILE_SUFFIX.length());
                    classNames.add(className.replace('/', '.'));
                    break;
                }
            }
        }
    }

    /**
     * @return Can be {@code null}.
     */
    private static ServletContext findServletContext() {
        ServletContext context = THREAD_DEFAULT_SERVLET_CONTEXT.get();
        if (context == null) {
            try {
                context = PageContextFilter.Static.getServletContext();
            } catch (IllegalStateException ignored) {
                // ignored
            }
        }
        return context;
    }

    /**
     * {@link ClassFinder} utility methods.
     *
     * @deprecated Use {@link ClassFinder} instead. Deprecated 2015-07-23.
     */
    @Deprecated
    public static final class Static {

        /**
         * Finds all classes that are compatible with the given {@code baseClass}
         * within the given class {@code loader}.
         *
         * @param loader
         *        If {@code null}, uses the current class loader.
         * @param baseClass
         *        Can't be {@code null}.
         *
         * @return Never {@code null}.
         *
         * @deprecated Use {@link ClassFinder#findClassesFromLoader(ClassLoader, Class)}
         *             instead. Deprecated 2015-07-23.
         */
        @Deprecated
        public static <T> Set<Class<? extends T>> findClassesFromLoader(ClassLoader loader, Class<T> baseClass) {
            return ClassFinder.findClassesFromLoader(loader, baseClass);
        }

        /**
         * Finds all classes that are compatible with the given {@code baseClass}
         * within the current class loader.
         *
         * @param baseClass
         *        Can't be {@code null}.
         *
         * @return Never {@code null}.
         *
         * @deprecated Use {@link ClassFinder#findClasses(Class)} instead.
         *             Deprecated 2015-07-23.
         */
        @Deprecated
        public static <T> Set<Class<? extends T>> findClasses(Class<T> baseClass) {
            return ClassFinder.findClasses(baseClass);
        }

        /**
         * Finds all non-interface, non-abstract classes that are compatible with
         * the given {@code baseClass} within the current class loader.
         *
         * @param baseClass
         *        Can't be {@code null}.
         *
         * @return Never {@code null}.
         *
         * @deprecated Use {@link ClassFinder#findConcreteClasses(Class)}
         *             instead. Deprecated 2015-07-23.
         */
        @Deprecated
        public static <T> Set<Class<? extends T>> findConcreteClasses(Class<T> baseClass) {
            return ClassFinder.findConcreteClasses(baseClass);
        }
    }
}
