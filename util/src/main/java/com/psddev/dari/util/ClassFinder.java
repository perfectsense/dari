package com.psddev.dari.util;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarInputStream;
import java.util.jar.Manifest;
import java.util.regex.Pattern;

import javax.tools.JavaFileObject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

/** For finding classes that are compatible with an arbitrary class. */
public class ClassFinder {

    /**
     * Manifest attribute that should be set to {@code true} for
     * this class to look inside the JAR file.
     */
    public static final String INCLUDE_ATTRIBUTE = "Dari-ClassFinder-Include";

    private static final String CLASS_FILE_SUFFIX = JavaFileObject.Kind.CLASS.extension;
    private static final Logger LOGGER = LoggerFactory.getLogger(ClassFinder.class);

    private Set<String> classLoaderExclusions = new HashSet<String>(Arrays.asList(
            "sun.misc.Launcher$ExtClassLoader",
            "sun.misc.Launcher$AppClassLoader",
            "org.apache.catalina.loader.StandardClassLoader",
            "org.apache.jasper.servlet.JasperLoader"));

    /** Returns the set of class loader exclusions. */
    public Set<String> getClassLoaderExclusions() {
        if (classLoaderExclusions == null) {
            classLoaderExclusions = new HashSet<String>();
        }
        return classLoaderExclusions;
    }

    /** Sets the set of class loader exclusions. */
    public void setClassLoaderExclusions(Set<String> classLoaderExclusions) {
        this.classLoaderExclusions = classLoaderExclusions;
    }

    /**
     * Finds all classes that are compatible with the given
     * {@code baseClass} within the given {@code loader}.
     */
    public <T> Set<Class<? extends T>> find(ClassLoader loader, Class<T> baseClass) {
        Set<String> classNames = new HashSet<String>();
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

        Set<Class<? extends T>> classes = new HashSet<Class<? extends T>>();
        for (String className : classNames) {
            try {
                Class<?> c = Class.forName(className, false, loader);
                if (!baseClass.equals(c) && baseClass.isAssignableFrom(c)) {
                    @SuppressWarnings("unchecked")
                    Class<? extends T> tc = (Class<? extends T>) c;
                    classes.add(tc);
                }

            } catch (ClassNotFoundException error) {
                // Ignore classes that can't be found by name.

            } catch (NoClassDefFoundError error) {
                // Ignore classes that can't be somehow resolved at runtime.
            }
        }

        return classes;
    }

    /**
     * Processes the given {@code url} and adds all associated class
     * files to the given {@code classNames}.
     */
    private void processUrl(Set<String> classNames, URL url) {

        if (url.getPath().endsWith(".jar")) {
            try {
                InputStream urlInput = url.openStream();
                try {

                    @SuppressWarnings("all")
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

                } finally {
                    urlInput.close();
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

    /**
     * Processes the given {@code path} under the given {@code root} and
     * adds all associated class files to the given {@code classNames}.
     */
    private void processFile(Set<String> classNames, File root, String path) {
        File file = new File(root, path);

        if (file.isDirectory()) {
            for (File child : file.listFiles()) {
                processFile(classNames, root, path.isEmpty()
                        ? child.getName()
                        : path + File.separator + child.getName());
            }

        } else {
            if (path.endsWith(CLASS_FILE_SUFFIX)) {
                String className = path.substring(0, path.length() - CLASS_FILE_SUFFIX.length());
                className = StringUtils.replaceAll(className, Pattern.quote(File.separator), ".");
                classNames.add(className);
            }
        }
    }

    /**
     * {@link ClassFinder} utility methods.
     */
    public static final class Static {

        private static final ClassFinder INSTANCE = new ClassFinder();

        private static final LoadingCache<ClassLoader, LoadingCache<Class<?>, Set<?>>> CLASSES_BY_BASE_CLASS_BY_LOADER = CacheBuilder.newBuilder()
                .build(new CacheLoader<ClassLoader, LoadingCache<Class<?>, Set<?>>>() {
                    @Override
                    public LoadingCache<Class<?>, Set<?>> load(final ClassLoader loader) {
                        return CacheBuilder.newBuilder()
                                .build(new CacheLoader<Class<?>, Set<?>>() {
                                    @Override
                                    public Set<?> load(Class<?> baseClass) {
                                        return INSTANCE.find(loader, baseClass);
                                    }
                                });
                    }
                });

        static {
            CodeUtils.addRedefineClassesListener(new CodeUtils.RedefineClassesListener() {
                @Override
                public void redefined(Set<Class<?>> classes) {
                    CLASSES_BY_BASE_CLASS_BY_LOADER.invalidateAll();
                }
            });
        }

        /**
         * Finds all classes that are compatible with the given {@code baseClass}
         * within the given class {@code loader}.
         *
         * @param loader If {@code null}, uses the current class loader.
         * @param baseClass Can't be {@code null}.
         * @return Never {@code null}.
         */
        @SuppressWarnings("unchecked")
        public static <T> Set<Class<? extends T>> findClassesFromLoader(ClassLoader loader, Class<T> baseClass) {
            ErrorUtils.errorIfNull(baseClass, "baseClass");

            if (loader == null) {
                loader = ObjectUtils.getCurrentClassLoader();
            }

            return (Set<Class<? extends T>>) CLASSES_BY_BASE_CLASS_BY_LOADER.getUnchecked(loader).getUnchecked(baseClass);
        }

        /**
         * Finds all classes that are compatible with the given {@code baseClass}
         * within the current class loader.
         *
         * @param baseClass Can't be {@code null}.
         * @return Never {@code null}.
         */
        public static <T> Set<Class<? extends T>> findClasses(Class<T> baseClass) {
            return findClassesFromLoader(null, baseClass);
        }
    }
}
