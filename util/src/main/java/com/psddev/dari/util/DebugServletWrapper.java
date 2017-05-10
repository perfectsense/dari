package com.psddev.dari.util;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.Servlet;
import javax.servlet.ServletConfig;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

final class DebugServletWrapper implements ServletConfig {

    private static final Logger LOGGER = LoggerFactory.getLogger(DebugServletWrapper.class);

    public static final LoadingCache<ServletContext, Map<Class<? extends Servlet>, DebugServletWrapper>> INSTANCES = CacheBuilder
            .newBuilder()
            .weakKeys()

            // Make sure that the debug servlets are destroyed properly.
            .removalListener((RemovalListener<ServletContext, Map<Class<? extends Servlet>, DebugServletWrapper>>) notification -> {
                Map<Class<? extends Servlet>, DebugServletWrapper> wrappers = notification.getValue();

                if (wrappers != null) {
                    wrappers.values().forEach(DebugServletWrapper::destroy);
                }
            })

            // Find all debug servlets.
            .build(new CacheLoader<ServletContext, Map<Class<? extends Servlet>, DebugServletWrapper>>() {

                @Override
                public Map<Class<? extends Servlet>, DebugServletWrapper> load(ServletContext context) {
                    Map<Class<? extends Servlet>, DebugServletWrapper> wrappers = new HashMap<>();

                    for (Class<? extends Servlet> servletClass : ClassFinder.findConcreteClasses(Servlet.class)) {
                        try {
                            Servlet servlet = TypeDefinition.getInstance(servletClass).newInstance();
                            String name;
                            List<String> paths = new ArrayList<>();

                            if (servlet instanceof DebugServlet) {
                                DebugServlet debugServlet = (DebugServlet) servlet;
                                name = debugServlet.getName();
                                List<String> debugPaths = debugServlet.getPaths();

                                if (debugPaths != null && !debugPaths.isEmpty()) {
                                    paths.addAll(debugPaths);
                                }

                            } else {

                                // Handle legacy debug servlets.
                                @SuppressWarnings("deprecation")
                                DebugFilter.Path pathAnnotation = servletClass.getAnnotation(DebugFilter.Path.class);
                                String path = null;

                                if (pathAnnotation != null) {
                                    paths.add(pathAnnotation.value());
                                }

                                @SuppressWarnings("deprecation")
                                DebugFilter.Name nameAnnotation = servletClass.getAnnotation(DebugFilter.Name.class);
                                if (nameAnnotation != null) {
                                    paths.add(nameAnnotation.value());
                                }

                                if (paths.isEmpty()) {
                                    continue;

                                } else {
                                    name = StringUtils.toLabel(paths.get(0));
                                }
                            }

                            if (!StringUtils.isBlank(name)) {
                                wrappers.put(servletClass, new DebugServletWrapper(context, servlet, name, paths));
                            }

                        } catch (Throwable error) {
                            LOGGER.warn(
                                    String.format(
                                            "Can't load debug servlet! [%s: %s]",
                                            context.getServletContextName(),
                                            servletClass.getName()),
                                    error);
                        }
                    }

                    LOGGER.info(
                            "Found debug servlets: [{}: {}]",
                            context.getServletContextName(),
                            wrappers.keySet().stream()
                                    .map(Class::getName)
                                    .collect(Collectors.joining(", ")));

                    return wrappers;
                }
            });

    static {
        CodeUtils.addRedefineClassesListener(classes ->
                classes.stream()
                        .filter(Servlet.class::isAssignableFrom)
                        .findFirst()
                        .ifPresent(c -> INSTANCES.invalidateAll()));
    }

    private final ServletContext context;
    private final Servlet servlet;
    private final String name;
    private final List<String> paths;
    private final AtomicBoolean initialized = new AtomicBoolean();

    private DebugServletWrapper(ServletContext context, Servlet servlet, String name, List<String> paths) {
        this.context = context;
        this.servlet = servlet;
        this.name = name;

        // Change all paths to "foo/bar/" format to make them easier to compare
        // against the servlet path.
        this.paths = Collections.unmodifiableList(
                paths.stream()
                        .map(p -> StringUtils.removeStart(p, "/"))
                        .map(p -> StringUtils.ensureEnd(p, "/"))
                        .collect(Collectors.toList()));
    }

    public String getName() {
        return name;
    }

    public List<String> getPaths() {
        return paths;
    }

    public void service(HttpServletRequest request, String pathInfo, HttpServletResponse response) throws IOException, ServletException {
        if (initialized.compareAndSet(false, true)) {
            servlet.init(this);
            LOGGER.info("Initialized debug servlet [{}]", getServletName());
        }

        servlet.service(
                new HttpServletRequestWrapper(request) {

                    @Override
                    public String getPathInfo() {
                        return pathInfo;
                    }
                },

                response);
    }

    public void destroy() {
        if (initialized.compareAndSet(true, false)) {
            servlet.destroy();
            LOGGER.info("Destroyed debug servlet [{}]", getServletName());
        }
    }

    @Override
    public String getInitParameter(String name) {
        return null;
    }

    @Override
    public Enumeration<String> getInitParameterNames() {
        return Collections.enumeration(Collections.emptyList());
    }

    @Override
    public ServletContext getServletContext() {
        return context;
    }

    @Override
    public String getServletName() {
        return getServletContext().getServletContextName() + ": " + servlet.getClass().getName();
    }
}
