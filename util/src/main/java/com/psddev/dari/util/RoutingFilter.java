package com.psddev.dari.util;

import java.io.IOException;
import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.servlet.FilterChain;
import javax.servlet.Servlet;
import javax.servlet.ServletConfig;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;
import javax.servlet.http.HttpServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Automatically routes page requests to a appropriate servlet. */
public class RoutingFilter extends AbstractFilter {

    public static final String APPLICATION_PATH_SETTING_PREFIX = "dari/routingFilter/applicationPath";

    private static final Logger LOGGER = LoggerFactory.getLogger(RoutingFilter.class);

    private List<ServletWrapper> servletWrappers;

    @Override
    protected void doInit() {
        servletWrappers = new ArrayList<ServletWrapper>();

        Set<Class<? extends Servlet>> servletClasses = new HashSet<>();
        ServletContext context = getServletContext();

        if (context != null) {
            ClassFinder.getThreadDefaultServletContext().with(context, () -> {
                servletClasses.addAll(ClassFinder.findClasses(Servlet.class));
            });

        } else {
            servletClasses.addAll(ClassFinder.findClasses(Servlet.class));
        }

        for (Class<? extends Servlet> servletClass : servletClasses) {
            try {
                if (Modifier.isAbstract(servletClass.getModifiers())) {
                    continue;
                }

                Path pathAnnotation = servletClass.getAnnotation(Path.class);

                if (pathAnnotation == null) {
                    continue;
                }

                servletWrappers.add(new ServletWrapper(pathAnnotation, servletClass));

            } catch (Throwable ex) {
                LOGGER.warn(String.format(
                        "Can't load servlet [%s]!",
                        servletClass.getName()), ex);
            }
        }

        Collections.sort(servletWrappers, PATH_LENGTH_COMPARATOR);
    }

    private static final Comparator<ServletWrapper> PATH_LENGTH_COMPARATOR = new Comparator<ServletWrapper>() {
        @Override
        public int compare(ServletWrapper x, ServletWrapper y) {
            return y.getPath().length() - x.getPath().length();
        }
    };

    @Override
    protected void doDestroy() {
        for (ServletWrapper wrapper : servletWrappers) {
            wrapper.destroy();
        }

        servletWrappers = null;
    }

    @Override
    protected void doRequest(
            HttpServletRequest request,
            HttpServletResponse response,
            FilterChain chain)
            throws IOException, ServletException {

        String path = request.getServletPath();

        for (ServletWrapper servletWrapper : servletWrappers) {
            String pathInfo = StringUtils.getPathInfo(path, servletWrapper.getPath());

            if (pathInfo != null) {
                servletWrapper.service(new PathInfoRequest(request, pathInfo), response);
                return;
            }
        }

        chain.doFilter(request, response);
    }

    /**
     * Specifies that the target servlet should be available at the given
     * path {@code value}.
     */
    @Documented
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.TYPE)
    public @interface Path {
        String application() default "";
        String value();
    }

    /** {@link RoutingFilter} utility methods. */
    public static final class Static {

        public static String getApplicationPath(String application) {
            if (application == null || application.length() == 0) {
                return "";

            } else {
                application = StringUtils.removeEnd(StringUtils.ensureStart(application, "/"), "/");
                application = Settings.getOrDefault(String.class, APPLICATION_PATH_SETTING_PREFIX + application, application);
                application = StringUtils.removeEnd(StringUtils.ensureStart(application, "/"), "/");

                return application;
            }
        }
    }

    private class ServletWrapper implements ServletConfig {

        private final String application;
        private final String path;
        private final Servlet servlet;
        private final AtomicBoolean initialized = new AtomicBoolean();

        private String applicationPath(String application) {
            return StringUtils.removeEnd(StringUtils.ensureStart(application, "/"), "/");
        }

        public ServletWrapper(Path pathAnnotation, Class<? extends Servlet> servletClass) {
            this.application = applicationPath(pathAnnotation.application());
            this.path = StringUtils.ensureStart(pathAnnotation.value(), "/");
            this.servlet = TypeDefinition.getInstance(servletClass).newInstance();
        }

        public String getPath() {
            return applicationPath(Settings.getOrDefault(String.class, APPLICATION_PATH_SETTING_PREFIX + application, application)) + path;
        }

        public void service(
                HttpServletRequest request,
                HttpServletResponse response)
                throws IOException, ServletException {

            if (initialized.compareAndSet(false, true)) {
                servlet.init(this);
                LOGGER.debug("Initialized [{}] servlet", getServletName());
            }
            servlet.service(request, response);
        }

        public void destroy() {
            if (initialized.compareAndSet(true, false)) {
                servlet.destroy();
                LOGGER.debug("Destroyed [{}] servlet", getServletName());
            }
        }

        // --- ServletConfig support ---

        @Override
        public String getInitParameter(String name) {
            return null;
        }

        @Override
        public Enumeration<String> getInitParameterNames() {
            return Collections.enumeration(Collections.<String>emptyList());
        }

        @Override
        public ServletContext getServletContext() {
            return RoutingFilter.this.getServletContext();
        }

        @Override
        public String getServletName() {
            return getFilterConfig().getFilterName() + "$" + servlet.getClass().getName();
        }
    }

    private static class PathInfoRequest extends HttpServletRequestWrapper {

        private final String pathInfo;

        public PathInfoRequest(HttpServletRequest request, String pathInfo) {
            super(request);
            this.pathInfo = pathInfo;
        }

        @Override
        public String getPathInfo() {
            return pathInfo;
        }
    }
}
