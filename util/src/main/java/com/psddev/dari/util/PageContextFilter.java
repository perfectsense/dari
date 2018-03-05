package com.psddev.dari.util;

import java.io.IOException;

import javax.servlet.FilterChain;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * Provides servlet page context information statically so that they can
 * be accessed from anywhere.
 */
public class PageContextFilter extends AbstractFilter {

    private static final ThreadLocalStack<HttpServletRequest> REQUEST = new ThreadLocalStack<>();
    private static final ThreadLocalStack<HttpServletResponse> RESPONSE = new ThreadLocalStack<>();
    private static final ThreadLocalStack<ServletContext> SERVLET_CONTEXT = new ThreadLocalStack<>();

    private static final String ATTRIBUTE_PREFIX = PageContextFilter.class.getName() + ".";
    private static final String RESPONSE_ATTRIBUTE = ATTRIBUTE_PREFIX + "response";
    private static final String SERVLET_CONTEXT_ATTRIBUTE = ATTRIBUTE_PREFIX + "servletContext";

    /**
     * @deprecated Use {@link Static#getRequestOrNull} instead.
     */
    @Deprecated
    public static HttpServletRequest getRequest() {
        return REQUEST.get();
    }

    /**
     * @deprecated Use {@link Static#getResponseOrNull} instead.
     */
    @Deprecated
    public static HttpServletResponse getResponse() {
        return RESPONSE.get();
    }

    // --- AbstractFilter support ---

    @Override
    protected void doRequest(
            HttpServletRequest request,
            HttpServletResponse response,
            FilterChain chain)
            throws IOException, ServletException {

        ServletContext context = getServletContext();

        try {
            REQUEST.set(request);
            RESPONSE.set(response);
            SERVLET_CONTEXT.set(context);

            request.setAttribute(RESPONSE_ATTRIBUTE, response);
            request.setAttribute(SERVLET_CONTEXT_ATTRIBUTE, context);

            chain.doFilter(request, response);

        } finally {
            REQUEST.remove();
            RESPONSE.remove();
            SERVLET_CONTEXT.remove();
        }
    }

    @Override
    protected void doError(
            HttpServletRequest request,
            HttpServletResponse response,
            FilterChain chain)
            throws IOException, ServletException {

        doRequest(request, response, chain);
    }

    /**
     * {@link PageContextFilter} utility methods.
     */
    public static final class Static {

        public static ThreadLocalStack<HttpServletRequest> getThreadRequest() {
            return REQUEST;
        }

        public static ThreadLocalStack<HttpServletResponse> getThreadResponse() {
            return RESPONSE;
        }

        public static ThreadLocalStack<ServletContext> getThreadServletContext() {
            return SERVLET_CONTEXT;
        }

        private static <T> T checkResult(T result) {
            if (result != null) {
                return result;

            } else {
                throw new IllegalStateException(String.format(
                        "[%s] not in web.xml filter chain!",
                        PageContextFilter.class));
            }
        }

        /**
         * Returns the HTTP request associated with the current thread.
         *
         * @return May be {@code null}.
         */
        public static HttpServletRequest getRequestOrNull() {
            return REQUEST.get();
        }

        /**
         * Returns the HTTP request associated with the current thread.
         *
         * @return Never {@code null}.
         * @throws IllegalStateException If no HTTP request is associated
         * with the current thread. Typically, this happens when
         * {@link PageContextFilter} isn't configured in the {@code web.xml}
         * deployment descriptor file.
         */
        public static HttpServletRequest getRequest() {
            return checkResult(getRequestOrNull());
        }

        /**
         * Returns the HTTP response associated with the current thread.
         *
         * @return May be {@code null}.
         */
        public static HttpServletResponse getResponseOrNull() {
            return RESPONSE.get();
        }

        /**
         * Returns the HTTP response associated with the current thread.
         *
         * @return Never {@code null}.
         * @throws IllegalStateException If no HTTP response is associated
         * with the current thread. Typically, this happens when
         * {@link PageContextFilter} isn't configured in the {@code web.xml}
         * deployment descriptor file.
         */
        public static HttpServletResponse getResponse() {
            return checkResult(getResponseOrNull());
        }

        /**
         * Returns the HTTP response associated with the given
         * {@code request}.
         */
        public static HttpServletResponse getResponse(HttpServletRequest request) {
            HttpServletResponse response = (HttpServletResponse) request.getAttribute(RESPONSE_ATTRIBUTE);
            return checkResult(response);
        }

        /** Returns the servlet context associated with the current thread. */
        public static ServletContext getServletContext() {
            return checkResult(SERVLET_CONTEXT.get());
        }

        /**
         * Returns the servlet context associated with the given
         * {@code request}.
         */
        public static ServletContext getServletContext(HttpServletRequest request) {
            ServletContext context = (ServletContext) request.getAttribute(SERVLET_CONTEXT_ATTRIBUTE);
            return checkResult(context);
        }
    }
}
