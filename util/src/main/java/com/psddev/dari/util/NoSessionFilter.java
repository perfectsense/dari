package com.psddev.dari.util;

import java.io.IOException;
import java.util.stream.Stream;

import javax.servlet.FilterChain;
import javax.servlet.Servlet;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpServletResponseWrapper;
import javax.servlet.http.HttpSession;
import javax.servlet.jsp.PageContext;

/**
 * Filter that "disables" sessions by invalidating them in various ways as
 * soon as they're created.
 *
 * <p>Session data should be persisted in a database instead.</p>
 */
public class NoSessionFilter extends AbstractFilter {

    @Override
    protected void doRequest(
            HttpServletRequest request,
            HttpServletResponse response,
            FilterChain chain)
            throws IOException, ServletException {

        // Invalidate any existing session.
        HttpSession session = request.getSession(false);

        if (session != null) {
            session.invalidate();
        }

        // Remove the JSESSIONID cookie if somehow set already.
        Cookie[] cookies = request.getCookies();

        if (cookies != null) {
            Stream.of(cookies)
                    .filter(c -> "JSESSIONID".equals(c.getName()))
                    .findFirst()
                    .ifPresent(c -> {
                        c.setMaxAge(0);
                        c.setPath("/");
                        c.setValue("");
                        response.addCookie(c);
                    });
        }

        // Make sure header response is wrapped as well.
        ServletResponse headerResponse = JspUtils.getHeaderResponse(request, response);

        if (headerResponse instanceof HttpServletResponse) {
            JspUtils.setHeaderResponse(request, new StrippingResponse((HttpServletResponse) headerResponse));
        }

        chain.doFilter(request, new StrippingResponse(response));
    }

    @Override
    protected void doInit() {
        JspUtils.wrapDefaultJspFactory(ThreadLocalSessionStrippingJspFactory.class);
    }

    @Override
    protected void doDestroy() {
        JspUtils.unwrapDefaultJspFactory(ThreadLocalSessionStrippingJspFactory.class);
    }

    // Don't let JSESSIONID be added to any URLs.
    private static final class StrippingResponse extends HttpServletResponseWrapper {

        public StrippingResponse(HttpServletResponse response) {
            super(response);
        }

        @Deprecated
        @Override
        @SuppressWarnings("deprecation")
        public String encodeRedirectUrl(String url) {
            return url;
        }

        @Override
        public String encodeRedirectURL(String url) {
            return url;
        }

        @Deprecated
        @Override
        @SuppressWarnings("deprecation")
        public String encodeUrl(String url) {
            return url;
        }

        @Override
        public String encodeURL(String url) {
            return url;
        }
    }

    // Don't let JSPs create sessions by overriding <%@ page session %>
    // directive.
    private static class ThreadLocalSessionStrippingJspFactory extends JspFactoryWrapper {

        @Override
        public PageContext getPageContext(Servlet servlet, ServletRequest request, ServletResponse response, String errorPageUrl, boolean needsSession, int buffer, boolean autoFlush) {
            return super.getPageContext(servlet, request, response, errorPageUrl, false, buffer, autoFlush);
        }
    }
}
