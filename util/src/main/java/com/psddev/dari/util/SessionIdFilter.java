package com.psddev.dari.util;

import java.io.IOException;

import javax.servlet.FilterChain;
import javax.servlet.Servlet;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpServletResponseWrapper;
import javax.servlet.http.HttpSession;
import javax.servlet.jsp.PageContext;

/**
 * Suppresses session IDs in URLs for security and disables sessions.
 *
 * @deprecated Use {@link NoSessionFilter} instead.
 */
@Deprecated
public class SessionIdFilter extends AbstractFilter {

    // --- AbstractFilter support ---

    @Override
    protected void doRequest(
            HttpServletRequest request,
            HttpServletResponse response,
            FilterChain chain)
            throws IOException, ServletException {

        if (request.isRequestedSessionIdFromURL()) {
            HttpSession session = request.getSession();
            if (session != null) {
                session.invalidate();
            }
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

    private static class ThreadLocalSessionStrippingJspFactory extends JspFactoryWrapper {

        @Override
        public PageContext getPageContext(Servlet servlet, ServletRequest request, ServletResponse response, String errorPageUrl, boolean needsSession, int buffer, boolean autoflush) {
            needsSession = false;

            return super.getPageContext(servlet, request, response, errorPageUrl, needsSession, buffer, autoflush);
        }
    }

    private static final class StrippingResponse extends HttpServletResponseWrapper {

        public StrippingResponse(HttpServletResponse response) {
            super(response);
        }

        @Deprecated
        @Override
        public String encodeRedirectUrl(String url) {
            return url;
        }

        @Override
        public String encodeRedirectURL(String url) {
            return url;
        }

        @Deprecated
        @Override
        public String encodeUrl(String url) {
            return url;
        }

        @Override
        public String encodeURL(String url) {
            return url;
        }
}
}
