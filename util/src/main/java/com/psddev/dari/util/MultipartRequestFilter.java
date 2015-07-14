package com.psddev.dari.util;

import java.io.IOException;

import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.fileupload.servlet.ServletFileUpload;

/**
 * Filter that automatically enables {@link MultipartRequest}.
 */
public class MultipartRequestFilter extends AbstractFilter {

    private static final String ATTRIBUTE_PREFIX = MultipartRequestFilter.class.getName() + ".";
    private static final String INSTANCE_ATTRIBUTE = ATTRIBUTE_PREFIX + "instance";

    // --- AbstractFilter support ---

    @Override
    protected void doRequest(
            HttpServletRequest request,
            HttpServletResponse response,
            FilterChain chain)
            throws IOException, ServletException {

        if (Static.getInstance(request) == null
                && ServletFileUpload.isMultipartContent(request)) {
            request = new MultipartRequest(request);
            request.setAttribute(INSTANCE_ATTRIBUTE, request);
        }

        chain.doFilter(request, response);
    }

    /**
     * {@link MultipartRequestFilter} utility methods.
     */
    public static final class Static {

        /**
         * Returns the {@link MultipartRequest} instance associated with the
         * given {@code request}.
         *
         * @param request Can't be {@code null}.
         * @return {@code null} if the request isn't multipart.
         */
        public static MultipartRequest getInstance(HttpServletRequest request) {
            return (MultipartRequest) request.getAttribute(INSTANCE_ATTRIBUTE);
        }
    }
}
