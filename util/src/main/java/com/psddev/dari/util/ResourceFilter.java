package com.psddev.dari.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import javax.servlet.FilterChain;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class ResourceFilter extends AbstractFilter {

    public static final String DEFAULT_EXTERNAL_PATH_PREFIX = "/_resource/";
    public static final String DEFAULT_INTERNAL_PATH_PREFIX = "_resource/";
    public static final String EXTERNAL_PATH_PREFIX_SETTING = "dari/resourceExternalPathPrefix";
    public static final String INTERNAL_PATH_PREFIX_SETTING = "dari/resourceInternalPathPrefix";

    // --- AbstractFilter support ---

    @Override
    protected void doRequest(
            HttpServletRequest request,
            HttpServletResponse response,
            FilterChain chain)
            throws IOException, ServletException {

        String externalPath = request.getServletPath();
        String externalPathPrefix = Settings.getOrDefault(
                String.class,
                EXTERNAL_PATH_PREFIX_SETTING,
                DEFAULT_EXTERNAL_PATH_PREFIX);

        if (externalPath.startsWith(externalPathPrefix)) {
            String internalPath = Settings.getOrDefault(
                    String.class,
                    INTERNAL_PATH_PREFIX_SETTING,
                    DEFAULT_INTERNAL_PATH_PREFIX)
                    + externalPath.substring(externalPathPrefix.length());

            ServletContext context = getServletContext();
            InputStream resourceStream = context.getResourceAsStream("/WEB-INF/" + internalPath);
            if (resourceStream == null) {
                resourceStream = context.getResourceAsStream("/WEB-INF/classes/" + internalPath);
                if (resourceStream == null) {
                    resourceStream = ObjectUtils.getCurrentClassLoader().getResourceAsStream(internalPath);
                }
            }

            if (resourceStream != null) {
                try {
                    OutputStream outputStream = response.getOutputStream();
                    response.setContentType(ObjectUtils.getContentType(internalPath));
                    IoUtils.copy(resourceStream, outputStream);
                    return;

                } finally {
                    resourceStream.close();
                }
            }
        }

        chain.doFilter(request, response);
    }
}
