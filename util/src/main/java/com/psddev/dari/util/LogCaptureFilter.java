package com.psddev.dari.util;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.Enumeration;
import java.util.List;
import java.util.Locale;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * Captures log messages and writes them to the response based on request
 * parameters.
 */
public class LogCaptureFilter extends AbstractFilter {

    /**
     * Setting key for prefix to parameters that control which logs to
     * capture.
     */
    public static final String PARAMETER_PREFIX_SETTING = "dari/logCaptureFilterParameterPrefix";

    /** Default prefix to parameters that control which logs to capture. */
    public static final String DEFAULT_PARAMETER_PREFIX = "_log.";

    // --- AbstractFilter support ---

    @Override
    @SuppressWarnings("unchecked")
    protected void doRequest(
            HttpServletRequest request,
            HttpServletResponse response,
            FilterChain chain)
            throws IOException, ServletException {

        if (!Settings.isProduction()) {
            chain.doFilter(request, response);
            return;
        }

        String parameterPrefix = Settings.getOrDefault(String.class, PARAMETER_PREFIX_SETTING, DEFAULT_PARAMETER_PREFIX);
        LogCapture capture = null;

        for (Enumeration<String> e = request.getParameterNames(); e.hasMoreElements();) {
            String name = e.nextElement();

            if (name.startsWith(parameterPrefix)) {
                if (capture == null) {
                    capture = new LogCapture();
                }
                capture.putLogger(
                        Logger.getLogger(name.substring(parameterPrefix.length())),
                        Level.parse(request.getParameter(name)));
            }
        }

        if (capture == null) {
            chain.doFilter(request, response);
            return;
        }

        try {
            capture.start();

            chain.doFilter(request, response);

        } finally {
            List<String> logs = capture.stop();
            String contentType = response.getContentType();
            PrintWriter writer = null;

            try {
                writer = response.getWriter();
            } catch (IllegalStateException ex) {
                writer = new PrintWriter(new OutputStreamWriter(
                        response.getOutputStream(),
                        response.getCharacterEncoding()));
            }

            if (contentType != null) {
                contentType = contentType.toLowerCase(Locale.ENGLISH);

                if (contentType.contains("json")
                        || contentType.contains("javascript")) {
                    writer.println("/*");
                    for (String log : logs) {
                        writer.println(log);
                    }
                    writer.println("*/");
                    return;

                } else if (contentType.contains("html")) {
                    writer.println("<pre>");
                    for (String log : logs) {
                        writer.println(StringUtils.escapeHtml(log));
                    }
                    writer.println("</pre>");
                    return;
                }
            }

            for (String log : logs) {
                writer.println(log);
            }
        }
    }
}
