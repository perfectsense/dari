package com.psddev.dari.util;

import java.io.IOException;
import java.util.Map;

import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/** Pings all available services and outputs their statuses. */
public class PingFilter extends AbstractFilter {

    /** Default path that will trigger the ping. */
    public static final String DEFAULT_INTERCEPT_PATH = "/_ping/";

    /** Setting that can be used to change the path that triggers the ping. */
    public static final String INTERCEPT_PATH_SETTING = "dari/pingFilterInterceptPath";

    /** Message output when everything's OK. */
    public static final String OK_MESSAGE = "OK";

    /** Message output when something's wrong. */
    public static final String ERROR_MESSAGE = "ERROR";

    @Override
    protected void doRequest(
            HttpServletRequest request,
            HttpServletResponse response,
            FilterChain chain)
            throws IOException, ServletException {

        String interceptPath = StringUtils.ensureSurrounding(Settings.getOrDefault(String.class, INTERCEPT_PATH_SETTING, DEFAULT_INTERCEPT_PATH), "/");
        String path = request.getServletPath();

        if (!path.equals(interceptPath.substring(0, interceptPath.length() - 1))
                && !path.startsWith(interceptPath)) {
            chain.doFilter(request, response);
            return;
        }

        Map<Class<?>, Throwable> errors = Ping.pingAll();

        for (Throwable error : errors.values()) {
            if (error == null) {
                continue;
            }

            if (Settings.isProduction()) {
                response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
                writePlainMessage(response, ERROR_MESSAGE);
                return;

            } else {
                throw new AggregateException(errors.values());
            }
        }

        writePlainMessage(response, OK_MESSAGE);
    }

    private void writePlainMessage(HttpServletResponse response, String message) throws IOException {
        response.setContentType("text/plain");
        response.getWriter().write(message);
    }
}
