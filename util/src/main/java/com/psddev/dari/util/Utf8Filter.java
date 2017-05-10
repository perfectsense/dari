package com.psddev.dari.util;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;
import javax.servlet.http.HttpServletResponse;

/**
 * Forces {@link StringUtils#UTF_8} character encoding on all requests
 * and responses.
 */
public class Utf8Filter extends AbstractFilter implements AbstractFilter.Auto {

    public static final String CHECK_PARAMETER = "_u";
    public static final String CHECK_VALUE = "\u2713";

    @Override
    public void updateDependencies(Class<? extends AbstractFilter> filterClass, List<Class<? extends Filter>> dependencies) {
        if (!getClass().isAssignableFrom(filterClass)) {
            dependencies.add(0, getClass());
        }
    }

    // --- AbstractFilter support ---

    @Override
    protected void doRequest(
            HttpServletRequest request,
            HttpServletResponse response,
            FilterChain chain)
            throws IOException, ServletException {

        String encoding = StandardCharsets.UTF_8.name();

        request.setCharacterEncoding(encoding);
        response.setCharacterEncoding(encoding);

        String check = request.getParameter(CHECK_PARAMETER);

        if (check != null && !CHECK_VALUE.equals(check)) {
            request = new HttpServletRequestWrapper(request) {

                private final Map<String, String[]> reEncoded;

                {
                    Map<String, String[]> oldMap = getRequest().getParameterMap();
                    Map<String, String[]> newMap  = new CompactMap<>();

                    for (Map.Entry<String, String[]> entry : oldMap.entrySet()) {
                        String[] values = entry.getValue();
                        String[] copy = new String[values.length];

                        for (int i = 0, length = values.length; i < length; ++ i) {
                            copy[i] = reEncode(values[i]);
                        }

                        newMap.put(reEncode(entry.getKey()), copy);
                    }

                    reEncoded = Collections.unmodifiableMap(newMap);
                }

                private String reEncode(String string) {
                    return new String(string.getBytes(StandardCharsets.ISO_8859_1), StandardCharsets.UTF_8);
                }

                @Override
                public String getParameter(String name) {
                    String[] values = reEncoded.get(name);

                    return values != null && values.length > 0 ? values[0] : null;
                }

                @Override
                public Map<String, String[]> getParameterMap() {
                    return reEncoded;
                }

                @Override
                public Enumeration<String> getParameterNames() {
                    return Collections.enumeration(reEncoded.keySet());
                }

                @Override
                public String[] getParameterValues(String name) {
                    return reEncoded.get(name);
                }
            };
        }

        chain.doFilter(request, response);
    }

    // --- Deprecated ---

    /** @deprecated Use {@link StringUtils#UTF_8} instead. */
    @Deprecated
    public static final String ENCODING = StringUtils.UTF_8.name();
}
