package com.psddev.dari.util;

import java.util.Collection;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;

// CHECKSTYLE:OFF
/** @deprecated No replacement. */
@Deprecated
@SuppressWarnings("all")
public class HttpServletRequestMap implements Map<String, String> {

    public static final String AUTHORIZATION_KEY = "AUTHORIZATION";
    public static final String AUTH_TYPE_KEY = "AUTH_TYPE";
    public static final String CONTENT_LENGTH_KEY = "CONTENT_LENGTH";
    public static final String CONTENT_TYPE_KEY = "CONTENT_TYPE";
    public static final String GATEWAY_INTERFACE_KEY = "GATEWAY_INTERFACE";
    public static final String HTTP_PREFIX = "HTTP_";
    public static final String PATH_INFO_KEY = "PATH_INFO";
    public static final String PATH_TRANSLATED_KEY = "PATH_TRANSLATED";
    public static final String PROXY_AUTHORIZATION_KEY = "PROXY_AUTHORIZATION";
    public static final String QUERY_STRING_KEY = "QUERY_STRING";
    public static final String REMOTE_ADDR_KEY = "REMOTE_ADDR";
    public static final String REMOTE_HOST_KEY = "REMOTE_HOST";
    public static final String REMOTE_USER_KEY = "REMOTE_USER";
    public static final String REQUEST_METHOD_KEY = "REQUEST_METHOD";
    public static final String REQUEST_URI_KEY = "REQUEST_URI";
    public static final String SCRIPT_NAME_KEY = "SCRIPT_NAME";
    public static final String SERVER_NAME_KEY = "SERVER_NAME";
    public static final String SERVER_PORT_KEY = "SERVER_PORT";
    public static final String SERVER_PROTOCOL_KEY = "SERVER_PROTOCOL";
    public static final String SERVER_SOFTWARE_KEY = "SERVER_SOFTWARE";

    private final ServletContext context;
    private final HttpServletRequest request;
    private final Map<String, String> all = new HashMap<String, String>();
    private final Map<String, Lazy> lazies = new HashMap<String, Lazy>();

    /**
     * Creates an instance based on the given {@code context} and
     * {@code request}.
     */
    public HttpServletRequestMap(ServletContext context, HttpServletRequest request) {

        this.context = context;
        this.request = request;

        all.put(AUTH_TYPE_KEY, request.getAuthType());
        all.put(CONTENT_TYPE_KEY, request.getContentType());
        all.put(GATEWAY_INTERFACE_KEY, "CGI/1.1");
        all.put(PATH_INFO_KEY, request.getPathInfo());
        all.put(QUERY_STRING_KEY, request.getQueryString());
        all.put(REMOTE_ADDR_KEY, request.getRemoteAddr());
        all.put(REMOTE_USER_KEY, request.getRemoteUser());
        all.put(REQUEST_METHOD_KEY, request.getMethod());
        all.put(REQUEST_URI_KEY, request.getRequestURI());
        all.put(SCRIPT_NAME_KEY, request.getServletPath());
        all.put(SERVER_NAME_KEY, request.getServerName());
        all.put(SERVER_PROTOCOL_KEY, request.getProtocol());
        all.put(SERVER_SOFTWARE_KEY, context.getServerInfo());

        int contentLength = request.getContentLength();
        all.put(CONTENT_LENGTH_KEY, contentLength > 0 ? String.valueOf(contentLength) : null);

        int port = request.getServerPort();
        all.put(SERVER_PORT_KEY, String.valueOf(port == 0 ? -1 : port));

        for (Enumeration<String> e = request.getHeaderNames(); e.hasMoreElements();) {
            String headerName = e.nextElement();
            String key = headerName.toUpperCase(Locale.ENGLISH).replace('-', '_');
            if (!(AUTHORIZATION_KEY.equals(key) ||
                    PROXY_AUTHORIZATION_KEY.equals(key))) {
                all.put(HTTP_PREFIX + key, request.getHeader(headerName));
            }
        }

        lazies.put(PATH_TRANSLATED_KEY, new Lazy() {

            @Override
            public Object resolve(String key) {
                return HttpServletRequestMap.this.request.getPathTranslated();
            }
        });

        lazies.put(REMOTE_HOST_KEY, new Lazy() {

            @Override
            public Object resolve(String key) {
                return HttpServletRequestMap.this.request.getRemoteHost();
            }
        });
    }

    /** Resolves all the lazy values. */
    private void resolveAll() {
        for (Iterator<Map.Entry<String, Lazy>> i = lazies.entrySet().iterator(); i.hasNext();) {
            Map.Entry<String, Lazy> e = i.next();
            i.remove();
            String key = e.getKey();
            Object value = e.getValue().resolve(key);
            all.put(key, ObjectUtils.isBlank(value) ? null : value.toString());
        }
    }

    // --- Map support ---

    @Override
    public void clear() {
        all.clear();
        lazies.clear();
    }

    @Override
    public boolean containsKey(Object key) {
        return all.containsKey(key);
    }

    @Override
    public boolean containsValue(Object value) {
        resolveAll();
        return all.containsValue(value);
    }

    @Override
    public Set<Map.Entry<String, String>> entrySet() {
        resolveAll();
        return all.entrySet();
    }

    @Override
    public String get(Object key) {
        Lazy lazy = lazies.get(key);
        if (lazy != null) {
            lazies.remove(key);
            String keyString = (String) key;
            Object value = lazy.resolve(keyString);
            all.put(keyString, ObjectUtils.isBlank(value) ? null : value.toString());
        }
        return all.get(key);
    }

    @Override
    public boolean isEmpty() {
        return all.isEmpty();
    }

    @Override
    public Set<String> keySet() {
        return all.keySet();
    }

    @Override
    public String put(String key, String value) {
        lazies.remove(key);
        return all.put(key, value);
    }

    @Override
    public void putAll(Map<? extends String, ? extends String> map) {
        lazies.keySet().removeAll(map.keySet());
        all.putAll(map);
    }

    @Override
    public String remove(Object key) {
        lazies.remove(key);
        return all.remove(key);
    }

    @Override
    public int size() {
        return all.size();
    }

    @Override
    public Collection<String> values() {
        resolveAll();
        return all.values();
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        } else if (other instanceof Map) {
            resolveAll();
            return all.equals(other);
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        resolveAll();
        return all.hashCode();
    }

    @Override
    public String toString() {
        resolveAll();
        return all.toString();
    }

    // --- Nested ---

    private interface Lazy {

        public Object resolve(String key);
    }
}
