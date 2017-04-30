package com.psddev.dari.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;

/**
 *  Inspect the request and update the request with a normalized version of the headers used to construct this response.
 *
 *  For example, the normalized headers might contain Mobile=false or A-B-Group=A or Theme=black or Country=US.
 *
 *  These values are suitable to construct a cache key, so requests with different normalized headers can be cached separately.
 *
 *  If any CacheableResponse returns shouldCacheResponse = false, the response will not be cached.
 *
 */
public interface CacheableResponse {

    default void normalizeRequest(NormalizingRequest request) { }

    default boolean shouldCacheResponse(HttpServletRequest request) {
        return true;
    }

    /** Implementations marked with Global will be found and executed automatically. */
    interface Global extends CacheableResponse { }

    class NormalizingRequest extends HttpServletRequestWrapper {

        private static final String ACCESSED_HEADER_NAMES_ATTRIBUTE_NAME = "requestNormalizer.accessedHeaderNames";
        private static final String NORMALIZED_HEADERS_ATTRIBUTE_NAME = "requestNormalizer.normalizedHeaders";
        private static final String NORMALIZED_COOKIES_ATTRIBUTE_NAME = "requestNormalizer.normalizedCookies";
        private static final String COOKIE_HEADER_NAME = "Cookie";
        private static final String ACCEPT_LANGUAGE_HEADER_NAME = "Accept-Language";
        private static final String ACCEPT_CHARSET_HEADER_NAME = "Accept-Charset";

        private final Collection<String> accessedHeaderNames = new HashSet<>();
        private final Map<String, Collection<String>> normalizedHeaders = new CompactMap<>();
        private final Collection<Cookie> normalizedCookies = new ArrayList<>();

        public NormalizingRequest(HttpServletRequest request) {
            super(request);
            request.setAttribute(ACCESSED_HEADER_NAMES_ATTRIBUTE_NAME, accessedHeaderNames);
            request.setAttribute(NORMALIZED_HEADERS_ATTRIBUTE_NAME, normalizedHeaders);
            request.setAttribute(NORMALIZED_COOKIES_ATTRIBUTE_NAME, normalizedCookies);
        }

        public void setNormalizedHeaders(String name, Collection<String> values) {
            if (name == null) {
                throw new NullPointerException();
            }
            normalizedHeaders.put(name, new ArrayList<>(values));
        }

        public void setNormalizedHeaders(String name, Enumeration<String> values) {
            if (name == null) {
                throw new NullPointerException();
            }
            if (values != null) {
                Collection<String> valuesCollection = new ArrayList<>();
                while (values.hasMoreElements()) {
                    valuesCollection.add(values.nextElement());
                }
                normalizedHeaders.put(name, valuesCollection);
            }
        }

        public void setNormalizedHeader(String name, String value) {
            if (name == null) {
                throw new NullPointerException();
            }
            Collection<String> headerValues = new ArrayList<>();
            headerValues.add(value);
            normalizedHeaders.put(name, headerValues);
        }

        public void addNormalizedHeader(String name, String value) {
            if (name == null) {
                throw new NullPointerException();
            }
            if (!normalizedHeaders.containsKey(name)) {
                normalizedHeaders.put(name, new ArrayList<>());
            }
            Collection<String> headerValues = normalizedHeaders.get(name);
            if (!headerValues.contains(value)) {
                headerValues.add(value);
            }
        }

        public void addNormalizedCookie(Cookie cookie) {
            if (cookie == null) {
                throw new NullPointerException();
            }
            normalizedCookies.add(cookie);
        }

        public String getNormalizedHeader(String name) {
            Collection<String> headers = getNormalizedHeaders(name);
            return headers != null && !headers.isEmpty() ? headers.iterator().next() : null;
        }

        public Collection<String> getNormalizedHeaders(String name) {
            return normalizedHeaders.get(name);
        }

        public Cookie[] getNormalizedCookies() {
            return normalizedCookies.toArray(new Cookie[normalizedCookies.size()]);
        }

        private void accessHeader(String headerName) {
            accessedHeaderNames.add(headerName);
        }

        @Override
        public String getHeader(String name) {
            accessHeader(name);
            return super.getHeader(name);
        }

        @Override
        public Enumeration<String> getHeaders(String name) {
            accessHeader(name);
            return super.getHeaders(name);
        }

        @Override
        public int getIntHeader(String name) {
            accessHeader(name);
            return super.getIntHeader(name);
        }

        @Override
        public long getDateHeader(String name) {
            accessHeader(name);
            return super.getDateHeader(name);
        }

        @Override
        public Cookie[] getCookies() {
            accessHeader(COOKIE_HEADER_NAME);
            return super.getCookies();
        }

        @Override
        public Locale getLocale() {
            accessHeader(ACCEPT_LANGUAGE_HEADER_NAME);
            return super.getLocale();
        }

        @Override
        public Enumeration<Locale> getLocales() {
            accessHeader(ACCEPT_LANGUAGE_HEADER_NAME);
            return super.getLocales();
        }

        @Override
        public String getCharacterEncoding() {
            accessHeader(ACCEPT_CHARSET_HEADER_NAME);
            return super.getCharacterEncoding();
        }
    }

    final class Static {

        private Static() { }

        public static Map<String, Collection<String>> getNormalizedHeaders(HttpServletRequest request) {
            @SuppressWarnings("unchecked")
            Map<String, Collection<String>> result = (Map<String, Collection<String>>) request.getAttribute(NormalizingRequest.NORMALIZED_HEADERS_ATTRIBUTE_NAME);
            if (result == null) {
                result = Collections.emptyMap();
            }
            return result;
        }

        public static Collection<String> getAccessedHeaderNames(HttpServletRequest request) {
            @SuppressWarnings("unchecked")
            Collection<String> result = (Collection<String>) request.getAttribute(NormalizingRequest.ACCESSED_HEADER_NAMES_ATTRIBUTE_NAME);
            if (result == null) {
                result = Collections.emptySet();
            }
            return result;
        }

        public static Cookie[] getNormalizedCookies(HttpServletRequest request) {
            Object r = request.getAttribute(NormalizingRequest.NORMALIZED_COOKIES_ATTRIBUTE_NAME);
            @SuppressWarnings("unchecked")
            Collection<Cookie> result = (Collection<Cookie>) request.getAttribute(NormalizingRequest.NORMALIZED_COOKIES_ATTRIBUTE_NAME);
            if (result == null) {
                result = new ArrayList<>();
            }
            return result.toArray(new Cookie[result.size()]);
        }

        public static boolean accessedCookies(HttpServletRequest request) {
            return getAccessedHeaderNames(request).contains(NormalizingRequest.COOKIE_HEADER_NAME);
        }

        public static boolean hasExecuted(HttpServletRequest request) {
            return request.getAttribute(NormalizingRequest.NORMALIZED_HEADERS_ATTRIBUTE_NAME) != null && request.getAttribute(NormalizingRequest.ACCESSED_HEADER_NAMES_ATTRIBUTE_NAME) != null;
        }

        public static Collection<CacheableResponse> findGlobalInstances() {
            Collection<CacheableResponse> requestNormalizers = new ArrayList<CacheableResponse>();
            for (Class<? extends CacheableResponse> requestNormalizerClass : ClassFinder.Static.findClasses(CacheableResponse.Global.class)) {
                requestNormalizers.add(TypeDefinition.getInstance(requestNormalizerClass).newInstance());
            }
            return requestNormalizers;
        }
    }
}
