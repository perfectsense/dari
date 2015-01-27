package com.psddev.dari.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;

public class HeaderStrippingRequest extends HttpServletRequestWrapper {

    private static final String ACCEPT_LANGUAGE_HEADER_NAME = "Accept-Language";
    private final Set<String> allowedHeaders;
    private final Map<String, Collection<String>> extraHeaders;
    private final Cookie[] cookies;

    public HeaderStrippingRequest(HttpServletRequest request, Iterable<String> allowedHeaders, Map<String, Collection<String>> extraHeaders, Cookie[] cookies) {
        super(request);
        this.allowedHeaders = new HashSet<>();
        for (String allowedHeader : allowedHeaders) {
            this.allowedHeaders.add(allowedHeader.toLowerCase(Locale.ENGLISH));
        }
        this.extraHeaders = new CompactMap<>();
        for (Map.Entry<String, Collection<String>> extraHeader : extraHeaders.entrySet()) {
            Collection<String> headerValues = extraHeader.getValue();
            if (headerValues != null && !headerValues.isEmpty()) {
                this.extraHeaders.put(extraHeader.getKey().toLowerCase(Locale.ENGLISH), new ArrayList<>(headerValues));
            }
        }
        this.cookies = new Cookie[cookies != null ? cookies.length : 0];
        int j = 0;
        for (int i = 0; i < (cookies != null ? cookies.length : 0); i++) {
            if (cookies[i] != null) {
                this.cookies[j++] = (Cookie) cookies[i].clone();
            }
        }
    }

    private boolean isAllowed(String headerName) {
        return headerName != null && allowedHeaders.contains(headerName.toLowerCase(Locale.ENGLISH));
    }

    @Override
    public String getHeader(String name) {
        String lowerName = name.toLowerCase(Locale.ENGLISH);
        if (extraHeaders.containsKey(lowerName)) {
            Collection<String> headerValues = extraHeaders.get(lowerName);
            return !headerValues.isEmpty() ? headerValues.iterator().next() : null;
        } else if (isAllowed(name)) {
            return super.getHeader(name);
        } else {
            return null;
        }
    }

    @Override
    public Enumeration<String> getHeaders(String name) {
        String lowerName = name.toLowerCase(Locale.ENGLISH);
        if (extraHeaders.containsKey(lowerName)) {
            return Collections.enumeration(extraHeaders.get(lowerName));
        } else if (isAllowed(name)) {
            return super.getHeaders(name);
        } else {
            return Collections.emptyEnumeration();
        }
    }

    @Override
    public Enumeration<String> getHeaderNames() {

        final Set<String> extraHeaderNames = extraHeaders.keySet();

        Enumeration<String> headerNames = new FilteringEnumeration<String>(super.getHeaderNames(), Collections.enumeration(extraHeaderNames)) {
            @Override
            public boolean accept(String element) {
                return !extraHeaderNames.contains(element) && isAllowed(element);
            }
        };

        return headerNames;
    }

    @Override
    public int getIntHeader(String name) {
        String lowerName = name.toLowerCase(Locale.ENGLISH);
        if (extraHeaders.containsKey(lowerName)) {
            return Integer.valueOf(getHeader(name));
        } else if (isAllowed(name)) {
            return super.getIntHeader(name);
        } else {
            return -1;
        }
    }

    @Override
    public long getDateHeader(String name) {
        String lowerName = name.toLowerCase(Locale.ENGLISH);
        if (extraHeaders.containsKey(lowerName)) {
            return Long.valueOf(getHeader(name));
        } else if (isAllowed(name)) {
            return super.getDateHeader(name);
        } else {
            return -1;
        }
    }

    @Override
    public String getRemoteUser() {
        return null;
    }

    @Override
    public String getAuthType() {
        return null;
    }

    @Override
    public Cookie[] getCookies() {
        return cookies;
    }

    @Override
    public Locale getLocale() {
        if (isAllowed(ACCEPT_LANGUAGE_HEADER_NAME)) {
            return super.getLocale();
        } else {
            return Locale.getDefault();
        }
    }

    @Override
    public Enumeration<Locale> getLocales() {
        if (isAllowed(ACCEPT_LANGUAGE_HEADER_NAME)) {
            return super.getLocales();
        } else {
            return Collections.enumeration(Collections.singleton(Locale.getDefault()));
        }
    }

    private abstract static class FilteringEnumeration<T> implements Enumeration<T> {

        private final Enumeration<T> delegate;
        private final Enumeration<T> extras;
        private T next;

        public FilteringEnumeration(Enumeration<T> delegate) {
            this(delegate, Collections.emptyEnumeration());
        }

        public FilteringEnumeration(Enumeration<T> delegate, Enumeration<T> extras) {
            this.delegate = delegate;
            this.extras = extras;
        }

        private void findNext() {
            if (next != null) {
                return;
            }
            while (delegate.hasMoreElements()) {
                T element = delegate.nextElement();
                if (accept(element)) {
                    next = element;
                    return;
                }
            }
            if (extras.hasMoreElements()) {
                next = extras.nextElement();
            }
        }

        @Override
        public boolean hasMoreElements() {
            findNext();
            return next != null;
        }

        @Override
        public T nextElement() {
            findNext();
            if (next == null) {
                throw new NoSuchElementException();
            }
            T result = next;
            next = null;
            return result;
        }

        public abstract boolean accept(T element);

    }
}
