package com.psddev.dari.util;

import java.util.Collections;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Locale;
import java.util.NoSuchElementException;
import java.util.Set;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;

public class HeaderStrippingRequest extends HttpServletRequestWrapper {

    private final Set<String> allowedHeaders;

    public HeaderStrippingRequest(HttpServletRequest request, Iterable<String> allowedHeaders) {
        super(request);
        this.allowedHeaders = new HashSet<>();
        for (String allowedHeader : allowedHeaders) {
            this.allowedHeaders.add(allowedHeader.toLowerCase(Locale.ENGLISH));
        }
    }

    private boolean isAllowed(String headerName) {
        return headerName != null && allowedHeaders.contains(headerName.toLowerCase(Locale.ENGLISH));
    }

    @Override
    public String getHeader(String name) {
        if (isAllowed(name)) {
            return super.getHeader(name);
        } else {
            return null;
        }
    }

    @Override
    public Enumeration<String> getHeaders(String name) {
        if (isAllowed(name)) {
            return super.getHeaders(name);
        } else {
            return Collections.emptyEnumeration();
        }
    }

    @Override
    public Enumeration<String> getHeaderNames() {

        Enumeration<String> headerNames = new FilteringEnumeration<String>(super.getHeaderNames()) {
            @Override
            public boolean accept(String element) {
                return isAllowed(element);
            }
        };

        return headerNames;
    }

    @Override
    public int getIntHeader(String name) {
        if (isAllowed(name)) {
            return super.getIntHeader(name);
        } else {
            return -1;
        }
    }

    @Override
    public long getDateHeader(String name) {
        if (isAllowed(name)) {
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
        return null;
    }

    @Override
    public Locale getLocale() {
        if (isAllowed("Accept-Language")) {
            return super.getLocale();
        } else {
            return Locale.getDefault();
        }
    }

    @Override
    public Enumeration<Locale> getLocales() {
        if (isAllowed("Accept-Language")) {
            return super.getLocales();
        } else {
            return Collections.enumeration(Collections.singleton(Locale.getDefault()));
        }
    }

    private abstract static class FilteringEnumeration<T> implements Enumeration<T> {

        private final Enumeration<T> delegate;
        private T next;

        public FilteringEnumeration(Enumeration<T> delegate) {
            this.delegate = delegate;
        }

        private void findNext() {
            if (next != null) {
                return;
            }
            while (delegate.hasMoreElements()) {
                T element = delegate.nextElement();
                if (accept(element)) {
                    next = element;
                    break;
                }
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
