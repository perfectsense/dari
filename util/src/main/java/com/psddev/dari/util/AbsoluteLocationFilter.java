package com.psddev.dari.util;

import javax.servlet.FilterChain;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpServletResponseWrapper;
import java.io.IOException;

public class AbsoluteLocationFilter extends AbstractFilter {

    private static final String LOCATION_HEADER = "Location";

    @Override
    protected void doRequest(HttpServletRequest request, HttpServletResponse response, FilterChain chain) throws Exception {
        response = new HttpServletResponseWrapper(response) {

            private String fixLocation(String location) {
                if (location != null && location.startsWith("/")) {
                    return (JspUtils.isSecure(request) ? "https" : "http")
                            + "://"
                            + JspUtils.getHost(request)
                            + location;

                } else {
                    return location;
                }
            }

            @Override
            public void setHeader(String name, String value) {
                super.setHeader(name, LOCATION_HEADER.equalsIgnoreCase(name) ? fixLocation(value) : value);
            }

            @Override
            public void addHeader(String name, String value) {
                super.addHeader(name, LOCATION_HEADER.equalsIgnoreCase(name) ? fixLocation(value) : value);
            }

            @Override
            public void sendRedirect(String location) throws IOException {
                super.sendRedirect(fixLocation(location));
            }
        };

        chain.doFilter(request, response);
    }
}
