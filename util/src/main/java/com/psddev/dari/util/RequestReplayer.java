package com.psddev.dari.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Map;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;

/** Stores request details for the purpose of replaying the request. Requests are restricted to HTTP GET to localhost. */
final class RequestReplayer {

    private static final String METHOD = "GET";
    private static final String EOL = "\r\n";

    private final String protocol;
    private final int port;
    private final String addr;
    private final Map<String, Collection<String>> headers;
    private final String uri;
    private final String queryString;
    private final Cookie[] cookies;

    /** Creates the RequestReplayer or null if the request is ineligible for replaying. */
    public static RequestReplayer createInstanceOrNull(HttpServletRequest request, String... extraHeaders) throws IOException {
        if (!"GET".equalsIgnoreCase(request.getMethod())  ||
                !"http".equalsIgnoreCase(request.getScheme())) {
            return null;
        }
        Map<String, Collection<String>>  headers = new CompactMap<>();
        Enumeration<String> requestHeaderNames = request.getHeaderNames();
        while (requestHeaderNames.hasMoreElements()) {
            String requestHeaderName = requestHeaderNames.nextElement();
            Collection<String> requestHeaderValues = Collections.list(request.getHeaders(requestHeaderName));
            if (!requestHeaderValues.isEmpty()) {
                headers.put(requestHeaderName, requestHeaderValues);
            }
        }
        // Add extra headers
        if (extraHeaders != null) {
            if ((extraHeaders.length % 2) != 0) {
                throw new IllegalArgumentException();
            }
            for (int i = 0; i < extraHeaders.length; i += 2) {
                String headerName = extraHeaders[i];
                Collection<String> headerValues = headers.get(headerName);
                if (headerValues == null) {
                    headerValues = new ArrayList<>();
                    headers.put(headerName, headerValues);
                }
                headerValues.add(extraHeaders[i + 1]);
            }
        }

        return new RequestReplayer(request.getProtocol(), request.getLocalPort(), request.getLocalAddr(), headers, request.getRequestURI(), request.getQueryString(), request.getCookies());
    }

    private RequestReplayer(String protocol, int port, String addr, Map<String, Collection<String>> headers, String uri, String queryString, Cookie[] cookies) {
        this.protocol = protocol;
        this.port = port;
        this.addr = addr;
        this.headers = headers;
        this.uri = uri;
        this.queryString = queryString;
        this.cookies = cookies;
    }

    /** Execute the request and copy the response to the given OutputStream. */
    public void execute(OutputStream outputStream) throws IOException {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(addr, port));
            try (PrintWriter request = new PrintWriter(socket.getOutputStream())) {
                request.print(getRequestLine() + EOL + getHeaderLines() + EOL);
                request.flush();
                InputStream response = socket.getInputStream();
                if (outputStream != null) {
                    IoUtils.copy(response, outputStream);
                }
            }
        }
    }

    private String getRequestLine() {
        StringBuilder requestLineBuilder = new StringBuilder(METHOD);
        requestLineBuilder.append(' ');
        requestLineBuilder.append(uri);
        if (queryString != null && !queryString.isEmpty()) {
            requestLineBuilder.append('?');
            requestLineBuilder.append(queryString);
        }
        requestLineBuilder.append(' ');
        requestLineBuilder.append(protocol);
        return requestLineBuilder.toString();
    }

    private String getHeaderLines() {
        StringBuilder headerLinesBuilder = new StringBuilder();
        for (Map.Entry<String, Collection<String>> headerEntry : headers.entrySet()) {
            String headerName = headerEntry.getKey();
            for (String headerValue : headerEntry.getValue()) {
                headerLinesBuilder.append(headerName);
                headerLinesBuilder.append(": ");
                headerLinesBuilder.append(headerValue);
                headerLinesBuilder.append(EOL);
            }
        }
        // Cookies
        if (cookies != null && cookies.length > 0) {
            StringBuilder cookieHeader = new StringBuilder("Cookie: ");
            for (Cookie cookie : cookies) {
                cookieHeader.append(cookie.getName());
                cookieHeader.append('=');
                cookieHeader.append(cookie.getValue());
                cookieHeader.append("; ");
            }
            cookieHeader.setLength(cookieHeader.length() - 2);
            headerLinesBuilder.append(cookieHeader);
            headerLinesBuilder.append(EOL);
        }
        // Extra headers
        headerLinesBuilder.append("Connection: close");
        headerLinesBuilder.append(EOL);
        return headerLinesBuilder.toString();
    }

    @Override
    public String toString() {
        Collection<String> cookieStrings = new ArrayList<>();
        if (cookies != null) {
            for (Cookie cookie : cookies) {
                cookieStrings.add(cookie.getName() + '=' + cookie.getValue());
            }
        }
        return "RequestReplayer{" +
                "addr='" + addr + '\'' +
                ", protocol='" + protocol + '\'' +
                ", port=" + port +
                ", headers=" + headers +
                ", uri='" + uri + '\'' +
                ", queryString='" + queryString + '\'' +
                ", cookies='" + cookieStrings + '\'' +
                '}';
    }
}
