package com.psddev.dari.util;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import javax.servlet.ReadListener;
import javax.servlet.ServletInputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;

/**
 * Enables reusing {@link javax.servlet.ServletRequest#getInputStream} or
 * {@link javax.servlet.ServletRequest#getReader}.
 *
 * <p>Note that the entire request is cached in memory, and no checks are
 * done to prevent misuse.</p>
 */
public class ReusableRequest extends HttpServletRequestWrapper {

    private byte[] requestBytes;
    private final String requestEncoding;

    /** Creates an instance that wraps the given {@code request}. */
    public ReusableRequest(HttpServletRequest request) {
        super(request);
        this.requestEncoding = request.getCharacterEncoding();
    }

    // --- HttpServletRequestWrapper support ---

    private void populateRequestBytes() throws IOException {
        if (requestBytes == null) {
            requestBytes = IoUtils.toByteArray(super.getInputStream());
        }
    }

    @Override
    public ServletInputStream getInputStream() throws IOException {
        populateRequestBytes();

        return new ServletInputStream() {

            private final InputStream stream = new ByteArrayInputStream(requestBytes);

            @Override
            public boolean isFinished() {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean isReady() {
                throw new UnsupportedOperationException();
            }

            @Override
            public void setReadListener(ReadListener readListener) {
                throw new UnsupportedOperationException();
            }

            @Override
            public int read() throws IOException {
                return stream.read();
            }
        };
    }

    @Override
    public BufferedReader getReader() throws IOException {
        populateRequestBytes();

        return new BufferedReader(
                new InputStreamReader(
                        new ByteArrayInputStream(requestBytes),
                        requestEncoding));
    }
}
