package com.psddev.dari.util;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;

import javax.servlet.ServletOutputStream;
import javax.servlet.WriteListener;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpServletResponseWrapper;

/**
 * {@link HttpServletResponse} implementation that buffers and captures
 * the response output.
 */
public class CapturingResponse extends HttpServletResponseWrapper {

    private ByteArrayServletOutputStream outputStreamCapture;
    private StringWriter writerCapture;
    private PrintWriter writer;

    public CapturingResponse(HttpServletResponse response) {
        super(response);
    }

    @Override
    public ServletOutputStream getOutputStream() throws IOException {
        if (writerCapture != null) {
            throw new IllegalStateException();

        } else {
            if (outputStreamCapture == null) {
                outputStreamCapture = new ByteArrayServletOutputStream();
            }

            return outputStreamCapture;
        }
    }

    @Override
    public PrintWriter getWriter() {
        if (outputStreamCapture != null) {
            throw new IllegalStateException();

        } else {
            if (writerCapture == null) {
                writerCapture = new StringWriter();
                writer = new PrintWriter(writerCapture);
            }

            return writer;
        }
    }

    /**
     * Returns the output captured so far. If {@link
     * ServletResponse#getOutputStream} was used to write the output,
     * always returns an empty string.
     *
     * @return Never {@code null}.
     */
    public String getOutput() {
        return writerCapture != null ? writerCapture.toString() : "";
    }

    /**
     * Writes the output captured so far to the underlying response.
     */
    public void writeOutput() throws IOException {
        if (outputStreamCapture != null) {
            getResponse().getOutputStream().write(outputStreamCapture.delegate.toByteArray());

        } else if (writerCapture != null) {
            getResponse().getWriter().write(writerCapture.toString());
        }
    }

    private static class ByteArrayServletOutputStream extends ServletOutputStream {

        private final ByteArrayOutputStream delegate = new ByteArrayOutputStream();

        @Override
        public boolean isReady() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setWriteListener(WriteListener writeListener) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void write(int b) {
            delegate.write(b);
        }
    }
}
