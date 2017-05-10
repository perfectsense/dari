package com.psddev.dari.util;

import java.io.IOException;
import java.io.InputStream;

/**
 * {@code CdnContext} allows for custom logic around how {@link Cdn} behaves.
 *
 * <p>Note that it's important to implement proper {@link #equals} and
 * {@link #hashCode} since the instances are used as cache keys.</p>
 */
public interface CdnContext {

    /**
     * Returns the last modified time of the file at given {@code servletPath}.
     *
     * @param servletPath Nonnull.
     */
    long getLastModified(String servletPath) throws IOException;

    /**
     * Opens the file at given {@code servletPath}.
     *
     * @param servletPath Nonnull.
     * @return Nonnull.
     */
    InputStream open(String servletPath) throws IOException;

    /**
     * Returns the path prefix that should be used in storing the file at the
     * CDN.
     *
     * @return Nonnull.
     */
    String getPathPrefix();
}
