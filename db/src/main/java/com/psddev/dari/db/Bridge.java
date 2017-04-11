package com.psddev.dari.db;

/**
 * Bridge is used to implement and make use of additional interfaces on type
 * {@code T}. For example:
 *
 * <p><blockquote><pre><code data-type="java">
 *     class DownloadableImage extends Bridge&lt;Image&gt; implements Downloadable {
 *
 *         {@literal @}Override
 *         public void downloadFiles(DownloadOptions options, Path root) { ... }
 *     }
 * </pre></blockquote></p>
 *
 * <p>The above snippet suggests that the Image type is now downloadable, in
 * that the interface method can be called by utilizing the API
 * {@link State#bridge(Class)} as such:</p>
 *
 *  <p><blockquote><pre><code data-type="java">
 *      image.bridge(Downloadable.class).downloadFiles(options, root);
 *  </pre></blockquote></p>
 */
public abstract class Bridge<T> extends Record {

    /** Returns the original object. */
    @SuppressWarnings("unchecked")
    public final T getOriginalObject() {
        return (T) getState().getOriginalObject();
    }
}
