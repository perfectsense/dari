package com.psddev.dari.db;

/**
 * A Bridge allows additional interface implementation on type {@code <T>}. For
 * example:
 *
 * <p><blockquote><pre><code data-type="java">
 *     class DownloadableImage extends Bridge&lt;Image&gt; implements ImageDownloadable {
 *         {@literal @}Override
 *         public void downloadFiles(ImageDownloadOptions options, Path root) {
 *             Image image = getOriginalObject();
 *             return options.download(image, image.getFile(), root);
 *         }
 *     }
 * </pre></blockquote></p>
 *
 * <p>The above snippet suggests that the Image type is now downloadable, in
 * that the interface method can be called by utilizing the API
 * {@link State#bridge(Class)} as such:
 * {@code image.bridge(ImageDownloadable.class).downloadFiles(options, root);}</p>
 */
@SuppressWarnings("WeakerAccess")
public abstract class Bridge<T> extends Record implements Relatable<T> {
}
