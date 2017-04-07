package com.psddev.dari.db;

/**
 * Interface to identify disambiguation when there is more than one candidate
 * when utilizing the API {@link State#bridge(Class)}. For example, consider
 * the following class definitions:
 *
 * <p><blockquote><pre><code data-type="java">
 *     class DownloadableImage extends Bridge&lt;Image&gt; implements ImageDownloadable { ... }
 *     class DownloadablePhoto extends Bridge&lt;Image&gt; implements ImageDownloadable { ... }
 * </pre></blockquote></p>
 *
 * <p>The result of calling the API would yield {@code null}:</p>
 *
 * <p><blockquote><pre><code data-type="java">
 *      image.bridge(ImageDownloadable.class);
 * </pre></blockquote></p>
 *
 * <p>However, an instance of this class can be defined such that the result of
 * that API call will return an instance of {@code DownloadablePhoto}:</p>
 *
 * <p><blockquote><pre><code data-type="java">
 *     class ImageBridgeDisambiguation implements BridgeDisambiguation {
 *
 *         {@literal @}Override
 *         public Class&lt;?&lt; getOriginalClass() {
 *             return Image.class;
 *         }
 *
 *         {@literal @}Override
 *         public Class&lt;?&lt; getTargetClass() {
 *             return ImageDownloadable.class;
 *         }
 *
 *         {@literal @}Override
 *         public Class&lt;? extends Bridge&lt; getPreferredBridgeClass() {
 *             return DownloadablePhoto.class;
 *         }
 *     }
 * </pre></blockquote></p>
 */
public interface BridgeDisambiguation {

    /**
     * Returns the class of the original object to be bridged.
     *
     * @return Nonnull.
     */
    Class<?> getOriginalClass();

    /**
     * Returns the target class which is used as the parameter when calling
     * {@link State#bridge(Class)};
     *
     * @return Nonnull.
     */
    Class<?> getTargetClass();

    /**
     * Returns the preferred {@link Bridge} class to use when there is
     * ambiguity.
     *
     * @return Nonnull.
     */
    Class<? extends Bridge> getPreferredBridgeClass();
}
