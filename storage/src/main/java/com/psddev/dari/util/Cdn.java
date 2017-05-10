package com.psddev.dari.util;

import com.google.common.base.Preconditions;

import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.net.URL;
import java.net.URLConnection;

/**
 * {@code Cdn} manages automatic synchronization of files between the local
 * environment and a content delivery network.
 */
public class Cdn {

    private static final String ATTRIBUTE_PREFIX = Cdn.class.getName() + ".";
    private static final String CONTEXT_ATTRIBUTE = ATTRIBUTE_PREFIX + "context";

    static final CdnCache PLAIN_CACHE = new CdnCache();
    static final CdnCache GZIPPED_CACHE = new GzipCdnCache();

    /**
     * Returns the CDN context associated with the given {@code request}.
     *
     * @param request Nonnull.
     * @return Nonnull.
     */
    public static CdnContext getContext(HttpServletRequest request) {
        Preconditions.checkNotNull(request);
        return (CdnContext) request.getAttribute(CONTEXT_ATTRIBUTE);
    }

    /**
     * Associates the given CDN {@code context} to the given {@code request}.
     *
     * @param request Nonnull.
     * @param context Nullable.
     */
    public static void setContext(HttpServletRequest request, CdnContext context) {
        Preconditions.checkNotNull(request);
        request.setAttribute(CONTEXT_ATTRIBUTE, context);
    }

    /**
     * Returns the CDN URL that's equivalent to the file at the given
     * {@code servletPath}.
     *
     * @param request Nonnull.
     * @param servletPath Nonnull.
     * @return Nonnull.
     */
    public static String getUrl(HttpServletRequest request, String servletPath) {
        Preconditions.checkNotNull(request);
        Preconditions.checkNotNull(servletPath);

        HtmlGrid.Static.addStyleSheet(request, servletPath);

        // Upload to the default storage?
        if (ObjectUtils.firstNonNull(
                Settings.get(Boolean.class, "cms/isResourceInStorage"),
                Settings.isProduction())) {

            CdnContext context = getContext(request);

            if (context == null) {
                context = new ServletCdnContext(request.getServletContext());
            }

            // Workaround for lack of gzip support in CloudFront.
            String encodings = request.getHeader("Accept-Encoding");
            StorageItem item = StringUtils.isBlank(encodings) || !encodings.contains("gzip")
                    ? PLAIN_CACHE.get(null, context, servletPath)
                    : GZIPPED_CACHE.get(null, context, servletPath);

            if (item != null) {
                return JspUtils.isSecure(request)
                        ? item.getSecurePublicUrl()
                        : item.getPublicUrl();
            }
        }

        // Serve the file as is with timestamp query parameter to bust the
        // browser cache.
        try {
            URL resource = CodeUtils.getResource(request.getServletContext(), servletPath);

            if (resource != null) {
                URLConnection resourceConnection = resource.openConnection();

                try {
                    String contextPath = request.getContextPath();

                    return StringUtils.addQueryParameters(
                            contextPath + StringUtils.ensureStart(servletPath, "/"),
                            "_", resourceConnection.getLastModified());

                } finally {
                    resourceConnection.getInputStream().close();
                }
            }

        } catch (IOException error) {
            // Ignore any errors and just return the path as is.
        }

        return servletPath;
    }
}
