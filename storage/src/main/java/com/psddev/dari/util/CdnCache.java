package com.psddev.dari.util;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

class CdnCache {

    private static final Pattern CSS_URL_PATTERN = Pattern.compile("(?i)url\\((['\"]?)([^)?#]*)([?#][^)]+)?\\1\\)");

    private final LoadingCache<String, LoadingCache<CdnContext, LoadingCache<String, Value>>> cache = CacheBuilder
            .newBuilder()
            .build(new CacheLoader<String, LoadingCache<CdnContext, LoadingCache<String, Value>>>() {

                @Override
                public LoadingCache<CdnContext, LoadingCache<String, Value>> load(String storage) {
                    return CacheBuilder
                            .newBuilder()
                            .weakKeys()
                            .build(new CacheLoader<CdnContext, LoadingCache<String, Value>>() {

                                @Override
                                public LoadingCache<String, Value> load(CdnContext cdnContext) {
                                    return CacheBuilder
                                            .newBuilder()
                                            .build(new CacheLoader<String, Value>() {

                                                @Override
                                                public Value load(String servletPath) throws IOException, NoSuchAlgorithmException, URISyntaxException {
                                                    return CdnCache.this.load(storage, cdnContext, servletPath);
                                                }
                                            });
                                }
                            });
                }
            });

    protected boolean customizeItem(String contentType) {
        return false;
    }

    protected String createItemPath(String pathPrefix, String extension) {
        throw new UnsupportedOperationException();
    }

    protected void updateItemMetadata(Map<String, Object> metadata, Map<String, List<String>> httpHeaders) {
        throw new UnsupportedOperationException();
    }

    protected byte[] transformItemContent(byte[] content) throws IOException {
        throw new UnsupportedOperationException();
    }

    protected Value load(String storage, CdnContext cdnContext, String servletPath) throws IOException, NoSuchAlgorithmException, URISyntaxException {
        byte[] content;

        try (InputStream input = cdnContext.open(servletPath)) {
            content = IoUtils.toByteArray(input);
        }

        // Unique hash based on the file content.
        MessageDigest md5 = MessageDigest.getInstance("MD5");
        md5.update((byte) 16);
        String hash = StringUtils.hex(md5.digest(content));

        String itemPathPrefix = "resource"
                + StringUtils.ensureSurrounding(cdnContext.getPathPrefix(), "/")
                + StringUtils.removeStart(servletPath, "/");

        int dotAt = itemPathPrefix.lastIndexOf('.');
        String itemExtension;

        if (dotAt > -1) {
            itemExtension = itemPathPrefix.substring(dotAt + 1);
            itemPathPrefix = itemPathPrefix.substring(0, dotAt) + "." + hash;

        } else {
            itemExtension = null;
            itemPathPrefix += "-" + hash;
        }

        String contentType = ObjectUtils.getContentType(servletPath);
        boolean customizeItem = customizeItem(contentType);
        String itemPath = customizeItem
                ? createItemPath(itemPathPrefix, itemExtension)
                : (itemExtension != null ? itemPathPrefix + "." + itemExtension : itemPathPrefix);

        // Look into CSS files and change all the URLs.
        if ("text/css".equals(contentType)) {
            String css = new String(content, StandardCharsets.UTF_8);
            StringBuilder newCss = new StringBuilder();
            Matcher urlMatcher = CSS_URL_PATTERN.matcher(css);
            int previousEnd = 0;

            while (urlMatcher.find()) {
                newCss.append(css.substring(previousEnd, urlMatcher.start()));

                previousEnd = urlMatcher.end();
                String childPath = urlMatcher.group(2);
                String extra = urlMatcher.group(3);

                newCss.append("url(");

                if (childPath.length() == 0) {
                    newCss.append("''");

                } else if (childPath.startsWith("data:")
                        || childPath.endsWith(".htc")) {
                    newCss.append(childPath);

                } else {
                    URI childUri = new URI(servletPath).resolve(childPath);

                    if (childUri.isAbsolute()) {
                        newCss.append(childUri);

                    } else {
                        StorageItem childItem = get(storage, cdnContext, childUri.toString());

                        // Make the new URL relative.
                        for (int slashAt = 1; (slashAt = itemPath.indexOf('/', slashAt)) > -1; ++slashAt) {
                            newCss.append("../");
                        }

                        newCss.append(childItem != null ? childItem.getPath() : childPath);
                    }

                    if (extra != null) {
                        newCss.append(extra);
                    }
                }

                newCss.append(')');
            }

            newCss.append(css.substring(previousEnd, css.length()));
            content = newCss.toString().getBytes(StandardCharsets.UTF_8);
        }

        StorageItem item = StorageItem.Static.createIn(storage);

        item.setPath(itemPath);

        if (!item.isInStorage()) {
            item.setContentType(contentType);

            // Cache "forever".
            Map<String, Object> metadata = new HashMap<>();
            Map<String, List<String>> httpHeaders = new HashMap<>();

            httpHeaders.put("Cache-Control", Collections.singletonList("public, max-age=31536000"));

            if (customizeItem) {
                updateItemMetadata(metadata, httpHeaders);
            }

            metadata.put(AbstractStorageItem.HTTP_HEADERS, httpHeaders);
            item.setMetadata(metadata);

            item.setData(new ByteArrayInputStream(
                    customizeItem
                            ? transformItemContent(content)
                            : content));

            item.save();
        }

        return new Value(item, cdnContext.getLastModified(servletPath));
    }

    public StorageItem get(String storage, CdnContext cdnContext, String servletPath) {
        if (servletPath == null) {
            return null;
        }

        try {
            if (storage == null) {
                storage = "";
            }

            LoadingCache<String, Value> valueCache = cache.getUnchecked(storage).getUnchecked(cdnContext);
            Value value = valueCache.getUnchecked(servletPath);

            if (cdnContext.getLastModified(servletPath) > value.time) {
                valueCache.invalidate(servletPath);
                value = valueCache.getUnchecked(servletPath);
            }

            return value.item;

        } catch (Exception error) {
            return null;
        }
    }

    private static class Value {

        public final StorageItem item;
        public final long time;

        public Value(StorageItem item, long time) {
            this.item = item;
            this.time = time;
        }
    }
}
