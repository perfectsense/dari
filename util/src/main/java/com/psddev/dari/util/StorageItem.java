package com.psddev.dari.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPOutputStream;

import javax.servlet.ServletContext;

/**
 * Item in a storage system. Typically, this is used to reference a file
 * in a remote service such as Amazon S3.
 */
public interface StorageItem extends SettingsBackedObject {

    /** Setting key for default storage name. */
    public static final String DEFAULT_STORAGE_SETTING = "dari/defaultStorage";

    /** Setting key for all storage configuration. */
    public static final String SETTING_PREFIX = "dari/storage";

    /** Returns the storage name. */
    public String getStorage();

    /** Sets the storage name. */
    public void setStorage(String storage);

    /**
     * Returns the path that can uniquely identify this item within
     * the storage system.
     */
    public String getPath();

    /**
     * Sets the path that can uniquely identify this item within
     * the storage system.
     */
    public void setPath(String path);

    /** Returns the content type. */
    public String getContentType();

    /** Sets the content type. */
    public void setContentType(String contentType);

    /** Returns the collection of metadata. */
    public Map<String, Object> getMetadata();

    /** Sets the collection of metadata. */
    public void setMetadata(Map<String, Object> metadata);

    /** Returns the data stream. */
    public InputStream getData() throws IOException;

    /** Sets the data stream. */
    public void setData(InputStream data);

    /**
     * Returns the URL object for accessing this item externally.
     *
     * @deprecated Use {@link #getPublicUrl} instead.
     */
    @Deprecated
    public URL getUrl();

    /** Returns the URL for accessing this item publicly. */
    public String getPublicUrl();

    /** Returns the URL for accessing this item securely and publicly. */
    public String getSecurePublicUrl();

    /** Saves this item in a storage system. */
    public void save() throws IOException;

    /** Returns {@code true} if this item in storage. */
    public boolean isInStorage();

    /**
     * {@linkplain StorageItem Storage item} utility methods.
     *
     * <p>The factory methods, {@link #create} and {@link #createUrl}, use
     * {@linkplain Settings settings} to construct instances.
     */
    public static final class Static {

        /** Creates an item in the given {@code storage}. */
        public static StorageItem createIn(String storage) {
            if (UrlStorageItem.DEFAULT_STORAGE.equals(storage)) {
                return new UrlStorageItem();

            } else {
                if (ObjectUtils.isBlank(storage)) {
                    storage = Settings.get(String.class, DEFAULT_STORAGE_SETTING);
                }

                StorageItem item = Settings.newInstance(StorageItem.class, SETTING_PREFIX + "/" + storage);
                item.setStorage(storage);

                if (item instanceof AbstractStorageItem) {
                    AbstractStorageItem base = (AbstractStorageItem) item;
                    base.registerListener(new ImageResizeStorageItemListener());
                }

                return item;
            }
        }

        /** Creates an item in the default storage. */
        public static StorageItem create() {
            return createIn(null);
        }

        /** Creates a one-off storage item backed by the given {@code url}. */
        public static UrlStorageItem createUrl(String url) {
            UrlStorageItem item = new UrlStorageItem();
            item.setPath(url);
            return item;
        }

        /** Returns an unmodifiable set of all storage names. */
        @SuppressWarnings("unchecked")
        public static Set<String> getStorages() {
            Object storageSettings = Settings.get(SETTING_PREFIX);
            if (storageSettings instanceof Map) {
                return ((Map<String, Object>) storageSettings).keySet();
            } else {
                return Collections.emptySet();
            }
        }

        /**
         * Copies the given {@code item} into the given {@code newStorage}
         * system and returns the newly created item.
         */
        public static StorageItem copy(StorageItem item, String newStorage) throws IOException {
            InputStream data = null;

            try {
                data = item.getData();
                StorageItem newItem = createIn(newStorage);
                newItem.setPath(item.getPath());
                newItem.setContentType(item.getContentType());
                newItem.setData(data);
                newItem.save();
                return newItem;

            } finally {
                if (data != null) {
                    data.close();
                }
            }
        }

        // --- Metadata ---

        /**
         * Adds the metadata with the given {@code key} and {@code value}
         * to the given {@code item}.
         */
        public static void addMetadata(StorageItem item, String key, String value) {
            item.getMetadata().put(key, value);
        }

        /**
         * Removes the metadata with the given {@code key} and {@code value}
         * from the given {@code item}.
         */
        public static void removeMetadata(StorageItem item, String key, String value) {
            Map<String, Object> metadata = item.getMetadata();
            if (metadata.containsKey(key) && value.equals(metadata.get(key))) {
                metadata.remove(key);
            }
        }

        /**
         * Removes all metadata associated with the given {@code key}
         * from the given {@code item}.
         */
        public static void removeAllMetadata(StorageItem item, String key) {
            item.getMetadata().remove(key);
        }

        public static void resetListeners(StorageItem item) {
            if (item instanceof AbstractStorageItem) {
                AbstractStorageItem base = (AbstractStorageItem) item;
                base.resetListeners();
            }
        }

        // --- Resource ---

        private static final String CACHE_CONTROL_KEY = "Cache-Control";
        private static final String CACHE_CONTROL_VALUE = "public, max-age=31536000";
        private static final Pattern CSS_URL_PATTERN = Pattern.compile("(?i)url\\((['\"]?)([^)?#]*)([?#][^)]+)?\\1\\)");

        // Map of plain resources by storage, servlet context, and servlet path.
        private static final ResourceCache PLAIN_RESOURCES = new ResourceCache();

        // Map of gzipped resources by storage, servlet context, and servlet path.
        private static final ResourceCache GZIPPED_RESOURCES = new ResourceCache() {

            @Override
            protected String createPath(String contentType, String pathPrefix, String extension) {
                if (!contentType.startsWith("text/")) {
                    return super.createPath(contentType, pathPrefix, extension);

                } else {
                    return extension != null ? pathPrefix + ".gz." + extension : pathPrefix + "-gz";
                }
            }

            @Override
            protected void saveItem(String contentType, StorageItem item, byte[] source) throws IOException {
                if (!contentType.startsWith("text/")) {
                    super.saveItem(contentType, item, source);

                } else {
                    ByteArrayOutputStream byteOutput = new ByteArrayOutputStream();
                    GZIPOutputStream gzipOutput = new GZIPOutputStream(byteOutput);
                    try {
                        gzipOutput.write(source);
                    } finally {
                        gzipOutput.close();
                    }

                    item.setContentType(contentType);
                    Map<String, Object> metaDataMap = new HashMap<String, Object>();
                    Map<String, List<String>> httpHeaderMap = new HashMap<String, List<String>>();
                    httpHeaderMap.put(CACHE_CONTROL_KEY, Arrays.asList(CACHE_CONTROL_VALUE));
                    httpHeaderMap.put("Content-Encoding", Arrays.asList("gzip"));
                    metaDataMap.put(AbstractStorageItem.HTTP_HEADERS, httpHeaderMap);
                    item.setMetadata(metaDataMap);

                    item.setData(new ByteArrayInputStream(byteOutput.toByteArray()));
                    item.save();
                }
            }
        };

        // Actual implementation of the *_RESOURCES maps.
        private static class ResourceCache extends PullThroughCache<String, Map<ServletContext, Map<String, StorageItem>>> {

            protected String createPath(String contentType, String pathPrefix, String extension) {
                return extension != null ? pathPrefix + "." + extension : pathPrefix;
            }

            protected void saveItem(String contentType, StorageItem item, byte[] source) throws IOException {
                item.setContentType(contentType);

                Map<String, Object> metaDataMap = new HashMap<String, Object>();
                Map<String, List<String>> httpHeaderMap = new HashMap<String, List<String>>();
                httpHeaderMap.put(CACHE_CONTROL_KEY, Arrays.asList(CACHE_CONTROL_VALUE));
                metaDataMap.put(AbstractStorageItem.HTTP_HEADERS, httpHeaderMap);
                item.setMetadata(metaDataMap);

                item.setData(new ByteArrayInputStream(source));
                item.save();
            }

            @Override
            protected Map<ServletContext, Map<String, StorageItem>> produce(final String storage) {
                return new PullThroughCache<ServletContext, Map<String, StorageItem>>() {

                    @Override
                    protected Map<String, StorageItem> produce(final ServletContext servletContext) {
                        return new PullThroughCache<String, StorageItem>() {

                            @Override
                            protected boolean isExpired(String servletPath, Date lastProduceDate) {
                                String filePath = servletContext.getRealPath(servletPath);
                                if (filePath != null) {
                                    long lastModified = new File(filePath).lastModified();
                                    if (lastModified > 0) {
                                        return lastModified > lastProduceDate.getTime();
                                    }
                                }
                                return false;
                            }

                            @Override
                            protected StorageItem produce(String servletPath) throws IOException, NoSuchAlgorithmException, URISyntaxException {
                                InputStream sourceInput = servletContext.getResourceAsStream(servletPath);
                                if (sourceInput == null) {
                                    return null;
                                }

                                byte[] source;
                                try {
                                    source = IoUtils.toByteArray(sourceInput);
                                } finally {
                                    sourceInput.close();
                                }

                                // path -> resource/context/path
                                String contentType = ObjectUtils.getContentType(servletPath);
                                String pathPrefix = "resource" + servletContext.getContextPath() + servletPath;
                                String path;

                                MessageDigest md5 = MessageDigest.getInstance("MD5");
                                md5.update((byte) 16);
                                String hash = StringUtils.hex(md5.digest(source));

                                // name.ext -> createPath(name.hash, ext)
                                int dotAt = pathPrefix.lastIndexOf('.');
                                if (dotAt > -1) {
                                    String extension = pathPrefix.substring(dotAt + 1);
                                    pathPrefix = pathPrefix.substring(0, dotAt) + "." + hash;
                                    path = createPath(contentType, pathPrefix, extension);

                                // name.ext -> createPath(name-hash, null)
                                } else {
                                    pathPrefix += "-" + hash;
                                    path = createPath(contentType, pathPrefix, null);
                                }

                                // Look into CSS files and change all the URLs.
                                if ("text/css".equals(contentType)) {
                                    String css = new String(source, StandardCharsets.UTF_8);
                                    StringBuilder newCssBuilder = new StringBuilder();
                                    Matcher urlMatcher = CSS_URL_PATTERN.matcher(css);
                                    int previous = 0;
                                    String childPath;
                                    URI childUri;
                                    StorageItem childItem;
                                    String extra;
                                    int slashAt;

                                    while (urlMatcher.find()) {
                                        newCssBuilder.append(css.substring(previous, urlMatcher.start()));
                                        previous = urlMatcher.end();
                                        childPath = urlMatcher.group(2);
                                        extra = urlMatcher.group(3);

                                        newCssBuilder.append("url(");

                                        if (childPath.length() == 0) {
                                            newCssBuilder.append("''");

                                        } else if (childPath.startsWith("data:")
                                                || childPath.endsWith(".htc")) {
                                            newCssBuilder.append(childPath);

                                        } else {
                                            childUri = new URI(servletPath).resolve(childPath);

                                            if (childUri.isAbsolute()) {
                                                newCssBuilder.append(childUri);

                                            } else {
                                                childItem = get(childUri.toString());
                                                for (slashAt = 1; (slashAt = path.indexOf('/', slashAt)) > -1; ++ slashAt) {
                                                    newCssBuilder.append("../");
                                                }
                                                newCssBuilder.append(childItem != null ? childItem.getPath() : childPath);
                                            }

                                            if (extra != null) {
                                                newCssBuilder.append(extra);
                                            }
                                        }

                                        newCssBuilder.append(')');
                                    }

                                    newCssBuilder.append(css.substring(previous, css.length()));
                                    source = newCssBuilder.toString().getBytes(StandardCharsets.UTF_8);
                                }

                                StorageItem item = createIn(storage);
                                item.setPath(path);
                                if (!item.isInStorage()) {
                                    saveItem(contentType, item, source);
                                }

                                return item;
                            }
                        };
                    }
                };
            }
        }

        /**
         * Finds the resource at the given {@code servletPath} within the given
         * {@code servletContext}, stores it in the given {@code storage},
         * and returns its {@linkplain StorageItem#getPublicUrl URL}.
         *
         * @param storage May be {@code null} to use the default storage.
         */
        public static StorageItem getPlainResource(String storage, ServletContext servletContext, String servletPath) {
            return getResource(PLAIN_RESOURCES, storage, servletContext, servletPath);
        }

        /**
         * Finds and gzips the resource at the at the given {@code servletPath}
         * within the given {@code servletContext}, stores it in the given
         * {@code storage}, and returns its {@linkplain
         * StorageItem#getPublicUrl URL}.
         *
         * @param storage May be {@code null} to use the default storage.
         */
        public static StorageItem getGzippedResource(String storage, ServletContext servletContext, String servletPath) {
            return getResource(GZIPPED_RESOURCES, storage, servletContext, servletPath);
        }

        // Actual implementation of the public get*Resource methods.
        private static StorageItem getResource(
                ResourceCache resources,
                String storage,
                ServletContext servletContext,
                String servletPath) {

            if (servletPath == null) {
                return null;
            }

            StorageItem item = resources
                    .get(storage != null ? storage : "")
                    .get(servletContext)
                    .get(servletPath);

            return item;
        }
    }
}
