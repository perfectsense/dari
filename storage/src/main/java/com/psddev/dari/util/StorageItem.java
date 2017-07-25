package com.psddev.dari.util;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

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
    public default URL getUrl() {
	    String url = getPublicUrl();
	    try {
	        return new URL(url);
	    } catch (MalformedURLException ex) {
	        throw new IllegalStateException(String.format("[%s] is not a valid URL!", url));
	    }
	}

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
                    Class<?> listenerClass = ObjectUtils.getClassByName("com.psddev.dari.util.ImageResizeStorageItemListener");

                    if (listenerClass != null && StorageItemListener.class.isAssignableFrom(listenerClass)) {
                        base.registerListener((StorageItemListener) TypeDefinition.getInstance(listenerClass).newInstance());
                    }
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
                newItem.setMetadata(item.getMetadata());
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

        /**
         * Finds the resource at the given {@code servletPath} within the given
         * {@code servletContext}, stores it in the given {@code storage},
         * and returns the storage item that represents it.
         *
         * @param storage Nullable (to indicate default storage).
         *
         * @deprecated Use {@link Cdn#getUrl(javax.servlet.http.HttpServletRequest, String)} instead.
         */
        @Deprecated
        public static StorageItem getPlainResource(String storage, ServletContext servletContext, String servletPath) {
            return Cdn.PLAIN_CACHE.get(storage, ServletCdnContext.getInstance(servletContext), servletPath);
        }

        /**
         * Finds and gzips the resource at the at the given {@code servletPath}
         * within the given {@code servletContext}, stores it in the given
         * {@code storage}, and returns the storage item that represents it.
         *
         * @param storage Nullable (to indicate default storage).
         *
         * @deprecated Use {@link Cdn#getUrl(javax.servlet.http.HttpServletRequest, String)} instead.
         */
        @Deprecated
        public static StorageItem getGzippedResource(String storage, ServletContext servletContext, String servletPath) {
            return Cdn.GZIPPED_CACHE.get(storage, ServletCdnContext.getInstance(servletContext), servletPath);
        }
    }
}
