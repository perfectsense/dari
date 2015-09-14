package com.psddev.dari.util;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Skeletal implementation of a storage item. Subclasses should further
 * implement the following:
 *
 * <ul>
 * <li>{@link #createData}</li>
 * <li>{@link #saveData}</li>
 * </ul>
 */
public abstract class AbstractStorageItem implements StorageItem {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractStorageItem.class);

    /**
     * Sub-setting key for the base URL that's used to construct the
     * {@linkplain #getPublicUrl public URL}.
     */
    public static final String BASE_URL_SUB_SETTING = "baseUrl";

    /**
     * Sub-setting key for the base URL that's used to construct the
     * {@linkplain #getPublicUrl public URL} by distributing it across the
     * defined base URLs.
     */
    public static final String BASE_URLS_SUB_SETTING = "baseUrls";

    /**
     * Sub-setting key for the base URL that's used to construct the
     * {@linkplain #getSecurePublicUrl secure public URL}.
     */
    public static final String SECURE_BASE_URL_SUB_SETTING = "secureBaseUrl";

    /**
     * Sub-setting key for the base URL that's used to construct the
     * {@linkplain #getSecurePublicUrl public URL} by distributing it across the
     * defined base URLs.
     */
    public static final String SECURE_BASE_URLS_SUB_SETTING = "secureBaseUrls";

    /**
     * Sub-setting key for the name used to construct the
     * {@linkplain #getHashAlgorithm() hash algorithm} used for multi-CDN
     * support.
     */
    public static final String HASH_ALGORITHM_SUB_SETTING = "hashAlgorithm";

    public static final String HTTP_HEADERS = "http.headers";

    private transient String baseUrl;
    private transient String secureBaseUrl;
    private transient List<String> baseUrls;
    private transient List<String> secureBaseUrls;
    private String storage;
    private String path;
    private String contentType;
    private Map<String, Object> metadata;
    private transient InputStream data;
    private transient List<StorageItemListener> listeners;
    private transient StorageItemHash hashAlgorithm;

    /**
     * Returns the base URL that's used to construct the
     * {@linkplain #getPublicUrl public URL}.
     */
    public String getBaseUrl() {
        if (baseUrl != null) {
            return baseUrl;
        } else {
            return getBaseUrlFromHash(getBaseUrls());
        }
    }

    /**
     * Sets the base URL that's used to construct the
     * {@linkplain #getPublicUrl public URL}.
     */
    public void setBaseUrl(String baseUrl) {
        this.baseUrl = baseUrl;
    }

    /**
     * Returns the base URL that's used to construct the
     * {@linkplain #getSecurePublicUrl secure public URL}.
     */
    public String getSecureBaseUrl() {
        if (secureBaseUrl != null) {
            return secureBaseUrl;
        } else {
            return getBaseUrlFromHash(getSecureBaseUrls());
        }
    }

    /**
     * Sets the base URL that's used to construct the
     * {@linkplain #getSecurePublicUrl secure public URL}.
     */
    public void setSecureBaseUrl(String secureBaseUrl) {
        this.secureBaseUrl = secureBaseUrl;
    }

    /** Returns the list of available base URLs that can be used to construct
     *  the {@linkplain #getPublicUrl public URL}. */
    public List<String> getBaseUrls() {
        if (baseUrls == null) {
            baseUrls = new ArrayList<String>();
        }
        return baseUrls;
    }

    /** Sets the list of available base URLs that can be used to construct
     *  the {@linkplain #getPublicUrl public URL}. */
    public void setBaseUrls(List<String> baseUrls) {
        this.baseUrls = baseUrls;
    }

    /** Returns the list of available base URLs that can be used to construct
     *  the {@linkplain #getSecurePublicUrl secure public URL}. */
    public List<String> getSecureBaseUrls() {
        if (secureBaseUrls == null) {
            secureBaseUrls = new ArrayList<>();
        }
        return secureBaseUrls;
    }

    /** Sets the list of available base URLs that can be used to construct
     *  the {@linkplain #getSecurePublicUrl secure public URL}. */
    public void setSecureBaseUrls(List<String> secureBaseUrls) {
        this.secureBaseUrls = secureBaseUrls;
    }

    /** Register a StorageItemListener. */
    public void registerListener(StorageItemListener plugin) {
        if (listeners == null) {
            resetListeners();
        }

        listeners.add(plugin);
    }

    /** Reset plugins. */
    public void resetListeners() {
        listeners = new ArrayList<StorageItemListener>();
    }

    /** Returns the hashing algorithm for this storage item. */
    public StorageItemHash getHashAlgorithm() {
        return hashAlgorithm;
    }

    /** Sets the hashing algorithm for this storage item. */
    public void setHashAlgorithm(StorageItemHash hashAlgorithm) {
        this.hashAlgorithm = hashAlgorithm;
    }

    /** Selects a base URL from the {@code baseUrls} list using this storage
     *  item's configured hash algorithm. */
    private String getBaseUrlFromHash(List<String> baseUrls) {

        String baseUrlFromHash = null;

        if (baseUrls.size() > 0) {

            if (getHashAlgorithm() != null) {

                int bucketIndex = getHashAlgorithm().hashStorageItem(this) % baseUrls.size();
                // make sure the index is always positive.
                if (bucketIndex < 0) {
                    bucketIndex *= -1;
                }

                baseUrlFromHash = baseUrls.get(bucketIndex);

            } else {
                baseUrlFromHash = baseUrls.get(0);
            }
        }

        return baseUrlFromHash;

    }

    // --- StorageItem support ---

    @Override
    public void initialize(String settingsKey, Map<String, Object> settings) {
        setBaseUrl(ObjectUtils.to(String.class, settings.get(BASE_URL_SUB_SETTING)));
        setSecureBaseUrl(ObjectUtils.to(String.class, settings.get(SECURE_BASE_URL_SUB_SETTING)));

        @SuppressWarnings("unchecked")
        Map<String, String> baseUrls = (Map<String, String>) settings.get(BASE_URLS_SUB_SETTING);
        if (baseUrls != null) {
            setBaseUrls(new ArrayList<String>(baseUrls.values()));
        }

        @SuppressWarnings("unchecked")
        Map<String, String> secureBaseUrls = (Map<String, String>) settings.get(SECURE_BASE_URLS_SUB_SETTING);
        if (secureBaseUrls != null) {
            setSecureBaseUrls(new ArrayList<String>(secureBaseUrls.values()));
        }

        setHashAlgorithm(StorageItemHash.Static.getInstanceOrDefault(
                ObjectUtils.to(String.class, settings.get(HASH_ALGORITHM_SUB_SETTING))));
    }

    @Override
    public String getStorage() {
        return storage;
    }

    @Override
    public void setStorage(String storage) {
        this.storage = storage;
    }

    @Override
    public String getPath() {
        return path;
    }

    @Override
    public void setPath(String path) {
        this.path = path;
    }

    @Override
    public String getContentType() {
        if (!ObjectUtils.isBlank(contentType)) {
            return contentType;
        }

        String path = getPath();

        if (!ObjectUtils.isBlank(path)) {

            try {
                path = new URI(path).getPath();
            } catch (URISyntaxException e) {
                return null;
            }

            return ObjectUtils.getContentType(path);
        } else {
            return null;
        }
    }

    @Override
    public void setContentType(String contentType) {
        this.contentType = contentType;
    }

    @Override
    public Map<String, Object> getMetadata() {
        if (metadata == null) {
            metadata = new CompactMap<String, Object>();
        }
        return metadata;
    }

    @Override
    public void setMetadata(Map<String, Object> metadata) {
        this.metadata = metadata;
    }

    @Override
    public InputStream getData() throws IOException {
        if (data == null) {
            data = createData();
        }
        return data;
    }

    /** Creates the data stream. */
    protected abstract InputStream createData() throws IOException;

    @Override
    public void setData(InputStream data) {
        this.data = data;
    }

    @Deprecated
    @Override
    public URL getUrl() {
        String url = getPublicUrl();
        try {
            return new URL(url);
        } catch (MalformedURLException ex) {
            throw new IllegalStateException(String.format("[%s] is not a valid URL!", url));
        }
    }

    @Override
    public String getPublicUrl() {
        return createPublicUrl(getBaseUrl(), getPath());
    }

    @Override
    public String getSecurePublicUrl() {
        return createPublicUrl(getSecureBaseUrl(), getPath());
    }

    private String createPublicUrl(String baseUrl, String path) {
        if (!ObjectUtils.isBlank(baseUrl)) {
            path = StringUtils.ensureEnd(baseUrl, "/") + path;
            try {
                URL url = new URL(path);
                path = new URI(
                        url.getProtocol(),
                        url.getAuthority(),
                        url.getPath(),
                        url.getQuery(),
                        url.getRef())
                        .toASCIIString();
            } catch (MalformedURLException error) {
                // Return the path as is if the given path is malformed.
            } catch (URISyntaxException error) {
                // Return the path as is if the resolved path is malformed.
            }
        }
        return path;
    }

    @Override
    public void save() throws IOException {
        InputStream data = getData();
        try {
            saveData(data);
            setData(null);
        } finally {
            data.close();
        }

        if (listeners != null) {
            for (StorageItemListener listener : listeners) {
                try {
                    listener.afterSave(this);
                } catch (Exception error) {
                    LOGGER.warn(String.format("Can't execute [%s] on [%s]!", listener, this), error);
                }
            }
        }
    }

    /** Saves the given {@code data} stream. */
    protected abstract void saveData(InputStream data) throws IOException;

    // --- Object support ---

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        } else if (other instanceof StorageItem) {
            StorageItem otherItem = (StorageItem) other;
            return ObjectUtils.equals(getStorage(), otherItem.getStorage())
                    && ObjectUtils.equals(getPath(), otherItem.getPath());
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return String.format("%s\0%s", getStorage(), getPath()).hashCode();
    }

    @Override
    public String toString() {
        return String.format("storageItem:%s:%s", getStorage(), getPath());
    }

    // --- Deprecated ---

    /** @deprecated Use {@link #BASE_URL_SUB_SETTING} instead. */
    @Deprecated
    public static final String BASE_URL_SETTING = BASE_URL_SUB_SETTING;
}
