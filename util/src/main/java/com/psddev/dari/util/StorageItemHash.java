package com.psddev.dari.util;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

/**
 * Hashing algorithm for {@link StorageItem}s used
 * primarily to support multiple CDN sub-domains.
 */
public interface StorageItemHash extends SettingsBackedObject {

    /** Setting key for default storage hash name. */
    public static final String DEFAULT_STORAGE_HASH_SETTING = "dari/defaultStorageHash";

    /** Setting key for all storage hash configuration. */
    public static final String SETTING_PREFIX = "dari/storageHash";

    /** Setting key for the default hashing algorithm name, that's based on the storage item's path. */
    public static final String PATH_HASH_CODE_STORAGE_ITEM_HASH_NAME = "_pathHashCode";

    /** Returns the storage item hash algorithm name. */
    public String getName();

    /** Sets the storage item hash algorithm name. */
    public void setName(String name);

    /** Returns a hash code for the storage item. */
    public int hashStorageItem(StorageItem storageItem);

    /**
     * {@linkplain StorageItemHash Storage item hash} utility methods.
     *
     * <p>The factory method, {@link #getInstance(String)} and
     * {@link #getDefault()}, use {@linkplain Settings settings} to construct
     * instances.
     */
    public static final class Static {

        private static final LoadingCache<String, StorageItemHash> INSTANCES = CacheBuilder
                .newBuilder()
                .build(new CacheLoader<String, StorageItemHash>() {

                    @Override
                    public StorageItemHash load(String name) {
                        String settingsName = SETTING_PREFIX + "/" + name;
                        StorageItemHash instance = null;

                        if (Settings.get(settingsName) != null) {
                            instance = Settings.newInstance(StorageItemHash.class, settingsName);

                        } else if (name.equals(PATH_HASH_CODE_STORAGE_ITEM_HASH_NAME)) {
                            instance = new StorageItemPathHash();
                        }

                        if (instance != null) {
                            instance.setName(name);
                        }

                        return instance;
                    }
                });

        /** Returns the storage item hash algorithm with the given {@code name}. */
        public static StorageItemHash getInstance(String name) {
            return INSTANCES.getUnchecked(name);
        }

        /** Returns the default storage item hash algorithm. */
        public static StorageItemHash getDefault() {
            return getInstance(Settings.getOrDefault(String.class,
                    DEFAULT_STORAGE_HASH_SETTING, PATH_HASH_CODE_STORAGE_ITEM_HASH_NAME));
        }

        /**
         * Safely returns an image editor with the given {@code name},
         * or if it's {@code null}, the default.
         */
        public static StorageItemHash getInstanceOrDefault(String name) {
            StorageItemHash hashAlgorithm;
            if (ObjectUtils.isBlank(name)) {
                hashAlgorithm = getDefault();
                if (hashAlgorithm == null) {
                    throw new IllegalStateException("No default storage item hash algorithm!");
                }
            } else {
                hashAlgorithm = getInstance(name);
                if (hashAlgorithm == null) {
                    throw new IllegalArgumentException(String.format(
                            "[%s] is not a valid storage item hash algorithm name!",
                            name));
                }
            }
            return hashAlgorithm;
        }
    }
}
