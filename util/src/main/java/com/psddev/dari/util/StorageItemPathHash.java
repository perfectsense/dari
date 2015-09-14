package com.psddev.dari.util;

import java.nio.ByteBuffer;
import java.util.Map;

/**
 * Storage item hash algorithm that grabs the storage item's path and calls
 * {@code hashCode} on it.
 */
public class StorageItemPathHash extends AbstractStorageItemHash {

    /** Returns the hash code of the storage item's path, or 0 if the path is null. */
    @Override
    public int hashStorageItem(StorageItem storageItem) {
        String path = storageItem.getPath();
        return path != null ? ByteBuffer.wrap(StringUtils.md5(path)).getInt() : 0;
    }

    @Override
    public void initialize(String settingsKey, Map<String, Object> settings) {
        // nothing to do.
    }
}
