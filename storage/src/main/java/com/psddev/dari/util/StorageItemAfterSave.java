package com.psddev.dari.util;

import java.io.IOException;

/**
 * StorageItemAfterSaves are executed after a StorageItem
 * has executed {@code saveData()} in {@link AbstractStorageItem#save()}.
 */
public interface StorageItemAfterSave {

    /**
     * Invoked from {@link AbstractStorageItem#save()}
     *
     * @param storageItem
     *        A StorageItem after it has been saved to storage.
     */
    void afterSave(StorageItem storageItem) throws IOException;
}
