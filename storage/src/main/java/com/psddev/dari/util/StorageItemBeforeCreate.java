package com.psddev.dari.util;

import java.io.IOException;

/**
 * StorageItemBeforeCreate allows an Application to implement custom
 * validation of StorageItems prior to creating them. The validation
 * is performed by {@link StorageItemFilter}.
 */
public interface StorageItemBeforeCreate {

    /**
     * Invoked by {@link StorageItemFilter}
     *
     * @param part
     *        StorageItemPart containing relevant data
     * @throws IOException
     */
    void beforeCreate(StorageItemUploadPart part) throws IOException;
}
