package com.psddev.dari.util;

/**
 * StorageItemPathGenerator for a specific storageName
 * can be defined in Settings.
 */
public interface StorageItemPathGenerator {

    String createPath(String fullFileName);
}
