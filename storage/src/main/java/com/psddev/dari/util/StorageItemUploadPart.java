package com.psddev.dari.util;

import java.io.File;

/**
 * Wraps values needed to create and validating StorageItems
 * in {@link StorageItemFilter}.
 */
public class StorageItemUploadPart {

    private String name;
    private String contentType;
    private File file;
    private String storageName;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getContentType() {
        return contentType;
    }

    public void setContentType(String contentType) {
        this.contentType = contentType;
    }

    public File getFile() {
        return file;
    }

    public void setFile(File file) {
        this.file = file;
    }

    public String getStorageName() {
        return storageName;
    }

    public void setStorageName(String storageName) {
        this.storageName = storageName;
    }

    public long getSize() {
        if (getFile() != null) {
            return getFile().length();
        }
        return 0;
    }
}
