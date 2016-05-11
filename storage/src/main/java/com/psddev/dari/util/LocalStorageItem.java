package com.psddev.dari.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

/** Item stored in the local file system. */
public class LocalStorageItem extends AbstractStorageItem implements StorageItemOriginUrl {

    /** Setting key for root path. */
    public static final String ROOT_PATH_SETTING = "rootPath";

    /**
     * Sub-setting key for the private base URL that's used to construct the
     * {@linkplain #getOriginUrl private URL}.
     */
    public static final String ORIGIN_BASE_URL_SUB_SETTING = "originBaseUrl";

    private transient String rootPath;

    private transient String originBaseUrl;

    /** Returns the root path. */
    public String getRootPath() {
        return rootPath;
    }

    /** Sets the root path. */
    public void setRootPath(String rootPath) {
        this.rootPath = rootPath;
    }

    // --- AbstractStorageItem support ---

    @Override
    public void initialize(String settingsKey, Map<String, Object> settings) {
        super.initialize(settingsKey, settings);

        setRootPath(ObjectUtils.to(String.class, settings.get(ROOT_PATH_SETTING)));
        setOriginBaseUrl(ObjectUtils.to(String.class, settings.get(ORIGIN_BASE_URL_SUB_SETTING)));
        if (ObjectUtils.isBlank(getRootPath())) {
            throw new SettingsException(settingsKey + "/" + ROOT_PATH_SETTING, "No root path!");
        }
    }

    @Override
    protected InputStream createData() throws IOException {
        return new FileInputStream(new File(getRootPath() + "/" + getPath()));
    }

    @Override
    protected void saveData(InputStream data) throws IOException {
        File file = new File(getRootPath() + "/" + getPath());
        IoUtils.createParentDirectories(file);
        FileOutputStream output = null;

        try {
            output = new FileOutputStream(file);
            int bytesRead;
            byte[] buffer = new byte[4096];
            while ((bytesRead = data.read(buffer)) > 0) {
                output.write(buffer, 0, bytesRead);
            }

        } finally {
            if (output != null) {
                output.close();
            }
        }
    }

    @Override
    public boolean isInStorage() {
        return new File(getRootPath() + "/" + getPath()).exists();
    }

    public void setOriginBaseUrl(String originBaseUrl) {
        this.originBaseUrl = originBaseUrl;
    }

    public String getOriginBaseUrl() {
        return originBaseUrl;
    }

    @Override
    public String getOriginUrl() {
        if (ObjectUtils.isBlank(getOriginBaseUrl())) {
            return null;
        }

        return createPublicUrl(getOriginBaseUrl(), getPath());
    }
}
