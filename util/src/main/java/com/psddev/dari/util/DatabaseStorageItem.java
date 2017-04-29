package com.psddev.dari.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Base64;
import java.util.Map;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * DatabaseStorageItem stores binary data internal as a {@link java.util.zip gzipped},
 * {@link java.util.Base64 Base64}-encoded String.  {@link #getPublicUrl()} and {@link #getSecurePublicUrl()}
 * both require use of another Storage as a proxy.  When invoked, these methods will
 * push the binary data into the proxy storage if it does not already exist.
 * The SHA-256 digest of the binary {@code byte[]} in the proxy storage path segment
 * ensures that unique binary files are always stored at unique paths in the proxy storage.
 *
 */
public class DatabaseStorageItem extends AbstractStorageItem {

    private static final Logger LOGGER = LoggerFactory.getLogger(DatabaseStorageItem.class);

    /** Storage name assigned to all instances by default. */
    public static final String BASE_PATH = "_dbStorage";

    public static final String PROXY_STORAGE_SETTING = "proxyStorage";

    private String binaryData;

    private transient volatile String proxyStorage;

    public String getBinaryData() {
        return binaryData;
    }

    public void setBinaryData(String binaryData) {
        this.binaryData = binaryData;
    }

    public String getProxyStorage() {
        return proxyStorage;
    }

    public void setProxyStorage(String proxyStorage) {
        this.proxyStorage = proxyStorage;
    }

    @Override
    protected InputStream createData() throws IOException {

        return new GZIPInputStream(new ByteArrayInputStream(Base64.getDecoder().decode(getBinaryData())));
    }

    @Override
    protected void saveData(InputStream data) throws IOException {

        byte[] source;
        try {
            ByteArrayOutputStream byteOutput = new ByteArrayOutputStream();
            try (GZIPOutputStream gzipOutput = new GZIPOutputStream(byteOutput)) {

                gzipOutput.write(IoUtils.toByteArray(data));
            }
            source = byteOutput.toByteArray();
        } finally {
            data.close();
        }

        setBinaryData(Base64.getEncoder().encodeToString(source));
        setPath(BASE_PATH + "/" + StringUtils.encodeUri(Base64.getEncoder().encodeToString(StringUtils.hash("SHA-256", getBinaryData()))));
    }

    @Override
    public String getPublicUrl() {

        try {
            return getProxyStorageItem().getPublicUrl();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String getSecurePublicUrl() {

        try {
            return getProxyStorageItem().getSecurePublicUrl();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Implements support for {@link StorageItem#isInStorage()}.  Data is always
     * present in storage.
     * @return {@code true}
     */
    @Override
    public boolean isInStorage() {
        return true;
    }

    @Override
    public void initialize(
            String settingsKey,
            Map<String, Object> settings) {

        setProxyStorage(ObjectUtils.to(String.class, settings.get(PROXY_STORAGE_SETTING)));
    }

    private StorageItem getProxyStorageItem() throws IOException {

        LOGGER.debug("Proxying through " + getProxyStorage());

        StorageItem proxyStorageItem = StorageItem.Static.createIn(getProxyStorage());
        if (DatabaseStorageItem.class.isAssignableFrom(proxyStorageItem.getClass())) {
            throw new IllegalStateException("Default or proxy StorageItem must not be a DatabaseStorageItem!");
        }
        proxyStorageItem.setPath(getPath());
        proxyStorageItem.setContentType(getContentType());
        proxyStorageItem.setMetadata(getMetadata());

        if (!proxyStorageItem.isInStorage()) {

            proxyStorageItem.setData(createData());
            proxyStorageItem.save();
        }
        return proxyStorageItem;
    }
}
