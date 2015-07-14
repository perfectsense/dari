package com.psddev.dari.util;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;

import org.jclouds.ContextBuilder;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.BlobStoreContext;
import org.jclouds.blobstore.domain.Blob;
import org.jclouds.blobstore.domain.BlobBuilder;
import org.jclouds.blobstore.options.CreateContainerOptions;
import org.jclouds.openstack.swift.v1.blobstore.RegionScopedBlobStoreContext;

/**
 * {@link StorageItem} stored in the cloud.
 *
 * <p>To use this class, it must be able to access the
 * <a href="http://www.jclouds.org">jclouds library</a>.
 * If you use Maven, you should add the following dependency:</p>
 *
 * <blockquote><pre><code data-type="xml">{@literal
 *<dependency>
 *    <groupId>org.jclouds</groupId>
 *    <artifactId>jclouds-all</artifactId>
 *    <version>1.6.0</version>
 *</dependency>}</code></pre></blockquote>
 */
public class CloudStorageItem extends AbstractStorageItem {

    /** Setting key for storage provider. */
    public static final String PROVIDER_SETTING = "provider";

    /** Setting key for identity. */
    public static final String IDENTITY_SETTING = "identity";

    /** Setting key for credential. */
    public static final String CREDENTIAL_SETTING = "credential";

    /** Setting key for container. */
    public static final String CONTAINER_SETTING = "container";

    /** Setting key for region. */
    public static final String REGION_SETTING = "region";

    private transient String provider;
    private transient String identity;
    private transient String credential;
    private transient String container;
    private transient String region;

    /** Returns the storage provider. */
    public String getProvider() {
        return provider;
    }

    /** Sets the storage provider. */
    public void setProvider(String newProvider) {
        provider = newProvider;
    }

    /** Returns the identity. */
    public String getIdentity() {
        return identity;
    }

    /** Sets the identity. */
    public void setIdentity(String newIdentity) {
        identity = newIdentity;
    }

    /** Returns the credential. */
    public String getCredential() {
        return credential;
    }

    /** Sets the credential. */
    public void setCredential(String newCredential) {
        credential = newCredential;
    }

    /** Returns the container. */
    public String getContainer() {
        return container;
    }

    /** Sets the container. */
    public void setContainer(String newContainer) {
        container = newContainer;
    }

    /** Returns the region. */
    public String getRegion() {
        return region;
    }

    /** Sets the region. */
    public void setRegion(String region) {
        this.region = region;
    }

    // --- AbstractStorageItem support ---

    @Override
    public void initialize(String settingsKey, Map<String, Object> settings) {
        super.initialize(settingsKey, settings);

        String provider = ObjectUtils.to(String.class, settings.get(PROVIDER_SETTING));
        if (ObjectUtils.isBlank(provider)) {
            throw new SettingsException(settingsKey + "/" + PROVIDER_SETTING, "No storage provider!");
        } else {
            setProvider(provider);
        }

        String identity = ObjectUtils.to(String.class, settings.get(IDENTITY_SETTING));
        if (ObjectUtils.isBlank(identity)) {
            throw new SettingsException(settingsKey + "/" + IDENTITY_SETTING, "No identity!");
        } else {
            setIdentity(identity);
        }

        String credential = ObjectUtils.to(String.class, settings.get(CREDENTIAL_SETTING));
        if (ObjectUtils.isBlank(credential)) {
            throw new SettingsException(settingsKey + "/" + CREDENTIAL_SETTING, "No credential!");
        } else {
            setCredential(credential);
        }

        String container = ObjectUtils.to(String.class, settings.get(CONTAINER_SETTING));
        if (ObjectUtils.isBlank(container)) {
            throw new SettingsException(settingsKey + "/" + CONTAINER_SETTING, "No container!");
        } else {
            setContainer(container);
        }

        String region = ObjectUtils.to(String.class, settings.get(REGION_SETTING));
        if (!ObjectUtils.isBlank(region)) {
            setRegion(region);
        }
    }

    private BlobStoreContext createContext() {
        if (!ObjectUtils.isBlank(getRegion())) {
            return createRegionContext();
        }

        return ContextBuilder
                .newBuilder(getProvider())
                .credentials(getIdentity(), getCredential())
                .buildView(BlobStoreContext.class);
    }

    private RegionScopedBlobStoreContext createRegionContext() {
        return ContextBuilder
                .newBuilder(getProvider())
                .credentials(getIdentity(), getCredential())
                .buildView(RegionScopedBlobStoreContext.class);
    }

    private BlobStore createBlobStore(BlobStoreContext context) {
        BlobStore blobStore = null;

        if (!ObjectUtils.isBlank(getRegion()) && context instanceof RegionScopedBlobStoreContext) {
            RegionScopedBlobStoreContext regionContext = (RegionScopedBlobStoreContext) context;
            blobStore = regionContext.getBlobStore(getRegion());
        } else {
            blobStore = context.getBlobStore();
        }

        return blobStore;
    }

    @Override
    protected InputStream createData() throws IOException {
        BlobStoreContext context = createContext();

        try {
            BlobStore store = createBlobStore(context);

            Blob blob = store.getBlob(getContainer(), getPath());
            return blob.getPayload().getInput();

        } finally {
            context.close();
        }
    }

    protected void saveData(File file) throws IOException {
        BlobStoreContext context = createContext();

        try {
            BlobStore store = createBlobStore(context);
            if (!store.containerExists(getContainer())) {
                store.createContainerInLocation(null, getContainer(), new CreateContainerOptions().publicRead());
            }

            BlobBuilder blobBuilder = store.blobBuilder(getPath());
            BlobBuilder.PayloadBlobBuilder payloadBuilder = blobBuilder.payload(file);

            Map<String, Object> metadata = getMetadata();
            @SuppressWarnings("unchecked")
            Map<String, List<String>> headers = (Map<String, List<String>>) metadata.get("http.headers");

            if (headers != null) {
                for (Map.Entry<String, List<String>> entry : headers.entrySet()) {
                    String key = entry.getKey();
                    List<String> values = entry.getValue();

                    if (values != null && !values.isEmpty()) {
                        if (key.equalsIgnoreCase("Content-Disposition")) {
                            for (String value : values) {
                                payloadBuilder.contentDisposition(value);
                                break;
                            }

                        } else if (key.equalsIgnoreCase("Content-Language")) {
                            for (String value : values) {
                                payloadBuilder.contentLanguage(value);
                                break;
                            }

                        } else if (key.equalsIgnoreCase("Content-Length")) {
                            for (String value : values) {
                                payloadBuilder.contentLength(ObjectUtils.to(long.class, value));
                                break;
                            }

                        } else if (key.equalsIgnoreCase("Content-Encoding")) {
                            for (String value : values) {
                                payloadBuilder.contentEncoding(value);
                                break;
                            }

                        } else if (key.equalsIgnoreCase("Content-Type")) {
                            for (String value : values) {
                                payloadBuilder.contentType(value);
                                break;
                            }
                        }
                    }
                }

                // TODO: Decide which additional metadata is relevant to this StorageItem
                /*
                Map<String, String> userMetadata = new HashMap<String, String>();
                for (Map.Entry<String, Object> entry : metadata.entrySet()) {
                    Object value = entry.getValue();
                    if (value instanceof String) {
                        userMetadata.put(entry.getKey(), (String) value);
                    }
                }
                payloadBuilder.userMetadata(userMetadata);
                */
            }

            payloadBuilder.contentType(getContentType());

            store.putBlob(getContainer(), blobBuilder.build());

        } finally {
            context.close();
        }
    }

    @Override
    protected void saveData(InputStream dataInput) throws IOException {
        File temp = File.createTempFile("jclouds", null);

        try {
            OutputStream tempOutput = new FileOutputStream(temp);
            try {
                IoUtils.copy(dataInput, tempOutput);
            } finally {
                tempOutput.close();
            }

            saveData(temp);

        } finally {
            if (!temp.delete()) {
                temp.deleteOnExit();
            }
        }
    }

    @Override
    public boolean isInStorage() {
        BlobStoreContext context = createContext();

        try {
            BlobStore store = createBlobStore(context);
            return store.blobExists(getContainer(), getPath());

        } finally {
            context.close();
        }
    }
}
