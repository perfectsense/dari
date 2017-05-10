package com.psddev.dari.aws;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.Headers;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.psddev.dari.util.AbstractStorageItem;
import com.psddev.dari.util.ObjectUtils;
import com.psddev.dari.util.StorageItemOriginUrl;
import com.psddev.dari.util.SettingsException;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;

/**
 * {@link com.psddev.dari.util.StorageItem} implementation that uses
 * <a href="http://aws.amazon.com/s3/">Amazon S3</a>.
 */
public class S3StorageItem extends AbstractStorageItem implements StorageItemOriginUrl {

    /**
     * Sub-setting key for S3 bucket name.
     */
    public static final String BUCKET_SUB_SETTING = "bucket";

    /**
     * Sub-setting key for S3 access key.
     */
    public static final String ACCESS_SUB_SETTING = "access";

    /**
     * Sub-setting key for S3 secret access key.
     */
    public static final String SECRET_SUB_SETTING = "secret";

    /**
     * Sub-setting key for the private base URL that's used to construct the
     * {@linkplain #getOriginUrl private URL}.
     */
    public static final String ORIGIN_BASE_URL_SUB_SETTING = "originBaseUrl";

    /**
     * Sub-setting key for S3 {@link CannedAccessControlList}.
     */
    public static final String CANNED_ACCESS_CONTROL_LIST_SETTING = "cannedAccessControlList";

    private transient String secret;
    private transient String bucket;
    private transient String access;
    private transient String originBaseUrl;
    private transient CannedAccessControlList cannedAccessControlList;

    public String getBucket() {
        return bucket;
    }

    public void setBucket(String bucket) {
        this.bucket = bucket;
    }

    public String getAccess() {
        return access;
    }

    public void setAccess(String access) {
        this.access = access;
    }

    public String getSecret() {
        return secret;
    }

    public void setSecret(String secret) {
        this.secret = secret;
    }

    public String getOriginBaseUrl() {
        return originBaseUrl;
    }

    public void setOriginBaseUrl(String originBaseUrl) {
        this.originBaseUrl = originBaseUrl;
    }

    public CannedAccessControlList getCannedAccessControlList() {
        return cannedAccessControlList;
    }

    public void setCannedAccessControlList(CannedAccessControlList cannedAccessControlList) {
        this.cannedAccessControlList = cannedAccessControlList;
    }

    @Override
    public void initialize(String settingsKey, Map<String, Object> settings) {
        super.initialize(settingsKey, settings);

        setBucket(ObjectUtils.to(String.class, settings.get(BUCKET_SUB_SETTING)));

        if (ObjectUtils.isBlank(getBucket())) {
            throw new SettingsException(settingsKey + "/" + BUCKET_SUB_SETTING, "No bucket name!");
        }

        setAccess(ObjectUtils.to(String.class, settings.get(ACCESS_SUB_SETTING)));
        setSecret(ObjectUtils.to(String.class, settings.get(SECRET_SUB_SETTING)));
        setOriginBaseUrl(ObjectUtils.to(String.class, settings.get(ORIGIN_BASE_URL_SUB_SETTING)));
        setCannedAccessControlList(ObjectUtils.to(CannedAccessControlList.class, settings.get(CANNED_ACCESS_CONTROL_LIST_SETTING)));
    }

    private AmazonS3Client createClient() {
        String access = getAccess();
        String secret = getSecret();

        return !ObjectUtils.isBlank(access) && !ObjectUtils.isBlank(secret)
                ? new AmazonS3Client(new BasicAWSCredentials(access, secret))
                : new AmazonS3Client(new DefaultAWSCredentialsProviderChain());
    }

    @Override
    protected InputStream createData() throws IOException {
        return createClient().getObject(getBucket(), getPath()).getObjectContent();
    }

    @Override
    protected void saveData(InputStream data) throws IOException {
        ObjectMetadata metadata = new ObjectMetadata();

        metadata.setContentType(getContentType());

        @SuppressWarnings("unchecked")
        Map<String, List<String>> headers = (Map<String, List<String>>) getMetadata().get(HTTP_HEADERS);

        if (headers != null) {
            headers.forEach((key, values) -> {
                if (values != null) {
                    switch (key) {

                        case Headers.CONTENT_LENGTH:
                            values.forEach(value -> metadata.setHeader(key, ObjectUtils.to(Long.class, value)));
                            break;

                        default:
                            values.forEach(value -> metadata.setHeader(key, value));
                            break;
                    }
                }
            });
        }

        PutObjectRequest poRequest = new PutObjectRequest(getBucket(), getPath(), data, metadata);
        poRequest.setCannedAcl(ObjectUtils.firstNonNull(getCannedAccessControlList(), CannedAccessControlList.PublicRead));
        createClient().putObject(poRequest);
    }

    @Override
    public boolean isInStorage() {
        try {
            createClient().getObjectMetadata(getBucket(), getPath());

            return true;

        } catch (AmazonServiceException error) {
            if (error.getStatusCode() == 404) {
                return false;

            } else {
                throw error;
            }
        }
    }

    @Override
    public String getOriginUrl() {
        if (ObjectUtils.isBlank(getOriginBaseUrl())) {
            return null;
        }

        return createPublicUrl(getOriginBaseUrl(), getPath());
    }

}
