package com.psddev.dari.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;

import org.jets3t.service.ServiceException;
import org.jets3t.service.acl.AccessControlList;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.jets3t.service.model.S3Object;
import org.jets3t.service.security.AWSCredentials;

/**
 * {@link StorageItem} stored in
 * <a href="http://aws.amazon.com/s3/">Amazon S3</a>.
 *
 * <p>To use this class, it must be able to access the
 * <a href="http://jets3t.s3.amazonaws.com/index.html">JetS3t library</a>.
 * If you use Maven, you should add the following dependency:</p>
 *
 * <blockquote><pre><code data-type="xml">{@literal
 *<dependency>
 *    <groupId>net.java.dev.jets3t</groupId>
 *    <artifactId>jets3t</artifactId>
 *    <version>0.8.0</version>
 *</dependency>}</code></pre></blockquote>
 *
 * @deprecated Use {@code com.psddev.dari.aws.S3StorageItem} in
 * {@code com.psddev:dari-aws} instead.
 */
@Deprecated
public class AmazonStorageItem extends AbstractStorageItem {

    /** Setting key for S3 access key. */
    public static final String ACCESS_SETTING = "access";

    /** Setting key for S3 secret key. */
    public static final String SECRET_SETTING = "secret";

    /** Setting key for S3 bucket name. */
    public static final String BUCKET_SETTING = "bucket";

    private transient String access;
    private transient String secret;
    private transient String bucket;

    /** Returns the S3 access key. */
    public String getAccess() {
        return access;
    }

    /** Sets the S3 access key. */
    public void setAccess(String access) {
        this.access = access;
    }

    /** Returns the S3 secret key. */
    public String getSecret() {
        return secret;
    }

    /** Returns the S3 secret key. */
    public void setSecret(String secret) {
        this.secret = secret;
    }

    /** Returns the S3 bucket name. */
    public String getBucket() {
        return bucket;
    }

    /** Returns the S3 bucket name. */
    public void setBucket(String bucket) {
        this.bucket = bucket;
    }

    // --- AbstractStorageItem support ---

    @Override
    public void initialize(String settingsKey, Map<String, Object> settings) {
        super.initialize(settingsKey, settings);

        setAccess(ObjectUtils.to(String.class, settings.get(ACCESS_SETTING)));
        if (ObjectUtils.isBlank(getAccess())) {
            throw new SettingsException(settingsKey + "/" + ACCESS_SETTING, "No access key!");
        }

        setSecret(ObjectUtils.to(String.class, settings.get(SECRET_SETTING)));
        if (ObjectUtils.isBlank(getSecret())) {
            throw new SettingsException(settingsKey + "/" + SECRET_SETTING, "No secret key!");
        }

        setBucket(ObjectUtils.to(String.class, settings.get(BUCKET_SETTING)));
        if (ObjectUtils.isBlank(getBucket())) {
            throw new SettingsException(settingsKey + "/" + BUCKET_SETTING, "No bucket name!");
        }
    }

    @Override
    protected InputStream createData() throws IOException {
        RestS3Service service = null;

        try {
            service = new RestS3Service(new AWSCredentials(getAccess(), getSecret()));
            S3Object object = service.getObject(getBucket(), getPath());
            return new ServiceInputStream(service, object.getDataInputStream());

        } catch (ServiceException ex) {
            throw new IOException(String.format("Unable to store [%s] file!", getPath()), ex);
        }
    }

    @Override
    protected void saveData(InputStream data) throws IOException {
        try {
            RestS3Service service = null;

            try {
                service = new RestS3Service(new AWSCredentials(getAccess(), getSecret()));
                S3Object object = new S3Object(getPath());

                object.setContentType(getContentType());

                Map<String, Object> metadata = getMetadata();
                @SuppressWarnings("unchecked")
                Map<String, List<String>> headers = (Map<String, List<String>>) metadata.get(HTTP_HEADERS);

                if (headers != null) {
                    for (Map.Entry<String, List<String>> entry : headers.entrySet()) {
                        String key = entry.getKey();
                        List<String> values = entry.getValue();

                        if (values != null && !values.isEmpty()) {
                            String value = values.get(0);

                            if (key.equalsIgnoreCase("Content-Disposition")) {
                                object.setContentDisposition(value);

                            } else if (key.equalsIgnoreCase("Content-Language")) {
                                object.setContentLanguage(value);

                            } else if (key.equalsIgnoreCase("Content-Length")) {
                                object.setContentLength(ObjectUtils.to(long.class, value));

                            } else if (key.equalsIgnoreCase("Content-Encoding")) {
                                object.setContentEncoding(value);

                            } else if (key.equalsIgnoreCase("Content-Type")) {
                                object.setContentType(value);

                            } else {
                                object.addMetadata(key, value);
                            }
                        }
                    }
                }

                // TODO: Decide which additional metadata is relevant to this StorageItem
                /*
                for (Map.Entry<String, Object> entry : metadata.entrySet()) {
                    Object value = entry.getValue();
                    if (value instanceof String) {
                        object.addMetadata(entry.getKey(), (String) value);
                    }
                }
                */

                object.setDataInputStream(data);
                object.setAcl(AccessControlList.REST_CANNED_PUBLIC_READ);
                service.putObject(getBucket(), object);

            } finally {
                if (service != null) {
                    service.shutdown();
                }
            }

        } catch (ServiceException error) {
            throw new IOException(String.format(
                    "Can't store [%s] file!", getPath()),
                    error);
        }
    }

    @Override
    public boolean isInStorage() {
        try {
            RestS3Service service = null;

            try {
                service = new RestS3Service(new AWSCredentials(getAccess(), getSecret()));
                service.getObjectDetails(getBucket(), getPath());
                return true;

            } finally {
                if (service != null) {
                    service.shutdown();
                }
            }

        } catch (ServiceException error) {
            return false;
        }
    }

    // --- Nested ---

    /** Wraps an input stream to shut down the S3 service on close. */
    private static class ServiceInputStream extends InputStream {

        private final RestS3Service service;
        private final InputStream input;

        public ServiceInputStream(RestS3Service service, InputStream input) {
            this.service = service;
            this.input = input;
        }

        @Override
        public int available() throws IOException {
            return input.available();
        }

        @Override
        public void close() throws IOException {
            try {
                input.close();
            } finally {
                try {
                    service.shutdown();
                } catch (ServiceException error) {
                    throw new IOException(String.format(
                            "Can't shut down [%s] service!", service),
                            error);
                }
            }
        }

        @Override
        public void mark(int readLimit) {
            input.mark(readLimit);
        }

        @Override
        public boolean markSupported() {
            return input.markSupported();
        }

        @Override
        public int read() throws IOException {
            return input.read();
        }

        @Override
        public int read(byte[] bytes) throws IOException {
            return input.read(bytes);
        }

        @Override
        public int read(byte[] bytes, int offset, int length) throws IOException {
            return input.read(bytes, offset, length);
        }

        @Override
        public void reset() throws IOException {
            input.reset();
        }

        @Override
        public long skip(long number) throws IOException {
            return input.skip(number);
        }
    }
}
