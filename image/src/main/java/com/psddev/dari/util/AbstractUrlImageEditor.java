package com.psddev.dari.util;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public abstract class AbstractUrlImageEditor extends AbstractImageEditor implements ImageEditorPrivateUrl {

    /** Setting key for the base URL to the image editor implementation. */
    public static final String BASE_URL_SETTING = "baseUrl";

    /**
     * Sub-setting key for the base URL that's used to construct the
     * {@linkplain #getBaseUrl base URL} by distributing it across the
     * defined base URLs.
     */
    public static final String BASE_URLS_SUB_SETTING = "baseUrls";

    /** Setting key for the private base URL to the image editor implementation. */
    public static final String PRIVATE_BASE_URL_SETTING = "privateBaseUrl";

    protected String baseUrl;
    protected List<String> baseUrls;
    protected String privateBaseUrl;

    /** Returns the base URL. */
    public String getBaseUrl() {
        if (StringUtils.isBlank(baseUrl) && !ObjectUtils.isBlank(getBaseUrls())) {
            return getBaseUrls().get(0);
        }

        return baseUrl;
    }

    /** Sets the base URL. */
    public void setBaseUrl(String baseUrl) {
        this.baseUrl = baseUrl;
    }

    public List<String> getBaseUrls() {
        if (baseUrls == null) {
            baseUrls = new ArrayList<String>();
        }
        return baseUrls;
    }

    public void setBaseUrls(List<String> baseUrls) {
        this.baseUrls = baseUrls;
    }

    public String getPrivateBaseUrl() {
        return privateBaseUrl;
    }

    public void setPrivateBaseUrl(String privateBaseUrl) {
        this.privateBaseUrl = privateBaseUrl;
    }

    /**
     * Returns the appropriate base URL for the {@code imageUrl}. The
     * {@code imageUrl} is hashed and a base URL is picked from the pool.
     *
     * @param imageUrl the image URL to check.
     * @return the base URL.
     */
    public String getBaseUrlForImageUrl(String imageUrl) {
        String baseUrl = getBaseUrl();

        List<String> baseUrls = getBaseUrls();
        if (!baseUrls.isEmpty()) {
            int bucketIndex = ByteBuffer.wrap(StringUtils.md5(imageUrl)).getInt() % baseUrls.size();
            if (bucketIndex < 0) {
                bucketIndex *= -1;
            }

            baseUrl = baseUrls.get(bucketIndex);
        }

        return baseUrl;
    }

    @Override
    public void initialize(String settingsKey, Map<String, Object> settings) {
        setBaseUrl(ObjectUtils.to(String.class, settings.get(BASE_URL_SETTING)));
        setPrivateBaseUrl(ObjectUtils.to(String.class, settings.get(PRIVATE_BASE_URL_SETTING)));

        @SuppressWarnings("unchecked")
        Map<String, String> baseUrls = (Map<String, String>) settings.get(BASE_URLS_SUB_SETTING);
        if (baseUrls != null) {
            setBaseUrls(new ArrayList<String>(baseUrls.values()));
        }
    }

}
