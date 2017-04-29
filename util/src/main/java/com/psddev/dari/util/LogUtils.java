package com.psddev.dari.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Log utility methods */
public final class LogUtils {

    protected static final Logger LOGGER = LoggerFactory.getLogger(LogUtils.class);

    public static void logAuthRequest(String context, String userId, String domain, String ipAddress, boolean status,
            boolean enabled) {
        if (enabled) {
            if (status) {
                LOGGER.info(context + " {userId:" + userId + ", status:success, domain:" + domain + ", ipAddress:"
                        + ipAddress + "}");
            } else {
                LOGGER.info(context + " {userId:" + userId + ", status:fail, domain:" + domain + ", ipAddress:"
                        + ipAddress + "}");
            }
        }
    }

    public static String getDomain(String siteUrl) {
        String domain = siteUrl;
        if (!ObjectUtils.isBlank(siteUrl)) {
            domain = siteUrl.replaceFirst("^(?i)(?:https?://)?(?:www\\.)?", "");
            int slashAt = domain.indexOf('/');

            if (slashAt > -1) {
                domain = domain.substring(0, slashAt);
            }

            int colonAt = domain.indexOf(':');

            if (colonAt > -1) {
                domain = domain.substring(0, colonAt);
            }
        }
        return domain;
    }

    public static String getIpAddress(String xForReqParam, String remoteAddrReqParam) {
        String ipAddress = xForReqParam;
        if (ipAddress == null) {
            ipAddress = remoteAddrReqParam;
        }
        return ipAddress;
    }
}
