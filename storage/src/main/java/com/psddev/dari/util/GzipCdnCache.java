package com.psddev.dari.util;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPOutputStream;

class GzipCdnCache extends CdnCache {

    @Override
    protected boolean customizeItem(String contentType) {
        return contentType.startsWith("text/");
    }

    @Override
    protected String createItemPath(String pathPrefix, String extension) {
        return extension != null ? pathPrefix + ".gz." + extension : pathPrefix + "-gz";
    }

    @Override
    protected void updateItemMetadata(Map<String, Object> metadata, Map<String, List<String>> httpHeaders) {
        httpHeaders.put("Content-Encoding", Collections.singletonList("gzip"));
    }

    @Override
    protected byte[] transformItemContent(byte[] content) throws IOException {
        ByteArrayOutputStream byteOutput = new ByteArrayOutputStream();

        try (GZIPOutputStream gzipOutput = new GZIPOutputStream(byteOutput)) {
            gzipOutput.write(content);
        }

        return byteOutput.toByteArray();
    }
}
