package com.psddev.dari.util;

import javax.servlet.http.HttpServletRequest;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import javax.servlet.FilterChain;
import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.fileupload.FileItem;
import com.google.common.base.Preconditions;

/**
 * For creating {@link StorageItem}(s) from a {@link MultipartRequest}
 */
public class StorageItemFilter extends AbstractFilter {

    private static final String UPLOAD_PATH = "/_dari/upload";
    private static final String FILE_PARAM = "fileParam";
    private static final String STORAGE_PARAM = "storageName";

    /**
     * Intercepts requests to {@code UPLOAD_PATH},
     * creates a {@link StorageItem} and returns the StorageItem as json.
     *
     * @param request  Can't be {@code null}.
     * @param response Can't be {@code null}.
     * @param chain    Can't be {@code null}.
     * @throws Exception
     */
    @Override
    protected void doRequest(HttpServletRequest request, HttpServletResponse response, FilterChain chain) throws Exception {

        if (request.getRequestURI().equals(UPLOAD_PATH)) {
            String fileParam = request.getParameter(FILE_PARAM);
            String storageName = request.getParameter(STORAGE_PARAM);
            StorageItem storageItem = StorageItemFilter.getParameter(request, fileParam, storageName);

            WebPageContext page = new WebPageContext((ServletContext) null, request, response);
            response.setContentType("application/json");
            page.write(ObjectUtils.toJson(storageItem));
            return;
        }

        chain.doFilter(request, response);
    }

    /**
     * Creates {@link StorageItem} from a request and request parameter.
     *
     * @param request     Can't be {@code null}.
     * @param paramName   The parameter name for the file input. Can't be {@code null} or blank.
     * @param storageName Optionally accepts a storageName. will default to using {@code StorageItem.DEFAULT_STORAGE_SETTING}
     * @return the created {@link StorageItem}
     */
    public static StorageItem getParameter(HttpServletRequest request, String paramName, String storageName) throws IOException {
        Preconditions.checkNotNull(request);
        Preconditions.checkArgument(!StringUtils.isBlank(paramName));

        String storageItemJson = request.getParameter(paramName);

        StorageItem storageItem = null;

        //TODO: improve parameter type detection of file parameter
        if (storageItemJson != null && !storageItemJson.equals("file")) {
            storageItem = createStorageItem(storageItemJson);
        }

        if (storageItem == null) {
            MultipartRequest mpRequest = MultipartRequestFilter.Static.getInstance(request);

            if (mpRequest != null) {
                FileItem fileItem = mpRequest.getFileItem(paramName);
                if (fileItem != null) {
                    storageItem = createStorageItem(fileItem, storageName);
                }
            }
        }

        return storageItem;
    }

    private static StorageItem createStorageItem(FileItem fileItem, String storageName) throws IOException {

        if (StringUtils.isBlank(storageName)) {
            storageName = StorageItem.DEFAULT_STORAGE_SETTING;
        }

        String storageSetting = Preconditions.checkNotNull(Settings.get(String.class, storageName),
                "Storage setting with key [" + storageName + "] not found in application settings.");

        File file;
        try {
            file = File.createTempFile("cms.", ".tmp");
            fileItem.write(file);
        } catch (Exception e) {
            throw new IOException("Unable to write [fileItem] to [file]", e);
        }

        String fileName = Preconditions.checkNotNull(fileItem.getName());

        String fileContentType = fileItem.getContentType();
        long fileSize = fileItem.getSize();

        Preconditions.checkState(fileSize > 0,
                "File [" + fileName + "] is empty");

        checkContentType(file, fileContentType);

        StorageItem storageItem = StorageItem.Static.createIn(storageSetting);
        storageItem.setData(new FileInputStream(file));
        storageItem.setContentType(fileContentType);
        storageItem.setPath(createStorageItemPath(fileName));

        return storageItem;
    }

    private static StorageItem createStorageItem(String jsonString) {
        Preconditions.checkNotNull(jsonString);
        Map<String, Object> json = Preconditions.checkNotNull(
                ObjectUtils.to(new TypeReference<Map<String, Object>>() {
                }, ObjectUtils.fromJson(jsonString)));
        Object path = Preconditions.checkNotNull(json.get("path"));
        String storage = ObjectUtils
                .firstNonBlank(json.get("storage"), Settings.get(StorageItem.DEFAULT_STORAGE_SETTING))
                .toString();
        String contentType = ObjectUtils.to(String.class, json.get("contentType"));

        Map<String, Object> metadata = null;
        if (!ObjectUtils.isBlank(json.get("metadata"))) {
            metadata = ObjectUtils.to(
                    new TypeReference<Map<String, Object>>() {
                    },
                    json.get("metadata"));
        }

        StorageItem storageItem = StorageItem.Static.createIn(storage);
        storageItem.setContentType(contentType);
        storageItem.setPath(path.toString());
        storageItem.setMetadata(metadata);

        return storageItem;
    }

    private static String createStorageItemPath(String fullFileName) {
        Preconditions.checkArgument(!StringUtils.isBlank(fullFileName));

        String extension = "";
        String fileName = "";
        String path = createStoragePathPrefix();

        int lastDotAt = fullFileName.indexOf('.');

        if (lastDotAt > -1) {
            extension = fullFileName.substring(lastDotAt);
            fileName = fullFileName.substring(0, lastDotAt);
        }

        if (ObjectUtils.isBlank(fileName)) {
            fileName = UUID.randomUUID().toString().replace("-", "");
        }

        path += StringUtils.toNormalized(fileName);
        path += extension;

        return path;
    }

    private static String createStoragePathPrefix() {
        String idString = UUID.randomUUID().toString().replace("-", "");
        StringBuilder pathBuilder = new StringBuilder();

        pathBuilder.append(idString.substring(0, 2));
        pathBuilder.append('/');
        pathBuilder.append(idString.substring(2, 4));
        pathBuilder.append('/');
        pathBuilder.append(idString.substring(4));
        pathBuilder.append('/');

        return pathBuilder.toString();
    }

    private static void checkContentType(File file, String fileContentType) throws IOException {
        Preconditions.checkNotNull(file);

        if (fileContentType == null) {
            return;
        }

        String groupsPattern = Settings.get(String.class, "cms/tool/fileContentTypeGroups");
        Set<String> contentTypeGroups = new SparseSet(ObjectUtils.isBlank(groupsPattern) ? "+/" : groupsPattern);

        Preconditions.checkState(contentTypeGroups.contains(fileContentType),
                "Invalid content type " + fileContentType + ". Must match the pattern " + contentTypeGroups + ".");

        // Disallow HTML disguising as other content types per:
        // http://www.adambarth.com/papers/2009/barth-caballero-song.pdf
        if (!contentTypeGroups.contains("text/html")) {
            try (InputStream input = new FileInputStream(file)) {
                byte[] buffer = new byte[1024];
                String data = new String(buffer, 0, input.read(buffer)).toLowerCase(Locale.ENGLISH);
                String ptr = data.trim();

                if (ptr.startsWith("<!")
                        || ptr.startsWith("<?")
                        || data.startsWith("<html")
                        || data.startsWith("<script")
                        || data.startsWith("<title")
                        || data.startsWith("<body")
                        || data.startsWith("<head")
                        || data.startsWith("<plaintext")
                        || data.startsWith("<table")
                        || data.startsWith("<img")
                        || data.startsWith("<pre")
                        || data.startsWith("text/html")
                        || data.startsWith("<a")
                        || ptr.startsWith("<frameset")
                        || ptr.startsWith("<iframe")
                        || ptr.startsWith("<link")
                        || ptr.startsWith("<base")
                        || ptr.startsWith("<style")
                        || ptr.startsWith("<div")
                        || ptr.startsWith("<p")
                        || ptr.startsWith("<font")
                        || ptr.startsWith("<applet")
                        || ptr.startsWith("<meta")
                        || ptr.startsWith("<center")
                        || ptr.startsWith("<form")
                        || ptr.startsWith("<isindex")
                        || ptr.startsWith("<h1")
                        || ptr.startsWith("<h2")
                        || ptr.startsWith("<h3")
                        || ptr.startsWith("<h4")
                        || ptr.startsWith("<h5")
                        || ptr.startsWith("<h6")
                        || ptr.startsWith("<b")
                        || ptr.startsWith("<br")) {
                    throw new IOException("Can't upload [" + fileContentType + "] file disguising as HTML!");
                }
            }
        }
    }
}
