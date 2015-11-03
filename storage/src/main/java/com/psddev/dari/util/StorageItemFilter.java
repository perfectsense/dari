package com.psddev.dari.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;

import javax.servlet.FilterChain;
import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.fileupload.FileItem;
import com.google.common.base.Preconditions;

/**
 * For creating {@link StorageItem}(s) from a {@link MultipartRequest}
 */
public class StorageItemFilter extends AbstractFilter {

    private static final String DEFAULT_UPLOAD_PATH = "/_dari/upload";
    private static final String FILE_PARAM = "fileParameter";
    private static final String STORAGE_PARAM = "storageName";
    private static final String SETTING_PREFIX = "dari/upload";

    /**
     * Intercepts requests to {@code UPLOAD_PATH},
     * creates a {@link StorageItem} and returns the StorageItem as json.
     *
     * @param request
     *        Can't be {@code null}.
     * @param response
     *        Can't be {@code null}.
     * @param chain
     *        Can't be {@code null}.
     * @throws Exception
     */
    @Override
    protected void doRequest(HttpServletRequest request, HttpServletResponse response, FilterChain chain) throws Exception {

        if (request.getServletPath().equals(Settings.getOrDefault(String.class, SETTING_PREFIX + "/path", DEFAULT_UPLOAD_PATH))) {
            WebPageContext page = new WebPageContext((ServletContext) null, request, response);

            String fileParam = page.param(String.class, FILE_PARAM);
            String storageName = page.param(String.class, STORAGE_PARAM);

            Object responseObject = StorageItemFilter.getParameter(request, fileParam, storageName);

            response.setContentType("application/json");
            page.write(ObjectUtils.toJson(responseObject));
            return;
        }

        chain.doFilter(request, response);
    }

    /**
     * Creates {@link StorageItem} from a request and request parameter.
     *
     * @param request
     *        Can't be {@code null}.
     * @param parameterName
     *        The parameter name for the file input. Can't be {@code null} or blank.
     * @param storageName
     *        Optionally accepts a storageName, will default to using {@code StorageItem.DEFAULT_STORAGE_SETTING}
     * @return the created {@link StorageItem}
     */
    public static StorageItem getParameter(HttpServletRequest request, String parameterName, String storageName) throws IOException {
        Preconditions.checkNotNull(request);
        Preconditions.checkArgument(!StringUtils.isBlank(parameterName));

        StorageItem storageItem = null;

        MultipartRequest mpRequest = MultipartRequestFilter.Static.getInstance(request);

        if (mpRequest != null) {
            FileItem item = mpRequest.getFileItem(parameterName);

            if (item != null) {
                if (item.isFormField()) {
                    storageItem = createStorageItem(mpRequest.getParameter(parameterName));
                } else {
                    storageItem = createStorageItem(item, StringUtils.isBlank(storageName)
                            ? Settings.get(String.class, StorageItem.DEFAULT_STORAGE_SETTING)
                            : storageName);
                }
            }
        } else {
            storageItem = createStorageItem(request.getParameter(parameterName));
        }

        return storageItem;
    }

    private static StorageItem createStorageItem(String jsonString) {
        Preconditions.checkNotNull(jsonString);
        Map<String, Object> json = Preconditions.checkNotNull(
                ObjectUtils.to(
                        new TypeReference<Map<String, Object>>() { },
                        ObjectUtils.fromJson(jsonString)));
        Object path = Preconditions.checkNotNull(json.get("path"));
        String storage = ObjectUtils
                .firstNonBlank(json.get("storage"), Settings.get(StorageItem.DEFAULT_STORAGE_SETTING))
                .toString();
        String contentType = ObjectUtils.to(String.class, json.get("contentType"));

        Map<String, Object> metadata = null;
        if (!ObjectUtils.isBlank(json.get("metadata"))) {
            metadata = ObjectUtils.to(
                    new TypeReference<Map<String, Object>>() { },
                    json.get("metadata"));
        }

        StorageItem storageItem = StorageItem.Static.createIn(storage);
        storageItem.setContentType(contentType);
        storageItem.setPath(path.toString());
        storageItem.setMetadata(metadata);

        return storageItem;
    }

    private static StorageItem createStorageItem(FileItem fileItem, String storageName) throws IOException {

        File file;
        try {
            file = File.createTempFile("cms.", ".tmp");
            fileItem.write(file);
        } catch (Exception e) {
            throw new IOException("Unable to write [" + (StringUtils.isBlank(fileItem.getName()) ? fileItem.getName() : "fileItem") + "] to temporary file.", e);
        }

        StorageItemUploadPart part = new StorageItemUploadPart();
        part.setContentType(fileItem.getContentType());
        part.setName(fileItem.getName());
        part.setFile(file);
        part.setStorageName(storageName);

        // Add additional beforeCreate logic by creating StorageItemBeforeCreate implementations
        beforeCreate(part);

        StorageItem storageItem = StorageItem.Static.createIn(part.getStorageName());
        storageItem.setContentType(part.getContentType());
        storageItem.setPath(createPath(part));
        storageItem.setData(new FileInputStream(file));

        if (storageItem instanceof AbstractStorageItem) {
            ((AbstractStorageItem) storageItem).setPart(part);
        }

        storageItem.save();

        return storageItem;
    }

    private static void beforeCreate(final StorageItemUploadPart part) {
        String fileName = Preconditions.checkNotNull(part.getName());

        Preconditions.checkState(part.getSize() > 0,
                "File [" + fileName + "] is empty");

        ClassFinder.findConcreteClasses(StorageItemBeforeCreate.class)
                .forEach(c -> {
                    try {
                        TypeDefinition.getInstance(c).newInstance().beforeCreate(part);
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                });
    }

    private static String createPath(StorageItemUploadPart part) {

        String pathGeneratorClassName = Settings.get(String.class, SETTING_PREFIX + "/" + part.getStorageName() + "/pathGenerator");

        Class<?> pathGeneratorClass = null;

        if (!StringUtils.isBlank(pathGeneratorClassName)) {
            pathGeneratorClass = ObjectUtils.getClassByName(pathGeneratorClassName);
        }

        StorageItemPathGenerator pathGenerator;

        if (pathGeneratorClass != null) {
            Object instance = TypeDefinition.getInstance(pathGeneratorClass).newInstance();
            Preconditions.checkState(
                    instance instanceof StorageItemPathGenerator,
                    "Class [" + pathGeneratorClass.getName() + "] does not implement StorageItemPathGenerator.");
            pathGenerator = (StorageItemPathGenerator) instance;
        } else {
            pathGenerator = TypeDefinition.getInstance(RandomUuidStorageItemPathGenerator.class).newInstance();
        }

        return pathGenerator.createPath(part.getName());
    }
}
