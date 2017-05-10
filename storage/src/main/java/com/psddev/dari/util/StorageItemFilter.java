package com.psddev.dari.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.servlet.FilterChain;
import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.google.common.base.Preconditions;
import org.apache.commons.fileupload.FileItem;

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
     * @param request  Can't be {@code null}.
     * @param response Can't be {@code null}.
     * @param chain    Can't be {@code null}.
     * @throws Exception
     */
    @Override
    protected void doRequest(HttpServletRequest request, HttpServletResponse response, FilterChain chain) throws Exception {

        if (request.getServletPath().equals(Settings.getOrDefault(String.class, SETTING_PREFIX + "/path", DEFAULT_UPLOAD_PATH))) {
            WebPageContext page = new WebPageContext((ServletContext) null, request, response);

            String fileParam = page.param(String.class, FILE_PARAM);
            String storageName = page.param(String.class, STORAGE_PARAM);

            Object responseObject = StorageItemFilter.getParameters(request, fileParam, storageName);

            if (responseObject != null && ((List) responseObject).size() == 1) {
                responseObject = ((List) responseObject).get(0);
            }

            response.setContentType("application/json");
            page.write(ObjectUtils.toJson(responseObject));
            return;
        }

        chain.doFilter(request, response);
    }

    /**
     * Creates {@link StorageItem} from a request and request parameter.
     *
     * @param request     Can't be {@code null}. May be multipart or otherwise.
     * @param parameterName   The parameter name for the file input. Can't be {@code null} or blank.
     * @param storageName Optionally accepts a storageName, will default to using {@code StorageItem.DEFAULT_STORAGE_SETTING}
     * @return the created {@link StorageItem}
     * @throws IOException
     */
    public static StorageItem getParameter(HttpServletRequest request, String parameterName, String storageName) throws IOException {
        List<StorageItem> storageItems = getParameters(request, parameterName, storageName);
        return !ObjectUtils.isBlank(storageItems) ? storageItems.get(0) : null;
    }

    /**
     * Creates a {@link List} of {@link StorageItem} from a request and request parameter.
     *
     * @param request     Can't be {@code null}. May be multipart or otherwise.
     * @param parameterName   The parameter name for the file inputs. Can't be {@code null} or blank.
     * @param storageName Optional storageName, will default to using {@code StorageItem.DEFAULT_STORAGE_SETTING}.
     * @return the {@link List} of created {@link StorageItem}(s).
     * @throws IOException
     */
    public static List<StorageItem> getParameters(HttpServletRequest request, String parameterName, String storageName) throws IOException {
        Preconditions.checkNotNull(request);
        Preconditions.checkArgument(!StringUtils.isBlank(parameterName));

        List<StorageItem> storageItems = new ArrayList<>();

        MultipartRequest mpRequest = MultipartRequestFilter.Static.getInstance(request);

        if (mpRequest != null) {

            FileItem[] items = mpRequest.getFileItems(parameterName);

            if (!ObjectUtils.isBlank(items)) {
                for (int i = 0; i < items.length; i++) {
                    FileItem item = items[i];

                    if (item == null) {
                        continue;
                    }

                    // handles input non-file input types in case of mixed input scenario
                    if (item.isFormField()) {
                        storageItems.add(createStorageItem(request.getParameterValues(parameterName)[i]));
                        continue;
                    }

                    // No file value found attached to input
                    if (StringUtils.isBlank(item.getName())) {
                        continue;
                    }

                    storageItems.add(createStorageItem(item, storageName));
                }
            }
        } else {
            String[] parameterValues = request.getParameterValues(parameterName);
            if (!ObjectUtils.isBlank(parameterValues)) {
                for (String json : parameterValues) {
                    storageItems.add(createStorageItem(json));

                }
            }
        }

        return storageItems;
    }

    private static StorageItem createStorageItem(String jsonString) {
        Preconditions.checkNotNull(jsonString);
        Map<String, Object> json = Preconditions.checkNotNull(
                ObjectUtils.to(
                        new TypeReference<Map<String, Object>>() { },
                        ObjectUtils.fromJson(jsonString)));

        Object storage = json.get("storage");
        StorageItem storageItem = StorageItem.Static.createIn(storage != null ? storage.toString() : null);
        new ObjectMap(storageItem).putAll(json);

        return storageItem;
    }

    private static StorageItem createStorageItem(FileItem fileItem, String storageName) throws IOException {

        File file = null;

        try {

            try {
                file = File.createTempFile("cms.", ".tmp");
                fileItem.write(file);
            } catch (Exception e) {
                throw new IOException("Unable to write [" + (StringUtils.isBlank(fileItem.getName()) ? fileItem.getName() : "fileItem") + "] to temporary file.", e);
            }

            StorageItemUploadPart part = new StorageItemUploadPart();
            part.setContentType(fileItem.getContentType());

            // Using StringUtils.getFileName as fileItem.getName may include the path
            part.setName(StringUtils.getFileName(fileItem.getName()));
            part.setFile(file);
            part.setStorageName(storageName);

            StorageItem storageItem = StorageItem.Static.createIn(part.getStorageName());
            storageItem.setContentType(part.getContentType());
            storageItem.setPath(createPath(part));
            storageItem.setData(new FileInputStream(file));

            // Add additional beforeSave functionality through StorageItemBeforeSave implementations
            ClassFinder.findConcreteClasses(StorageItemBeforeSave.class)
                    .forEach(c -> {
                        try {
                            TypeDefinition.getInstance(c).newInstance().beforeSave(storageItem, part);
                        } catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }
                    });

            storageItem.save();

            // Add additional afterSave functionality through StorageItemAfterSave implementations
            ClassFinder.findConcreteClasses(StorageItemAfterSave.class)
                    .forEach(c -> {
                        try {
                            TypeDefinition.getInstance(c).newInstance().afterSave(storageItem);
                        } catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }
                    });

            return storageItem;

        } finally {
            if (file != null && file.exists()) {
                file.delete();
            }
        }
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
