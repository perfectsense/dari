package com.psddev.dari.util;

import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import javax.imageio.ImageIO;

import org.imgscalr.Scalr;

public class ImageResizeStorageItemListener implements StorageItemAfterSave {

    @SuppressWarnings("unchecked")
    public static boolean overridePathWithNearestSize(StorageItem item, Integer width, Integer height) {
        Map<String, Object> metadata = item.getMetadata();

        if (metadata == null) {
            return false;
        }

        List<Object> items = (List<Object>) metadata.get("resizes");

        if (items == null || Settings.get(boolean.class, "dari/disableIntermediateImageSizes")) {
            return false;
        }

        for (Object object : items) {
            StorageItem resizedItem;

            if (object instanceof StorageItem) {
                resizedItem = (StorageItem) object;

            } else if (object instanceof Map) {
                Map<String, Object> map = (Map<String, Object>) object;
                String storage = ObjectUtils.to(String.class, map.get("storage"));
                resizedItem = StorageItem.Static.createIn(storage);
                new ObjectMap(resizedItem).putAll(map);

            } else {
                continue;
            }

            // Make sure the "resizes" StorageItems are of the same type of
            // storage.
            if (!resizedItem.getStorage().equals(item.getStorage())) {
                continue;
            }

            Map<String, Object> resizedMetadata = resizedItem.getMetadata();

            if (resizedMetadata != null && !resizedMetadata.isEmpty()) {
                int w = ObjectUtils.to(Integer.class, resizedMetadata.get("width"));
                int h = ObjectUtils.to(Integer.class, resizedMetadata.get("height"));

                if ((width != null && width < w) && (height != null && height < h)) {
                    item.setPath((String) resizedItem.getPath());
                    metadata.put("width", w);
                    metadata.put("height", h);
                    return true;
                }
            }
        }

        return false;
    }

    public void afterSave(StorageItem item) throws IOException {
        if (item.getPublicUrl().startsWith("file://")) {
            return;
        }

        String contentType = item.getContentType();
        InputStream data = item.getData();
        try {
            if (contentType != null && contentType.startsWith("image/")) {
                BufferedImage original = ImageIO.read(data);

                if (original == null) {
                    return;
                }

                String imageType = contentType.substring(6);

                List<StorageItem> dimsItems = new ArrayList<StorageItem>();
                item.getMetadata().put("resizes", dimsItems);

                processSize(item, 500, original, imageType, dimsItems);
                processSize(item, 1500, original, imageType, dimsItems);
            }
        } finally {
            if (data != null) {
                data.close();
            }

            item.setData(null);
        }
    }

    private void processSize(StorageItem item, int newSize, BufferedImage original, String imageType, List<StorageItem> items) throws IOException {
        int width = original.getWidth();
        int height = original.getHeight();
        float aspect = (float) width / (float) height;
        if (width > newSize || height > newSize) {
            if (aspect > 1.0) {
                width = newSize;
                height = Math.round(width / aspect);
            } else {
                height = newSize;
                width = Math.round(height * aspect);
            }

            String method = Settings.getOrDefault(String.class, "dari/intermediateImageSizeQuality", "AUTOMATIC");
            BufferedImage resizedImage = Scalr.resize(original, Scalr.Method.valueOf(method), width, height);
            String url = item.getPath();
            List<String> parts = Arrays.asList(url.split("/"));

            StringBuilder pathBuilder = new StringBuilder();
            pathBuilder.append(StringUtils.join(parts.subList(0, parts.size() - 1), "/"));
            pathBuilder.append("/resizes/");
            pathBuilder.append(newSize);
            pathBuilder.append('/');
            pathBuilder.append(parts.get(parts.size() - 1));

            StorageItem dimsItem = StorageItem.Static.create();
            StorageItem.Static.resetListeners(dimsItem);

            dimsItem.setPath(pathBuilder.toString());
            dimsItem.setContentType(item.getContentType());

            ByteArrayOutputStream os = new ByteArrayOutputStream();
            ImageIO.write(resizedImage, imageType, os);
            InputStream is = new ByteArrayInputStream(os.toByteArray());

            dimsItem.getMetadata().put("width", width);
            dimsItem.getMetadata().put("height", height);
            dimsItem.setData(is);
            dimsItem.save();

            items.add(dimsItem);
        }
    }
}
