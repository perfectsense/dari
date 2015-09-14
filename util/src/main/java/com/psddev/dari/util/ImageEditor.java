package com.psddev.dari.util;

import java.util.Map;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

/**
 * Image editor can manipulate an {@linkplain StorageItem image item in a
 * storage system}. Typically, this is used to crop or resize an image.
 */
public interface ImageEditor extends SettingsBackedObject {

    /** Setting key for default image editor name. */
    public static final String DEFAULT_IMAGE_EDITOR_SETTING = "dari/defaultImageEditor";

    /** Setting key for all image editor configuration. */
    public static final String SETTING_PREFIX = "dari/imageEditor";

    /** Setting key for default Java image editor name. */
    public static final String JAVA_IMAGE_EDITOR_NAME = "_java";

    /** Common command string for cropping an image. */
    public static final String CROP_COMMAND = "crop";

    public static final String CROP_OPTION = "crop";
    public static final String CROP_OPTION_NONE = "none";
    public static final String CROP_OPTION_AUTOMATIC = "automatic";
    public static final String CROP_OPTION_CIRCLE = "circle";
    public static final String CROP_OPTION_STAR = "star";
    public static final String CROP_OPTION_STARBURST = "starburst";

    /** Common command string for resizing an image. */
    public static final String RESIZE_COMMAND = "resize";

    public static final String RESIZE_OPTION = "resize";
    public static final String RESIZE_OPTION_IGNORE_ASPECT_RATIO = "ignoreAspectRatio";
    public static final String RESIZE_OPTION_ONLY_SHRINK_LARGER = "onlyShrinkLarger";
    public static final String RESIZE_OPTION_ONLY_ENLARGE_SMALLER = "onlyEnlargeSmaller";
    public static final String RESIZE_OPTION_FILL_AREA = "fillArea";

    /** Returns the editor name. */
    public String getName();

    /** Sets the editor name. */
    public void setName(String name);

    /**
     * Edits the given {@code image} using the given {@code command},
     * {@code options}, and {@code arguments} and returns the modified image.
     */
    public StorageItem edit(
            StorageItem image,
            String command,
            Map<String, Object> options,
            Object... arguments);

    /**
     * {@linkplain ImageEditor Image editor} utility methods.
     *
     * <p>The factory methods, {@link #getInstance} and {@link #getDefault},
     * use {@linkplain Settings settings} to construct instances.
     *
     * <p>For the image editing functions, such as {@link #crop} and
     * {@link #resize}, the {@code editorName} parameter may be {@code null}
     * to use the default image editor.
     */
    public static final class Static {

        private static final LoadingCache<String, ImageEditor> INSTANCES = CacheBuilder
                .newBuilder()
                .build(new CacheLoader<String, ImageEditor>() {

            @Override
            public ImageEditor load(String name) {
                String settingsName = SETTING_PREFIX + "/" + name;
                ImageEditor instance = null;
                if (Settings.get(settingsName) != null) {
                    instance = Settings.newInstance(ImageEditor.class, settingsName);
                } else if (name.equals(JAVA_IMAGE_EDITOR_NAME)) {
                    instance = new JavaImageEditor();
                    ((JavaImageEditor) instance).initWatchService(null);
                }

                instance.setName(name);
                return instance;
            }
        });

        /** Returns the image editor with the given {@code name}. */
        public static ImageEditor getInstance(String name) {
            return INSTANCES.getUnchecked(name);
        }

        /** Returns the default image editor. */
        public static ImageEditor getDefault() {
            return getInstance(Settings.getOrDefault(String.class, DEFAULT_IMAGE_EDITOR_SETTING, JAVA_IMAGE_EDITOR_NAME));
        }

        /**
         * Safely returns an image editor with the given {@code name},
         * or if it's {@code null}, the default.
         */
        private static ImageEditor getInstanceOrDefault(String name) {
            ImageEditor editor;
            if (ObjectUtils.isBlank(name)) {
                editor = getDefault();
                if (editor == null) {
                    throw new IllegalStateException("No default image editor!");
                }
            } else {
                editor = getInstance(name);
                if (editor == null) {
                    throw new IllegalArgumentException(String.format(
                            "[%s] is not a valid image editor name!",
                            name));
                }
            }
            return editor;
        }

        /**
         * @deprecated Use {@link #crop(ImageEditor, StorageItem, Map, Integer, Integer, Integer, Integer)} instead.
         */
        @Deprecated
        public static StorageItem crop(String editorName, StorageItem image, Map<String, Object> options, Integer x, Integer y, Integer width, Integer height) {
            return crop(getInstanceOrDefault(editorName), image, options, x, y, width, height);
        }

        /**
         * @deprecated Use {@link #resize(ImageEditor, StorageItem, Map, Integer, Integer)} instead.
         */
        @Deprecated
        public static StorageItem resize(String editorName, StorageItem image, Map<String, Object> options, Integer width, Integer height) {
            return resize(getInstanceOrDefault(editorName), image, options, width, height);
        }

        /**
         * Crops the given {@code image} from the given {@code x} and {@code y}
         * by the given {@code width} and {@code height} using the given
         * {@code editor}. Uses the default if {@code editor} is null.
         */
        public static StorageItem crop(ImageEditor editor, StorageItem image, Map<String, Object> options, Integer x, Integer y, Integer width, Integer height) {
            if (editor == null) {
                editor = getInstanceOrDefault(null);
            }
            return editor.edit(image, CROP_COMMAND, options, x, y, width, height);
        }

        /**
         * Resizes the given {@code image} by the given {@code width} and
         * {@code height} using the given {@code editor}.  Uses the default if
         * {@code editor} is null.
         */
        public static StorageItem resize(ImageEditor editor, StorageItem image, Map<String, Object> options, Integer width, Integer height) {
            if (editor == null) {
                editor = getInstanceOrDefault(null);
            }
            return editor.edit(image, RESIZE_COMMAND, options, width, height);
        }

    }
}
