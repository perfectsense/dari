package com.psddev.dari.util;

import java.awt.AlphaComposite;
import java.awt.Color;
import java.awt.Graphics;
import java.awt.Graphics2D;
import java.awt.Polygon;
import java.awt.RenderingHints;
import java.awt.geom.Ellipse2D;
import java.awt.image.BufferedImage;
import java.awt.image.BufferedImageOp;
import java.awt.image.ConvolveOp;
import java.awt.image.Kernel;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.WatchService;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.servlet.http.HttpServletRequest;
import org.imgscalr.Scalr;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Image editor built on {@link javax.imageio.ImageIO}.
 * ImageIO's image type compatibly can be expanded with the inclusion of
 * <a href="https://github.com/haraldk/TwelveMonkeys">TwelveMonkeys</a>.
 *
 * <pre>
 *
 * Example usage of TwelveMonkeys :
 * </pre>
 *
 * <pre>
 * <strong><u>TwelveMonkeys' listener that implements dynamic loading and unloading of ImageIO plugins for web applications</u></strong>
 * {@code
 *
 * <listener>
 *    <display-name>ImageIO service provider loader/unloader</display-name>
 *    <listener-class>com.twelvemonkeys.servlet.image.IIOProviderContextListener</listener-class>
 * </listener>
 * }
 * </pre>
 *
 * <pre>
 * <strong><u>TwelveMonkeys' dependencies</u></strong>
 * {@code
 *
 * <properties>
 *    <twelvemonkeys.version>3.2.1</twelvemonkeys.version>
 * </properties>
 *
 * <dependency>
 *    <groupId>com.twelvemonkeys.common</groupId>
 *    <artifactId>common-io</artifactId>
 *    <version>${twelvemonkeys.version}</version>
 * </dependency>
 * <dependency>
 *    <groupId>com.twelvemonkeys.common</groupId>
 *    <artifactId>common-lang</artifactId>
 *    <version>${twelvemonkeys.version}</version>
 * </dependency>
 * <dependency>
 *    <groupId>com.twelvemonkeys.common</groupId>
 *    <artifactId>common-image</artifactId>
 *    <version>${twelvemonkeys.version}</version>
 * </dependency>
 * <dependency>
 *    <groupId>com.twelvemonkeys.imageio</groupId>
 *    <artifactId>imageio-core</artifactId>
 *    <version>${twelvemonkeys.version}</version>
 * </dependency>
 * <dependency>
 *     <groupId>com.twelvemonkeys.imageio</groupId>
 *     <artifactId>imageio-metadata</artifactId>
 *     <version>${twelvemonkeys.version}</version>
 * </dependency>
 * <dependency>
 *     <groupId>com.twelvemonkeys.imageio</groupId>
 *     <artifactId>imageio-jpeg</artifactId>
 *     <version>${twelvemonkeys.version}</version>
 * </dependency>
 * <dependency>
 *     <groupId>com.twelvemonkeys.imageio</groupId>
 *     <artifactId>imageio-tiff</artifactId>
 *     <version>${twelvemonkeys.version}</version>
 * </dependency>
 * <dependency>
 *     <groupId>com.twelvemonkeys.servlet</groupId>
 *     <artifactId>servlet</artifactId>
 *     <version>${twelvemonkeys.version}</version>
 * </dependency>
 * }
 * </pre>
 */
public class JavaImageEditor extends AbstractUrlImageEditor {

    private static final String DEFAULT_IMAGE_FORMAT = "png";
    private static final String DEFAULT_IMAGE_CONTENT_TYPE = "image/" + DEFAULT_IMAGE_FORMAT;
    private static final String ORIGINAL_WIDTH_METADATA_PATH = "image/originalWidth";
    private static final String ORIGINAL_HEIGHT_METADATA_PATH = "image/originalHeight";
    private static final List<String> EXTRA_CROPS = Arrays.asList(CROP_OPTION_CIRCLE, CROP_OPTION_STAR, CROP_OPTION_STARBURST);
    private static final Logger LOGGER = LoggerFactory.getLogger(JavaImageEditor.class);
    public static final long MEGA_BYTE = 1048576;

    /** Setting key for quality to use for the output images. */
    private static final String QUALITY_SETTING = "quality";

    protected static final String TIFF_READER_CLASS = "com.sun.media.imageioimpl.plugins.tiff.TIFFImageReaderSpi";
    protected static final String THUMBNAIL_COMMAND = "thumbnail";

    private Scalr.Method quality = Scalr.Method.AUTOMATIC;
    private String basePath;
    private String sharedSecret;
    private String errorImage;
    private boolean disableCache;
    private String cachePath;
    private Long cacheLimitInMegaBytes = 500L;

    public Scalr.Method getQuality() {
        return quality;
    }

    public void setQuality(Scalr.Method quality) {
        this.quality = quality;
    }

    public String getBasePath() {
        if (StringUtils.isBlank(basePath) && !(StringUtils.isBlank(baseUrl))) {
            basePath = baseUrl.substring(baseUrl.indexOf("//") + 2);
            basePath = basePath.substring(basePath.indexOf("/") + 1);
            if (!basePath.endsWith("/")) {
                basePath += "/";
            }
        }
        return basePath;
    }

    public String getSharedSecret() {
        return sharedSecret;
    }

    public void setSharedSecret(String sharedSecret) {
        this.sharedSecret = sharedSecret;
    }

    public String getErrorImage() {
        return errorImage;
    }

    public void setErrorImage(String errorImage) {
        this.errorImage = errorImage;
    }

    public boolean isDisableCache() {
        return disableCache;
    }

    public void setDisableCache(boolean disableCache) {
        this.disableCache = disableCache;
    }

    public String getCachePath() {
        if (StringUtils.isBlank(cachePath)) {
            String property = "java.io.tmpdir";
            String tempDir = System.getProperty(property);
            if (!StringUtils.isBlank(tempDir)) {
                cachePath = tempDir;
            }
        }
        return cachePath;
    }

    public void setCachePath(String cachePath) {
        this.cachePath = cachePath;
    }

    public Long getCacheLimitInMegaBytes() {
        return cacheLimitInMegaBytes;
    }

    public void setCacheLimitInMegaBytes(Long cacheLimitInMegaBytes) {
        this.cacheLimitInMegaBytes = cacheLimitInMegaBytes;
    }

    @Override
    public String getBaseUrl() {

        String baseUrl = this.baseUrl;

        if (StringUtils.isBlank(baseUrl)) {
            baseUrl = getBaseUrlFromRequest(PageContextFilter.Static.getRequestOrNull());
        }

        if (StringUtils.isBlank(baseUrl)) {
            return super.getBaseUrl();
        }

        return baseUrl;
    }

    @Override
    public StorageItem edit(StorageItem storageItem, String command, Map<String, Object> options, Object... arguments) {

        if (ImageEditor.CROP_COMMAND.equals(command)
                && options != null
                && options.containsKey(ImageEditor.CROP_OPTION)
                && options.get(ImageEditor.CROP_OPTION).equals(ImageEditor.CROP_OPTION_NONE)) {
            return storageItem;
        }

        String imageUrl = new OriginStorageItem(storageItem).getPublicUrl();
        List<String> commands = new ArrayList<String>();

        boolean usePrivateUrl = false;
        if (options != null && options.containsKey(ImageEditorPrivateUrl.PRIVATE_URL_OPTION)
                && ObjectUtils.to(Boolean.class, options.get(ImageEditorPrivateUrl.PRIVATE_URL_OPTION))) {
            usePrivateUrl = true;
        }

        String privateBaseUrl = usePrivateUrl ? getPrivateBaseUrl() : null;
        String baseUrl = privateBaseUrl != null ? privateBaseUrl : getBaseUrl();

        if (imageUrl.startsWith(baseUrl) && imageUrl.contains("?url=")) {
            String[] imageComponents = imageUrl.split("\\?url=");
            imageUrl = StringUtils.decodeUri(imageComponents[1]);

            String path = imageComponents[0].substring(baseUrl.length());
            for (String parameter : path.split("/")) {
                if (!EXTRA_CROPS.contains(parameter)) {
                    commands.add(parameter);
                }
            }

            if (!StringUtils.isBlank(this.getSharedSecret())) {
                commands = commands.subList(2, commands.size());
            }
        }

        Object cropOption = options != null ? options.get(ImageEditor.CROP_OPTION) : null;

        Dimension originalDimension = null;
        Dimension outputDimension = null;

        Map<String, Object> oldMetadata = storageItem.getMetadata();
        if (oldMetadata != null) {
            Integer originalWidth = null;
            Integer originalHeight = null;
            // grab the original width and height of the image
            originalWidth = ObjectUtils.to(Integer.class,
                    CollectionUtils.getByPath(oldMetadata, ORIGINAL_WIDTH_METADATA_PATH));
            if (originalWidth == null) {
                originalWidth = ObjectUtils.to(Integer.class, oldMetadata.get("width"));
            }

            originalHeight = ObjectUtils.to(Integer.class,
                    CollectionUtils.getByPath(oldMetadata, ORIGINAL_HEIGHT_METADATA_PATH));
            if (originalHeight == null) {
                originalHeight = ObjectUtils.to(Integer.class, oldMetadata.get("height"));
            }
            originalDimension = new Dimension(originalWidth, originalHeight);
        }

        if (ImageEditor.CROP_COMMAND.equals(command)
                && (cropOption != null && cropOption.equals(CROP_OPTION_AUTOMATIC)
                || (ObjectUtils.isBlank(arguments) || arguments.length < 2)
                || (ObjectUtils.to(Integer.class, arguments[0]) == null
                && ObjectUtils.to(Integer.class, arguments[1]) == null))) {
            commands.add(THUMBNAIL_COMMAND);
            command = RESIZE_COMMAND;
            if (arguments.length > 3) {
                arguments[0] = arguments[2];
                arguments[1] = arguments[3];
            } else if (arguments.length > 2) {
                arguments[0] = arguments[2];
                arguments[1] = null;
            }
        } else {
            commands.add(command);
        }

        if (ImageEditor.CROP_COMMAND.equals(command)) {
            Integer width = ObjectUtils.to(Integer.class, arguments[2]);
            Integer height = ObjectUtils.to(Integer.class, arguments[3]);

                commands.add(arguments[0] + "x" + arguments[1] + "x" + width + "x" + height);
            if (originalDimension != null) {
                outputDimension = new Dimension(originalDimension.width != null ? Math.min(originalDimension.width, width) : width,
                                                originalDimension.height != null ? Math.min(originalDimension.height, height) : height);
            }

        } else if (ImageEditor.RESIZE_COMMAND.equals(command)) {
            Integer width =  !ObjectUtils.isBlank(arguments) && arguments.length > 0 ? ObjectUtils.to(Integer.class, arguments[0]) : null;
            Integer height = !ObjectUtils.isBlank(arguments) && arguments.length > 1 ? ObjectUtils.to(Integer.class, arguments[1]) : null;

            StringBuilder resizeBuilder = new StringBuilder();
            if (width != null) {
                resizeBuilder.append(width);
            }
            resizeBuilder.append("x");
            if (height != null) {
                resizeBuilder.append(height);
            }
            Object resizeOption = options != null ? options.get(ImageEditor.RESIZE_OPTION) : null;

            if (resizeOption != null
                    && (cropOption == null || !cropOption.equals(ImageEditor.CROP_OPTION_AUTOMATIC))) {
                if (resizeOption.equals(ImageEditor.RESIZE_OPTION_IGNORE_ASPECT_RATIO)) {
                    resizeBuilder.append("!");
                } else if (resizeOption.equals(ImageEditor.RESIZE_OPTION_ONLY_SHRINK_LARGER)) {
                    resizeBuilder.append(">");
                    if (originalDimension != null) {
                        if (width != null && height != null) {
                            outputDimension = new Dimension(originalDimension.width != null ? Math.min(originalDimension.width, width) : width,
                                                            originalDimension.height != null ? Math.min(originalDimension.height, height) : height);
                        } else {
                            outputDimension = getResizeDimension(originalDimension.width, originalDimension.height, width, height);
                        }
                    }
                } else if (resizeOption.equals(ImageEditor.RESIZE_OPTION_ONLY_ENLARGE_SMALLER)) {
                    resizeBuilder.append("<");
                    if (originalDimension != null) {
                        if (width != null && height != null) {
                            outputDimension = new Dimension(originalDimension.width != null ? Math.max(originalDimension.width, width) : width,
                                                            originalDimension.height != null ? Math.max(originalDimension.height, height) : height);
                        } else {
                            outputDimension = getResizeDimension(originalDimension.width, originalDimension.height, width, height);
                        }
                    }
                } else if (resizeOption.equals(ImageEditor.RESIZE_OPTION_FILL_AREA)) {
                    resizeBuilder.append("^");
                }
            }

            if (originalDimension != null) {
                if (width != null && height != null && (resizeOption == null
                        || resizeOption.equals(ImageEditor.RESIZE_OPTION_IGNORE_ASPECT_RATIO)
                        || resizeOption.equals(ImageEditor.RESIZE_OPTION_FILL_AREA))) {
                    outputDimension = new Dimension(originalDimension.width != null ? Math.min(originalDimension.width, width) : width,
                                                    originalDimension.height != null ? Math.min(originalDimension.height, height) : height);
                } else if (resizeOption == null) {
                    outputDimension = getResizeDimension(originalDimension.width, originalDimension.height, width, height);
                }
            }
            commands.add(resizeBuilder.toString());

        } else if (command.equals("blur") && arguments[0] instanceof List) {
            for (Object blur : (List) arguments[0]) {
                storageItem = this.edit(storageItem, "blur", null, blur);
            }
            return storageItem;
        } else if (!ObjectUtils.isBlank(arguments)) {
            commands.add(ObjectUtils.to(String.class, arguments[0]));
        }

        if (cropOption != null
                && EXTRA_CROPS.contains(cropOption)) {
            commands.add(ObjectUtils.to(String.class, cropOption));
        }

        StringBuilder storageItemUrlBuilder = new StringBuilder();
        storageItemUrlBuilder.append(baseUrl);
        if (!StringUtils.isBlank(this.getSharedSecret())) {
            StringBuilder commandsBuilder = new StringBuilder();

            for (String parameter : commands) {
                commandsBuilder.append(StringUtils.encodeUri(parameter))
                               .append('/');
            }

            Long expireTs = (long) Integer.MAX_VALUE;
            String signature = expireTs + this.getSharedSecret() + StringUtils.decodeUri("/" + commandsBuilder.toString()) + imageUrl;

            String md5Hex = StringUtils.hex(StringUtils.md5(signature));
            String requestSig = md5Hex.substring(0, 7);

            storageItemUrlBuilder.append(requestSig)
                .append("/")
                .append(expireTs.toString())
                .append("/");
        }

        for (String parameter : commands) {
            storageItemUrlBuilder.append(parameter)
                .append("/");
        }

        storageItemUrlBuilder.append("?url=")
                .append(StringUtils.encodeUri(imageUrl));

        UrlStorageItem newStorageItem = StorageItem.Static.createUrl(storageItemUrlBuilder.toString());
        String contentType = DEFAULT_IMAGE_CONTENT_TYPE;
        if (storageItem.getContentType() != null && storageItem.getContentType().contains("/")) {
            contentType = storageItem.getContentType();
        }

        newStorageItem.setContentType(contentType);

        Map<String, Object> metadata = new HashMap<String, Object>();

        // store the new width and height in the metadata map
        if (outputDimension != null && outputDimension.width != null) {
            metadata.put("width", outputDimension.width);
        }
        if (outputDimension != null && outputDimension.height != null) {
            metadata.put("height", outputDimension.height);
        }

        // store the original width and height in the map for use with future image edits.
        if (originalDimension != null && originalDimension.width != null) {
            CollectionUtils.putByPath(metadata, ORIGINAL_WIDTH_METADATA_PATH, originalDimension.width);
        }
        if (originalDimension != null && originalDimension.height != null) {
            CollectionUtils.putByPath(metadata, ORIGINAL_HEIGHT_METADATA_PATH, originalDimension.height);
        }

        newStorageItem.setMetadata(metadata);

        return newStorageItem;
    }

    @Override
    public void initialize(String settingsKey, Map<String, Object> settings) {
        super.initialize(settingsKey, settings);

        if (!ObjectUtils.isBlank(settings.get(QUALITY_SETTING))) {
            Object qualitySetting = settings.get(QUALITY_SETTING);

            if (qualitySetting instanceof Integer) {
                Integer qualityInteger = ObjectUtils.to(Integer.class, qualitySetting);
                quality = findQualityByInteger(qualityInteger);
            } else if (qualitySetting instanceof String) {
                quality = Scalr.Method.valueOf(ObjectUtils.to(String.class, qualitySetting));
            }
        }

        if (!ObjectUtils.isBlank(settings.get("sharedSecret"))) {
            setSharedSecret(ObjectUtils.to(String.class, settings.get("sharedSecret")));
        }

        if (!ObjectUtils.isBlank(settings.get("errorImage"))) {
            setErrorImage(ObjectUtils.to(String.class, settings.get("errorImage")));
        }

        if (!ObjectUtils.isBlank(settings.get("disableCache"))) {
            setDisableCache(ObjectUtils.to(Boolean.class, settings.get("disableCache")));
        }

        if (!disableCache) {
            if (!ObjectUtils.isBlank(settings.get("cachePath"))) {
                setCachePath(ObjectUtils.to(String.class, settings.get("cachePath")));
            }

            if (!ObjectUtils.isBlank(settings.get("cacheLimitInMegaBytes"))) {
                setCacheLimitInMegaBytes(ObjectUtils.to(Long.class, settings.get("cacheLimitInMegaBytes")));
            }

            initWatchService(getCacheLimitInMegaBytes() * MEGA_BYTE);

        }

    }

    public void initWatchService(Long maximumCacheSizeInBytes) {

        if (maximumCacheSizeInBytes == null) {
            maximumCacheSizeInBytes = cacheLimitInMegaBytes * MEGA_BYTE;
        }

        WatchService watcher;
        try {
            watcher = FileSystems.getDefault().newWatchService();
            JavaImageDirectoryWatch javaImageDirectoryWatch = new JavaImageDirectoryWatch(this, watcher, maximumCacheSizeInBytes);
            Thread javaImageDirectoryWatchThread = new Thread(javaImageDirectoryWatch);
            javaImageDirectoryWatchThread.start();

        } catch (IOException ex) {
            LOGGER.error("Unable to init Java Image Directory Watch", ex);
        }
    }

    protected String getBaseUrlFromRequest(HttpServletRequest request) {

        StringBuilder baseUrlBuilder = new StringBuilder();

        if (request != null) {
            baseUrlBuilder.append("http");
            if (request.isSecure()) {
                baseUrlBuilder.append("s");
            }

            baseUrlBuilder.append("://")
                    .append(request.getServerName());

            if (request.getServerPort() != 80 && request.getServerPort() != 443) {
                baseUrlBuilder.append(":")
                        .append(request.getServerPort());
            }
        }

        baseUrlBuilder.append(JavaImageServlet.SERVLET_PATH);
        return baseUrlBuilder.toString();
    }

    protected Scalr.Method findQualityByInteger(Integer quality) {
        if (quality >= 80) {
            return Scalr.Method.ULTRA_QUALITY;
        } else if (quality >= 60) {
            return Scalr.Method.QUALITY;
        } else if (quality >= 40) {
            return Scalr.Method.AUTOMATIC;
        } else if (quality >= 20) {
            return Scalr.Method.BALANCED;
        } else {
            return Scalr.Method.SPEED;
        }
    }

    /** Helper class so that width and height can be returned in a single object */
    protected static class Dimension {
        public final Integer width;
        public final Integer height;
        public Dimension(Integer width, Integer height) {
            this.width = width;
            this.height = height;
        }
    }

    public BufferedImage reSize(BufferedImage bufferedImage, Integer width, Integer height, String option, Scalr.Method quality) {
        if (quality == null) {
            quality = this.quality;
        }
        if (width != null || height != null) {
            if (!StringUtils.isBlank(option)
                    && option.equals(ImageEditor.RESIZE_OPTION_ONLY_SHRINK_LARGER)) {
                if ((height == null && width >= bufferedImage.getWidth())
                        || (width == null && height >= bufferedImage.getHeight())
                        || (width != null && height != null && width >= bufferedImage.getWidth() && height >= bufferedImage.getHeight())) {
                    return bufferedImage;
                }

            } else if (!StringUtils.isBlank(option)
                    && option.equals(ImageEditor.RESIZE_OPTION_ONLY_ENLARGE_SMALLER)) {
                if ((height == null && width <= bufferedImage.getWidth())
                        || (width == null && height <= bufferedImage.getHeight())
                        || (width != null && height != null && (width <= bufferedImage.getWidth() || height <= bufferedImage.getHeight()))) {
                    return bufferedImage;
                }
            }

            if (StringUtils.isBlank(option)
                    || option.equals(ImageEditor.RESIZE_OPTION_ONLY_SHRINK_LARGER)
                    || option.equals(ImageEditor.RESIZE_OPTION_ONLY_ENLARGE_SMALLER)) {
                if (height == null) {
                    return Scalr.resize(bufferedImage, quality, Scalr.Mode.FIT_TO_WIDTH, width);
                } else if (width == null) {
                    return Scalr.resize(bufferedImage, quality, Scalr.Mode.FIT_TO_HEIGHT, height);
                } else {
                    return Scalr.resize(bufferedImage, quality, width, height);
                }

            } else if (height != null && width != null) {
                if (option.equals(ImageEditor.RESIZE_OPTION_IGNORE_ASPECT_RATIO)) {
                    return Scalr.resize(bufferedImage, quality, Scalr.Mode.FIT_EXACT, width, height);
                } else if (option.equals(ImageEditor.RESIZE_OPTION_FILL_AREA)) {
                    Dimension dimension = getFillAreaDimension(bufferedImage.getWidth(), bufferedImage.getHeight(), width, height);
                    return Scalr.resize(bufferedImage, quality, Scalr.Mode.FIT_EXACT, dimension.width, dimension.height);
                }

            }

        }
        return null;
    }

    public BufferedImage crop(BufferedImage bufferedImage, Integer x, Integer y, Integer width, Integer height) {

        if (width != null || height != null) {
            if (height == null) {
                height = (int) ((double) bufferedImage.getHeight() / (double) bufferedImage.getWidth() * (double) width);
            } else if (width == null) {
                width = (int) ((double) bufferedImage.getWidth() / (double) bufferedImage.getHeight() * (double) height);
            }

            if (x == null) {
                x = bufferedImage.getWidth() / 2;
            }

            if (y == null) {
                y = bufferedImage.getHeight() / 2;
            }

            if (x + width > bufferedImage.getWidth()) {
                width = bufferedImage.getWidth() - x;
            }

            if (y + height > bufferedImage.getHeight()) {
                height = bufferedImage.getHeight() - y;
            }

            return Scalr.crop(bufferedImage, x, y, width, height);
        }

        return null;
    }

    public BufferedImage grayscale(BufferedImage sourceImage) {
        BufferedImage resultImage = new BufferedImage(sourceImage.getWidth(), sourceImage.getHeight(), BufferedImage.TYPE_BYTE_GRAY);
        Graphics g = resultImage.getGraphics();
        g.drawImage(sourceImage, 0, 0, null);
        g.dispose();
        return resultImage;
    }

    public BufferedImage brightness(BufferedImage sourceImage, int brightness, int contrast) {
        BufferedImage resultImage =  new BufferedImage(sourceImage.getWidth(), sourceImage.getHeight(), sourceImage.getType());

        int multiply = 100;
        int add;

        if (contrast == 0) {
            add = Math.round(brightness / 100.0f * 255);
        } else {
            if (contrast > 0) {
                contrast = contrast * 4;
            }
            contrast = 100 - (contrast * -1);
            multiply = contrast;
            brightness = Math.round(brightness / 100.0f * 255);

            add = ((Double) (((brightness - 128) * (multiply / 100.0d) + 128))).intValue();

        }

        for (int x = 0; x < sourceImage.getWidth(); x++) {
            for (int y = 0; y < sourceImage.getHeight(); y++) {
                int rgb = sourceImage.getRGB(x, y);
                int alpha = (rgb >> 24) & 0xFF;
                int red   = (rgb >> 16) & 0xFF;
                int green = (rgb >> 8) & 0xFF;
                int blue  = rgb & 0xFF;

                red = adjustColor(red, multiply, add);
                green = adjustColor(green, multiply, add);
                blue = adjustColor(blue, multiply, add);

                int newRgb = (alpha << 24) | (red << 16) | (green << 8) | blue;

                resultImage.setRGB(x, y, newRgb);
            }
        }

        return resultImage;
    }

    /**
     *
     * @param sourceImage
     * @param amount recommended amount is between 1 and 10
     * @return
     */
    public BufferedImage sharpen(BufferedImage sourceImage, int amount) {
        BufferedImage resultImage =  new BufferedImage(sourceImage.getWidth(), sourceImage.getHeight(), sourceImage.getType());
        float sharpenAmmount = amount * 0.2f;

        float[] data = new float[]{
                0.0f, -sharpenAmmount, 0.0f,
                -sharpenAmmount, 4f * sharpenAmmount + 1f, -sharpenAmmount,
                0.0f, -sharpenAmmount, 0.0f
        };

        Kernel kernel = new Kernel(3, 3, data);

        BufferedImageOp bufferedImageOp = new ConvolveOp(kernel);
        bufferedImageOp.filter(sourceImage, resultImage);

        return resultImage;
    }

    /**
     *
     * @param sourceImage
     * @param amount recommended amount is between 1 and 10
     * @param x
     * @param y
     * @param width
     * @param height
     * @return
     */
    public BufferedImage blurArea(BufferedImage sourceImage, int amount, int x, int y, int width, int height) {
        BufferedImage resultImage = new BufferedImage(sourceImage.getWidth(), sourceImage.getHeight(), sourceImage.getType());
        Graphics2D graphics = resultImage.createGraphics();

        graphics.drawImage(sourceImage, null, 0, 0);

        BufferedImage blurArea = blur(sourceImage.getSubimage(x, y, width, height), amount);
        graphics.drawImage(blurArea, null, x, y);
        graphics.dispose();

        return resultImage;
    }

    /**
     *
     * @param sourceImage
     * @param amount recommended amount is between 1 and 10
     * @return
     */
    public BufferedImage blur(BufferedImage sourceImage, int amount) {
        BufferedImage horizontalBlur = new BufferedImage(sourceImage.getWidth(), sourceImage.getHeight(), sourceImage.getType());
        BufferedImage resultImage = new BufferedImage(sourceImage.getWidth(), sourceImage.getHeight(), sourceImage.getType());

        //A good blur is between 10 and 20
        getGaussianBlurFilter(amount + 10, true).filter(sourceImage, horizontalBlur);
        getGaussianBlurFilter(amount + 10, false).filter(horizontalBlur, resultImage);

        return resultImage;
    }

    public BufferedImage flipHorizontal(BufferedImage sourceImage) {
        return Scalr.rotate(sourceImage, Scalr.Rotation.FLIP_HORZ);
    }

    public BufferedImage flipVertical(BufferedImage sourceImage) {
        return Scalr.rotate(sourceImage, Scalr.Rotation.FLIP_VERT);
    }

    public BufferedImage invert(BufferedImage sourceImage) {
        BufferedImage resultImage =  new BufferedImage(sourceImage.getWidth(), sourceImage.getHeight(), sourceImage.getType());

        for (int x = 0; x < sourceImage.getWidth(); x++) {
            for (int y = 0; y < sourceImage.getHeight(); y++) {
                int rgb = sourceImage.getRGB(x, y);
                int alpha = (rgb >> 24) & 0xFF;
                int red   = 255 - (rgb >> 16) & 0xFF;
                int green = 255 - (rgb >> 8) & 0xFF;
                int blue  = 255 - rgb & 0xFF;

                int newRgb = (alpha << 24) | (red << 16) | (green << 8) | blue;

                resultImage.setRGB(x, y, newRgb);
            }
        }

        return resultImage;
    }

    public BufferedImage rotate(BufferedImage sourceImage, int degrees) {
        Scalr.Rotation rotation;
        if (degrees == 90) {
            rotation = Scalr.Rotation.CW_90;
        } else if (degrees == 180) {
            rotation = Scalr.Rotation.CW_180;
        } else if (degrees == 270 || degrees == -90) {
            rotation = Scalr.Rotation.CW_270;
        } else {
            double radians = Math.toRadians(degrees);
            int w = (new Double(Math.abs(sourceImage.getWidth() * Math.cos(radians)) + Math.abs(sourceImage.getHeight() * Math.sin(radians)))).intValue();
            int h = (new Double(Math.abs(sourceImage.getWidth() * Math.sin(radians)) + Math.abs(sourceImage.getHeight() * Math.cos(radians)))).intValue();

            BufferedImage resultImage = new BufferedImage(w, h, sourceImage.getType());
            Graphics2D graphics = resultImage.createGraphics();

            graphics.setColor(Color.WHITE);
            graphics.fillRect(0, 0, w, h);

            int x = -1 * (sourceImage.getWidth() - w) / 2;
            int y = -1 * (sourceImage.getHeight() - h) / 2;

            graphics.rotate(Math.toRadians(degrees), (w / 2), (h / 2));
            graphics.drawImage(sourceImage, null, x, y);
            return resultImage;
        }
        return Scalr.rotate(sourceImage, rotation);
    }

    public BufferedImage sepia(BufferedImage sourceImage) {
        BufferedImage resultImage = new BufferedImage(sourceImage.getWidth(), sourceImage.getHeight(), sourceImage.getType());

        for (int x = 0; x < sourceImage.getWidth(); x++) {
            for (int y = 0; y < sourceImage.getHeight(); y++) {
                int rgb = sourceImage.getRGB(x, y);
                int alpha = (rgb >> 24) & 0xFF;
                int red   = (rgb >> 16) & 0xFF;
                int green = (rgb >> 8) & 0xFF;
                int blue  = rgb & 0xFF;

                int newRed = (new Double((red * .393) + (green * .769) + (blue * .189))).intValue();
                int newGreen = (new Double((red * .349) + (green * .686) + (blue * .168))).intValue();
                int newBlue = (new Double((red * .272) + (green * .534) + (blue * .131))).intValue();

                newRed = colorMinMax(newRed);
                newGreen = colorMinMax(newGreen);
                newBlue = colorMinMax(newBlue);

                int newRgb = (alpha << 24) | (newRed << 16) | (newGreen << 8) | newBlue;

                resultImage.setRGB(x, y, newRgb);
            }
        }

        return resultImage;
    }

    public BufferedImage circle(BufferedImage sourceImage) {
        return roundCorners(sourceImage, 20);
    }

    public BufferedImage roundCorners(BufferedImage sourceImage, int cornerRadius) {
        int w = sourceImage.getWidth();
        int h = sourceImage.getHeight();
        int cropX = 0;
        int cropY = 0;

        if (h > w) {
            h = w;
            cropY = ((sourceImage.getHeight() - h) / 2);
        } else if (w > h) {
            w = h;
            cropX = ((sourceImage.getWidth() - w) / 2);
        }

        sourceImage = sourceImage.getSubimage(cropX, cropY, w, h);

        BufferedImage resultImage = new BufferedImage(w, h, BufferedImage.TYPE_INT_ARGB);

        Graphics2D g2 = resultImage.createGraphics();
        g2.setComposite(AlphaComposite.Src);
        g2.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON);
        g2.setColor(Color.WHITE);
        g2.fill(new Ellipse2D.Float(0, 0, w , h));

        // using the white shape from above as alpha source
        g2.setComposite(AlphaComposite.SrcAtop);
        g2.drawImage(sourceImage, 0, 0, null);

        g2.dispose();

        return resultImage;
    }

    public BufferedImage star(BufferedImage sourceImage) {
        int w = sourceImage.getWidth();
        int h = sourceImage.getHeight();
        int cropX = 0;
        int cropY = 0;

        if (h > w) {
            h = w;
            cropY = ((sourceImage.getHeight() - h) / 2);
        } else if (w > h) {
            w = h;
            cropX = ((sourceImage.getWidth() - w) / 2);
        }

        sourceImage = sourceImage.getSubimage(cropX, cropY, w, h);

        BufferedImage resultImage = new BufferedImage(w, h, BufferedImage.TYPE_INT_ARGB);

        Graphics2D g2 = resultImage.createGraphics();
        g2.setComposite(AlphaComposite.Src);
        g2.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON);
        g2.setColor(Color.BLUE);

        Polygon starPolygon = new Polygon();
        addPoint(starPolygon, h, 50, 0);
        addPoint(starPolygon, h, 65, 34);
        addPoint(starPolygon, h, 97, 39);
        addPoint(starPolygon, h, 73, 60);
        addPoint(starPolygon, h, 79, 93);

        addPoint(starPolygon, h, 50, 78);

        addPoint(starPolygon, h, 21, 93);
        addPoint(starPolygon, h, 22, 60);
        addPoint(starPolygon, h, 3 , 39);
        addPoint(starPolygon, h, 35, 34);
        addPoint(starPolygon, h, 50, 0);

        g2.fillPolygon(starPolygon);

        // using the white shape from above as alpha source
        g2.setComposite(AlphaComposite.SrcAtop);
        g2.drawImage(sourceImage, 0, 0, null);

        g2.dispose();

        return resultImage;
    }

    public BufferedImage starburst(BufferedImage image, int size, int count) {
        int w = image.getWidth();
        int h = image.getHeight();
        int cropX = 0;
        int cropY = 0;

        if (h > w) {
            h = w;
            cropY = ((image.getHeight() - h) / 2);
        } else if (w > h) {
            w = h;
            cropX = ((image.getWidth() - w) / 2);
        }

        image = image.getSubimage(cropX, cropY, w, h);

        BufferedImage resultImage = new BufferedImage(w, h, BufferedImage.TYPE_INT_ARGB);

        Graphics2D g2 = resultImage.createGraphics();
        g2.setComposite(AlphaComposite.Src);
        g2.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON);
        g2.setColor(Color.BLUE);

        Polygon starburstPolygon = new Polygon();

        int x = 0;
        int y = 0;

        int exteriorWidth = 50;
        int interiorWidth = exteriorWidth - size;
        double interval = 6.28 / count;
        for (double i = 0.0; i < 6.28; i += interval) {
            x = ((Double) (50 + interiorWidth * Math.cos(i))).intValue();
            y = ((Double) (50 + interiorWidth * Math.sin(i))).intValue();
            addPoint(starburstPolygon, h, x, y);

            x = ((Double) (50 + exteriorWidth * Math.cos(i + interval / 2))).intValue();
            y = ((Double) (50 + exteriorWidth * Math.sin(i + interval / 2))).intValue();
            addPoint(starburstPolygon, h, x, y);
        }

        x = ((Double) (50 + interiorWidth * Math.cos(0))).intValue();
        y = ((Double) (50 + interiorWidth * Math.sin(0))).intValue();
        addPoint(starburstPolygon, h, x, y);

        g2.fillPolygon(starburstPolygon);

        // using the white shape from above as alpha source
        g2.setComposite(AlphaComposite.SrcAtop);
        g2.drawImage(image, 0, 0, null);

        g2.dispose();

        return resultImage;
    }

    private static void addPoint(Polygon polygon, int h, int x, int y) {
        if (x > 0) {
            Double size = h * (x / 100.0);
            x = size.intValue();
        }

        if (y > 0) {
            Double size = h * (y / 100.0);
            y = size.intValue();
        }
        polygon.addPoint(x, y);
    }

    private static int adjustColor(int color, int multiply, int add) {
        color =  Math.round(color * (multiply / 100.0f)) + add;
        return colorMinMax(color);
    }

    private static int colorMinMax(int color) {
        if (color < 0) {
            return 0;
        } else if (color > 255) {
            return 255;
        }
        return color;
    }

    private static Dimension getFillAreaDimension(Integer originalWidth, Integer originalHeight, Integer requestedWidth, Integer requestedHeight) {
        Integer actualWidth = null;
        Integer actualHeight = null;

        if (originalWidth != null && originalHeight != null
                && (requestedWidth != null || requestedHeight != null)) {

            float originalRatio = (float) originalWidth / (float) originalHeight;
            if (requestedWidth != null && requestedHeight != null) {

                Integer potentialWidth = Math.round((float) requestedHeight * originalRatio);
                Integer potentialHeight = Math.round((float) requestedWidth / originalRatio);

                if (potentialWidth > requestedWidth) {
                    actualWidth = potentialWidth;
                    actualHeight = requestedHeight;

                } else { // potentialHeight > requestedHeight
                    actualWidth = requestedWidth;
                    actualHeight = potentialHeight;
                }

            } else if (originalWidth > originalHeight) {
                actualHeight = requestedHeight != null ? requestedHeight : requestedWidth;
                actualWidth = Math.round((float) actualHeight * originalRatio);

            } else { // originalWidth <= originalHeight
                actualWidth = requestedWidth != null ? requestedWidth : requestedHeight;
                actualHeight = Math.round((float) actualWidth / originalRatio);
            }
        }

        return new Dimension(actualWidth, actualHeight);
    }

    /*
    * Copyright (c) 2007, Romain Guy
    * All rights reserved.
    *
    * Redistribution and use in source and binary forms, with or without
    * modification, are permitted provided that the following conditions
    * are met:
    *
    *   * Redistributions of source code must retain the above copyright
    *     notice, this list of conditions and the following disclaimer.
    *   * Redistributions in binary form must reproduce the above
    *     copyright notice, this list of conditions and the following
    *     disclaimer in the documentation and/or other materials provided
    *     with the distribution.
    *   * Neither the name of the TimingFramework project nor the names of its
    *     contributors may be used to endorse or promote products derived
    *     from this software without specific prior written permission.
    *
    * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
    * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
    * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
    * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
    * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
    * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
    * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
    * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
    * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
    * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
    * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
    *
    *
    * @see <a href="http://www.java2s.com/Code/Java/Advanced-Graphics/GaussianBlurDemo.htm">Gaussian Blur Demo</a>
    */
    private static ConvolveOp getGaussianBlurFilter(int radius, boolean horizontal) {
        if (radius < 1) {
            throw new IllegalArgumentException("Radius must be >= 1");
        }

        int size = radius * 2 + 1;
        float[] data = new float[size];

        float sigma = radius / 3.0f;
        float twoSigmaSquare = 2.0f * sigma * sigma;
        float sigmaRoot = (float) Math.sqrt(twoSigmaSquare * Math.PI);
        float total = 0.0f;

        for (int i = -radius; i <= radius; i++) {
            float distance = i * i;
            int index = i + radius;
            data[index] = (float) Math.exp(-distance / twoSigmaSquare) / sigmaRoot;
            total += data[index];
        }

        for (int i = 0; i < data.length; i++) {
            data[i] /= total;
        }

        Kernel kernel = null;
        if (horizontal) {
            kernel = new Kernel(size, 1, data);
        } else {
            kernel = new Kernel(1, size, data);
        }
        return new ConvolveOp(kernel, ConvolveOp.EDGE_NO_OP, null);
    }

    private static Dimension getResizeDimension(Integer originalWidth, Integer originalHeight, Integer requestedWidth, Integer requestedHeight) {
        Integer actualWidth = null;
        Integer actualHeight = null;

        if (originalWidth != null && originalHeight != null
                && (requestedWidth != null || requestedHeight != null)) {

            float originalRatio = (float) originalWidth / (float) originalHeight;
            if (requestedWidth != null && requestedHeight != null) {

                float requestedRatio = (float) requestedWidth / (float) requestedHeight;
                if (originalRatio > requestedRatio) {
                    actualWidth = requestedWidth;
                    actualHeight = (int) Math.round((float) requestedWidth * originalHeight / originalWidth);
                } else if (originalRatio < requestedRatio) {
                    actualWidth = (int) Math.round((float) requestedHeight * originalWidth / originalHeight);
                    actualHeight = requestedHeight;
                } else {
                    actualWidth = requestedWidth;
                    actualHeight = requestedHeight;
                }
            } else if (requestedWidth == null) {
                actualHeight = requestedHeight;
                actualWidth = Math.round((float) requestedHeight * originalRatio);
            } else if (requestedHeight == null) {
                actualWidth = requestedWidth;
                actualHeight = Math.round((float) requestedWidth / originalRatio);
            }
        }

        return new Dimension(actualWidth, actualHeight);
    }
}
