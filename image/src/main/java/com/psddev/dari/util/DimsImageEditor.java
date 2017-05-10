package com.psddev.dari.util;

import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Locale;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Image editor backed by
 * <a href="https://github.com/beetlebugorg/mod_dims">mod_dims</a>.
 */
public class DimsImageEditor extends AbstractUrlImageEditor {

    protected static final Logger LOGGER = LoggerFactory.getLogger(DimsImageEditor.class);

    /** Setting key for the shared secret to use when signing URLs. */
    public static final String SHARED_SECRET_SETTING = "sharedSecret";

    /** Setting key for the expiration date of a signed URL. Only one of either
     *  this setting or {@link #EXPIRE_DURATION_SETTING} should be set. */
    public static final String EXPIRE_TIMESTAMP_SETTING = "expireTimestamp";

    /** Setting key for the expiration duration (in seconds) of a signed URL.
     *  Only one of either this setting or {@link #EXPIRE_TIMESTAMP_SETTING}
     *  should be set. */
    public static final String EXPIRE_DURATION_SETTING = "expireDuration";

    /** Setting key for quality to use for the output images. */
    public static final String QUALITY_SETTING = "quality";

    /** Setting key for enabling the use of DIMS' legacy crop command. */
    public static final String USE_LEGACY_CROP_SETTING = "useLegacyCrop";

    /** Setting key for enabling the use of DIMS' legacy thumbnail command. */
    public static final String USE_LEGACY_THUMBNAIL_SETTING = "useLegacyThumbnail";

    /** Setting key for enabling the preservation of the image's metadata. */
    public static final String PRESERVE_METADATA_SETTING = "preserveMetadata";

    /** Setting key for enabling appending image URLs instead of passing them as a parameter. */
    public static final String APPEND_IMAGE_URLS_SETTING = "appendImageUrls";

    private String sharedSecret;

    private Date expireTimestamp;
    private Long expireDuration;

    private Integer quality;
    private boolean useLegacyCrop;
    private boolean useLegacyThumbnail;
    private boolean preserveMetadata;
    private boolean appendImageUrls;

    public String getSharedSecret() {
        return sharedSecret;
    }

    public void setSharedSecret(String sharedSecret) {
        this.sharedSecret = sharedSecret;
    }

    public Date getExpireTimestamp() {
        return expireTimestamp;
    }

    public void setExpireTimestamp(Date expireTimestamp) {
        this.expireTimestamp = expireTimestamp;
    }

    public Long getExpireDuration() {
        return expireDuration;
    }

    public void setExpireDuration(Long expireDuration) {
        this.expireDuration = expireDuration;
    }

    public Integer getQuality() {
        return quality;
    }

    public void setQuality(Integer quality) {
        this.quality = quality;
    }

    public boolean isUseLegacyCrop() {
        return useLegacyCrop;
    }

    public void setUseLegacyCrop(boolean useLegacyCrop) {
        this.useLegacyCrop = useLegacyCrop;
    }

    public boolean isUseLegacyThumbnail() {
        return useLegacyThumbnail;
    }

    public void setUseLegacyThumbnail(boolean useLegacyThumbnail) {
        this.useLegacyThumbnail = useLegacyThumbnail;
    }

    public boolean isPreserveMetadata() {
        return preserveMetadata;
    }

    public void setPreserveMetadata(boolean preserveMetadata) {
        this.preserveMetadata = preserveMetadata;
    }

    public boolean isAppendImageUrls() {
        return appendImageUrls;
    }

    public void setAppendImageUrls(boolean appendImageUrls) {
        this.appendImageUrls = appendImageUrls;
    }

    // --- AbstractImageEditor support ---

    @Override
    public void initialize(String settingsKey, Map<String, Object> settings) {
        super.initialize(settingsKey, settings);

        setSharedSecret(ObjectUtils.to(String.class, settings.get(SHARED_SECRET_SETTING)));

        setExpireTimestamp(ObjectUtils.to(Date.class, settings.get(EXPIRE_TIMESTAMP_SETTING)));
        setExpireDuration(ObjectUtils.to(Long.class, settings.get(EXPIRE_DURATION_SETTING)));

        setQuality(ObjectUtils.to(Integer.class, settings.get(QUALITY_SETTING)));

        setUseLegacyCrop(ObjectUtils.to(boolean.class, settings.get(USE_LEGACY_CROP_SETTING)));
        setUseLegacyThumbnail(ObjectUtils.to(boolean.class, settings.get(USE_LEGACY_THUMBNAIL_SETTING)));
        setPreserveMetadata(ObjectUtils.to(boolean.class, settings.get(PRESERVE_METADATA_SETTING)));
        setAppendImageUrls(ObjectUtils.to(boolean.class, settings.get(APPEND_IMAGE_URLS_SETTING)));
    }

    @Override
    public StorageItem edit(
            StorageItem image,
            String command,
            Map<String, Object> options,
            Object... arguments) {

        StorageItem newImage = null;

        boolean usePrivateUrl = false;
        if (options != null && options.containsKey(ImageEditorPrivateUrl.PRIVATE_URL_OPTION)
                && ObjectUtils.to(Boolean.class, options.get(ImageEditorPrivateUrl.PRIVATE_URL_OPTION))) {
            usePrivateUrl = true;
        }

        DimsUrl dimsUrl = null;
        try {
            dimsUrl = this.new DimsUrl(new OriginStorageItem(image), usePrivateUrl);
        } catch (Exception e) {
            LOGGER.error("Failed to generate a DIMS URL.", e);
        }
        if (dimsUrl != null) {
            Object resizeOption = options != null ? options.get(RESIZE_OPTION) : null;
            Object cropOption = options != null ? options.get(CROP_OPTION) : null;

            if (CROP_COMMAND.equals(command)) {
                Object x = arguments[0];
                Object y = arguments[1];
                Object width = arguments[2];
                Object height = arguments[3];

                /*
                 * This logic is a bit confusing, but is here to preserve backward compatibility.  ImageEditor.CROP_OPTION_AUTOMATIC
                 * is new and is meant to signify that no matter what, all crops should be done that way, regardless of whether other
                 * parameters like the x & y coordinates are present.  It is synonymous with the DIMS thumbnail command which does
                 * a resize AND crop in a single operation. Before its addition, the behavior was such that if there were crop
                 * dimensions (x, y, width, height) defined then a normal crop would take place. If however, only the width and height
                 * are present then an "Automatic" crop (thumbnail command) would be done in its place.
                 */

                if (CROP_OPTION_AUTOMATIC.equals(cropOption) || (cropOption == null && x == null && y == null)) {
                    dimsUrl.thumbnail(width, height, resizeOption);

                } else if (CROP_OPTION_NONE.equals(cropOption)) {
                    // don't do any cropping, just return the image.
                    return image;

                } else {
                    // the x & y coordinates cannot be null, otherwise just
                    // return the original image unchanged
                    if (x != null && y != null) {
                        dimsUrl.crop(x, y, width, height);
                    } else {
                        return image;
                    }
                }
            } else if (RESIZE_COMMAND.equals(command)) {

                Integer width = ObjectUtils.to(Integer.class, arguments[0]);
                Integer height = ObjectUtils.to(Integer.class, arguments[1]);

                /*
                 * Check the last command and if it was a thumbnail command
                 * with the same width and height as the arguments then don't do
                 * anything and just return the old URL "as is" since the
                 * thumbnail command already did both a crop AND a resize.
                 *
                 * We go into this if block even when the cropOption is null
                 * due to backward compatibility. See the block comment above.
                 */

                if (CROP_OPTION_AUTOMATIC.equals(cropOption) || cropOption == null) {

                    Command lastResizeCommand = dimsUrl.getLastResizeCommand();
                    if (lastResizeCommand instanceof ThumbnailCommand) {
                        ThumbnailCommand thumbnailCommand = (ThumbnailCommand) lastResizeCommand;

                        Integer thumbnailWidth = thumbnailCommand.getWidth();
                        Integer thumbnailHeight = thumbnailCommand.getHeight();

                        if (width != null && height != null
                                && width.equals(thumbnailWidth) && height.equals(thumbnailHeight)) {
                            return image;
                        }
                    }
                }

                dimsUrl.resize(width, height, resizeOption);

            } else if ("brightness".equals(command)) {
                Double brightness = ObjectUtils.to(Double.class, arguments[0]);
                if (brightness != null) {
                    dimsUrl.addCommand(new BrightnessCommand((int) (brightness * 100 / 1.5), 0));
                }

            } else if ("contrast".equals(command)) {
                Double contrast = ObjectUtils.to(Double.class, arguments[0]);
                if (contrast != null) {
                    dimsUrl.addCommand(new BrightnessCommand(0, (int) (contrast * 100)));
                }

            } else if ("flipH".equals(command)) {
                if (ObjectUtils.to(boolean.class, arguments[0])) {
                    dimsUrl.addCommand(new FlipFlopCommand("horizontal"));
                }

            } else if ("flipV".equals(command)) {
                if (ObjectUtils.to(boolean.class, arguments[0])) {
                    dimsUrl.addCommand(new FlipFlopCommand("vertical"));
                }

            } else if ("grayscale".equals(command)) {
                if (ObjectUtils.to(boolean.class, arguments[0])) {
                    dimsUrl.addCommand(new GrayscaleCommand(true));
                }

            } else if ("invert".equals(command)) {
                if (ObjectUtils.to(boolean.class, arguments[0])) {
                    dimsUrl.addCommand(new InvertCommand(true));
                }

            } else if ("rotate".equals(command)) {
                Integer angle = ObjectUtils.to(Integer.class, arguments[0]);
                if (angle != null) {
                    dimsUrl.addCommand(new RotateCommand(angle));
                }

            } else if ("sepia".equals(command)) {
                if (ObjectUtils.to(boolean.class, arguments[0])) {
                    dimsUrl.addCommand(new SepiaCommand(0.8));
                }

            } else if ("format".equals(command)) {
                String format = ObjectUtils.to(String.class, arguments[0]);
                if (format != null) {
                    dimsUrl.addCommand(new FormatCommand(format));
                }
            }

            newImage = dimsUrl.toStorageItem();
        }

        if (newImage != null) {
            return newImage;
        } else { // failed to create a new DimsUrl so return the original
            return image;
        }
    }

    /**
     * Returns the appropriate DIMS base URL for the {@code imageUrl}. If the
     * {@code imageUrl} is already a DIMS URL, then the DIMS base URL matching
     * the base URL of the {@code imageUrl} is returned. Otherwise, the
     * {@code imageUrl} is hashed and a DIMS base URL is picked from the pool.
     *
     * @param imageUrl the image URL to check.
     * @return the base DIMS URL.
     */
    public String getBaseUrlForImageUrl(String imageUrl) {
        String baseUrl = super.getBaseUrlForImageUrl(imageUrl);

        List<String> baseUrls = getBaseUrls();
        if (!baseUrls.isEmpty()) {
            for (String baseUrlItem : baseUrls) {
                if (imageUrl.startsWith(StringUtils.removeEnd(baseUrlItem, "/"))) {
                    baseUrl = baseUrlItem;
                    break;
                }
            }
        }

        return baseUrl;
    }

    private class DimsUrl {

        /** DIMS specific path to the original width of the image in the metadata */
        private static final String ORIGINAL_WIDTH_METADATA_PATH = "dims/originalWidth";

        /** DIMS specific Path to the original height of the image in the metadata */
        private static final String ORIGINAL_HEIGHT_METADATA_PATH = "dims/originalHeight";

        private List<Command> commands;
        private final StorageItem item;
        private URL imageUrl;
        private boolean usePrivateUrl;

        public void setImageUrl(URL imageUrl) {
            this.imageUrl = imageUrl;
        }

        public DimsUrl(StorageItem item) throws MalformedURLException {
            this(item, false);
        }

        public DimsUrl(StorageItem item, boolean usePrivateUrl) throws MalformedURLException {
            this.item = item;
            this.usePrivateUrl = usePrivateUrl;

            String url = item.getPublicUrl();
            LOGGER.trace("Creating new DIMS URL from [" + url + "]");
            if (url == null) {
                throw new MalformedURLException("Cannot create DIMS URL for item [" + item + "] with url [null]");
            }

            String baseUrl = null;
            if (isUsePrivateUrl()) {
                baseUrl = StringUtils.removeEnd(DimsImageEditor.this.getPrivateBaseUrl(), "/");
            }

            if (!isUsePrivateUrl() || baseUrl == null) {
                baseUrl = StringUtils.removeEnd(DimsImageEditor.this.getBaseUrlForImageUrl(url), "/");
            }

            if (url.startsWith(baseUrl)) {

                // It's an existing DIMS URL that we're further modifying

                String commandsString = null;

                int commandsOffset = baseUrl.length() + 1;
                if (DimsImageEditor.this.getSharedSecret() != null && baseUrl.contains("/dims4/")) {

                    int slashAt = url.indexOf('/', commandsOffset);
                    commandsOffset = url.indexOf('/', slashAt + 1) + 1;
                }

                if (DimsImageEditor.this.isAppendImageUrls()) {
                    int httpAt = url.indexOf("http", 1);
                    commandsString = url.substring(commandsOffset, httpAt);

                    imageUrl = new URL(url.substring(httpAt, url.length()));

                } else {
                    int questionAt = url.indexOf("?");
                    commandsString = url.substring(commandsOffset, questionAt);

                    String paramValue = StringUtils.getQueryParameterValue(url, "url");
                    if (paramValue.startsWith("/")) {
                        paramValue = "file:" + paramValue;
                    }
                    imageUrl = new URL(paramValue);
                }

                String[] parts = commandsString.split("/");
                String name = null;
                String value = null;
                for (int i = 0; i < parts.length; ++ i) {
                    if (i % 2 == 0) {
                        name = StringUtils.decodeUri(parts[i]);
                    } else {
                        value = StringUtils.decodeUri(parts[i]);
                        Command command = parseCommand(name, value);
                        if (command != null) {
                            addCommand(command);
                        }
                    }
                }

            } else {
                // It's a new DIMS request
                // If it's relative, prepend the path with "file:" so it can be validated as a proper file URL.
                if (url.startsWith("/")) {
                    url = "file:" + url;
                }

                // need to decode the url because DIMS expects a non encoded URL
                // and will take care of encoding it before it fetches the image
                url = StringUtils.decodeUri(url);
                imageUrl = new URL(url);

                // Add some commands by default based on the editor's preferences
                if (DimsImageEditor.this.isPreserveMetadata()) {
                    addCommand(new StripCommand(false));
                }

                String imagePath = imageUrl.getPath();

                if ("image/tiff".equals(ObjectUtils.getContentType(imagePath))) {
                    addCommand(new FormatCommand(ImageFormat.png));
                }

                Integer quality = DimsImageEditor.this.getQuality();
                if (quality != null) {
                    addCommand(new QualityCommand(quality));
                }
            }
        }

        /** Parses a command by it's command name, and value string from the raw URL */
        private Command parseCommand(String name, String value) {
            if ("resize".equals(name)) {
                return new ResizeCommand(value);

            } else if ("crop".equals(name)) {
                return new CropCommand(value, false);

            } else if ("legacy_crop".equals(name)) {
                return new CropCommand(value, true);

            } else if ("thumbnail".equals(name)) {
                return new ThumbnailCommand(value, false);

            } else if ("legacy_thumbnail".equals(name)) {
                return new ThumbnailCommand(value, true);

            } else if ("quality".equals(name)) {
                return new QualityCommand(value);

            } else if ("strip".equals(name)) {
                return new StripCommand(value);

            } else if ("format".equals(name)) {
                return new FormatCommand(value);

            } else if ("brightness".equals(name)) {
                return new BrightnessCommand(value);

            } else if ("flipflop".equals(name)) {
                return new FlipFlopCommand(value);

            } else if ("grayscale".equals(name)) {
                return new GrayscaleCommand(value);

            } else if ("invert".equals(name)) {
                return new InvertCommand(value);

            } else if ("rotate".equals(name)) {
                return new RotateCommand(value);

            } else if ("sepia".equals(name)) {
                return new SepiaCommand(value);

            } else {
                return null;
            }
        }

        /** Returns the list of commands for this DIMS URL */
        private List<Command> getCommands() {
            if (commands == null) {
                commands = new ArrayList<Command>();
            }
            return commands;
        }

        /** Inserts all ResizingCommands which can change the dimensions
         *  of the image after the last ResizingCommand found, and appends
         *  all other commands to the end of the list. */
        private void addCommand(Command command) {
            List<Command> commands = getCommands();
            if (!commands.isEmpty() && command instanceof ResizingCommand) {
                int lastResizeIndex = 0;
                for (Command cmd : commands) {
                    if (cmd instanceof ResizingCommand) {
                        lastResizeIndex++;
                    } else {
                        break;
                    }
                }
                commands.add(lastResizeIndex, command);
            } else {
                commands.add(command);
            }
        }

        /** Returns the last command that changed the dimensions of the image. */
        private Command getLastResizeCommand() {
            for (ListIterator<Command> commandIter = getCommands().listIterator(getCommands().size()); commandIter.hasPrevious();) {
                Command command = commandIter.previous();
                if (command instanceof ResizingCommand) {
                    return command;
                }
            }
            return null;
        }

        /** Adds a DIMS thumbnail command (resize+crop) to this URL */
        public DimsUrl thumbnail(Object width, Object height, Object option) {
            addCommand(new ThumbnailCommand(
                    ObjectUtils.to(Integer.class, width),
                    ObjectUtils.to(Integer.class, height),
                    String.valueOf(option),
                    DimsImageEditor.this.isUseLegacyThumbnail()));

            // Special handling here because the thumbnail command does not
            // behave properly when resizing GIF images, so we add an extra
            // command that will convert the image to JPG so the thumbnail
            // command can process it correctly.
            String imagePath = imageUrl.getPath();
            if (imagePath.toLowerCase(Locale.ENGLISH).endsWith(".gif")) {
                addCommand(new FormatCommand(ImageFormat.jpg));
            }
            return this;
        }

        /** Adds a DIMS resize command to this URL */
        public DimsUrl resize(Object width, Object height, Object option) {
            addCommand(new ResizeCommand(
                    ObjectUtils.to(Integer.class, width),
                    ObjectUtils.to(Integer.class, height),
                    String.valueOf(option)));
            return this;
        }

        /** Adds a DIMS crop command to this URL */
        public DimsUrl crop(Object x, Object y, Object width, Object height) {
            addCommand(new CropCommand(
                    ObjectUtils.to(Integer.class, x),
                    ObjectUtils.to(Integer.class, y),
                    ObjectUtils.to(Integer.class, width),
                    ObjectUtils.to(Integer.class, height),
                    DimsImageEditor.this.isUseLegacyCrop()));
            return this;
        }

        /** Returns a new StorageItem with the new dimensions of the image
         *  stored in the item's metadata. */
        public StorageItem toStorageItem() {

            Integer originalWidth = null;
            Integer originalHeight = null;
            Dimension outputDimension = null;

            Map<String, Object> oldMetadata = item.getMetadata();
            if (oldMetadata != null) {

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

                // calculate the dimensions of the new image
                outputDimension = new Dimension(originalWidth, originalHeight);
                for (Command command : getCommands()) {
                    if (command instanceof ResizingCommand) {
                        outputDimension = ((ResizingCommand) command).getOutputDimension(outputDimension);
                    }
                }
            }

            UrlStorageItem item = StorageItem.Static.createUrl(toString());

            Map<String, Object> metadata = new HashMap<String, Object>();

            // store the new width and height in the metadata map
            if (outputDimension != null && outputDimension.width != null) {
                metadata.put("width", outputDimension.width);
            }
            if (outputDimension != null && outputDimension.height != null) {
                metadata.put("height", outputDimension.height);
            }

            // store the original width and height in the map for use with future image edits.
            if (originalWidth != null) {
                CollectionUtils.putByPath(metadata, ORIGINAL_WIDTH_METADATA_PATH, originalWidth);
            }
            if (originalHeight != null) {
                CollectionUtils.putByPath(metadata, ORIGINAL_HEIGHT_METADATA_PATH, originalHeight);
            }

            item.setMetadata(metadata);

            return item;
        }

        public boolean isUsePrivateUrl() {
            return usePrivateUrl;
        }

        @Override
        public String toString() {
            StringBuilder dimsUrlBuilder = new StringBuilder();

            String imageUrl = this.imageUrl.toString();
            String baseUrl = null;
            if (isUsePrivateUrl()) {
                baseUrl = StringUtils.ensureEnd(DimsImageEditor.this.getPrivateBaseUrl(), "/");
            }

            if (!isUsePrivateUrl() || baseUrl == null) {
                baseUrl = StringUtils.ensureEnd(DimsImageEditor.this.getBaseUrlForImageUrl(imageUrl), "/");
            }

            if (imageUrl.startsWith("file:")) {
                imageUrl = imageUrl.substring(5);
            }

            dimsUrlBuilder.append(baseUrl);

            StringBuilder commandsBuilder = new StringBuilder();
            for (Command command : getCommands()) {
                commandsBuilder
                .append(StringUtils.encodeUri(command.getName()))
                .append('/')
                .append(StringUtils.encodeUri(command.getValue()))
                .append('/');
            }

            Long expireTs = null;
            if (expireTs == null || expireTs <= 0) {
                // Sets it to 2038-01-19
                expireTs = (long) Integer.MAX_VALUE;
            }

            String sharedSecret = DimsImageEditor.this.getSharedSecret();
            // construct url for dims developer key support
            if (sharedSecret != null && baseUrl.contains("/dims4/")) {
                String signature = expireTs + sharedSecret + StringUtils.decodeUri(commandsBuilder.toString()) + imageUrl;

                String md5Hex = StringUtils.hex(StringUtils.md5(signature));
                // Get first 7 characters... I think it actually only needs 6
                String requestSig = md5Hex.substring(0, 7);

                dimsUrlBuilder
                .append(requestSig).append('/')
                .append(expireTs).append('/');
            }

            dimsUrlBuilder.append(commandsBuilder);

            if (!DimsImageEditor.this.isAppendImageUrls()) {
                try {
                    // DIMS doesn't like the '+' character in the URL so convert
                    // them all to %20
                    dimsUrlBuilder.append("?url=").append(
                            URLEncoder.encode(
                                    imageUrl, "UTF-8").replaceAll("\\x2B", "%20"));

                } catch (UnsupportedEncodingException e) {
                    dimsUrlBuilder.append("?url=").append(imageUrl);
                }
            } else {
                dimsUrlBuilder.append(imageUrl);
            }

            return dimsUrlBuilder.toString();
        }
    }

    /** Represents the various image formats that DIMS can convert an image to. */
    private static enum ImageFormat {
        png, jpg, gif;
    }

    /** Helper class so that width and height can be returned in a single object */
    private static class Dimension {
        public final Integer width;
        public final Integer height;
        public Dimension(Integer width, Integer height) {
            this.width = width;
            this.height = height;
        }
    }

    /** Represents the various image manipulation commands supported by the DIMS API. */
    private static interface Command {
        /** Returns the name of the DIMS command */
        public String getName();
        /** Returns the command's arguments as a String */
        public String getValue();
    }

    /** Sub-interface of Command signifying that the command may alter the
     *  dimensions of the image. */
    private static interface ResizingCommand extends Command {
        /** Given a starting dimension, returns the resulting dimension after
         *  this command has been applied. */
        public Dimension getOutputDimension(Dimension originalDimension);
    }

    /** An abstract ResizingCommand that alters the dimensions of an image
     *  based on a width and height parameter. */
    private abstract static class AbstractResizeCommand implements ResizingCommand {
        protected Integer width;
        protected Integer height;

        public Integer getWidth() {
            return width;
        }

        public Integer getHeight() {
            return height;
        }

        protected String parseResizeOption(String option) {
            if (RESIZE_OPTION_IGNORE_ASPECT_RATIO.equals(option)) {
                return "!";
            } else if (RESIZE_OPTION_ONLY_SHRINK_LARGER.equals(option)) {
                return ">";
            } else if (RESIZE_OPTION_ONLY_ENLARGE_SMALLER.equals(option)) {
                return "<";
            } else if (RESIZE_OPTION_FILL_AREA.equals(option)) {
                return "^";
            } else {
                return null;
            }
        }
    }

    private static class CropCommand extends AbstractResizeCommand {
        private final Integer x;
        private final Integer y;
        private final boolean useLegacy;

        public CropCommand(String value, boolean useLegacy) {
            // example crop value: 208x208+15+93 (none of the values can be empty)

            int plusAt = value.indexOf('+');
            String coords = value.substring(plusAt + 1);
            int coordsPlusAt = coords.indexOf('+');
            this.x = ObjectUtils.to(Integer.class, coords.substring(0, coordsPlusAt));
            this.y = ObjectUtils.to(Integer.class, coords.substring(coordsPlusAt + 1));

            String dimensions = value.substring(0, plusAt);
            int xAt = dimensions.indexOf('x');
            this.width = ObjectUtils.to(Integer.class, dimensions.substring(0, xAt));
            this.height = ObjectUtils.to(Integer.class, dimensions.substring(xAt + 1));

            this.useLegacy = useLegacy;

            if (this.x == null || this.y == null || this.width == null || this.height == null) {
                throw new IllegalArgumentException("All of x, y, width and height must not be null!");
            }
        }

        public CropCommand(Integer x, Integer y, Integer width, Integer height, boolean useLegacy) {
            this.x = x;
            this.y = y;
            this.width = width;
            this.height = height;
            this.useLegacy = useLegacy;

            if (this.x == null || this.y == null || this.width == null || this.height == null) {
                throw new IllegalArgumentException("All of x, y, width and height must not be null!");
            }
        }

        @Override
        public String getName() {
            if (useLegacy) {
                return "legacy_crop";
            } else {
                return "crop";
            }
        }

        @Override
        public String getValue() {
            return width + "x" + height + "+" + x + "+" + y;
        }

        @Override
        public Dimension getOutputDimension(Dimension dimension) {
            if (dimension != null
                    && dimension.width != null && dimension.height != null
                    && this.width != null && this.height != null) {
                return new Dimension(
                        Math.min(this.width, dimension.width),
                        Math.min(this.height, dimension.height));
            } else {
                return null;
            }
        }
    }

    private static class ThumbnailCommand extends AbstractResizeCommand {
        private String option;
        private final boolean useLegacy;

        public ThumbnailCommand(String value, boolean useLegacy) {
            // thumbnail value: 100x100> (width nor height can be empty, but option can be empty)
            char last = value.charAt(value.length() - 1);
            if (!Character.isDigit(last) && last != 'x') {
                this.option = String.valueOf(last);
                value = value.substring(0, value.length() - 1);
            }
            int xAt = value.indexOf('x');
            if (xAt != -1) {
                this.width = xAt > 0 ? ObjectUtils.to(Integer.class, value.substring(0, xAt)) : null;
                this.height = xAt < value.length() - 1 ? ObjectUtils.to(Integer.class, value.substring(xAt + 1)) : null;
            }
            this.useLegacy = useLegacy;

            if (width == null || height == null) {
                throw new IllegalArgumentException("Both width and height must not be null!");
            }
        }

        public ThumbnailCommand(Integer width, Integer height, String option, boolean useLegacy) {
            this.width = width;
            this.height = height;
            this.option = parseResizeOption(option);
            this.useLegacy = useLegacy;

            if (width == null || height == null) {
                throw new IllegalArgumentException("Both width and height must not be null!");
            }
        }

        @Override
        public String getName() {
            if (useLegacy) {
                return "legacy_thumbnail";
            } else {
                return "thumbnail";
            }
        }

        @Override
        public String getValue() {
            return width + "x" + height + (option != null ? option : "");
        }

        @Override
        public Dimension getOutputDimension(Dimension dimension) {

            if (dimension != null
                    && dimension.width != null && dimension.height != null
                    && this.width != null && this.height != null) {

                if (option == null || option.equals("!") || option.equals("^")) {
                    return new Dimension(this.width, this.height);

                } else if (">".equals(option)) { // only shrink larger images
                    return new Dimension(
                            Math.min(this.width, dimension.width),
                            Math.min(this.height, dimension.height));

                } else if ("<".equals(option)) { // only enlarge smaller images
                    return new Dimension(
                            Math.max(this.width, dimension.width),
                            Math.max(this.height, dimension.height));
                }
            }

            return null;
        }
    }

    private static class ResizeCommand extends AbstractResizeCommand {
        private String option;

        public ResizeCommand(String value) {
            // resize value: 400x300! (width or height can be empty but not both, and option can be empty)
            char last = value.charAt(value.length() - 1);
            if (!Character.isDigit(last) && last != 'x') {
                this.option = String.valueOf(last);
                value = value.substring(0, value.length() - 1);
            }
            int xAt = value.indexOf('x');
            if (xAt != -1) {
                this.width = xAt > 0 ? ObjectUtils.to(Integer.class, value.substring(0, xAt)) : null;
                this.height = xAt < value.length() - 1 ? ObjectUtils.to(Integer.class, value.substring(xAt + 1)) : null;
            }

            if (this.width == null && this.height == null) {
                throw new IllegalArgumentException("At least one of width or height must not be null!");
            }
        }

        public ResizeCommand(Integer width, Integer height, String option) {
            this.width = width;
            this.height = height;
            this.option = parseResizeOption(option);

            if (this.width == null && this.height == null) {
                throw new IllegalArgumentException("At least one of width or height must not be null!");
            }
        }

        @Override
        public String getName() {
            return "resize";
        }

        @Override
        public String getValue() {
            return (width == null ? "" : width) + "x" + (height == null ? "" : height) + (option != null ? option : "");
        }

        @Override
        public Dimension getOutputDimension(Dimension original) {
            Integer actualWidth = null;
            Integer actualHeight = null;

            // in all cases we require the original dimensions to calculate the output dimensions
            if (original != null && original.width != null && original.height != null) {

                if (option == null) {
                    Dimension actualDimension = getResizeDimension(original.width, original.height, this.width, this.height);
                    actualWidth = actualDimension.width;
                    actualHeight = actualDimension.height;

                } else if ("!".equals(option)) { // ignore aspect ratio
                    actualWidth = this.width != null ? this.width : original.width;
                    actualHeight = this.height != null ? this.height : original.height;

                } else if ("^".equals(option)) { // fill area
                    Dimension actualDimension = getFillAreaDimension(original.width, original.height, this.width, this.height);
                    actualWidth = actualDimension.width;
                    actualHeight = actualDimension.height;

                } else if (">".equals(option)) { // only shrink larger images
                    if ((this.height == null && this.width >= original.width)
                            || (this.width == null && this.height >= original.height) //               -->  <-- this is an AND
                            || (this.width != null && this.height != null && this.width >= original.width && this.height >= original.height)) {

                        actualWidth = original.width;
                        actualHeight = original.height;

                    } else {
                        Dimension actualDimension = getResizeDimension(original.width, original.height, this.width, this.height);
                        actualWidth = actualDimension.width;
                        actualHeight = actualDimension.height;
                    }

                } else if ("<".equals(option)) { // only enlarge smaller images
                    if ((this.height == null && this.width <= original.width)
                            || (this.width == null && this.height <= original.height) //                -->  <-- This is an OR
                            || (this.width != null && this.height != null && (this.width <= original.width || this.height <= original.height))) {

                        actualWidth = original.width;
                        actualHeight = original.height;

                    } else {
                        Dimension actualDimension = getResizeDimension(original.width, original.height, this.width, this.height);
                        actualWidth = actualDimension.width;
                        actualHeight = actualDimension.height;
                    }
                }
            }

            if (actualWidth != null || actualHeight != null) {
                return new Dimension(actualWidth, actualHeight);
            } else {
                return null;
            }
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

    private static class QualityCommand implements Command {
        private final Integer quality;

        public QualityCommand(String quality) {
            this(ObjectUtils.to(Integer.class, quality));
        }

        public QualityCommand(Integer quality) {
            this.quality = quality;
        }

        @Override
        public String getName() {
            return "quality";
        }

        @Override
        public String getValue() {
            return quality != null ? String.valueOf(quality) : null;
        }
    }

    private static class StripCommand implements Command {
        private final Boolean doStripMetadata;

        public StripCommand(String value) {
            this(ObjectUtils.to(Boolean.class, value));
        }

        public StripCommand(Boolean doStripMetadata) {
            this.doStripMetadata = doStripMetadata;
        }

        @Override
        public String getName() {
            return "strip";
        }

        @Override
        public String getValue() {
            return doStripMetadata != null ? String.valueOf(doStripMetadata) : null;
        }
    }

    private static class FormatCommand implements Command {
        private final ImageFormat format;

        public FormatCommand(String value) {
            this(ObjectUtils.to(ImageFormat.class, value));
        }

        public FormatCommand(ImageFormat format) {
            this.format = format;
        }

        @Override
        public String getName() {
            return "format";
        }

        @Override
        public String getValue() {
            return format != null ? format.name() : null;
        }
    }

    private static class BrightnessCommand implements Command {

        private Integer brightness;
        private Integer contrast;

        public BrightnessCommand(Integer brightness, Integer contrast) {
            this.brightness = brightness;
            this.contrast = contrast;
        }

        public BrightnessCommand(String argument) {
            if (argument != null) {
                int xAt = argument.indexOf('x');
                if (xAt > -1) {
                    brightness = ObjectUtils.to(Integer.class, argument.substring(0, xAt));
                    contrast = ObjectUtils.to(Integer.class, argument.substring(xAt + 1));
                }
            }
        }

        @Override
        public String getName() {
            return "brightness";
        }

        @Override
        public String getValue() {
            if (brightness == null) {
                if (contrast == null) {
                    return null;

                } else {
                    return "0x" + contrast;
                }

            } else if (contrast == null) {
                return brightness + "x0";

            } else {
                return brightness + "x" + contrast;
            }
        }
    }

    private static class FlipFlopCommand implements Command {

        private final String orientation;

        public FlipFlopCommand(String orientation) {
            this.orientation = orientation;
        }

        @Override
        public String getName() {
            return "flipflop";
        }

        @Override
        public String getValue() {
            return orientation;
        }
    }

    private static class GrayscaleCommand implements Command {

        private final Boolean grayscale;

        public GrayscaleCommand(Boolean grayscale) {
            this.grayscale = grayscale;
        }

        public GrayscaleCommand(String grayscale) {
            this(ObjectUtils.to(Boolean.class, grayscale));
        }

        @Override
        public String getName() {
            return "grayscale";
        }

        @Override
        public String getValue() {
            return grayscale != null ? grayscale.toString() : null;
        }
    }

    private static class InvertCommand implements Command {

        private final Boolean invert;

        public InvertCommand(Boolean invert) {
            this.invert = invert;
        }

        public InvertCommand(String invert) {
            this(ObjectUtils.to(Boolean.class, invert));
        }

        @Override
        public String getName() {
            return "invert";
        }

        @Override
        public String getValue() {
            return invert != null ? invert.toString() : null;
        }
    }

    private static class RotateCommand implements ResizingCommand {

        private final Integer angle;

        public RotateCommand(Integer angle) {
            this.angle = angle;
        }

        public RotateCommand(String angle) {
            this(ObjectUtils.to(Integer.class, angle));
        }

        @Override
        public String getName() {
            return "rotate";
        }

        @Override
        public String getValue() {
            return angle != null ? String.valueOf(angle) : null;
        }

        @Override
        public Dimension getOutputDimension(Dimension dimension) {
            if (angle != null && angle % 90 == 0) {
                if (angle % 180 != 0) {
                    return new Dimension(dimension.height, dimension.width);
                } else {
                    return dimension;
                }
            } else {
                // TODO: Handle the cases where the angle is not a multiple of 90.
                return null;
            }
        }
    }

    private static class SepiaCommand implements Command {

        private final Double threshold;

        public SepiaCommand(Double threshold) {
            this.threshold = threshold;
        }

        public SepiaCommand(String threshold) {
            this(ObjectUtils.to(Double.class, threshold));
        }

        @Override
        public String getName() {
            return "sepia";
        }

        @Override
        public String getValue() {
            return threshold != null ? String.valueOf(threshold) : null;
        }
    }
}
