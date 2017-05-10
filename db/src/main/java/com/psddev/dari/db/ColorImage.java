package com.psddev.dari.db;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.psddev.dari.util.StorageItem;

/**
 * Implement this interface to automatically analyze an image for
 * {@link ColorDistribution} information and enable search by color.
 */
public interface ColorImage extends Recordable {

    /**
     * Returns the image that should be analyzed for {@link ColorDistribution}
     * information.
     *
     * @return May be {@code null}.
     */
    public StorageItem getColorImage();
}

class ColorImageData extends Modification<ColorImage> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ColorImageData.class);

    @Override
    protected void beforeCommit() {
        ColorDistribution.Data distributionData = as(ColorDistribution.Data.class);

        if (distributionData.getDistribution() == null) {
            StorageItem image = getOriginalObject().getColorImage();

            if (image != null) {
                try {
                    distributionData.setDistribution(ColorDistribution.Static.createDistribution(image));

                } catch (IOException error) {
                    LOGGER.error("Exception during ColorImage", error);
                } catch (RuntimeException error) {
                    LOGGER.error("Exception during ColorImage", error);
                }
            }
        }
    }
}
