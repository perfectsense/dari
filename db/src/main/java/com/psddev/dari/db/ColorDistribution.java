package com.psddev.dari.db;

import java.awt.Color;
import java.awt.image.BufferedImage;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import javax.imageio.ImageIO;

import org.imgscalr.Scalr;

import com.psddev.dari.util.HuslColorSpace;
import com.psddev.dari.util.StorageItem;

/**
 * Stores information about how much of each color is in an image.
 *
 * @see ColorImage
 */
@ColorDistribution.Embedded
public class ColorDistribution extends Record {

    // CHECKSTYLE:OFF
    // Grayscale
    @Indexed private Double n_0_0_0;
    @Indexed private Double n_0_0_20;
    @Indexed private Double n_0_0_40;
    @Indexed private Double n_0_0_60;
    @Indexed private Double n_0_0_80;
    @Indexed private Double n_0_0_100;

    // Color
    @Indexed private Double n_0_40_20; @Indexed private Double n_0_40_40; @Indexed private Double n_0_40_60; @Indexed private Double n_0_40_80;
    @Indexed private Double n_0_60_20; @Indexed private Double n_0_60_40; @Indexed private Double n_0_60_60; @Indexed private Double n_0_60_80;
    @Indexed private Double n_0_80_20; @Indexed private Double n_0_80_40; @Indexed private Double n_0_80_60; @Indexed private Double n_0_80_80;
    @Indexed private Double n_0_100_20; @Indexed private Double n_0_100_40; @Indexed private Double n_0_100_60; @Indexed private Double n_0_100_80;

    @Indexed private Double n_24_40_20; @Indexed private Double n_24_40_40; @Indexed private Double n_24_40_60; @Indexed private Double n_24_40_80;
    @Indexed private Double n_24_60_20; @Indexed private Double n_24_60_40; @Indexed private Double n_24_60_60; @Indexed private Double n_24_60_80;
    @Indexed private Double n_24_80_20; @Indexed private Double n_24_80_40; @Indexed private Double n_24_80_60; @Indexed private Double n_24_80_80;
    @Indexed private Double n_24_100_20; @Indexed private Double n_24_100_40; @Indexed private Double n_24_100_60; @Indexed private Double n_24_100_80;

    @Indexed private Double n_48_40_20; @Indexed private Double n_48_40_40; @Indexed private Double n_48_40_60; @Indexed private Double n_48_40_80;
    @Indexed private Double n_48_60_20; @Indexed private Double n_48_60_40; @Indexed private Double n_48_60_60; @Indexed private Double n_48_60_80;
    @Indexed private Double n_48_80_20; @Indexed private Double n_48_80_40; @Indexed private Double n_48_80_60; @Indexed private Double n_48_80_80;
    @Indexed private Double n_48_100_20; @Indexed private Double n_48_100_40; @Indexed private Double n_48_100_60; @Indexed private Double n_48_100_80;

    @Indexed private Double n_72_40_20; @Indexed private Double n_72_40_40; @Indexed private Double n_72_40_60; @Indexed private Double n_72_40_80;
    @Indexed private Double n_72_60_20; @Indexed private Double n_72_60_40; @Indexed private Double n_72_60_60; @Indexed private Double n_72_60_80;
    @Indexed private Double n_72_80_20; @Indexed private Double n_72_80_40; @Indexed private Double n_72_80_60; @Indexed private Double n_72_80_80;
    @Indexed private Double n_72_100_20; @Indexed private Double n_72_100_40; @Indexed private Double n_72_100_60; @Indexed private Double n_72_100_80;

    @Indexed private Double n_96_40_20; @Indexed private Double n_96_40_40; @Indexed private Double n_96_40_60; @Indexed private Double n_96_40_80;
    @Indexed private Double n_96_60_20; @Indexed private Double n_96_60_40; @Indexed private Double n_96_60_60; @Indexed private Double n_96_60_80;
    @Indexed private Double n_96_80_20; @Indexed private Double n_96_80_40; @Indexed private Double n_96_80_60; @Indexed private Double n_96_80_80;
    @Indexed private Double n_96_100_20; @Indexed private Double n_96_100_40; @Indexed private Double n_96_100_60; @Indexed private Double n_96_100_80;

    @Indexed private Double n_120_40_20; @Indexed private Double n_120_40_40; @Indexed private Double n_120_40_60; @Indexed private Double n_120_40_80;
    @Indexed private Double n_120_60_20; @Indexed private Double n_120_60_40; @Indexed private Double n_120_60_60; @Indexed private Double n_120_60_80;
    @Indexed private Double n_120_80_20; @Indexed private Double n_120_80_40; @Indexed private Double n_120_80_60; @Indexed private Double n_120_80_80;
    @Indexed private Double n_120_100_20; @Indexed private Double n_120_100_40; @Indexed private Double n_120_100_60; @Indexed private Double n_120_100_80;

    @Indexed private Double n_144_40_20; @Indexed private Double n_144_40_40; @Indexed private Double n_144_40_60; @Indexed private Double n_144_40_80;
    @Indexed private Double n_144_60_20; @Indexed private Double n_144_60_40; @Indexed private Double n_144_60_60; @Indexed private Double n_144_60_80;
    @Indexed private Double n_144_80_20; @Indexed private Double n_144_80_40; @Indexed private Double n_144_80_60; @Indexed private Double n_144_80_80;
    @Indexed private Double n_144_100_20; @Indexed private Double n_144_100_40; @Indexed private Double n_144_100_60; @Indexed private Double n_144_100_80;

    @Indexed private Double n_168_40_20; @Indexed private Double n_168_40_40; @Indexed private Double n_168_40_60; @Indexed private Double n_168_40_80;
    @Indexed private Double n_168_60_20; @Indexed private Double n_168_60_40; @Indexed private Double n_168_60_60; @Indexed private Double n_168_60_80;
    @Indexed private Double n_168_80_20; @Indexed private Double n_168_80_40; @Indexed private Double n_168_80_60; @Indexed private Double n_168_80_80;
    @Indexed private Double n_168_100_20; @Indexed private Double n_168_100_40; @Indexed private Double n_168_100_60; @Indexed private Double n_168_100_80;

    @Indexed private Double n_192_40_20; @Indexed private Double n_192_40_40; @Indexed private Double n_192_40_60; @Indexed private Double n_192_40_80;
    @Indexed private Double n_192_60_20; @Indexed private Double n_192_60_40; @Indexed private Double n_192_60_60; @Indexed private Double n_192_60_80;
    @Indexed private Double n_192_80_20; @Indexed private Double n_192_80_40; @Indexed private Double n_192_80_60; @Indexed private Double n_192_80_80;
    @Indexed private Double n_192_100_20; @Indexed private Double n_192_100_40; @Indexed private Double n_192_100_60; @Indexed private Double n_192_100_80;

    @Indexed private Double n_216_40_20; @Indexed private Double n_216_40_40; @Indexed private Double n_216_40_60; @Indexed private Double n_216_40_80;
    @Indexed private Double n_216_60_20; @Indexed private Double n_216_60_40; @Indexed private Double n_216_60_60; @Indexed private Double n_216_60_80;
    @Indexed private Double n_216_80_20; @Indexed private Double n_216_80_40; @Indexed private Double n_216_80_60; @Indexed private Double n_216_80_80;
    @Indexed private Double n_216_100_20; @Indexed private Double n_216_100_40; @Indexed private Double n_216_100_60; @Indexed private Double n_216_100_80;

    @Indexed private Double n_240_40_20; @Indexed private Double n_240_40_40; @Indexed private Double n_240_40_60; @Indexed private Double n_240_40_80;
    @Indexed private Double n_240_60_20; @Indexed private Double n_240_60_40; @Indexed private Double n_240_60_60; @Indexed private Double n_240_60_80;
    @Indexed private Double n_240_80_20; @Indexed private Double n_240_80_40; @Indexed private Double n_240_80_60; @Indexed private Double n_240_80_80;
    @Indexed private Double n_240_100_20; @Indexed private Double n_240_100_40; @Indexed private Double n_240_100_60; @Indexed private Double n_240_100_80;

    @Indexed private Double n_264_40_20; @Indexed private Double n_264_40_40; @Indexed private Double n_264_40_60; @Indexed private Double n_264_40_80;
    @Indexed private Double n_264_60_20; @Indexed private Double n_264_60_40; @Indexed private Double n_264_60_60; @Indexed private Double n_264_60_80;
    @Indexed private Double n_264_80_20; @Indexed private Double n_264_80_40; @Indexed private Double n_264_80_60; @Indexed private Double n_264_80_80;
    @Indexed private Double n_264_100_20; @Indexed private Double n_264_100_40; @Indexed private Double n_264_100_60; @Indexed private Double n_264_100_80;

    @Indexed private Double n_288_40_20; @Indexed private Double n_288_40_40; @Indexed private Double n_288_40_60; @Indexed private Double n_288_40_80;
    @Indexed private Double n_288_60_20; @Indexed private Double n_288_60_40; @Indexed private Double n_288_60_60; @Indexed private Double n_288_60_80;
    @Indexed private Double n_288_80_20; @Indexed private Double n_288_80_40; @Indexed private Double n_288_80_60; @Indexed private Double n_288_80_80;
    @Indexed private Double n_288_100_20; @Indexed private Double n_288_100_40; @Indexed private Double n_288_100_60; @Indexed private Double n_288_100_80;

    @Indexed private Double n_312_40_20; @Indexed private Double n_312_40_40; @Indexed private Double n_312_40_60; @Indexed private Double n_312_40_80;
    @Indexed private Double n_312_60_20; @Indexed private Double n_312_60_40; @Indexed private Double n_312_60_60; @Indexed private Double n_312_60_80;
    @Indexed private Double n_312_80_20; @Indexed private Double n_312_80_40; @Indexed private Double n_312_80_60; @Indexed private Double n_312_80_80;
    @Indexed private Double n_312_100_20; @Indexed private Double n_312_100_40; @Indexed private Double n_312_100_60; @Indexed private Double n_312_100_80;

    @Indexed private Double n_336_40_20; @Indexed private Double n_336_40_40; @Indexed private Double n_336_40_60; @Indexed private Double n_336_40_80;
    @Indexed private Double n_336_60_20; @Indexed private Double n_336_60_40; @Indexed private Double n_336_60_60; @Indexed private Double n_336_60_80;
    @Indexed private Double n_336_80_20; @Indexed private Double n_336_80_40; @Indexed private Double n_336_80_60; @Indexed private Double n_336_80_80;
    @Indexed private Double n_336_100_20; @Indexed private Double n_336_100_40; @Indexed private Double n_336_100_60; @Indexed private Double n_336_100_80;

    // CHECKSTYLE:ON
    /**
     * Global modification for associating a {@link ColorDistribution}
     * instance to an object.
     */
    @FieldInternalNamePrefix("color.")
    public static class Data extends Modification<Object> {

        @Indexed
        private ColorDistribution distribution;

        public ColorDistribution getDistribution() {
            return distribution;
        }

        public void setDistribution(ColorDistribution distribution) {
            this.distribution = distribution;
        }
    }

    /**
     * {@link ColorDistribution} utility methods.
     */
    public static final class Static {

        /**
         * Analyzes the given image {@code item} and creates a
         * {@link ColorDistribution} instance.
         *
         * @param item Can't be {@code null}.
         * @return Never {@code null}.
         */
        public static ColorDistribution createDistribution(StorageItem item) throws IOException {
            InputStream itemData = item.getData();

            try {
                BufferedImage itemImage = Scalr.resize(ImageIO.read(itemData), 250);
                double pixelCount = itemImage.getWidth() * itemImage.getHeight();
                List<Cluster> clusters = findClusters(itemImage, 5);
                ColorDistribution distribution = new ColorDistribution();
                List<DominantColor> dominantColors = new ArrayList<DominantColor>();

                for (Cluster cluster : clusters) {
                    int[] center = cluster.center;
                    double percentage = cluster.colors.size() / pixelCount;
                    boolean found = false;

                    distribution.getState().put(
                            "o_" + center[0] + "_" + center[1] + "_" + center[2],
                            percentage);

                    int normalized0 = center[0];
                    int normalized1 = center[1];
                    int normalized2 = center[2];

                    if (normalized1 < 30 || normalized2 < 15 || normalized2 > 85) {
                        normalized0 = 0;
                        normalized1 = 0;
                    }

                    int[] normalized = new int[] {
                            (((int) Math.round(normalized0 / 24.0)) * 24) % 360,
                            ((int) Math.round(normalized1 / 20.0)) * 20,
                            ((int) Math.round(normalized2 / 20.0)) * 20 };

                    for (DominantColor dc : dominantColors) {
                        if (dc.color[0] == normalized[0]
                                && dc.color[1] == normalized[1]
                                && dc.color[2] == normalized[2]) {
                            found = true;
                            dc.percentage += percentage;
                            break;
                        }
                    }

                    if (!found) {
                        DominantColor dc = new DominantColor();
                        dc.color = normalized;
                        dc.percentage = percentage;

                        dominantColors.add(dc);
                    }
                }

                for (DominantColor dc : dominantColors) {
                    distribution.getState().put(
                            "n_" + dc.color[0] + "_" + dc.color[1] + "_" + dc.color[2],
                            dc.percentage);
                }

                return distribution;

            } finally {
                itemData.close();
            }
        }

        private static class DominantColor {

            public int[] color;
            public double percentage;
        }

        private static class Cluster {

            public int[] center;
            public List<int[]> colors;
            public List<int[]> newColors;

            public Cluster(int[] center, int imageSize) {
                this.center = center;
                this.colors = new ArrayList<int[]>(imageSize);
                this.colors.add(center);
                this.newColors = new ArrayList<int[]>(imageSize);
            }
        }

        private static List<Cluster> findClusters(BufferedImage image, int count) {
            int imageHeight = image.getHeight();
            int imageWidth = image.getWidth();
            int imageSize = imageHeight * imageWidth;
            List<int[]> imageColors = new ArrayList<int[]>(imageSize);

            for (int j = 0; j < imageHeight; ++ j) {
                for (int i = 0; i < imageWidth; ++ i) {
                    imageColors.add(HuslColorSpace.Static.toHUSL(new Color(image.getRGB(i, j))));
                }
            }

            // Initialize the clusters with random points within the image.
            List<Cluster> clusters = new ArrayList<Cluster>();
            Random random = new Random();
            Set<Integer> usedIndexes = new HashSet<Integer>();

            while (clusters.size() < count) {
                int index = random.nextInt(imageSize);

                if (!usedIndexes.contains(index)) {
                    usedIndexes.add(index);
                    clusters.add(new Cluster(imageColors.get(index), imageSize));
                }
            }

            while (true) {
                for (Cluster cluster : clusters) {
                    cluster.newColors.clear();
                }

                // Add the colors to the closest cluster.
                for (int[] color : imageColors) {
                    double minDist = Double.POSITIVE_INFINITY;
                    Cluster minDistCluster = null;

                    for (Cluster cluster : clusters) {
                        double dist = euclideanDistance(color, cluster.center);

                        if (dist < minDist) {
                            minDist = dist;
                            minDistCluster = cluster;
                        }
                    }

                    minDistCluster.newColors.add(color);
                }

                // Re-calculate the cluster center.
                double diff = 0.0;

                for (Cluster cluster : clusters) {
                    int[] oldCenter = cluster.center;
                    List<int[]> oldColors = cluster.colors;
                    List<int[]> newColors = cluster.newColors;
                    double c0 = 0.0;
                    double c1 = 0.0;
                    double c2 = 0.0;

                    for (int[] color : newColors) {
                        c0 += color[0];
                        c1 += color[1];
                        c2 += color[2];
                    }

                    int newColorsSize = newColors.size();
                    c0 /= newColorsSize;
                    c1 /= newColorsSize;
                    c2 /= newColorsSize;

                    cluster.center = new int[] { (int) c0, (int) c1, (int) c2 };
                    cluster.colors = newColors;
                    cluster.newColors = oldColors;
                    diff = Math.max(diff, euclideanDistance(oldCenter, cluster.center));
                }

                // If the new center is close to the old one, stop.
                if (diff < 1.0) {
                    break;
                }
            }

            return clusters;
        }

        private static double euclideanDistance(int[] color1, int[] color2) {
            return Math.sqrt(
                    Math.pow(
                            color1[0] - color2[0], 2)
                            + Math.pow(color1[1] - color2[1], 2)
                            + Math.pow(color1[2] - color2[2], 2));
        }
    }
}
