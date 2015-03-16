package com.psddev.dari.util;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Iterator;
import java.util.TreeMap;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.psddev.dari.util.JavaImageEditor.MEGA_BYTE;

public class JavaImageDirectoryWatch implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(JavaImageDirectoryWatch.class);

    private WatchService watchService;
    private Long maximumCacheSizeInBytes;
    private JavaImageEditor javaImageEditor;

    public JavaImageDirectoryWatch(JavaImageEditor javaImageEditor, WatchService watchService, Long maximumCacheSizeInBytes) throws IOException {
        this.javaImageEditor = javaImageEditor;
        this.watchService = watchService;
        this.maximumCacheSizeInBytes = maximumCacheSizeInBytes;
        if (javaImageEditor == null || watchService == null) {
            throw new IOException("javaImageEditor and watchService must not be null");
        }
    }

    public Long getMaximumCacheSizeInBytes() {
        return maximumCacheSizeInBytes;
    }

    public void setMaximumCacheSizeInBytes(Long maximumCacheSizeInBytes) {
        this.maximumCacheSizeInBytes = maximumCacheSizeInBytes;
    }

    @Override
    public void run() {

        try {
            WatchService watcher = FileSystems.getDefault().newWatchService();
            File imageFolder = new File(javaImageEditor.getCachePath());
            Path dir = Paths.get(imageFolder.getPath());
            dir.register(watcher, java.nio.file.StandardWatchEventKinds.ENTRY_CREATE);

            WatchKey key = watcher.take();
            while (key != null) {
                if (!ObjectUtils.isBlank(key.pollEvents())) {
                    Long cacheSize = FileUtils.sizeOfDirectory(imageFolder);

                    if (cacheSize > maximumCacheSizeInBytes) {

                        LOGGER.info(String.format("Image cache size (%.2fmb) exceeds limit of %.2fmb. Attempting to resize to 80%% of limit.", ((float) cacheSize / MEGA_BYTE), ((float) maximumCacheSizeInBytes / 1048576)));

                        //Re-size the cache to 80% that of CACHE_LIMIT
                        long minimumBytesToDelete = cacheSize - maximumCacheSizeInBytes + (new Double(maximumCacheSizeInBytes * 0.2)).longValue();
                        long bytesDeleted = 0;

                        //Sorting files by last accessed, last modified if last accessed is not available
                        TreeMap<String, File> fileMap = new TreeMap<String, File>();

                        Iterator iterator = FileUtils.iterateFiles(imageFolder, null, true);

                        FileSystem fileSystem = FileSystems.getDefault();
                        while (iterator.hasNext()) {
                            File file = (File) iterator.next();
                            if (file.isFile() && !file.isHidden()) {
                                Path path = fileSystem.getPath(file.getAbsolutePath());
                                BasicFileAttributes basicFileAttributes = Files.readAttributes(path, BasicFileAttributes.class);
                                if (basicFileAttributes.lastAccessTime() != null) {
                                    fileMap.put(basicFileAttributes.lastAccessTime().toMillis() + file.getAbsolutePath(), file);
                                } else {
                                    fileMap.put(file.lastModified() + file.getAbsolutePath(), file);
                                }
                            }
                        }

                        int filesDeleted = 0;
                        for (File file : fileMap.values()) {
                            long fileSize = file.length(); //Get before deleting
                            if (file.delete()) {
                                filesDeleted++;
                                bytesDeleted += fileSize;
                                if (bytesDeleted >= minimumBytesToDelete) {
                                    break;
                                }
                            }
                        }

                        if (bytesDeleted < minimumBytesToDelete) {
                            LOGGER.error(String.format("Unable to resize image cache to less than %.2fmb. Current size %.2fmb", ((float) (maximumCacheSizeInBytes * .8 / MEGA_BYTE)), ((float) (cacheSize - bytesDeleted) / MEGA_BYTE)));
                        } else {
                            LOGGER.info(String.format("Removed %s files, recoverd %.2fmb. Current size %.2fmb", filesDeleted, ((float) bytesDeleted / MEGA_BYTE), ((float) (cacheSize - bytesDeleted) / MEGA_BYTE)));
                        }

                    }
                }
                key.reset();
                key = watcher.take();
            }
        } catch (IOException ex) {
            LOGGER.error("Unable to create watch service", ex);
        } catch (InterruptedException ex) {
            LOGGER.error("Watch service Interrupted", ex);
        }
    }

}
