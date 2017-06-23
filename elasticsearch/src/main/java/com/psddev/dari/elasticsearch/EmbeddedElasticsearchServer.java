package com.psddev.dari.elasticsearch;

import org.apache.commons.io.FileUtils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.InternalSettingsPreparer;
import org.elasticsearch.node.Node;
import org.elasticsearch.painless.PainlessPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.transport.Netty4Plugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class EmbeddedElasticsearchServer {

    private static final String DEFAULT_DATA_DIRECTORY = "elasticsearch-data";
    private static final int BULK_QUEUE_SIZE = 500;
    private static final int BULK_SIZE = 1;
    private static final int SEARCH_QUEUE_SIZE = 1000;
    private static final int SEARCH_SIZE = 1;
    private static final Logger LOGGER = LoggerFactory.getLogger(EmbeddedElasticsearchServer.class);
    private static Node node = null;
    private static boolean initialized = false;
    private static boolean installPainlessPlugin = true;

    public static boolean getInitialized() {
        return initialized;
    }

    public static void setInitialized(boolean initialized) {
        EmbeddedElasticsearchServer.initialized = initialized;
    }

    public static boolean getPainlessPlugin() {
            return installPainlessPlugin;
    }

    public static void setInstallPainlessPlugin(boolean installPainlessPlugin) {
        EmbeddedElasticsearchServer.installPainlessPlugin = installPainlessPlugin;
    }

    /**
     * setup as no params
     */
    public static void setup() {
        setup("elasticdari");
    }

    /**
     * setup with clusterName
     */
    @SuppressWarnings("unchecked")
    public static synchronized void setup(String clusterName) {

        List plugins = new ArrayList();
        //noinspection unchecked
        plugins.add(Netty4Plugin.class);
        if (installPainlessPlugin) {
            //noinspection unchecked
            plugins.add(PainlessPlugin.class);
        }

        try {
            LOGGER.info("Setting up new Elasticsearch embedded node");
            initialized = true;
            //noinspection unchecked
            node = new MyNode(
                    Settings.builder()
                            .put("transport.type", "netty4")
                            .put("http.type", "netty4")
                            .put("cluster.name", clusterName)
                            .put("http.enabled", "true")
                            .put("path.home", DEFAULT_DATA_DIRECTORY)
                            .put("thread_pool.bulk.size", BULK_SIZE)
                            .put("thread_pool.bulk.queue_size", BULK_QUEUE_SIZE)
                            .put("thread_pool.search.size", SEARCH_SIZE)
                            .put("thread_pool.search.queue_size", SEARCH_QUEUE_SIZE)
                            .build(),
                    plugins);

            node.start();
            node.client().admin().cluster().prepareHealth()
                    .setWaitForYellowStatus()
                    .get();
        } catch (Exception error) {
            LOGGER.warn(
                    String.format("EmbeddedElasticsearchServer cannot create embedded node [%s: %s]",
                            error.getClass().getName(),
                            error.getMessage()),
                    error);
        }
    }

    /**
     * Set the Plugins for Embedded
     */
    private static class MyNode extends Node {
        public MyNode(Settings preparedSettings, Collection<Class<? extends Plugin>> classpathPlugins) {
            super(InternalSettingsPreparer.prepareEnvironment(preparedSettings, null), classpathPlugins);
        }
    }

    /**
     * Get Node for Embedded Elastic
     */
    public static Node getNode() {
        return node;
    }

    /**
     *
     */
    public static synchronized void shutdown() {
        try {
            if (node != null) {
                node.close();
            }
        } catch (Exception e) {
            LOGGER.warn("EmbeddedElasticsearchServer cannot shutdown");
        }
        deleteDataDirectory();
    }

    /**
     * Cleanup the directory created for Embedded Elastic
     */
    public static void deleteDataDirectory() {
        try {
            LOGGER.info("Cleanup directory {}", DEFAULT_DATA_DIRECTORY);
            FileUtils.deleteDirectory(new File(DEFAULT_DATA_DIRECTORY));
        } catch (IOException e) {
            LOGGER.info("Cleanup directory is not there... {}", DEFAULT_DATA_DIRECTORY);
        }
    }
}
