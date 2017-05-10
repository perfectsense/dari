package com.psddev.dari.mysql;

import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.BinaryLogClient.AbstractLifecycleListener;
import com.google.common.cache.Cache;

class MySQLBinaryLogLifecycleListener extends AbstractLifecycleListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(MySQLBinaryLogLifecycleListener.class);

    private final Cache<UUID, Object[]> cache;
    private volatile boolean connected;

    public MySQLBinaryLogLifecycleListener(Cache<UUID, Object[]> cache) {
        this.cache = cache;
    }

    public boolean isConnected() {
        return connected;
    }

    @Override
    public void onConnect(BinaryLogClient client) {
        LOGGER.info("Connected to MySQL as a slave");
        connected = true;
    }

    @Override
    public void onCommunicationFailure(BinaryLogClient client, Exception error) {
        LOGGER.warn("Can't communicate with MySQL as a slave!", error);
    }

    @Override
    public void onDisconnect(BinaryLogClient client) {
        LOGGER.info("Disconnected from MySQL as a slave");
        connected = false;
        cache.invalidateAll();
    }
}
