package com.psddev.dari.db;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.security.SecureRandom;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.sql.DataSource;

import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDataDeserializer;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer;
import com.github.shyiko.mysql.binlog.event.deserialization.EventHeaderV4Deserializer;
import com.github.shyiko.mysql.binlog.event.deserialization.FormatDescriptionEventDataDeserializer;
import com.github.shyiko.mysql.binlog.event.deserialization.GtidEventDataDeserializer;
import com.github.shyiko.mysql.binlog.event.deserialization.NullEventDataDeserializer;
import com.github.shyiko.mysql.binlog.event.deserialization.RotateEventDataDeserializer;
import com.github.shyiko.mysql.binlog.event.deserialization.RowsQueryEventDataDeserializer;
import com.github.shyiko.mysql.binlog.event.deserialization.TableMapEventDataDeserializer;
import com.github.shyiko.mysql.binlog.event.deserialization.XidEventDataDeserializer;
import com.google.common.cache.Cache;
import com.psddev.dari.db.shyiko.DariDeleteRowsEventDataDeserializer;
import com.psddev.dari.db.shyiko.DariQueryEventDataDeserializer;
import com.psddev.dari.db.shyiko.DariUpdateRowsEventDataDeserializer;
import com.psddev.dari.db.shyiko.DariWriteRowsEventDataDeserializer;
import com.psddev.dari.util.ObjectUtils;

class MySQLBinaryLogReader {

    private static final Logger LOGGER = LoggerFactory.getLogger(MySQLBinaryLogReader.class);
    private static final Pattern MYSQL_JDBC_URL_PATTERN = Pattern.compile("(?i)jdbc:mysql://([^:/]+)(?::(\\d+))?/([^?]+).*");
    private static final SecureRandom RANDOM = new SecureRandom();

    private final SqlDatabase database;
    private final BinaryLogClient client;
    private final MySQLBinaryLogLifecycleListener lifecycleListener;
    private final AtomicBoolean running = new AtomicBoolean();

    public MySQLBinaryLogReader(SqlDatabase database, Cache<UUID, Object[]> cache, DataSource dataSource) {
        this.database = database;

        Class<?> dataSourceClass = dataSource.getClass();
        String dataSourceClassName = dataSourceClass.getName();
        String jdbcUrl = null;
        String username = null;
        String password = null;
        Throwable dataSourceError = null;

        try {
            if (dataSourceClassName.equals("com.jolbox.bonecp.BoneCPDataSource")) {
                jdbcUrl = (String) dataSourceClass.getMethod("getJdbcUrl").invoke(dataSource);
                username = (String) dataSourceClass.getMethod("getUsername").invoke(dataSource);
                password = (String) dataSourceClass.getMethod("getPassword").invoke(dataSource);

            } else if (dataSource instanceof HikariDataSource) {
                HikariDataSource hikari = (HikariDataSource) dataSource;
                jdbcUrl = hikari.getJdbcUrl();
                username = hikari.getUsername();
                password = hikari.getPassword();

            } else if (dataSourceClassName.equals("org.apache.tomcat.jdbc.pool.DataSource")) {
                jdbcUrl = (String) dataSourceClass.getMethod("getUrl").invoke(dataSource);
                Properties dbProperties = (Properties) dataSourceClass.getMethod("getDbProperties").invoke(dataSource);
                username = dbProperties.getProperty("user");
                password = dbProperties.getProperty("password");

            } else {
                jdbcUrl = (String) dataSourceClass.getMethod("getUrl").invoke(dataSource);
                username = (String) dataSourceClass.getMethod("getUsername").invoke(dataSource);
                password = (String) dataSourceClass.getMethod("getPassword").invoke(dataSource);
            }

        } catch (IllegalAccessException error) {
            dataSourceError = error;

        } catch (InvocationTargetException error) {
            dataSourceError = error.getCause();

        } catch (NoSuchMethodException error) {
            dataSourceError = error;
        }

        if (dataSourceError != null) {
            throw new IllegalArgumentException(String.format(
                    "Can't extract MySQL information from data source [%s]!",
                    dataSource.getClass().getName()),
                    dataSourceError);
        }

        Matcher jdbcUrlMatcher = MYSQL_JDBC_URL_PATTERN.matcher(jdbcUrl);

        if (!jdbcUrlMatcher.matches()) {
            throw new IllegalArgumentException(String.format(
                    "[%s] isn't a valid MySQL JDBC URL!",
                    jdbcUrl));
        }

        String host = jdbcUrlMatcher.group(1);
        int port = ObjectUtils.firstNonNull(ObjectUtils.to(Integer.class, jdbcUrlMatcher.group(2)), 3306);
        String catalog = jdbcUrlMatcher.group(3);
        username = ObjectUtils.firstNonNull(username, "");
        password = ObjectUtils.firstNonNull(password, "");
        this.client = new BinaryLogClient(host, port, catalog, username, password);
        this.lifecycleListener = new MySQLBinaryLogLifecycleListener(cache);

        client.setServerId(RANDOM.nextLong());
        client.registerLifecycleListener(lifecycleListener);
        client.registerEventListener(new MySQLBinaryLogEventListener(database, cache, catalog));

        @SuppressWarnings("rawtypes")
        Map<EventType, EventDataDeserializer> eventDataDeserializers = new HashMap<EventType, EventDataDeserializer>();
        Map<Long, TableMapEventData> tableMapEventByTableId = new HashMap<Long, TableMapEventData>();

        eventDataDeserializers.put(EventType.FORMAT_DESCRIPTION, new FormatDescriptionEventDataDeserializer());
        eventDataDeserializers.put(EventType.ROTATE, new RotateEventDataDeserializer());
        eventDataDeserializers.put(EventType.QUERY, new DariQueryEventDataDeserializer());
        eventDataDeserializers.put(EventType.TABLE_MAP, new TableMapEventDataDeserializer());
        eventDataDeserializers.put(EventType.XID, new XidEventDataDeserializer());
        eventDataDeserializers.put(EventType.WRITE_ROWS, new DariWriteRowsEventDataDeserializer(tableMapEventByTableId));
        eventDataDeserializers.put(EventType.UPDATE_ROWS, new DariUpdateRowsEventDataDeserializer(tableMapEventByTableId));
        eventDataDeserializers.put(EventType.DELETE_ROWS, new DariDeleteRowsEventDataDeserializer(tableMapEventByTableId));
        eventDataDeserializers.put(EventType.EXT_WRITE_ROWS, new DariWriteRowsEventDataDeserializer(tableMapEventByTableId).setMayContainExtraInformation(true));
        eventDataDeserializers.put(EventType.EXT_UPDATE_ROWS, new DariUpdateRowsEventDataDeserializer(tableMapEventByTableId).setMayContainExtraInformation(true));
        eventDataDeserializers.put(EventType.EXT_DELETE_ROWS, new DariDeleteRowsEventDataDeserializer(tableMapEventByTableId).setMayContainExtraInformation(true));
        eventDataDeserializers.put(EventType.ROWS_QUERY, new RowsQueryEventDataDeserializer());
        eventDataDeserializers.put(EventType.GTID, new GtidEventDataDeserializer());

        client.setEventDeserializer(
                new EventDeserializer(
                        new EventHeaderV4Deserializer(),
                        new NullEventDataDeserializer(),
                        eventDataDeserializers,
                        tableMapEventByTableId));
    }

    public void start() {
        if (running.compareAndSet(false, true)) {
            Thread connectThread = new Thread() {

                @Override
                public void run() {
                    try {
                        client.connect();

                    } catch (IOException error) {
                        LOGGER.warn("Can't connect to MySQL as a slave!", error);
                    }
                }
            };

            connectThread.start();
        }
    }

    public boolean isRunning() {
        return running.get();
    }

    public boolean isConnected() {
        return isRunning() && lifecycleListener.isConnected();
    }

    public void stop() {
        if (running.compareAndSet(true, false)) {
            try {
                client.disconnect();

            } catch (IOException error) {
                LOGGER.warn("Can't disconnect from MySQL as a slave!", error);
            }
        }
    }
}
