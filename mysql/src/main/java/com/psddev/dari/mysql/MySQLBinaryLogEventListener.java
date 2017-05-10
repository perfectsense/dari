package com.psddev.dari.mysql;

import com.github.shyiko.mysql.binlog.BinaryLogClient.EventListener;
import com.github.shyiko.mysql.binlog.event.DeleteRowsEventData;
import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventData;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.github.shyiko.mysql.binlog.event.UpdateRowsEventData;
import com.github.shyiko.mysql.binlog.event.WriteRowsEventData;
import com.google.common.cache.Cache;
import com.psddev.dari.db.State;
import com.psddev.dari.db.StateSerializer;
import com.psddev.dari.db.shyiko.DariQueryEventData;
import com.psddev.dari.util.StringUtils;
import com.psddev.dari.util.UuidUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

class MySQLBinaryLogEventListener implements EventListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(MySQLBinaryLogEventListener.class);

    private final MySQLDatabase mysqlDatabase;
    private final Cache<UUID, Object[]> cache;
    private final String databaseName;
    private final String recordTableName;

    private Long recordTableId;
    private boolean invalidateCacheOnCommit;
    private final List<Serializable[]> pendingUpdates = new ArrayList<>();
    private final List<Serializable[]> pendingInvalidates = new ArrayList<>();

    public MySQLBinaryLogEventListener(MySQLDatabase mysqlDatabase, Cache<UUID, Object[]> cache, String databaseName, String recordTableName) {
        this.mysqlDatabase = mysqlDatabase;
        this.cache = cache;
        this.databaseName = databaseName;
        this.recordTableName = recordTableName;
    }

    @Override
    public void onEvent(Event event) {
        EventData eventData = event.getData();

        if (eventData instanceof DariQueryEventData) {
            DariQueryEventData queryEventData = (DariQueryEventData) eventData;
            String sql = queryEventData.getSql();

            if (sql.equalsIgnoreCase("BEGIN")) {
                LOGGER.debug("Begin");
                reset();

            } else if (sql.equalsIgnoreCase("COMMIT")) {
                LOGGER.debug("Commit");
                commit();

            } else if (sql.equalsIgnoreCase("ROLLBACK")) {
                LOGGER.debug("Rollback");
                reset();

            } else if (queryEventData.getErrorCode() == 0
                    && queryEventData.getDatabase().equals(databaseName)) {

                int firstBackQuote = sql.indexOf('`');

                if (firstBackQuote > -1) {
                    ++ firstBackQuote;
                    int secondBackQuote = sql.indexOf('`', firstBackQuote);
                    String table = sql.substring(firstBackQuote, secondBackQuote);

                    if (recordTableName.equalsIgnoreCase(table)) {
                        invalidateCacheOnCommit = true;
                    }
                }
            }

        } else if (event.getHeader().getEventType() == EventType.XID) {
            LOGGER.debug("XID");
            commit();

        // Only work on changes to the Record table.
        } else if (eventData instanceof TableMapEventData) {
            TableMapEventData tableMapEventData = (TableMapEventData) eventData;

            if (tableMapEventData.getDatabase().equals(databaseName)
                    && tableMapEventData.getTable().equalsIgnoreCase(recordTableName)) {

                recordTableId = tableMapEventData.getTableId();
                LOGGER.debug("Table ID: {}", recordTableId);
            }

        } else if (recordTableId != null) {
            try {
                if (eventData instanceof WriteRowsEventData) {
                    WriteRowsEventData d = (WriteRowsEventData) eventData;

                    if (d.getTableId() == recordTableId) {
                        d.getRows().forEach(row -> {
                            if (LOGGER.isInfoEnabled()) {
                                LOGGER.debug("Pending write: {}", StringUtils.hex((byte[]) row[0]));
                            }

                            pendingUpdates.add(row);
                        });
                    }

                } else if (eventData instanceof UpdateRowsEventData) {
                    UpdateRowsEventData d = (UpdateRowsEventData) eventData;

                    if (d.getTableId() == recordTableId) {
                        d.getRows().stream().map(Map.Entry::getValue).forEach(row -> {
                            if (LOGGER.isInfoEnabled()) {
                                LOGGER.debug("Pending update: {}", StringUtils.hex((byte[]) row[0]));
                            }

                            pendingUpdates.add(row);
                        });
                    }

                } else if (eventData instanceof DeleteRowsEventData) {
                    DeleteRowsEventData d = (DeleteRowsEventData) eventData;

                    if (d.getTableId() == recordTableId) {
                        d.getRows().forEach(row -> {
                            if (LOGGER.isInfoEnabled()) {
                                LOGGER.debug("Pending delete: {}", StringUtils.hex((byte[]) row[0]));
                            }

                            pendingInvalidates.add(row);
                        });
                    }
                }

            } finally {
                recordTableId = null;
            }
        }
    }

    private void reset() {
        invalidateCacheOnCommit = false;
        pendingUpdates.clear();
        pendingInvalidates.clear();
    }

    private void commit() {
        try {
            if (invalidateCacheOnCommit) {
                LOGGER.debug("Invalidate all");
                cache.invalidateAll();

            } else {
                pendingUpdates.forEach(row -> {
                    UUID id = uuid((byte[]) row[0]);

                    if (id != null) {
                        byte[] data = (byte[]) row[2];
                        Map<String, Object> dataJson = StateSerializer.deserialize(data);
                        Object object = mysqlDatabase.createSavedObjectFromReplicationCache(id, data, dataJson, null);

                        mysqlDatabase.notifyUpdate(object);

                        if (cache.getIfPresent(id) != null) {
                            LOGGER.debug("Update: {}", id);
                            cache.put(id, new Object[]{
                                    UuidUtils.toBytes(State.getInstance(object).getTypeId()),
                                    data,
                                    dataJson});
                        }
                    }
                });

                pendingInvalidates.forEach(row -> {
                    UUID id = uuid((byte[]) row[0]);

                    if (id != null) {
                        LOGGER.debug("Invalidate: {}", id);
                        cache.invalidate(id);
                    }
                });
            }

        } finally {
            reset();
        }
    }

    // Binary fields don't include trailing 0s so add them back.
    private UUID uuid(byte[] bytes) {
        int bytesLength = bytes.length;

        if (bytesLength != 16) {
            byte[] fixed = new byte[16];
            System.arraycopy(bytes, 0, fixed, 0, bytesLength);
            return UuidUtils.fromBytes(fixed);

        } else {
            return UuidUtils.fromBytes(bytes);
        }
    }
}
