package com.psddev.dari.db;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.github.shyiko.mysql.binlog.event.WriteRowsEventData;
import com.google.common.base.Charsets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.shyiko.mysql.binlog.BinaryLogClient.EventListener;
import com.github.shyiko.mysql.binlog.event.DeleteRowsEventData;
import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventData;
import com.github.shyiko.mysql.binlog.event.EventHeader;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.github.shyiko.mysql.binlog.event.UpdateRowsEventData;
import com.google.common.cache.Cache;
import com.psddev.dari.db.shyiko.DariQueryEventData;
import com.psddev.dari.util.ObjectUtils;
import com.psddev.dari.util.StringUtils;
import com.psddev.dari.util.UuidUtils;

class MySQLBinaryLogEventListener implements EventListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(MySQLBinaryLogEventListener.class);
    private static final Pattern DELETE_PATTERN = Pattern.compile("DELETE\\s+FROM\\s+`?(?<table>\\p{Alnum}+)`?\\s+WHERE\\s+`?id`?\\s*(?:(?:IN\\s*\\()|(?:=))\\s*(?<id>(?:(?:[^\']+'){2},?\\s*){1,})\\)?", Pattern.CASE_INSENSITIVE);
    private static final Pattern UPDATE_PATTERN = Pattern.compile("UPDATE\\s+`?(?<table>\\p{Alnum}+)`?\\s+SET\\s+`?typeId`?\\s*=\\s*(?<typeId>(?:[^\']+'){2})\\s*,\\s*`?data`?\\s*=\\s*(?<data>.+)\\s*WHERE\\s+`?id`?\\s*(?:(?:IN\\s*\\()|(?:=))\\s*(?<id>(?:[^\']+'){2}).*", Pattern.CASE_INSENSITIVE);

    private final SqlDatabase database;
    private final Cache<UUID, Object[]> cache;
    private final String catalog;

    private boolean transactionBegin = false;
    private TableMapEventData tableMapEventData;
    private final List<Event> events = new ArrayList<Event>();
    private boolean isFlushCache = false;

    public MySQLBinaryLogEventListener(SqlDatabase database, Cache<UUID, Object[]> cache, String catalog) {
        this.database = database;
        this.cache = cache;
        this.catalog = catalog;
    }

    /**
     * Makes sure length of the given {@code in} is 16.
     */
    private byte[] confirm16Bytes(byte[] in) {
        if (in == null) {
            return null;
        }
        byte[] bytes16 = new byte[16];
        if (in.length == 16) {
            return in;
        }
        for (int i = 0; i < bytes16.length && i < in.length; i++) {
            bytes16[i] |= in[i];
        }
        return bytes16;
    }

    private void updateCache(byte[] id, byte[] typeId, byte[] data) {
        id = confirm16Bytes(id);
        if (id != null) {
            UUID bid = ObjectUtils.to(UUID.class, id);
            Object[] value = new Object[3];
            value[1] = data;
            Map<String, Object> jsonData = SqlDatabase.unserializeData(data);
            value[2] = jsonData;
            value[0] = UuidUtils.toBytes(ObjectUtils.to(UUID.class, jsonData.get(StateSerializer.TYPE_KEY)));

            database.notifyUpdate(database.createSavedObjectFromReplicationCache((byte[]) value[0], bid, (byte[]) value[1], jsonData, null));

            // populate cache
            if (cache.getIfPresent(bid) != null) {
                cache.put(bid, value);
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Cache update: typeId=[{}], id=[{}]", StringUtils.hex(typeId), StringUtils.hex(id));
                }
            }
        }
    }

    private void invalidateCache(byte[] id) {
        id = confirm16Bytes(id);
        if (id != null) {
            UUID bid = ObjectUtils.to(UUID.class, id);
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Cache invalidate: id=[{}]", StringUtils.hex(id));
            }
            cache.invalidate(bid);
        }
    }

    private void commitTransaction() {

        for (Event event : events) {
            EventHeader eventHeader = event.getHeader();
            EventType eventType = eventHeader.getEventType();
            EventData eventData = event.getData();

            if (LOGGER.isDebugEnabled()) {
                long timestamp = eventHeader.getTimestamp();
                long lag = System.currentTimeMillis() - timestamp;

                LOGGER.debug(
                        "Event: timestamp=[{}], lag=[{}], type=[{}], dataLength=[{}]",
                        new Object[] {
                                timestamp,
                                lag,
                                eventHeader.getEventType(),
                                eventHeader.getDataLength() });
            }

            if (eventType == EventType.WRITE_ROWS || eventType == EventType.EXT_WRITE_ROWS) {
                for (Serializable[] row : ((WriteRowsEventData) eventData).getRows()) {
                    byte[] data = row[2] instanceof byte[] ? (byte[]) row[2]
                            : row[2] instanceof String ? ((String) row[2]).getBytes(Charsets.UTF_8)
                            : null;

                    updateCache((byte[]) row[0], (byte[]) row[1], data);
                }

            } else if (eventType == EventType.UPDATE_ROWS || eventType == EventType.EXT_UPDATE_ROWS) {
                for (Map.Entry<Serializable[], Serializable[]> row : ((UpdateRowsEventData) eventData).getRows()) {
                    Serializable[] newValue = row.getValue();
                    byte[] data = newValue[2] instanceof byte[] ? (byte[]) newValue[2]
                            : newValue[2] instanceof String ? ((String) newValue[2]).getBytes(Charsets.UTF_8)
                            : null;

                    updateCache((byte[]) newValue[0], (byte[]) newValue[1], data);
                }
            } else if (eventType == EventType.DELETE_ROWS || eventType == EventType.EXT_DELETE_ROWS) {
                for (Serializable[] row : ((DeleteRowsEventData) eventData).getRows()) {
                    invalidateCache((byte[]) row[0]);
                }
            } else if (eventType == EventType.QUERY) {
                DariQueryEventData queryEventData = (DariQueryEventData) eventData;
                if (queryEventData.getAction() == DariQueryEventData.Action.UPDATE) {
                    updateCache(queryEventData.getId(), queryEventData.getTypeId(), queryEventData.getData());
                } else if (queryEventData.getAction() == DariQueryEventData.Action.DELETE) {
                    invalidateCache(queryEventData.getId());
                }

            } else {
                LOGGER.info("[{}] event type isn't valid!", eventType);
            }
        }
    }

    private void flushCache() {
        cache.invalidateAll();
    }

    private byte[] getByteData(byte[] source, String strSource, int begin, int end) {
        byte[] target = null;
        if (strSource.startsWith("_binary")) {
            int targetIndex = 0;
            target = new byte[end - begin - 9];
            for (int sourceIndex = begin + 8; sourceIndex < end - 1; sourceIndex++) {
                byte value = 0;
                if (source[sourceIndex] == 92) { // '\'
                    switch (source[++sourceIndex]) {
                        case 34: // "
                            value = 34;
                            break;
                        case 39: // '
                            value = 39;
                            break;
                        case 48: // 0
                            value = 0;
                            break;
                        case 97: // a
                            value = 7;
                            break;
                        case 98: // b
                            value = 8;
                            break;
                        case 116: // t
                            value = 9;
                            break;
                        case 110: // n
                            value = 10;
                            break;
                        case 118: // v
                            value = 11;
                            break;
                        case 102: // f
                            value = 12;
                            break;
                        case 114: // r
                            value = 13;
                            break;
                        case 101: // e
                            value = 27;
                            break;
                        default:
                            value = source[--sourceIndex];
                            break;
                    }
                } else {
                    value = source[sourceIndex];
                }
                target[targetIndex++] = value;
            }
        } else if (strSource.startsWith("X")) {
            String hex = strSource.substring(2, strSource.length() - 1);
            int len = hex.length();
            target = new byte[len / 2];
            for (int i = 0; i < len; i += 2) {
                target[i / 2] = (byte) ((Character.digit(hex.charAt(i), 16) << 4)
                        + Character.digit(hex.charAt(i + 1), 16));
            }

        } else {
            // TODO: error
        }
        return target;
    }

    private boolean processStatement(DariQueryEventData queryEventData) {

        boolean processed = false;
        if (queryEventData.getErrorCode() == 0 && queryEventData.getDatabase().equals(catalog)) {

            String sql = queryEventData.getSql();
            // TODO: parse sql statement to handle full syntax such as [LOW_PRIORITY | DELAYED | HIGH_PRIORITY] [IGNORE] [INTO]
            String[] statementParts = sql.split("`?\\s+`?", 4);
            String table = null;
            if (statementParts[0].equalsIgnoreCase("UPDATE")
                    || statementParts[0].equalsIgnoreCase("HANDLER")) {
                table = statementParts[1];
            } else if (statementParts[0].equalsIgnoreCase("DELETE")
                    || statementParts[0].equalsIgnoreCase("INSERT")
                    || statementParts[0].equalsIgnoreCase("REPLACE")) {
                table = statementParts[2];
            } else if ((statementParts[0].equalsIgnoreCase("ALTER")
                    || statementParts[0].equalsIgnoreCase("CREATE")
                    || statementParts[0].equalsIgnoreCase("RENAME")
                    || statementParts[0].equalsIgnoreCase("TRUNCATE")
                    || statementParts[0].equalsIgnoreCase("DROP"))
                    && statementParts[1].equalsIgnoreCase("TABLE")) {
                table = statementParts[2];
            }
            if (SqlDatabase.RECORD_TABLE.equalsIgnoreCase(table)) {
                byte[] byteStatement = queryEventData.getStatement();
                if (statementParts[0].equalsIgnoreCase("UPDATE")) {
                    queryEventData.setActionl(DariQueryEventData.Action.UPDATE);
                    Matcher matcher = UPDATE_PATTERN.matcher(sql);
                    if (matcher.matches()) {
                        queryEventData.setId(getByteData(byteStatement, matcher.group(4), matcher.start(4), matcher.end(4)));
                        queryEventData.setTypeId(getByteData(byteStatement, matcher.group(2), matcher.start(2), matcher.end(2)));
                        queryEventData.setData(getByteData(byteStatement, matcher.group(3), matcher.start(3), matcher.end(3)));
                        processed = true;
                    } else {
                        isFlushCache = true;
                    }
                } else if (statementParts[0].equalsIgnoreCase("DELETE")) {
                    queryEventData.setActionl(DariQueryEventData.Action.DELETE);
                    Matcher matcher = DELETE_PATTERN.matcher(sql);
                    if (matcher.matches()) {
                        queryEventData.setId(getByteData(byteStatement, matcher.group(2), matcher.start(2), matcher.end(2)));
                        processed = true;
                    } else {
                        isFlushCache = true;
                    }
                } else if (statementParts[0].equalsIgnoreCase("INSERT")) {
                    // Do nothing
                } else {
                    isFlushCache = true;
                }
            }
        }
        return processed;
    }

    @Override
    public void onEvent(Event event) {

        EventHeader eventHeader = event.getHeader();
        EventType eventType = eventHeader.getEventType();
        EventData eventData = event.getData();
        long tableId = 0;

        if (transactionBegin) {
            if ((eventType == EventType.QUERY && ((DariQueryEventData) eventData).getSql().equalsIgnoreCase("COMMIT"))
                    || (eventType == EventType.XID)) {
                try {
                    if (isFlushCache) {
                        flushCache();
                    } else {
                        commitTransaction();
                    }
                } finally {
                    events.clear();
                    isFlushCache = false;
                    transactionBegin = false;
                }
            } else {
                if (tableMapEventData != null) {
                    // TODO: check column metadata to get length.
                    try {
                        if (EventType.isWrite(eventType)) {
                            tableId = ((WriteRowsEventData) eventData).getTableId();
                        } else if (EventType.isUpdate(eventType)) {
                            tableId = ((UpdateRowsEventData) eventData).getTableId();
                        } else if (EventType.isDelete(eventType)) {
                            tableId = ((DeleteRowsEventData) eventData).getTableId();
                        } else {
                            LOGGER.info("[{}] event type isn't valid!", eventType);
                        }
                        if (tableMapEventData.getTableId() == tableId) {
                            events.add(event);
                        }
                    } finally {
                        tableMapEventData = null;
                    }
                } else if (eventType == EventType.TABLE_MAP) {
                    if (((TableMapEventData) eventData).getDatabase().equals(catalog) && ((TableMapEventData) eventData).getTable().equalsIgnoreCase(SqlDatabase.RECORD_TABLE)) {
                        tableMapEventData = (TableMapEventData) eventData;
                    }
                } else if (eventType == EventType.QUERY) {
                    if (processStatement((DariQueryEventData) eventData)) {
                        events.add(event);
                    }
                }
            }
        } else if (eventType == EventType.QUERY && ((DariQueryEventData) eventData).getSql().equalsIgnoreCase("BEGIN")) {
            transactionBegin = true;
        }
    }
}
