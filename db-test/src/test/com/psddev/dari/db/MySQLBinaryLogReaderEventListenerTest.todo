package com.psddev.dari.db;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.github.shyiko.mysql.binlog.event.DeleteRowsEventData;
import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventHeaderV4;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.github.shyiko.mysql.binlog.event.UpdateRowsEventData;
import com.github.shyiko.mysql.binlog.event.WriteRowsEventData;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.psddev.dari.db.MySQLBinaryLogReader.DariQueryEventData;

public class MySQLBinaryLogReaderEventListenerTest {

    private static final String JDBC_URL = "jdbc:mysql:loadbalance://test.com:3308/testdb?useConfigs=maxPerformance&amp;enableQueryTimeouts=true&amp;clobberStreamingResults=true";

    private static Cache<ByteBuffer, byte[][]> binLogCache;

    private static org.apache.tomcat.dbcp.dbcp.BasicDataSource dbcpBasicDataSource;

    private static final byte[] ID = {0, 0, 1, 65, 80, -88, -38, -36, -85, 93, 126, -65, 34, -104, 0, 0};

    private static final byte[] TYPE_ID = {0, 0, 1, 56, -76, 42, -43, -119, -95, -65, -68, -82, -13, -51, 0, 3};

    private static final byte[] DATA = {1};

    private static MySQLBinaryLogReader.CacheEventListener cacheEventListener;

    private static final long TABLE_ID = 2014L;

    private static Event transactionBeginEvent;

    private static Event tableMapEvent;

    private static Event transactionCommitEvent;


    @BeforeClass
    public static void beforeClass() {

        binLogCache = CacheBuilder.newBuilder().maximumSize(10).build();

        dbcpBasicDataSource = new org.apache.tomcat.dbcp.dbcp.BasicDataSource();
        dbcpBasicDataSource.setUsername("username");
        dbcpBasicDataSource.setPassword("password");
        dbcpBasicDataSource.setUrl(JDBC_URL);

        MySQLBinaryLogReader mySQLBinaryLogReader = MySQLBinaryLogReader.getInstance(binLogCache, dbcpBasicDataSource);
        cacheEventListener = mySQLBinaryLogReader.new CacheEventListener();

        EventHeaderV4 transactionBeginEventHeader = new EventHeaderV4();
        transactionBeginEventHeader.setEventType(EventType.QUERY);
        DariQueryEventData transactionBeginEventData = new DariQueryEventData();
        transactionBeginEventData.setSql("BEGIN");
        transactionBeginEvent = new Event(transactionBeginEventHeader, transactionBeginEventData);

        EventHeaderV4 tableMapEventHeader = new EventHeaderV4();
        tableMapEventHeader.setEventType(EventType.TABLE_MAP);
        TableMapEventData tableMapEventData = new TableMapEventData();
        tableMapEventData.setTable("Record");
        tableMapEventData.setDatabase("testdb");
        tableMapEventData.setTableId(TABLE_ID);
        tableMapEvent = new Event(tableMapEventHeader, tableMapEventData);

        EventHeaderV4 transactionCommitEventHeader = new EventHeaderV4();
        transactionCommitEventHeader.setEventType(EventType.QUERY);
        DariQueryEventData transactionCommitEventData = new DariQueryEventData();
        transactionCommitEventData.setSql("COMMIT");
        transactionCommitEvent = new Event(transactionCommitEventHeader, transactionCommitEventData);
    }

    @AfterClass
    public static void afterClass() throws NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException {
        Field instanceField = MySQLBinaryLogReader.class.getDeclaredField("instance");
        instanceField.setAccessible(true);
        if (instanceField.get(null) != null) {
            instanceField.set(null, null);
        }
    }

    @Before
    public void before() {
        // Cache is Empty
        assertEquals(binLogCache.size(), 0);
        byte[][] value = new byte[2][];
        value[0] = TYPE_ID;
        value[1] = DATA;
        binLogCache.put(ByteBuffer.wrap(ID), value);

        cacheEventListener.onEvent(transactionBeginEvent);
    }

    @After
    public void after() throws NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException {

        Field transactionBeginField = MySQLBinaryLogReader.CacheEventListener.class.getDeclaredField("transactionBegin");
        transactionBeginField.setAccessible(true);
        transactionBeginField.set(cacheEventListener, false);

        Field tableMapEventDataField = MySQLBinaryLogReader.CacheEventListener.class.getDeclaredField("tableMapEventData");
        tableMapEventDataField.setAccessible(true);
        tableMapEventDataField.set(cacheEventListener, null);

        Field eventsField = MySQLBinaryLogReader.CacheEventListener.class.getDeclaredField("events");
        eventsField.setAccessible(true);
        ((List<Event>) eventsField.get(cacheEventListener)).clear();

        Field isFlushCacheField = MySQLBinaryLogReader.CacheEventListener.class.getDeclaredField("isFlushCache");
        isFlushCacheField.setAccessible(true);
        isFlushCacheField.set(cacheEventListener, false);

        binLogCache.invalidateAll();
    }

    @Test
    public void confirm16Bytes() throws NoSuchMethodException, SecurityException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
        Method binlogClientMethod = MySQLBinaryLogReader.CacheEventListener.class.getDeclaredMethod("confirm16Bytes", byte[].class);
        binlogClientMethod.setAccessible(true);
        byte[] byte1 = {30};
        byte[] byte16 = {30, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
        assertArrayEquals((byte[]) binlogClientMethod.invoke(cacheEventListener, byte1), byte16);
        assertArrayEquals((byte[]) binlogClientMethod.invoke(cacheEventListener, byte16), byte16);
        byte[] byte14 = {0, 0, 1, 65, 80, -88, -38, -36, -85, 93, 126, -65, 34, -104};
        byte[] byte14_16 = {0, 0, 1, 65, 80, -88, -38, -36, -85, 93, 126, -65, 34, -104, 0, 0};
        assertArrayEquals((byte[]) binlogClientMethod.invoke(cacheEventListener, byte14), byte14_16);
    }

    @Test
    public void updateCache() throws NoSuchMethodException, SecurityException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {

        Method updateCacheMethod = MySQLBinaryLogReader.CacheEventListener.class.getDeclaredMethod("updateCache", byte[].class, byte[].class, byte[].class);
        updateCacheMethod.setAccessible(true);

        byte[] data2 = {2};
        updateCacheMethod.invoke(cacheEventListener, ID, TYPE_ID, data2);
        byte[][] value = new byte[2][];
        value[0] = TYPE_ID;
        value[1] = data2;
        assertArrayEquals(binLogCache.getIfPresent(ByteBuffer.wrap(ID)), value);
    }

    @Test
    public void updateCache_nonExistingId() throws NoSuchMethodException, SecurityException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {

        Method updateCacheMethod = MySQLBinaryLogReader.CacheEventListener.class.getDeclaredMethod("updateCache", byte[].class, byte[].class, byte[].class);
        updateCacheMethod.setAccessible(true);

        byte[] id = {1};
        updateCacheMethod.invoke(cacheEventListener, id, TYPE_ID, DATA);
        assertNull(binLogCache.getIfPresent(ByteBuffer.wrap(id)));
    }

    @Test
    public void updateCache_nullId() throws NoSuchMethodException, SecurityException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {

        Method updateCacheMethod = MySQLBinaryLogReader.CacheEventListener.class.getDeclaredMethod("updateCache", byte[].class, byte[].class, byte[].class);
        updateCacheMethod.setAccessible(true);

        // It shouldn't throw exception if id is null
        updateCacheMethod.invoke(cacheEventListener, null, TYPE_ID, DATA);

        // Nothing updated.
        assertEquals(binLogCache.size(), 1);
        byte[][] value = new byte[2][];
        value[0] = TYPE_ID;
        value[1] = DATA;
        assertArrayEquals(binLogCache.getIfPresent(ByteBuffer.wrap(ID)), value);
    }

    @Test
    public void updateCache_nullTypeId() throws NoSuchMethodException, SecurityException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {

        MySQLBinaryLogReader mySQLBinaryLogReader = MySQLBinaryLogReader.getInstance(binLogCache, dbcpBasicDataSource);
        MySQLBinaryLogReader.CacheEventListener cacheEventListener = mySQLBinaryLogReader.new CacheEventListener();
        Method updateCacheMethod = MySQLBinaryLogReader.CacheEventListener.class.getDeclaredMethod("updateCache", byte[].class, byte[].class, byte[].class);
        updateCacheMethod.setAccessible(true);

        byte[] data2 = {2};
        updateCacheMethod.invoke(cacheEventListener, ID, null, data2);
        byte[][] value = new byte[2][];
        value[0] = TYPE_ID;
        value[1] = data2;
        assertArrayEquals(binLogCache.getIfPresent(ByteBuffer.wrap(ID)), value);
    }

    @Test
    public void invalidateCache() throws NoSuchMethodException, SecurityException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {

        Method invalidateCacheMethod = MySQLBinaryLogReader.CacheEventListener.class.getDeclaredMethod("invalidateCache", byte[].class);
        invalidateCacheMethod.setAccessible(true);

        invalidateCacheMethod.invoke(cacheEventListener, ID);
        assertEquals(binLogCache.size(), 0);
    }

    @Test
    public void invalidateCache_nonExistingId() throws NoSuchMethodException, SecurityException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {

        Method invalidateCacheMethod = MySQLBinaryLogReader.CacheEventListener.class.getDeclaredMethod("invalidateCache", byte[].class);
        invalidateCacheMethod.setAccessible(true);

        byte[] id = {1};
        invalidateCacheMethod.invoke(cacheEventListener, id);
        assertEquals(binLogCache.size(), 1);
    }

    @Test
    public void onEvent_write_rows() {

        cacheEventListener.onEvent(tableMapEvent);

        EventHeaderV4 eventHeader = new EventHeaderV4();
        eventHeader.setEventType(EventType.WRITE_ROWS);
        WriteRowsEventData eventData = new WriteRowsEventData();
        eventData.setTableId(TABLE_ID);
        List<Serializable[]> rows = new ArrayList<Serializable[]>();

        Serializable[] row = new Serializable[1];
        byte[] id = {11};
        row[0] = id;
        rows.add(row);
        eventData.setRows(rows);
        Event event = new Event(eventHeader, eventData);
        cacheEventListener.onEvent(event);
        assertEquals(binLogCache.size(), 1);

        cacheEventListener.onEvent(transactionCommitEvent);
        assertEquals(binLogCache.size(), 1);

        byte[][] value = new byte[2][];
        value[0] = TYPE_ID;
        value[1] = DATA;
        assertArrayEquals(binLogCache.getIfPresent(ByteBuffer.wrap(ID)), value);
    }

    @Test
    public void onEvent_write_rows_existing() {

        cacheEventListener.onEvent(tableMapEvent);

        EventHeaderV4 eventHeader = new EventHeaderV4();
        eventHeader.setEventType(EventType.WRITE_ROWS);
        WriteRowsEventData eventData = new WriteRowsEventData();
        eventData.setTableId(TABLE_ID);
        List<Serializable[]> rows = new ArrayList<Serializable[]>();

        Serializable[] row = new Serializable[1];
        row[0] = ID;
        rows.add(row);
        eventData.setRows(rows);
        Event event = new Event(eventHeader, eventData);
        cacheEventListener.onEvent(event);
        assertEquals(binLogCache.size(), 1);

        cacheEventListener.onEvent(transactionCommitEvent);
        assertEquals(binLogCache.size(), 1);

        byte[][] value = new byte[2][];
        value[0] = TYPE_ID;
        value[1] = DATA;
        assertArrayEquals(binLogCache.getIfPresent(ByteBuffer.wrap(ID)), value);
    }

    @Test
    public void onEvent_delete_rows() {

        cacheEventListener.onEvent(tableMapEvent);

        EventHeaderV4 eventHeader = new EventHeaderV4();
        eventHeader.setEventType(EventType.DELETE_ROWS);
        DeleteRowsEventData eventData = new DeleteRowsEventData();
        eventData.setTableId(TABLE_ID);
        List<Serializable[]> rows = new ArrayList<Serializable[]>();

        Serializable[] row = new Serializable[1];
        row[0] = ID;
        rows.add(row);
        eventData.setRows(rows);
        Event event = new Event(eventHeader, eventData);
        cacheEventListener.onEvent(event);
        assertEquals(binLogCache.size(), 1);

        cacheEventListener.onEvent(transactionCommitEvent);
        assertEquals(binLogCache.size(), 0);
    }

    @Test
    public void onEvent_delete_rows_nonExisting() {

        cacheEventListener.onEvent(tableMapEvent);

        EventHeaderV4 eventHeader = new EventHeaderV4();
        eventHeader.setEventType(EventType.DELETE_ROWS);
        DeleteRowsEventData eventData = new DeleteRowsEventData();
        eventData.setTableId(TABLE_ID);
        List<Serializable[]> rows = new ArrayList<Serializable[]>();

        Serializable[] row = new Serializable[1];
        byte[] id = {11};
        row[0] = id;
        rows.add(row);
        eventData.setRows(rows);
        Event event = new Event(eventHeader, eventData);
        cacheEventListener.onEvent(event);
        assertEquals(binLogCache.size(), 1);

        cacheEventListener.onEvent(transactionCommitEvent);
        assertEquals(binLogCache.size(), 1);

        byte[][] value = new byte[2][];
        value[0] = TYPE_ID;
        value[1] = DATA;
        assertArrayEquals(binLogCache.getIfPresent(ByteBuffer.wrap(ID)), value);
    }

    @Test
    public void onEvent_delete_rows_wrongDB() {

        EventHeaderV4 tableMapEventHeader = new EventHeaderV4();
        tableMapEventHeader.setEventType(EventType.TABLE_MAP);
        TableMapEventData tableMapEventData = new TableMapEventData();
        tableMapEventData.setTable("Record");
        tableMapEventData.setDatabase("wrongdb");
        tableMapEventData.setTableId(TABLE_ID);
        Event tableMapEvent = new Event(tableMapEventHeader, tableMapEventData);
        cacheEventListener.onEvent(tableMapEvent);

        EventHeaderV4 eventHeader = new EventHeaderV4();
        eventHeader.setEventType(EventType.DELETE_ROWS);
        DeleteRowsEventData eventData = new DeleteRowsEventData();
        eventData.setTableId(TABLE_ID);
        List<Serializable[]> rows = new ArrayList<Serializable[]>();

        Serializable[] row = new Serializable[1];
        row[0] = ID;
        rows.add(row);
        eventData.setRows(rows);
        Event event = new Event(eventHeader, eventData);
        cacheEventListener.onEvent(event);
        assertEquals(binLogCache.size(), 1);

        cacheEventListener.onEvent(transactionCommitEvent);
        assertEquals(binLogCache.size(), 1);
    }

    @Test
    public void onEvent_delete_rows_wrongTable() {

        EventHeaderV4 tableMapEventHeader = new EventHeaderV4();
        tableMapEventHeader.setEventType(EventType.TABLE_MAP);
        TableMapEventData tableMapEventData = new TableMapEventData();
        tableMapEventData.setTable("WrongTable");
        tableMapEventData.setDatabase("testdb");
        tableMapEventData.setTableId(TABLE_ID);
        Event tableMapEvent = new Event(tableMapEventHeader, tableMapEventData);
        cacheEventListener.onEvent(tableMapEvent);

        EventHeaderV4 eventHeader = new EventHeaderV4();
        eventHeader.setEventType(EventType.DELETE_ROWS);
        DeleteRowsEventData eventData = new DeleteRowsEventData();
        eventData.setTableId(TABLE_ID);
        List<Serializable[]> rows = new ArrayList<Serializable[]>();

        Serializable[] row = new Serializable[1];
        row[0] = ID;
        rows.add(row);
        eventData.setRows(rows);
        Event event = new Event(eventHeader, eventData);
        cacheEventListener.onEvent(event);
        assertEquals(binLogCache.size(), 1);

        cacheEventListener.onEvent(transactionCommitEvent);
        assertEquals(binLogCache.size(), 1);
    }

    @Test
    public void onEvent_update_rows() {

        cacheEventListener.onEvent(tableMapEvent);

        EventHeaderV4 eventHeader = new EventHeaderV4();
        eventHeader.setEventType(EventType.UPDATE_ROWS);
        UpdateRowsEventData eventData = new UpdateRowsEventData();
        eventData.setTableId(TABLE_ID);
        List<Map.Entry<Serializable[], Serializable[]>> rows = new ArrayList<Map.Entry<Serializable[], Serializable[]>>();

        Serializable[] beforeData = new Serializable[3];
        beforeData[0] = ID;
        beforeData[1] = TYPE_ID;
        beforeData[2] = DATA;
        Serializable[] afterData = new Serializable[3];
        afterData[0] = ID;
        afterData[1] = TYPE_ID;
        byte[] data = {2};
        afterData[2] = data;

        rows.add(new AbstractMap.SimpleEntry<Serializable[], Serializable[]>(beforeData, afterData));
        eventData.setRows(rows);
        Event event = new Event(eventHeader, eventData);
        cacheEventListener.onEvent(event);
        assertEquals(binLogCache.size(), 1);

        cacheEventListener.onEvent(transactionCommitEvent);
        assertEquals(binLogCache.size(), 1);

        byte[][] value = new byte[2][];
        value[0] = TYPE_ID;
        value[1] = data;
        assertArrayEquals(binLogCache.getIfPresent(ByteBuffer.wrap(ID)), value);
    }

    @Test
    public void onEvent_update_rows_nonExisting() {

        cacheEventListener.onEvent(tableMapEvent);

        EventHeaderV4 eventHeader = new EventHeaderV4();
        eventHeader.setEventType(EventType.UPDATE_ROWS);
        UpdateRowsEventData eventData = new UpdateRowsEventData();
        eventData.setTableId(TABLE_ID);
        List<Map.Entry<Serializable[], Serializable[]>> rows = new ArrayList<Map.Entry<Serializable[], Serializable[]>>();

        Serializable[] beforeData = new Serializable[3];
        byte[] id = {11};
        beforeData[0] = id;
        beforeData[1] = TYPE_ID;
        beforeData[2] = DATA;
        Serializable[] afterData = new Serializable[3];
        afterData[0] = id;
        afterData[1] = TYPE_ID;
        byte[] data = {2};
        afterData[2] = data;

        rows.add(new AbstractMap.SimpleEntry<Serializable[], Serializable[]>(beforeData, afterData));
        eventData.setRows(rows);
        Event event = new Event(eventHeader, eventData);
        cacheEventListener.onEvent(event);
        assertEquals(binLogCache.size(), 1);

        cacheEventListener.onEvent(transactionCommitEvent);
        assertEquals(binLogCache.size(), 1);

        byte[][] value = new byte[2][];
        value[0] = TYPE_ID;
        value[1] = DATA;
        assertArrayEquals(binLogCache.getIfPresent(ByteBuffer.wrap(ID)), value);
    }

    @Test
    public void onEvent_update_rows_wrongDB() {

        EventHeaderV4 tableMapEventHeader = new EventHeaderV4();
        tableMapEventHeader.setEventType(EventType.TABLE_MAP);
        TableMapEventData tableMapEventData = new TableMapEventData();
        tableMapEventData.setTable("Record");
        tableMapEventData.setDatabase("wrongdb");
        tableMapEventData.setTableId(TABLE_ID);
        Event tableMapEvent = new Event(tableMapEventHeader, tableMapEventData);
        cacheEventListener.onEvent(tableMapEvent);

        EventHeaderV4 eventHeader = new EventHeaderV4();
        eventHeader.setEventType(EventType.UPDATE_ROWS);
        UpdateRowsEventData eventData = new UpdateRowsEventData();
        eventData.setTableId(TABLE_ID);
        List<Map.Entry<Serializable[], Serializable[]>> rows = new ArrayList<Map.Entry<Serializable[], Serializable[]>>();

        Serializable[] beforeData = new Serializable[3];
        beforeData[0] = ID;
        beforeData[1] = TYPE_ID;
        beforeData[2] = DATA;
        Serializable[] afterData = new Serializable[3];
        afterData[0] = ID;
        afterData[1] = TYPE_ID;
        byte[] data = {2};
        afterData[2] = data;

        rows.add(new AbstractMap.SimpleEntry<Serializable[], Serializable[]>(beforeData, afterData));
        eventData.setRows(rows);
        Event event = new Event(eventHeader, eventData);
        cacheEventListener.onEvent(event);
        assertEquals(binLogCache.size(), 1);

        cacheEventListener.onEvent(transactionCommitEvent);
        assertEquals(binLogCache.size(), 1);

        byte[][] value = new byte[2][];
        value[0] = TYPE_ID;
        value[1] = DATA;
        assertArrayEquals(binLogCache.getIfPresent(ByteBuffer.wrap(ID)), value);
    }

    @Test
    public void onEvent_update_rows_wrongTable() {

        EventHeaderV4 tableMapEventHeader = new EventHeaderV4();
        tableMapEventHeader.setEventType(EventType.TABLE_MAP);
        TableMapEventData tableMapEventData = new TableMapEventData();
        tableMapEventData.setTable("WrongTable");
        tableMapEventData.setDatabase("testdb");
        tableMapEventData.setTableId(TABLE_ID);
        Event tableMapEvent = new Event(tableMapEventHeader, tableMapEventData);
        cacheEventListener.onEvent(tableMapEvent);

        EventHeaderV4 eventHeader = new EventHeaderV4();
        eventHeader.setEventType(EventType.UPDATE_ROWS);
        UpdateRowsEventData eventData = new UpdateRowsEventData();
        eventData.setTableId(TABLE_ID);
        List<Map.Entry<Serializable[], Serializable[]>> rows = new ArrayList<Map.Entry<Serializable[], Serializable[]>>();

        Serializable[] beforeData = new Serializable[3];
        beforeData[0] = ID;
        beforeData[1] = TYPE_ID;
        beforeData[2] = DATA;
        Serializable[] afterData = new Serializable[3];
        afterData[0] = ID;
        afterData[1] = TYPE_ID;
        byte[] data = {2};
        afterData[2] = data;

        rows.add(new AbstractMap.SimpleEntry<Serializable[], Serializable[]>(beforeData, afterData));
        eventData.setRows(rows);
        Event event = new Event(eventHeader, eventData);
        cacheEventListener.onEvent(event);
        assertEquals(binLogCache.size(), 1);

        cacheEventListener.onEvent(transactionCommitEvent);
        assertEquals(binLogCache.size(), 1);

        byte[][] value = new byte[2][];
        value[0] = TYPE_ID;
        value[1] = DATA;
        assertArrayEquals(binLogCache.getIfPresent(ByteBuffer.wrap(ID)), value);
    }
}
