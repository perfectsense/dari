package com.psddev.dari.db;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

public class MySQLBinaryLogReaderTest {

    private static final String JDBC_URL = "jdbc:mysql:loadbalance://test.com:3308/testdb?useConfigs=maxPerformance&amp;enableQueryTimeouts=true&amp;clobberStreamingResults=true";

    private static final String JDBC_URL2 = "jdbc:mysql://test2.com/testdb2";

    private static final String JDBC_URL_WRONG = "jdbc://test2.com";

    private static Cache<ByteBuffer, byte[][]> binLogCache;

    private static org.apache.tomcat.dbcp.dbcp.BasicDataSource dbcpBasicDataSource;

    private static org.apache.commons.dbcp2.BasicDataSource dbcp2BasicDataSource;

    private static com.jolbox.bonecp.BoneCPDataSource boneCPDataSource;


    @BeforeClass
    public static void beforeClass() {

        binLogCache = CacheBuilder.newBuilder().maximumSize(10).build();

        dbcpBasicDataSource = new org.apache.tomcat.dbcp.dbcp.BasicDataSource();
        dbcpBasicDataSource.setUsername("username");
        dbcpBasicDataSource.setPassword("password");
        dbcpBasicDataSource.setUrl(JDBC_URL);

        dbcp2BasicDataSource = new org.apache.commons.dbcp2.BasicDataSource();
        dbcp2BasicDataSource.setUsername("username2");
        dbcp2BasicDataSource.setPassword("password2");
        dbcp2BasicDataSource.setUrl(JDBC_URL2);

        boneCPDataSource = new com.jolbox.bonecp.BoneCPDataSource();
        boneCPDataSource.setUsername("username3");
        boneCPDataSource.setPassword("password3");
        boneCPDataSource.setJdbcUrl(JDBC_URL);
    }

    @AfterClass
    public static void afterClass() {
    }

    @Before
    public void before() {
    }

    @After
    public void after() throws NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException {
        Field instanceField = MySQLBinaryLogReader.class.getDeclaredField("instance");
        instanceField.setAccessible(true);
        if (instanceField.get(null) != null) {
            instanceField.set(null, null);
        }
    }

    @Test
    public void getInstance() throws NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException {

        MySQLBinaryLogReader mySQLBinaryLogReader = MySQLBinaryLogReader.getInstance(binLogCache, dbcpBasicDataSource);

        // bin log cache
        Field cacheField = MySQLBinaryLogReader.class.getDeclaredField("binLogCache");
        cacheField.setAccessible(true);
        assertEquals(cacheField.get(mySQLBinaryLogReader), binLogCache);

        // username
        Field usernameField = MySQLBinaryLogReader.class.getDeclaredField("username");
        usernameField.setAccessible(true);
        assertEquals(usernameField.get(mySQLBinaryLogReader), "username");

        // password
        Field passwordField = MySQLBinaryLogReader.class.getDeclaredField("password");
        passwordField.setAccessible(true);
        assertEquals(passwordField.get(mySQLBinaryLogReader), "password");

        // host
        Field hostField = MySQLBinaryLogReader.class.getDeclaredField("host");
        hostField.setAccessible(true);
        assertEquals(hostField.get(mySQLBinaryLogReader), "test.com");

        // port
        Field portField = MySQLBinaryLogReader.class.getDeclaredField("port");
        portField.setAccessible(true);
        assertEquals(portField.get(mySQLBinaryLogReader), 3308);

        // catalog
        Field catalogField = MySQLBinaryLogReader.class.getDeclaredField("catalog");
        catalogField.setAccessible(true);
        assertEquals(catalogField.get(mySQLBinaryLogReader), "testdb");
    }

    @Test
    public void getInstance_singleton() throws NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException {

        MySQLBinaryLogReader mySQLBinaryLogReader = MySQLBinaryLogReader.getInstance(binLogCache, dbcp2BasicDataSource);
        MySQLBinaryLogReader mySQLBinaryLogReader2 = MySQLBinaryLogReader.getInstance(binLogCache, boneCPDataSource);
        assertEquals(mySQLBinaryLogReader, mySQLBinaryLogReader2);

        // once instantiated, you can't change configuration.
        Cache<ByteBuffer, byte[][]> binLogCache2 = CacheBuilder.newBuilder().maximumSize(3).build();
        mySQLBinaryLogReader = MySQLBinaryLogReader.getInstance(binLogCache2, boneCPDataSource);
        assertSame(mySQLBinaryLogReader, mySQLBinaryLogReader2);

        // bin log cache
        Field cacheField = MySQLBinaryLogReader.class.getDeclaredField("binLogCache");
        cacheField.setAccessible(true);
        assertEquals(cacheField.get(mySQLBinaryLogReader), binLogCache);

        // username
        Field usernameField = MySQLBinaryLogReader.class.getDeclaredField("username");
        usernameField.setAccessible(true);
        assertEquals(usernameField.get(mySQLBinaryLogReader), "username2");

        // password
        Field passwordField = MySQLBinaryLogReader.class.getDeclaredField("password");
        passwordField.setAccessible(true);
        assertEquals(passwordField.get(mySQLBinaryLogReader), "password2");

        // host
        Field hostField = MySQLBinaryLogReader.class.getDeclaredField("host");
        hostField.setAccessible(true);
        assertEquals(hostField.get(mySQLBinaryLogReader), "test2.com");

        // port: default to 3306
        Field portField = MySQLBinaryLogReader.class.getDeclaredField("port");
        portField.setAccessible(true);
        assertEquals(portField.get(mySQLBinaryLogReader), 3306);

        // catalog
        Field catalogField = MySQLBinaryLogReader.class.getDeclaredField("catalog");
        catalogField.setAccessible(true);
        assertEquals(catalogField.get(mySQLBinaryLogReader), "testdb2");
    }

    @Test
    public void getInstance_wrongJdbcUrl() {
        dbcpBasicDataSource.setUrl(JDBC_URL_WRONG);
        MySQLBinaryLogReader mySQLBinaryLogReader = MySQLBinaryLogReader.getInstance(binLogCache, dbcpBasicDataSource);
        assertNull(mySQLBinaryLogReader);
    }

    @Test
    public void getBinaryLogClient() throws NoSuchMethodException, SecurityException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
        MySQLBinaryLogReader mySQLBinaryLogReader = MySQLBinaryLogReader.getInstance(binLogCache, dbcp2BasicDataSource);
        Method binlogClientMethod = MySQLBinaryLogReader.class.getDeclaredMethod("getBinaryLogClient", null);
        binlogClientMethod.setAccessible(true);
        BinaryLogClient binaryLogClient1 = (BinaryLogClient) binlogClientMethod.invoke(mySQLBinaryLogReader, null);
        BinaryLogClient binaryLogClient2 = (BinaryLogClient) binlogClientMethod.invoke(mySQLBinaryLogReader, null);
        assertSame(binaryLogClient1, binaryLogClient2);
    }
}
