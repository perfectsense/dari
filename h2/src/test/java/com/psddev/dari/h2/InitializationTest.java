package com.psddev.dari.h2;

import com.psddev.dari.util.CollectionUtils;
import com.psddev.dari.util.SettingsException;
import com.zaxxer.hikari.HikariDataSource;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class InitializationTest {

    private static final String JDBC_URL = "jdbc:h2:mem:test" + UUID.randomUUID().toString().replaceAll("-", "") + ";DB_CLOSE_DELAY=-1";

    private H2Database database;
    private Map<String, Object> settings;

    @Before
    public void before() {
        database = new H2Database();
        settings = new HashMap<>();
    }

    private void put(String path, Object value) {
        CollectionUtils.putByPath(settings, path, value);
    }

    @Test
    public void dataSource() {
        HikariDataSource hikari = new HikariDataSource();
        hikari.setJdbcUrl(JDBC_URL);
        put(H2Database.DATA_SOURCE_SUB_SETTING, hikari);
        database.initialize("", settings);
    }

    @Test(expected = SettingsException.class)
    public void dataSourceNotDataSource() {
        put(H2Database.DATA_SOURCE_SUB_SETTING, "foo");
        database.initialize("", settings);
    }
}
