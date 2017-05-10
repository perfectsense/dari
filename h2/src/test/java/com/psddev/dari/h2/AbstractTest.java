package com.psddev.dari.h2;

import com.psddev.dari.util.Settings;
import com.zaxxer.hikari.HikariDataSource;
import org.junit.BeforeClass;

import java.util.UUID;

public abstract class AbstractTest {

    private static final String DATABASE_NAME = "h2";
    private static final String SETTING_KEY_PREFIX = "dari/database/" + DATABASE_NAME + "/";

    @BeforeClass
    public static void createDatabase() {
        HikariDataSource dataSource = new HikariDataSource();
        dataSource.setJdbcUrl("jdbc:h2:mem:test" + UUID.randomUUID().toString().replaceAll("-", "") + ";DB_CLOSE_DELAY=-1");

        Settings.setOverride("dari/defaultDatabase", DATABASE_NAME);
        Settings.setOverride(SETTING_KEY_PREFIX + "class", H2Database.class.getName());
        Settings.setOverride(SETTING_KEY_PREFIX + H2Database.DATA_SOURCE_SUB_SETTING, dataSource);
        Settings.setOverride(SETTING_KEY_PREFIX + H2Database.INDEX_SPATIAL_SUB_SETTING, Boolean.TRUE);
    }
}
