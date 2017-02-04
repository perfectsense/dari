package com.psddev.dari.elasticsearch;

import com.psddev.dari.util.Settings;
import com.zaxxer.hikari.HikariDataSource;
import org.junit.BeforeClass;

import java.util.UUID;

public abstract class AbstractTest {

    private static final String DATABASE_NAME = "elasticsearch";
    private static final String SETTING_KEY_PREFIX = "dari/database/" + DATABASE_NAME + "/";

    @BeforeClass
    public static void createDatabase() {
        Settings.setOverride("dari/defaultDatabase", DATABASE_NAME);
        Settings.setOverride(SETTING_KEY_PREFIX + "class", ElasticsearchDatabase.class.getName());
        Settings.setOverride(SETTING_KEY_PREFIX + "clusterName", "elasticsearch_a");
        Settings.setOverride(SETTING_KEY_PREFIX + "indexName", "index1");
        Settings.setOverride(SETTING_KEY_PREFIX + "clusterPort", "9300");
        Settings.setOverride(SETTING_KEY_PREFIX + "clusterHostname", "localhost");
    }
}
