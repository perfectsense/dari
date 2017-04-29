package com.psddev.dari.db;

import com.psddev.dari.test.DatabaseTestUtils;
import com.psddev.dari.test.TestDatabase;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class SqlQueryTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(SqlQueryTest.class);

    private static List<TestDatabase> TEST_DATABASES;
    private static List<Database> DATABASES;

    @BeforeClass
    public static void beforeClass() {

        TEST_DATABASES = DatabaseTestUtils.getNewDefaultTestDatabaseInstances();
        DATABASES = new ArrayList<Database>();

        for (TestDatabase testDb : TEST_DATABASES) {
            Database db = testDb.get();
            DATABASES.add(db);
        }

        LOGGER.info("Running tests against " + TEST_DATABASES.size() + " databases.");
    }

    @AfterClass
    public static void afterClass() {
        if (TEST_DATABASES != null) for (TestDatabase testDb : TEST_DATABASES) {
            testDb.close();
        }
    }

    @Before
    public void before() {

    }

    @After
    public void after() {

    }
}
