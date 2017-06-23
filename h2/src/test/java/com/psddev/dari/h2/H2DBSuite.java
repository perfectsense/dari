package com.psddev.dari.h2;

import com.psddev.dari.test.SingletonTest;
import com.psddev.dari.test.StateIndexTest;
import com.psddev.dari.test.TypeIndexTest;
import com.psddev.dari.test.WriteTest;
import com.psddev.dari.util.Settings;
import com.zaxxer.hikari.HikariDataSource;
import junit.framework.JUnit4TestAdapter;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import junit.framework.TestSuite;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

@RunWith(Suite.class)
@Suite.SuiteClasses({
        H2DBSuite.H2Tests.class
})
public class H2DBSuite {

    public static class H2Tests {

        private static final Logger LOGGER = LoggerFactory.getLogger(H2Tests.class);

        public static void H2SetupDatabase() {

            String DATABASE_NAME = "h2";
            String SETTING_KEY_PREFIX = "dari/database/" + DATABASE_NAME + "/";

            HikariDataSource dataSource = new HikariDataSource();
            dataSource.setJdbcUrl("jdbc:h2:mem:test" + UUID.randomUUID().toString().replaceAll("-", "") + ";DB_CLOSE_DELAY=-1");

            Settings.setOverride("dari/defaultDatabase", DATABASE_NAME);
            Settings.setOverride(SETTING_KEY_PREFIX + "class", H2Database.class.getName());
            Settings.setOverride(SETTING_KEY_PREFIX + H2Database.DATA_SOURCE_SUB_SETTING, dataSource);
            Settings.setOverride(SETTING_KEY_PREFIX + H2Database.INDEX_SPATIAL_SUB_SETTING, Boolean.TRUE);
        }

        public static TestSuite suite() {
            LOGGER.info("Starting H2 test");
            H2SetupDatabase();
            TestSuite suite = new TestSuite();
            suite.addTest(new JUnit4TestAdapter(H2InitializationTest.class));
            suite.addTest(new JUnit4TestAdapter(H2LocationIndexTest.class));
            suite.addTest(new JUnit4TestAdapter(H2ModificationDenormalizedTest.class));
            suite.addTest(new JUnit4TestAdapter(H2ModificationEmbeddedTest.class));
            suite.addTest(new JUnit4TestAdapter(H2NumberIndexTest.class));
            suite.addTest(new JUnit4TestAdapter(H2ReadTest.class));
            //suite.addTest(new JUnit4TestAdapter(RegionCircleIndexTest.class));  this needs work in H2
            suite.addTest(new JUnit4TestAdapter(H2RegionIndexTest.class));
            suite.addTest(new JUnit4TestAdapter(H2RegionLocationTest.class));
            suite.addTest(new JUnit4TestAdapter(H2SearchArticleIndexTest.class));
            suite.addTest(new JUnit4TestAdapter(H2SearchIndexTest.class));
            suite.addTest(new JUnit4TestAdapter(SingletonTest.class));
            suite.addTest(new JUnit4TestAdapter(StateIndexTest.class));
            suite.addTest(new JUnit4TestAdapter(H2StringIndexTest.class));
            suite.addTest(new JUnit4TestAdapter(TypeIndexTest.class));
            suite.addTest(new JUnit4TestAdapter(H2UuidIndexTest.class));
            suite.addTest(new JUnit4TestAdapter(WriteTest.class));
            return suite;
        }

    }

}