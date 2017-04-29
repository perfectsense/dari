package com.psddev.dari.db;

import static org.junit.Assert.assertNotNull;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.psddev.dari.test.DatabaseTestUtils;
import com.psddev.dari.test.TestDatabase;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.psddev.dari.db.Recordable.Abstract;
import com.psddev.dari.db.Recordable.Embedded;

public class EmbeddedIndexOnFieldWithSubclassTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(SimpleIndexTest.class);

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

        LOGGER.info("Running tests againsts " + TEST_DATABASES.size() + " databases.");
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

    @Test
    public void embeddedIndexOnFieldWithSubclassTest() {
        try {
            for (Database db : DATABASES) {

                Set<TermMapping> mappings = new HashSet<TermMapping>();

                UrlTermMapping urlMapping = new UrlTermMapping();
                urlMapping.name = "dari";
                urlMapping.url = "dari";

                State urlMappingState = urlMapping.getState();
                urlMappingState.setDatabase(db);

                mappings.add(urlMapping);

                Term term = new Term();
                term.mappings = mappings;

                State termState = term.getState();
                termState.setDatabase(db);

                termState.save();

                Term t1 = Query.from(Term.class).using(db).where("mappings/name = ?", "dari").first();
                Term t2 = Query.from(Term.class).using(db).where("mappings/com.psddev.dari.db.EmbeddedIndexOnFieldWithSubclassTest$UrlTermMapping/url = ?", "dari").first();

                assertNotNull(t1);
                assertNotNull(t2);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    static class Term extends Record {
        @Indexed @Embedded Set<TermMapping> mappings;
    }

    @Abstract
    @Embedded
    static class TermMapping extends Record {
        @Indexed String name;
    }

    @Embedded
    static class UrlTermMapping extends TermMapping {
        // We want to query on this field!
        @Indexed String url;
    }
}
