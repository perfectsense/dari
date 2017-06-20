package com.psddev.dari.test;

import com.psddev.dari.db.Database;
import com.psddev.dari.db.Query;
import com.psddev.dari.db.QueryPhrase;
import org.junit.After;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class PhraseIndexTest extends AbstractTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(PhraseIndexTest.class);

    @After
    public void deleteModels() {
        Query.from(PhraseIndexModel.class).deleteAllImmediately();
    }

    @Test
    public void testPhrase() {
        String test = "Hubble Astronomers Develop is a big item";

        PhraseIndexModel expected = new PhraseIndexModel();
        expected.testString = test;
        expected.save();

        PhraseIndexModel actual = Query.from(PhraseIndexModel.class).where("testString matches ?", test).first();
        assertThat("actual notnull", actual, is(notNullValue()));
        assertThat("actual expected", actual, is(expected));
        assertThat("actual expected.testString", actual.testString, is(expected.testString));

        PhraseIndexModel actual2 = Query.from(PhraseIndexModel.class).where("testString matches ?", "develop").first();
        assertThat("actual2 notnull", actual2, is(notNullValue()));
        assertThat("actual2 expected", actual2, is(expected));
        assertThat("actual2 expected.testString", actual2.testString, is(expected.testString));

        QueryPhrase qp = new QueryPhrase("Hubble Astronomers Develop");
        PhraseIndexModel actual1 = Query.from(PhraseIndexModel.class).where("testString matches ?", qp).first();
        assertThat("actual1 notnull", actual1, is(notNullValue()));
        assertThat("actual1 expected", actual1, is(expected));
        assertThat("actual1 expected.testString", actual1.testString, is(expected.testString));

        qp.setPhrase("Astronomers Develop");
        PhraseIndexModel actual4 = Query.from(PhraseIndexModel.class).where("testString matches ?", qp).first();
        assertThat("actual4 notnull", actual4, is(notNullValue()));
        assertThat("actual4 expected", actual4, is(expected));
        assertThat("actual4 expected.testString", actual4.testString, is(expected.testString));

        qp.setPhrase("Develop Astronomers");
        PhraseIndexModel actual5 = Query.from(PhraseIndexModel.class).where("testString matches ?", qp).first();
        assertThat("actual5 notnull", actual5, is(nullValue()));

        qp.setPhrase("Develop Astronomers");
        qp.setSlop(2.0f);
        PhraseIndexModel actual6 = Query.from(PhraseIndexModel.class).where("testString matches ?", qp).first();
        assertThat("actual6 notnull", actual6, is(notNullValue()));
        assertThat("actual6 expected", actual6, is(expected));
        assertThat("actual6 expected.testString", actual6.testString, is(expected.testString));

        qp.setPhrase("Develop item");
        qp.setSlop(3.0f);
        PhraseIndexModel actual7 = Query.from(PhraseIndexModel.class).where("testString matches ?", qp).first();
        assertThat("actual7 notnull", actual7, is(notNullValue()));
        assertThat("actual7 expected", actual7, is(expected));
        assertThat("actual7 expected.testString", actual7.testString, is(expected.testString));

        PhraseIndexModel expected2 = new PhraseIndexModel();
        expected2.testString = "Hubble are Astronomers that eat";
        expected2.save();

        qp.setPhrase("Hubble Astronomers");
        qp.setSlop(1.0f);
        qp.setBoost(10.0f);
        PhraseIndexModel actual8 = Query.from(PhraseIndexModel.class).where("testString matches ?", qp).
                or("testString matches ?", "eat")
                .sortRelevant(10.0d, "testString matches ?", qp).first();
        assertThat("actual8 notnull", actual8, is(notNullValue()));
        assertThat("actual8 expected", actual8, is(expected));
        assertThat("actual8 expected.testString", actual8.testString, is(expected.testString));
    }
}
