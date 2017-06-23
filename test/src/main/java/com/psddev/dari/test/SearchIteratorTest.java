package com.psddev.dari.test;

import com.psddev.dari.db.Query;
import org.junit.After;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.hamcrest.Matchers.is;

import static org.junit.Assert.assertThat;

public class SearchIteratorTest extends AbstractTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(SearchIteratorTest.class);

    @After
    public void deleteModels() {
        Query.from(SearchIndexModel.class).deleteAllImmediately();
    }

    @Test
    public void testQuery() throws Exception {

        for (int i = 0; i < 20; i++) {
            SearchIndexModel model = new SearchIndexModel();
            model.one = "test " + i;
            model.save();
        }

        Query<SearchIndexModel> query = Query.from(SearchIndexModel.class);

        Iterable<SearchIndexModel> iter = query.iterable(10);
        int i = 0;
        for (SearchIndexModel s : iter) {
            assertThat(s.one, is("test " + i));
            i++;
        }
    }
}

