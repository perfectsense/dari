package com.psddev.dari.elasticsearch;

import com.psddev.dari.db.Query;
import com.psddev.dari.db.UnsupportedIndexException;
import com.psddev.dari.util.Settings;
import org.apache.commons.lang3.time.DateUtils;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.index.query.QueryShardException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import java.util.List;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SearchElasticTest extends AbstractElasticTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(SearchElasticTest.class);

    private static final String FOO = "foo";

    private ElasticsearchDatabase database;

    @Before
    public void before() {

        this.database = new ElasticsearchDatabase();
        database.initialize("", getDatabaseSettings());
    }

    @After
    public void deleteModels() {
        Query.from(SearchElasticModel.class).deleteAll();
        try {
            database.commitTransaction(database.openConnection(), true);
        } catch (Exception error) {
            LOGGER.info("commit @After failed");
        }
    }


    @Test
    public void testOne() throws Exception {
        SearchElasticModel search = new SearchElasticModel();
        search.eid = "939393";
        search.name = "Bill";
        search.message = "tough";
        search.save();

        database.commitTransaction(database.openConnection(), true);

        List<SearchElasticModel> fooResult = Query
                .from(SearchElasticModel.class)
                .where("eid matches ?", "939393")
                .selectAll();

        assertThat(fooResult, hasSize(1));
        assertEquals("939393", fooResult.get(0).eid);
        assertEquals("Bill", fooResult.get(0).name);
        assertEquals("tough", fooResult.get(0).message);
    }

    @Test
    public void oneMatches() throws Exception {
        Stream.of(FOO, "bar", "qux").forEach(string -> {
            SearchElasticModel model = new SearchElasticModel();
            model.one = string;
            model.set.add(FOO);
            model.list.add(FOO);
            model.map.put(FOO, FOO);
            model.save();
        });

        database.commitTransaction(database.openConnection(), true);

        List<SearchElasticModel> fooResult = Query
                .from(SearchElasticModel.class)
                .where("one matches ?", FOO)
                .selectAll();

        assertThat(fooResult, hasSize(1));
        assertThat(fooResult.get(0).one, equalTo(FOO));
    }

    @Test
    public void setMatches() throws Exception {
        Stream.of(FOO, "bar", "qux").forEach(string -> {
            SearchElasticModel model = new SearchElasticModel();
            model.one = FOO;
            model.set.add(string);
            model.list.add(FOO);
            model.map.put(FOO, FOO);
            model.save();
        });

        database.commitTransaction(database.openConnection(), true);

        List<SearchElasticModel> fooResult = Query
                .from(SearchElasticModel.class)
                .where("set matches ?", FOO)
                .selectAll();

        assertThat(fooResult, hasSize(1));
        assertThat(fooResult.get(0).set, hasSize(1));
        assertThat(fooResult.get(0).set.iterator().next(), equalTo(FOO));
    }

    @Test
    public void listMatches() throws Exception {
        Stream.of(FOO, "bar", "qux").forEach(string -> {
            SearchElasticModel model = new SearchElasticModel();
            model.one = FOO;
            model.set.add(FOO);
            model.list.add(string);
            model.map.put(FOO, FOO);
            model.save();
        });

        database.commitTransaction(database.openConnection(), true);

        List<SearchElasticModel> fooResult = Query
                .from(SearchElasticModel.class)
                .where("list matches ?", FOO)
                .selectAll();

        assertThat(fooResult, hasSize(1));
        assertThat(fooResult.get(0).list, hasSize(1));
        assertThat(fooResult.get(0).list.get(0), equalTo(FOO));
    }

    @Test
    public void mapMatches() throws Exception {
        Stream.of(FOO, "bar", "qux").forEach(string -> {
            SearchElasticModel model = new SearchElasticModel();
            model.one = FOO;
            model.set.add(FOO);
            model.list.add(FOO);
            model.map.put(string, string);
            model.save();
        });

        database.commitTransaction(database.openConnection(), true);

        // note this is different from h2, but seems better since it is specific.
        List<SearchElasticModel> fooResult = Query
                .from(SearchElasticModel.class)
                .where("map.foo matches ?", FOO)
                .selectAll();

        assertThat("Size of result", fooResult, hasSize(1));
        assertThat("checking size of map", fooResult.get(0).map.size(), equalTo(1));
        assertThat("checking iterator", fooResult.get(0).map.values().iterator().next(), equalTo(FOO));
    }

    @Test
    public void anyMatches() throws Exception {
        Stream.of(FOO, "bar", "qux").forEach(string -> {
            SearchElasticModel model = new SearchElasticModel();
            model.one = string;
            model.set.add(FOO);
            model.save();
        });

        database.commitTransaction(database.openConnection(), true);

        List<SearchElasticModel> fooResult = Query
                .from(SearchElasticModel.class)
                .where("_any matches ?", FOO)
                .selectAll();

        assertThat(fooResult, hasSize(3));
    }

    @Test
    public void wildcard() throws Exception {
        Stream.of("f", "fo", "foo").forEach(string -> {
            SearchElasticModel model = new SearchElasticModel();
            model.one = string;
            model.save();
        });

        database.commitTransaction(database.openConnection(), true);

        assertThat(Query.from(SearchElasticModel.class).where("one matches ?", "f*").count(), equalTo(3L));
        assertThat(Query.from(SearchElasticModel.class).where("one matches ?", "fo*").count(), equalTo(2L));
        assertThat(Query.from(SearchElasticModel.class).where("one matches ?", "foo*").count(), equalTo(1L));
    }


    @Test
    public void sortRelevant() throws Exception {
        SearchElasticModel model = new SearchElasticModel();
        model.one = "foo";
        model.name = "qux";
        model.set.add("qux");
        model.list.add("qux");
        model.map.put("qux", "qux");
        model.eid = "1";
        model.save();

        model = new SearchElasticModel();
        model.one = "west";
        model.name = "west";
        model.set.add("west");
        model.list.add(FOO);
        model.map.put("west", "west");
        model.eid = "2";
        model.save();

        model = new SearchElasticModel();
        model.one = "qux";
        model.name = "west";
        model.set.add("west");
        model.list.add("qux");
        model.map.put("qux", "qux");
        model.eid = "3";
        model.save();

        database.commitTransaction(database.openConnection(), true);

        List<SearchElasticModel> fooResult = Query
                .from(SearchElasticModel.class)
                .where("_any matches ?", FOO)
                .sortRelevant(10.0, "one matches ?", FOO)
                .selectAll();

        assertThat(fooResult, hasSize(2));

        assertThat("check 0 and 1", fooResult.get(0).eid, is(equalTo("1")));
        assertThat("check 1 and 2", fooResult.get(1).eid, is(equalTo("2")));
    }

    @Test
    public void testSortString() throws Exception {

        Stream.of(FOO, "bar", "qux").forEach(string -> {
            SearchElasticModel model = new SearchElasticModel();
            model.one = string;
            model.set.add(FOO);
            model.save();
        });

        database.commitTransaction(database.openConnection(), true);

        List<SearchElasticModel> fooResult = Query
                .from(SearchElasticModel.class)
                .sortAscending("one")
                .selectAll();

        assertThat("check size", fooResult, hasSize(3));
        assertThat("check 0 and 1 order", fooResult.get(0).one, lessThan(fooResult.get(1).one));
        assertThat("check 1 and 2 order", fooResult.get(1).one, lessThan(fooResult.get(2).one));
    }

    @Test(expected = UnsupportedIndexException.class)
    public void testSortStringOneField() throws Exception {
        database.openConnection();
        database.deleteIndex();
        database.defaultMap();

        Stream.of(1.0f,2.0f,3.0f).forEach(f -> {
            SearchElasticModel model = new SearchElasticModel();
            model.f = f;
            model.save();
        });

        database.commitTransaction(database.openConnection(), true);

        List<SearchElasticModel> fooResult = Query
                .from(SearchElasticModel.class)
                .sortAscending("one")
                .selectAll();
    }

    @Test(expected = Query.NoFieldException.class)
    public void testSortStringNoSuchField() throws Exception {
        //database.openConnection();
        //database.deleteIndex();
        //database.defaultMap();

        Stream.of(1.0f,2.0f,3.0f).forEach(f -> {
            SearchElasticModel model = new SearchElasticModel();
            model.f = f;
            model.save();
        });

        database.commitTransaction(database.openConnection(), true);

        List<SearchElasticModel> fooResult = Query
                .from(SearchElasticModel.class)
                .sortAscending("nine")
                .selectAll();
    }


    @Test
    public void testReadAllAt2() throws Exception {

        Settings.setOverride(database.SETTING_KEY_PREFIX + "searchMaxRows", "2");

        for (int i = 0; i < 50; i++) {
            SearchElasticModel model = new SearchElasticModel();
            model.f = (float) i;
            model.save();
        }

        database.commitTransaction(database.openConnection(), true);

        List<SearchElasticModel> fooResult = Query
                .from(SearchElasticModel.class)
                .sortAscending("f")
                .selectAll();

        assertThat("check size", fooResult, hasSize(50));

        Settings.setOverride(database.SETTING_KEY_PREFIX + "searchMaxRows", "1000");
    }

    @Test
    public void testSortFloat() throws Exception {
        Stream.of(1.0f,2.0f,3.0f).forEach(f -> {
            SearchElasticModel model = new SearchElasticModel();
            model.f = f;
            model.save();
        });

        database.commitTransaction(database.openConnection(), true);

        List<SearchElasticModel> fooResult = Query
                .from(SearchElasticModel.class)
                .sortAscending("f")
                .selectAll();

        assertThat("check size", fooResult, hasSize(3));
        assertThat("check 0 and 1 order", fooResult.get(0).f, lessThan(fooResult.get(1).f));
        assertThat("check 1 and 2 order", fooResult.get(1).f, lessThan(fooResult.get(2).f));
    }

    @Test
    public void testQueryExtension() throws Exception {
        SearchElasticModel search = new SearchElasticModel();
        search.eid = "111111";
        search.name = "Bill";
        search.message = "Welcome";
        search.save();

        database.commitTransaction(database.openConnection(), true);

        List<SearchElasticModel> r = Query
                .from(SearchElasticModel.class)
                .where("eid matches ?", "111111")
                .selectAll();

        assertThat(r, notNullValue());
        if (r != null) {
            assertThat(r, hasSize(1));
            assertEquals("Bill", r.get(0).getName());
            assertEquals("Welcome", r.get(0).getMessage());
        }
    }

    @Test
    public void testReferenceAscending() throws Exception {
        Stream.of(1.0f,2.0f,3.0f).forEach(f -> {
            SearchElasticModel ref = new SearchElasticModel();
            ref.f = f;
            ref.save();
            SearchElasticModel model = new SearchElasticModel();
            //model.f = f;
            model.setReference(ref);
            model.save();
        });

        database.commitTransaction(database.openConnection(), true);

        List<SearchElasticModel> fooResult = Query
                .from(SearchElasticModel.class)
                .sortAscending("reference/f")
                .selectAll();

        assertThat("check size", fooResult, hasSize(6));
        assertThat("check 0 and 1 order", fooResult.get(0).f, lessThan(fooResult.get(1).f));
        assertThat("check 1 and 2 order", fooResult.get(1).f, lessThan(fooResult.get(2).f));
    }

    @Test
    public void testDateNewestBoost() throws Exception {
        Stream.of(new java.util.Date(), DateUtils.addHours(new java.util.Date(), -5), DateUtils.addDays(new java.util.Date(), -5), DateUtils.addDays(new java.util.Date(), -10)).forEach(d -> {
            SearchElasticModel model = new SearchElasticModel();
            model.post_date = d;
            model.save();
        });

        database.commitTransaction(database.openConnection(), true);

        List<SearchElasticModel> fooResult = Query
                .from(SearchElasticModel.class)
                .sortNewest(2.0, "post_date")
                .selectAll();

        assertThat("check size", fooResult, hasSize(4));
        assertThat("check 0 and 1 order", fooResult.get(0).post_date.getTime(), greaterThan(fooResult.get(1).post_date.getTime()));
        assertThat("check 1 and 2 order", fooResult.get(1).post_date.getTime(), greaterThan(fooResult.get(2).post_date.getTime()));
        assertThat("check 2 and 3 order", fooResult.get(2).post_date.getTime(), greaterThan(fooResult.get(3).post_date.getTime()));
    }

    @Test
    public void testDateOldestBoost() throws Exception {
        Stream.of(new java.util.Date(), DateUtils.addHours(new java.util.Date(), -5), DateUtils.addDays(new java.util.Date(), -5), DateUtils.addDays(new java.util.Date(), -10)).forEach(d -> {
            SearchElasticModel model = new SearchElasticModel();
            model.post_date = d;
            model.save();
        });

        database.commitTransaction(database.openConnection(), true);

        List<SearchElasticModel> fooResult = Query
                .from(SearchElasticModel.class)
                .sortOldest(2.0, "post_date")
                .selectAll();

        assertThat("check size", fooResult, hasSize(4));
        assertThat("check 0 and 1 order", fooResult.get(0).post_date.getTime(), lessThan(fooResult.get(1).post_date.getTime()));
        assertThat("check 1 and 2 order", fooResult.get(1).post_date.getTime(), lessThan(fooResult.get(2).post_date.getTime()));
        assertThat("check 2 and 3 order", fooResult.get(2).post_date.getTime(), lessThan(fooResult.get(3).post_date.getTime()));
    }

    @Test
    public void testDateOldestBoostRelevant() throws Exception {
        Stream.of(new java.util.Date(), DateUtils.addHours(new java.util.Date(), -5), DateUtils.addDays(new java.util.Date(), -5), DateUtils.addDays(new java.util.Date(), -10)).forEach(d -> {
            SearchElasticModel model = new SearchElasticModel();
            model.post_date = d;
            model.save();
        });

        database.commitTransaction(database.openConnection(), true);

        List<SearchElasticModel> fooResult = Query
                .from(SearchElasticModel.class)
                .sortOldest(2.0, "post_date").sortRelevant(10.0, "post_date matches ?", new java.util.Date())
                .selectAll();

        assertThat("check size", fooResult, hasSize(4));
        assertThat("check 0 and 1 order", fooResult.get(0).post_date.getTime(), lessThan(fooResult.get(1).post_date.getTime()));
        assertThat("check 1 and 2 order", fooResult.get(1).post_date.getTime(), lessThan(fooResult.get(2).post_date.getTime()));
        assertThat("check 2 and 3 order", fooResult.get(2).post_date.getTime(), lessThan(fooResult.get(3).post_date.getTime()));
    }


}
