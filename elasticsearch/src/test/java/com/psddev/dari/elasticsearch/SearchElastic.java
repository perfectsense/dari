package com.psddev.dari.elasticsearch;

import com.psddev.dari.db.Query;
import com.psddev.dari.util.Settings;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.BasicHttpEntity;
import org.apache.http.entity.StringEntity;
import org.junit.After;
import org.junit.Test;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.List;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.util.*;
import com.psddev.dari.util.PaginatedResult;
import org.apache.http.util.EntityUtils;
import org.junit.*;

import static com.psddev.dari.db.Database.DEFAULT_DATABASE_SETTING;
import static org.junit.Assert.*;
import org.json.JSONObject;
import java.io.IOException;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SearchElastic extends AbstractTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(SearchElastic.class);

    private static final String FOO = "foo";

    private ElasticsearchDatabase database;


    @Before
    public void before() {
        // verify it is running locally for testing - if not local short circuit it
        // embedded elasticsearch was dropped in 5.1.2
        // cmd> brew install elasticsearch
        // cmd> elasticsearch
        super.before();
        this.database = new ElasticsearchDatabase();
        database.initialize("", getDatabaseSettings());
    }

    @After
    public void deleteModels() {
        if (this.turnOff == false) {
            Query.from(SearchElasticModel.class).deleteAll();
        }
    }


    @Test
    public void testOne() throws Exception {
        if (this.turnOff == false) {
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
    }

    @Test
    public void oneMatches() throws Exception {
        if (this.turnOff == false) {
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
    }

    @Test
    public void setMatches() throws Exception {
        if (this.turnOff == false) {
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
    }

    @Test
    public void listMatches() throws Exception {
        if (this.turnOff == false) {
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
    }

    @Test
    public void mapMatches() throws Exception {
        if (this.turnOff == false) {
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
    }

    @Test
    public void anyMatches() throws Exception {
        if (this.turnOff == false) {
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
    }

    @Test
    public void wildcard() throws Exception {
        if (this.turnOff == false) {
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
    }


    @Test
    public void sortRelevant() throws Exception {
        if (this.turnOff == false) {
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

            //database.openConnection().admin().indices().prepareRefresh(database.getIndexName()).get();
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
    }

    @Test
    public void testSortString() throws Exception {
        if (this.turnOff == false) {

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
    }



    @Test
    public void testQueryandPagination() throws Exception {
        if (this.turnOff == false) {
            assertEquals(database.isAlive(), true);
            SearchElasticModel search = new SearchElasticModel();
            search.eid = "111111";
            search.name = "Bill";
            search.message = "Welcome";
            search.save();

            database.commitTransaction(database.openConnection(), true);

            Query<SearchElasticModel> fooResult = Query
                    .from(SearchElasticModel.class)
                    .where("eid matches ?", "111111");
            assertEquals(fooResult.getPredicate().toString(), "eid matchesany '111111'");

            PaginatedResult<SearchElasticModel> p = database.readPartial(fooResult, 0L, 1);

            assertThat(p, notNullValue());
            if (p != null) {
                assertEquals(1, p.getCount());
                assertEquals("111111", p.getItems().get(0).getEid());
                List<SearchElasticModel> r = p.getItems();
                assertThat(r, hasSize(1));
                assertEquals("Bill", r.get(0).getName());
                assertEquals("Welcome", r.get(0).getMessage());
            }
        }

    }

}
