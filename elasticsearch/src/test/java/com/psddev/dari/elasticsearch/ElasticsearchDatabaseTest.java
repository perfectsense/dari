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

public class ElasticsearchDatabaseTest extends AbstractTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(ElasticsearchDatabase.class);

    private boolean turnOff = false;
    private String nodeHost = "";

    private static final String FOO = "foo";

    private ElasticsearchDatabase database;

    private void setNodeSettings() {
        String host = (String) Settings.get("dari/database/elasticsearch/clusterHostname");
        this.nodeHost = "http://" + host + ":9200/";
    }

    /**
     *
     * Returns Map<String, Object> settings for database.initialize() to test local ELK settings
     *
     */
    private Map<String, Object> getDatabaseSettings() {
        Map<String, Object> settings = new HashMap<>();
        settings.put("clusterName", Settings.get(ElasticsearchDatabase.SETTING_KEY_PREFIX + "clusterName"));
        settings.put("indexName", Settings.get(ElasticsearchDatabase.SETTING_KEY_PREFIX + "indexName"));
        settings.put("clusterPort", Settings.get(ElasticsearchDatabase.SETTING_KEY_PREFIX + "clusterPort"));
        settings.put("clusterHostname", Settings.get(ElasticsearchDatabase.SETTING_KEY_PREFIX + "clusterHostname"));
        return settings;
    }

    public void deleteIndex(String index) {
        try {
            HttpClient httpClient = HttpClientBuilder.create().build();
            HttpDelete delete = new HttpDelete(this.nodeHost + index);
            delete.addHeader("accept", "application/json");
            HttpResponse response = httpClient.execute(delete);
            String json = EntityUtils.toString(response.getEntity());
        } catch (ClientProtocolException e) {
            e.printStackTrace();
            assertTrue("ClientProtocolException", 1==0);
        } catch (IOException e) {
            e.printStackTrace();
            assertTrue("IOException", 1==0);
        }
    }

    public void createIndexandMapping(String index) {
        try {
            String json = "{\n" +
                    "  \"mappings\": {\n" +
                    "    \"_default_\": {\n" +
                    "      \"dynamic_templates\": [\n" +
                    "        {\n" +
                    "          \"int_template\": {\n" +
                    "            \"match\": \"_*\",\n" +
                    "            \"match_mapping_type\": \"string\",\n" +
                    "            \"mapping\": {\n" +
                    "              \"type\": \"string\",\n" +
                    "              \"index\": \"not_analyzed\"\n" +
                    "            }\n" +
                    "          }\n" +
                    "        },\n" +
                    "        {\n" +
                    "          \"notanalyzed\": {\n" +
                    "            \"match\": \"*\",\n" +
                    "            \"match_mapping_type\": \"string\",\n" +
                    "            \"mapping\": {\n" +
                    "              \"type\": \"text\",\n" +
                    "              \"index\": \"analyzed\",\n" +
                    "              \"fields\": {\n" +
                    "                \"raw\": {\n" +
                    "                  \"type\": \"string\",\n" +
                    "                  \"index\": \"not_analyzed\"\n" +
                    "                }\n" +
                    "              }\n" +
                    "            }\n" +
                    "          }\n" +
                    "        }\n" +
                    "      ]\n" +
                    "    }\n" +
                    "  }\n" +
                    "}";
            HttpClient httpClient = HttpClientBuilder.create().build();
            HttpPut put = new HttpPut(this.nodeHost + index);
            put.addHeader("accept", "application/json");
            StringEntity input = new StringEntity(json);
            put.setEntity(input);
            HttpResponse response = httpClient.execute(put);
            if (response.getStatusLine().getStatusCode() > 201) {
                LOGGER.info("ELK createIndexandMapping Response > 201");
                assertTrue("Response > 201", 1==0);
            }
            json = EntityUtils.toString(response.getEntity());
        } catch (ClientProtocolException e) {
            LOGGER.info("ELK createIndexandMapping ClientProtocolException");
            e.printStackTrace();
            assertTrue("ClientProtocolException", 1==0);
        } catch (IOException e) {
            LOGGER.info("ELK createIndexandMapping IOException");
            e.printStackTrace();
            assertTrue("IOException", 1==0);
        }
    }


    @Before
    public void before() {
        // verify it is running locally for testing - if not local short circuit it
        // embedded elasticsearch was dropped in 5.1.2
        // cmd> brew install elasticsearch
        // cmd> elasticsearch
        this.database = new ElasticsearchDatabase();


        try {
            setNodeSettings();
            HttpClient httpClient = HttpClientBuilder.create().build();
            HttpGet getRequest = new HttpGet(this.nodeHost);
            getRequest.addHeader("accept", "application/json");
            HttpResponse response = httpClient.execute(getRequest);
            String json = EntityUtils.toString(response.getEntity());
            JSONObject j = new JSONObject(json);
            assertThat(j, notNullValue());
            if (j != null) {
                assertThat(j.get("cluster_name"), notNullValue());
                if (j.get("cluster_name") != null) {
                    String clusterName = j.getString("cluster_name");
                    Settings.setOverride(ElasticsearchDatabase.SETTING_KEY_PREFIX + "clusterName", clusterName);
                }
                assertThat(j.get("cluster_name"), notNullValue());
                if (j.get("version") != null) {
                    if (j.getJSONObject("version") != null) {
                        JSONObject jo = j.getJSONObject("version");
                        String version = jo.getString("number");
                        if (!version.equals("5.2.0")) {
                            LOGGER.warn("Warning: ELK {} version is not 5.2.0", version);
                        }
                        assertEquals(version.substring(0, 2), "5.");
                    }
                }
                // you must delete and set map for this all to work, 2nd run we can leave it.
                deleteIndex((String)Settings.get("dari/database/elasticsearch/indexName"));
                createIndexandMapping((String)Settings.get("dari/database/elasticsearch/indexName"));
                database.initialize("", getDatabaseSettings());
            }
        } catch (java.net.ConnectException e) {
            this.turnOff = true;
            LOGGER.info("ELK is not able to connect turning off tests for ELK. nodeHost {}", this.nodeHost);
        } catch (ClientProtocolException e) {
            e.printStackTrace();
            assertTrue("ClientProtocolException", 1==0);
        } catch (IOException e) {
            e.printStackTrace();
            assertTrue("IOException", 1==0);
        } catch (org.json.JSONException e) {
            e.printStackTrace();
            assertTrue("JSONException", 1==0);
        }
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
    public void oneMatches() {
        Stream.of(FOO, "bar", "qux").forEach(string -> {
            SearchElasticModel model = new SearchElasticModel();
            model.one = string;
            model.set.add(FOO);
            model.list.add(FOO);
            model.map.put(FOO, FOO);
            model.save();
        });

        List<SearchElasticModel> fooResult = Query
                .from(SearchElasticModel.class)
                .where("one matches ?", FOO)
                .selectAll();

        assertThat(fooResult, hasSize(1));
        assertThat(fooResult.get(0).one, equalTo(FOO));
    }

    @Test
    public void setMatches() {
        Stream.of(FOO, "bar", "qux").forEach(string -> {
            SearchElasticModel model = new SearchElasticModel();
            model.one = FOO;
            model.set.add(string);
            model.list.add(FOO);
            model.map.put(FOO, FOO);
            model.save();
        });

        List<SearchElasticModel> fooResult = Query
                .from(SearchElasticModel.class)
                .where("set matches ?", FOO)
                .selectAll();

        assertThat(fooResult, hasSize(1));
        assertThat(fooResult.get(0).set, hasSize(1));
        assertThat(fooResult.get(0).set.iterator().next(), equalTo(FOO));
    }

    @Test
    public void listMatches() {
        Stream.of(FOO, "bar", "qux").forEach(string -> {
            SearchElasticModel model = new SearchElasticModel();
            model.one = FOO;
            model.set.add(FOO);
            model.list.add(string);
            model.map.put(FOO, FOO);
            model.save();
        });

        List<SearchElasticModel> fooResult = Query
                .from(SearchElasticModel.class)
                .where("list matches ?", FOO)
                .selectAll();

        assertThat(fooResult, hasSize(1));
        assertThat(fooResult.get(0).list, hasSize(1));
        assertThat(fooResult.get(0).list.get(0), equalTo(FOO));
    }

    @Test
    public void mapMatches() {
        Stream.of(FOO, "bar", "qux").forEach(string -> {
            SearchElasticModel model = new SearchElasticModel();
            model.one = FOO;
            model.set.add(FOO);
            model.list.add(FOO);
            model.map.put(string, string);
            model.save();
        });

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
    public void anyMatches() {
        Stream.of(FOO, "bar", "qux").forEach(string -> {
            SearchElasticModel model = new SearchElasticModel();
            model.one = string;
            model.set.add(FOO);
            model.save();
        });

        List<SearchElasticModel> fooResult = Query
                .from(SearchElasticModel.class)
                .where("_any matches ?", FOO)
                .selectAll();

        assertThat(fooResult, hasSize(3));
    }

    @Test
    public void wildcard() {
        Stream.of("f", "fo", "foo").forEach(string -> {
            SearchElasticModel model = new SearchElasticModel();
            model.one = string;
            model.save();
        });

        assertThat(Query.from(SearchElasticModel.class).where("one matches ?", "f*").count(), equalTo(3L));
        assertThat(Query.from(SearchElasticModel.class).where("one matches ?", "fo*").count(), equalTo(2L));
        assertThat(Query.from(SearchElasticModel.class).where("one matches ?", "foo*").count(), equalTo(1L));
    }


    @Test
    public void sortRelevant() {
        SearchElasticModel model = new SearchElasticModel();
        model.one = FOO;
        model.name = FOO;
        model.set.add(FOO);
        model.list.add(FOO);
        model.map.put(FOO, FOO);
        model.eid = "1";
        model.save();

        model = new SearchElasticModel();
        model.one = FOO;
        model.name = "qux";
        model.set.add(FOO);
        model.list.add("qux");
        model.map.put("qux", "qux");
        model.eid = "2";
        model.save();

        model = new SearchElasticModel();
        model.one = "qux";
        model.name = "qux";
        model.set.add("foo");
        model.list.add("qux");
        model.map.put("qux", "qux");
        model.eid = "3";
        model.save();


        List<SearchElasticModel> fooResult = Query
                .from(SearchElasticModel.class)
                .where("_any matches ?", FOO)
                .sortRelevant(1.0, "_any matches ?", FOO)
                .selectAll();

        assertThat(fooResult, hasSize(3));
        assertThat(fooResult.get(0).eid, is(equalTo("1")));
        // 1,2,3 is the correct order based on IDF
        assertThat(fooResult.get(1).eid, is(equalTo("2")));
        assertThat(fooResult.get(2).eid, is(equalTo("3")));
    }

    @Test
    public void sortString() {
        Stream.of(FOO, "bar", "qux").forEach(string -> {
            SearchElasticModel model = new SearchElasticModel();
            model.one = string;
            model.set.add(FOO);
            model.save();
        });

        List<SearchElasticModel> fooResult = Query
                .from(SearchElasticModel.class)
                .sortAscending("one")
                .selectAll();

        assertThat("check size", fooResult, hasSize(3));
        assertThat("check 0 and 1 order", fooResult.get(0).one.toString(), lessThan(fooResult.get(1).one.toString()));
        assertThat("check 1 and 2 order", fooResult.get(1).one.toString(), greaterThan(fooResult.get(2).one.toString()));
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
