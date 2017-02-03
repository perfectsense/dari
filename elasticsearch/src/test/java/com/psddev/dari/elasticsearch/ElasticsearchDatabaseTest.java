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
import java.util.stream.Stream;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.HttpClientBuilder;


public class ElasticsearchDatabaseTest extends AbstractTest {
    private static final String DATABASE_NAME = "elasticsearch";
    private static final String SETTING_KEY_PREFIX = "dari/database/" + DATABASE_NAME + "/";

    private String clusterName;
    private final String host = "localhost";
    private final String nodeHost = "http://" + this.host + ":9200/";

    private ElasticsearchDatabase database;
    private Map<String, Object> settings;

    private void setSettings() {
        settings.put("clusterName", this.clusterName);
        settings.put("indexName", "index1");
        settings.put(DEFAULT_DATABASE_SETTING, DATABASE_NAME);
    }


    public void createIndexandMapping(String index) {
        try {
            String json = "{\n" +
                    "  \"mappings\": {\n" +
                    "    \"_default_\": {\n" +
                    "        \"dynamic_templates\": [\n" +
                    "            { \"notanalyzed\": {\n" +
                    "                  \"match\":              \"*\", \n" +
                    "                  \"match_mapping_type\": \"string\",\n" +
                    "                  \"mapping\": {\n" +
                    "                      \"type\":        \"string\",\n" +
                    "                      \"index\":       \"not_analyzed\"\n" +
                    "                  }\n" +
                    "               }\n" +
                    "            }\n" +
                    "          ]\n" +
                    "       }\n" +
                    "   }\n" +
                    "}";
            HttpClient httpClient = HttpClientBuilder.create().build();
            HttpPut put = new HttpPut(this.nodeHost + index);
            put.addHeader("accept", "application/json");
            StringEntity input = new StringEntity(json);
            put.setEntity(input);
            HttpResponse response = httpClient.execute(put);
            if (response.getStatusLine().getStatusCode() > 201) {
                assertTrue("Response > 201", 1==0);
            }
            json = EntityUtils.toString(response.getEntity());
        } catch (ClientProtocolException e) {
            e.printStackTrace();
            assertTrue("ClientProtocolException", 1==0);
        } catch (IOException e) {
            e.printStackTrace();
            assertTrue("IOException", 1==0);
        }
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

    @Before
    public void before() {
        // verify it is running locally for testing
        // embedded elasticsearch was dropped in 5.1.2
        // cmd> brew install elasticsearch
        // cmd> elasticsearch
        database = new ElasticsearchDatabase();
        settings = new HashMap<>();

        try {
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
                    this.clusterName = j.getString("cluster_name");
                }
                assertThat(j.get("cluster_name"), notNullValue());
                if (j.get("version") != null) {
                    if (j.getJSONObject("version") != null) {
                        JSONObject jo = j.getJSONObject("version");
                        String version = jo.getString("number");
                        assertEquals(version, "5.1.2");
                    }
                }
                setSettings();
                //deleteIndex((String)(this.settings).get("indexName"));
                //createIndexandMapping((String)(this.settings).get("indexName"));
                database.doInitialize(SETTING_KEY_PREFIX, this.settings);
                String json1 = "{\n" +
                        "    \"guid\": \"a1e83275-5bae-4304-8197-f936477d4378\",\n" +
                        "    \"name\" : \"Apollo 13\",\n" +
                        "    \"eid\" : \"a1e83275-5bae-4304-8197-f936477d4378\",\n" +
                        "    \"post_date\" : \"2009-11-15T14:12:12\",\n" +
                        "    \"message\" : \"trying out Elastic Search\"\n" +
                        "}\n";
                //database.saveJson(json1, "b1e83275-5bae-4304-8197-f936477d4378", "a1e83275-5bae-4304-8197-f936477d4378");
                json1 = "{\n" +
                        "    \"guid\": \"a1e83275-5bae-4304-8197-f936477d4379\",\n" +
                        "    \"name\" : \"Apollo 14\",\n" +
                        "    \"eid\" : \"a1e83275-5bae-4304-8197-f936477d4379\",\n" +
                        "    \"post_date\" : \"2009-11-15T14:12:12\",\n" +
                        "    \"message\" : \"This is message 2\"\n" +
                        "}";
                //database.saveJson(json1, "b1e83275-5bae-4304-8197-f936477d4379", "a1e83275-5bae-4304-8197-f936477d4379");

            }
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
    public void after() {
    }

    @After
    public void deleteModels() {
       Query.from(SearchElasticModel.class).deleteAll();
    }

    @Test
    public void testOne() throws Exception {

        SearchElasticModel search = new SearchElasticModel();
        search.eid = "939393";
        search.name = "Bill";
        search.message = "tough";
        search.save();

        String nameMatch = "939393";
        List<SearchElasticModel> fooResult = Query
                .from(SearchElasticModel.class)
                .where("eid matches ?", nameMatch)
                .selectAll();

        assertThat(fooResult, hasSize(1));
        //assertThat(fooResult.get(0).eid, equalTo(FOO));
    }


    @Test
    public void testQueryandPagination() throws Exception {
        SearchElasticModel search = new SearchElasticModel();
        search.eid = "111111";
        search.name = "Bill";
        search.message = "Welcome";
        search.save();

        settings.put("clusterName", this.clusterName);
        settings.put("indexName", "index1");
        settings.put("typeName", "mytype1");
        settings.put("typeClass", SearchElasticModel.class);
        settings.put(DEFAULT_DATABASE_SETTING, DATABASE_NAME);
        settings.put("class", ElasticsearchDatabase.class.getName());
        database.doInitialize(SETTING_KEY_PREFIX, settings);
        assertEquals(database.isAlive(), true);

        String nameMatch = "111111";
        Query<SearchElasticModel> fooResult = Query
                .from(SearchElasticModel.class)
                .where("eid matches ?", nameMatch);

        PaginatedResult<SearchElasticModel> p = database.readPartial(fooResult, 0L, 1);

        assertThat(p, notNullValue());
        if (p != null) {
            List<SearchElasticModel> r = p.getItems();
            assertThat(r, hasSize(1));
        }


    }

}
