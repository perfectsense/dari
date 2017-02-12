package com.psddev.dari.elasticsearch;

import com.psddev.dari.util.Settings;
import com.zaxxer.hikari.HikariDataSource;
import javafx.scene.NodeBuilder;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public abstract class AbstractTest {

    private static final String DATABASE_NAME = "elasticsearch";
    private static final String SETTING_KEY_PREFIX = "dari/database/" + DATABASE_NAME + "/";

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractTest.class);

    private String nodeHost = "";

    private EmbeddedElasticsearchServer embeddedElasticsearchServer = null;


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
                    "              \"type\": \"keyword\"\n" +
                    "            }\n" +
                    "          }\n" +
                    "        },\n" +
                    "        {\n" +
                    "          \"notanalyzed\": {\n" +
                    "            \"match\": \"*\",\n" +
                    "            \"match_mapping_type\": \"string\",\n" +
                    "            \"mapping\": {\n" +
                    "              \"type\": \"text\",\n" +
                    "              \"fields\": {\n" +
                    "                \"raw\": {\n" +
                    "                  \"type\": \"keyword\"\n" +
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

    private void setNodeSettings() {
        String host = (String) Settings.get("dari/database/elasticsearch/clusterHostname");
        this.nodeHost = "http://" + host + ":9200/";
    }

    public static Map<String, Object> getDatabaseSettings() {
        Map<String, Object> settings = new HashMap<>();
        settings.put("clusterName", Settings.get(ElasticsearchDatabase.SETTING_KEY_PREFIX + "clusterName"));
        settings.put("indexName", Settings.get(ElasticsearchDatabase.SETTING_KEY_PREFIX + "indexName"));
        settings.put("clusterPort", Settings.get(ElasticsearchDatabase.SETTING_KEY_PREFIX + "clusterPort"));
        settings.put("clusterHostname", Settings.get(ElasticsearchDatabase.SETTING_KEY_PREFIX + "clusterHostname"));
        return settings;
    }

    public void before() {
        // need to deleteIndex and Map it!
        setNodeSettings();
        ElasticsearchDatabase e = new ElasticsearchDatabase();
        String clusterName = e.getClusterName(this.nodeHost);
        Settings.setOverride(ElasticsearchDatabase.SETTING_KEY_PREFIX + "clusterName", clusterName);
        assertThat(clusterName, notNullValue());
        String version = e.getVersion(this.nodeHost);
        assertEquals(version.substring(0, 2), "5.");
        // you must delete and set map for this all to work, 2nd run we can leave it.
        deleteIndex((String)Settings.get("dari/database/elasticsearch/indexName"));
        createIndexandMapping((String)Settings.get("dari/database/elasticsearch/indexName"));
    }

    @Before
    public void startEmbeddedElasticsearchServer() {
        // check to see if you have a node already spun up locally, if not start embedded one
        setNodeSettings();
        ElasticsearchDatabase e = new ElasticsearchDatabase();
        String clusterName = e.getClusterName(this.nodeHost);
        if (clusterName == null) {
            embeddedElasticsearchServer = new EmbeddedElasticsearchServer();
        }
    }

    @After
    public void shutdownEmbeddedElasticsearchServer() {
        if (embeddedElasticsearchServer != null)
        embeddedElasticsearchServer.shutdown();
    }



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
