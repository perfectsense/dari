package com.psddev.dari.elasticsearch;

import com.psddev.dari.util.Settings;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.junit.Before;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public abstract class AbstractTest {

    private static final String DATABASE_NAME = "elasticsearch";
    private static final String SETTING_KEY_PREFIX = "dari/database/" + DATABASE_NAME + "/";

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractTest.class);


    public static void deleteIndex(String index, String nodeHost) {
        LOGGER.info("Deleting Index " + index);
        try {
            HttpClient httpClient = HttpClientBuilder.create().build();
            HttpDelete delete = new HttpDelete(nodeHost + index);
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

    public static void createIndexandMapping(String index, String nodeHost) {
        LOGGER.info("Mapping Index " + index);
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
            HttpPut put = new HttpPut(nodeHost + index);
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

    private static String getNodeHost() {
        String host = (String) Settings.get("dari/database/elasticsearch/clusterHostname");
        return "http://" + host + ":9200/";
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
        String nodeHost = getNodeHost();
        String clusterName = ElasticsearchDatabase.getClusterName(nodeHost);
        Settings.setOverride(ElasticsearchDatabase.SETTING_KEY_PREFIX + "clusterName", clusterName);
        assertThat(clusterName, notNullValue());
        String version = ElasticsearchDatabase.getVersion(nodeHost);
        assertEquals(version.substring(0, 2), "5.");
    }


    @BeforeClass
    public static void createDatabase() {
        Settings.setOverride("dari/defaultDatabase", DATABASE_NAME);
        Settings.setOverride(SETTING_KEY_PREFIX + "class", ElasticsearchDatabase.class.getName());
        Settings.setOverride(SETTING_KEY_PREFIX + "clusterName", "elasticsearch_a");
        Settings.setOverride(SETTING_KEY_PREFIX + "indexName", "index1");
        Settings.setOverride(SETTING_KEY_PREFIX + "clusterPort", "9300");
        Settings.setOverride(SETTING_KEY_PREFIX + "clusterHostname", "localhost");
        String nodeHost = getNodeHost();
        // see if there is one.
        String clusterName = ElasticsearchDatabase.getClusterName(nodeHost);
        if (clusterName == null) {
            // ok create embedded
            EmbeddedElasticsearchServer.setup();
            clusterName = ElasticsearchDatabase.getClusterName(nodeHost);
        }
        Settings.setOverride(SETTING_KEY_PREFIX + "clusterName", clusterName);
        deleteIndex((String)Settings.get("dari/database/elasticsearch/indexName"), nodeHost);
        createIndexandMapping((String)Settings.get("dari/database/elasticsearch/indexName"), nodeHost);
    }
}
