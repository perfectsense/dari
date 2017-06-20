package com.psddev.dari.elasticsearch;

import com.psddev.dari.test.LocationIndexTest;
import com.psddev.dari.test.ModificationDenormalizedTest;
import com.psddev.dari.test.ModificationEmbeddedTest;
import com.psddev.dari.test.NumberIndexTest;
import com.psddev.dari.test.PhraseIndexTest;
import com.psddev.dari.test.ReadTest;
import com.psddev.dari.test.RegionCircleIndexTest;
import com.psddev.dari.test.RegionIndexTest;
import com.psddev.dari.test.RegionLocationTest;
import com.psddev.dari.test.SearchArticleIndexTest;
import com.psddev.dari.test.SearchIndexTest;
import com.psddev.dari.test.SearchIteratorTest;
import com.psddev.dari.test.SingletonTest;
import com.psddev.dari.test.StateIndexTest;
import com.psddev.dari.test.StringIndexTest;
import com.psddev.dari.test.TypeIndexTest;
import com.psddev.dari.test.UuidIndexTest;
import com.psddev.dari.test.WriteTest;
import com.psddev.dari.util.Settings;
import junit.framework.JUnit4TestAdapter;
import junit.framework.TestSuite;
import org.apache.http.HttpEntity;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static com.psddev.dari.elasticsearch.ElasticsearchDatabase.TEMPLATE_NAME;

@RunWith(Suite.class)
@Suite.SuiteClasses({
        ElasticDBSuite.ElasticTests.class
})
public class ElasticDBSuite {
    public static class ElasticTests {

        private static final Logger LOGGER = LoggerFactory.getLogger(ElasticTests.class);
        private static boolean isEmbedded = false;
        static final String DEFAULT_DATABASE_NAME = "dari/defaultDatabase";
        static final String DATABASE_NAME = "elasticsearch";
        static final String SETTING_KEY_PREFIX = "dari/database/" + DATABASE_NAME + "/";

        public static boolean getIsEmbedded() {
            return isEmbedded;
        }

        public static void setIsEmbedded(boolean isEmbedded) {
            ElasticTests.isEmbedded = isEmbedded;
        }

        /**
         * delete Elastic Template
         */
        public static void deleteTemplate(String nodeHost) throws IOException {
            LOGGER.info("Deleting Template " + TEMPLATE_NAME);
            try {
                int timeout = 10;
                RequestConfig config = RequestConfig.custom()
                        .setConnectTimeout(timeout * 1000)
                        .setConnectionRequestTimeout(timeout * 1000)
                        .setSocketTimeout(timeout * 1000).build();
                CloseableHttpClient httpClient = HttpClientBuilder.create().setDefaultRequestConfig(config).build();

                HttpDelete delete = new HttpDelete(nodeHost + "_template/" + TEMPLATE_NAME);
                delete.addHeader("accept", "application/json");
                try (CloseableHttpResponse response = httpClient.execute(delete)) {
                    HttpEntity entity = response.getEntity();
                    String res = EntityUtils.toString(entity);
                    EntityUtils.consume(entity);
                    LOGGER.info("Deleted Template {} [{}] {}", TEMPLATE_NAME, res, response.getStatusLine().getStatusCode());
                }
            } catch (Exception error) {
                LOGGER.warn(
                        String.format("Warning: deleteTemplate[%s: %s]",
                                error.getClass().getName(),
                                error.getMessage()),
                        error);
                throw error;
            }
        }

        /**
         *
         */
        public static void deleteIndex(String index, String nodeHost) throws IOException {
            LOGGER.info("Deleting Index " + index);
            try {
                int timeout = 10;
                RequestConfig config = RequestConfig.custom()
                        .setConnectTimeout(timeout * 1000)
                        .setConnectionRequestTimeout(timeout * 1000)
                        .setSocketTimeout(timeout * 1000).build();
                CloseableHttpClient httpClient = HttpClientBuilder.create().setDefaultRequestConfig(config).build();

                HttpDelete delete = new HttpDelete(nodeHost + index);
                delete.addHeader("accept", "application/json");
                try (CloseableHttpResponse response = httpClient.execute(delete)) {
                    HttpEntity entity = response.getEntity();
                    String res = EntityUtils.toString(entity);
                    EntityUtils.consume(entity);
                    LOGGER.info("Deleted Index {} [{}] {}", index, res, response.getStatusLine().getStatusCode());
                }
            } catch (Exception error) {
                LOGGER.warn(
                        String.format("Warning: deleteIndex[%s: %s]",
                                error.getClass().getName(),
                                error.getMessage()),
                        error);
                throw error;
            }
        }

        public static void ElasticSetupDatabase() throws Exception {

            LOGGER.info("ElasticSetupDatabase");

            String clusterName = "elasticsearch_a";

            Settings.setOverride(DEFAULT_DATABASE_NAME, DATABASE_NAME);
            Settings.setOverride(SETTING_KEY_PREFIX + "class", ElasticsearchDatabase.class.getName());
            Settings.setOverride(SETTING_KEY_PREFIX + ElasticsearchDatabase.CLUSTER_NAME_SUB_SETTING, clusterName);
            Settings.setOverride(SETTING_KEY_PREFIX + ElasticsearchDatabase.INDEX_NAME_SUB_SETTING, "index1");
            Settings.setOverride(SETTING_KEY_PREFIX + ElasticsearchDatabase.SHARDS_MAX_SETTING, "1000");
            Settings.setOverride(SETTING_KEY_PREFIX + ElasticsearchDatabase.PREFERFILTERS_SETTING, "true");
            Settings.setOverride(SETTING_KEY_PREFIX + ElasticsearchDatabase.DFS_QUERY_THEN_FETCH_SETTING, "true");
            //Settings.setOverride(SETTING_KEY_PREFIX + ElasticsearchDatabase.DEFAULT_DATAFIELD_TYPE_SETTING, ElasticsearchDatabase.RAW_DATAFIELD_TYPE);
            Settings.setOverride(SETTING_KEY_PREFIX + ElasticsearchDatabase.DEFAULT_DATAFIELD_TYPE_SETTING, ElasticsearchDatabase.JSON_DATAFIELD_TYPE);
            Settings.setOverride(SETTING_KEY_PREFIX + ElasticsearchDatabase.DATA_TYPE_RAW_SETTING, "-* +com.psddev.dari.test.WriteModel ");
            Settings.setOverride(SETTING_KEY_PREFIX + "1/" + ElasticsearchDatabase.CLUSTER_PORT_SUB_SETTING, "9300");
            Settings.setOverride(SETTING_KEY_PREFIX + "1/" + ElasticsearchDatabase.CLUSTER_REST_PORT_SUB_SETTING, "9200");
            Settings.setOverride(SETTING_KEY_PREFIX + "1/" + ElasticsearchDatabase.HOSTNAME_SUB_SETTING, "localhost");
            Settings.setOverride(SETTING_KEY_PREFIX + ElasticsearchDatabase.SUBQUERY_RESOLVE_LIMIT_SETTING, "1000");

            String nodeHost = ElasticsearchDatabase.getNodeHost("localhost", "9200");
            if (!ElasticsearchDatabase.checkAlive(nodeHost)) {
                LOGGER.info("Starting Embedded");
                // ok create embedded since it is not already running for test
                isEmbedded = true;
                EmbeddedElasticsearchServer.deleteDataDirectory();
                EmbeddedElasticsearchServer.setup(clusterName);
            } else {
                LOGGER.info("Already running");
            }
            String verifyClusterName = ElasticsearchDatabase.getClusterName(nodeHost);
            Settings.setOverride(SETTING_KEY_PREFIX + ElasticsearchDatabase.CLUSTER_NAME_SUB_SETTING, verifyClusterName);
            deleteIndex(Settings.get(SETTING_KEY_PREFIX + ElasticsearchDatabase.INDEX_NAME_SUB_SETTING) + "*", nodeHost);
            deleteTemplate(nodeHost);
        }

        public static TestSuite suite() throws Exception {
            LOGGER.info("Starting Elastic test");
            ElasticSetupDatabase();
            Runtime.getRuntime().addShutdownHook(new Thread(EmbeddedElasticsearchServer::shutdown));
            TestSuite suite = new TestSuite();
            suite.addTest(new JUnit4TestAdapter(PhraseIndexTest.class));
            suite.addTest(new JUnit4TestAdapter(SearchIteratorTest.class));
            suite.addTest(new JUnit4TestAdapter(ElasticDatabaseConnectionTest.class));
            suite.addTest(new JUnit4TestAdapter(ElasticInitializationTest.class));
            suite.addTest(new JUnit4TestAdapter(LocationIndexTest.class));
            suite.addTest(new JUnit4TestAdapter(ModificationDenormalizedTest.class));
            suite.addTest(new JUnit4TestAdapter(ModificationEmbeddedTest.class));
            suite.addTest(new JUnit4TestAdapter(NumberIndexTest.class));
            suite.addTest(new JUnit4TestAdapter(ReadTest.class));
            suite.addTest(new JUnit4TestAdapter(RegionCircleIndexTest.class));
            suite.addTest(new JUnit4TestAdapter(RegionIndexTest.class));
            suite.addTest(new JUnit4TestAdapter(RegionLocationTest.class));
            suite.addTest(new JUnit4TestAdapter(SearchArticleIndexTest.class));
            suite.addTest(new JUnit4TestAdapter(SearchIndexTest.class));
            suite.addTest(new JUnit4TestAdapter(SingletonTest.class));
            suite.addTest(new JUnit4TestAdapter(StateIndexTest.class));
            suite.addTest(new JUnit4TestAdapter(StringIndexTest.class));
            suite.addTest(new JUnit4TestAdapter(TypeIndexTest.class));
            suite.addTest(new JUnit4TestAdapter(UuidIndexTest.class));
            suite.addTest(new JUnit4TestAdapter(WriteTest.class));
            suite.addTest(new JUnit4TestAdapter(ElasticWriteTest.class));

            return suite;
        }

    }

}
