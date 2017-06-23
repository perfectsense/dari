package com.psddev.dari.elasticsearch;

import com.psddev.dari.db.Database;
import com.psddev.dari.db.PredicateParser;
import com.psddev.dari.db.Query;
import com.psddev.dari.test.SearchIndexModel;
import com.psddev.dari.util.CollectionUtils;
import com.psddev.dari.util.ObjectUtils;
import com.psddev.dari.util.Settings;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.node.Node;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertThat;

public class ElasticInitializationTest {

    private ElasticsearchDatabase database;
    private Map<String, Object> settings;

    @Before
    public void before() {
        database = new ElasticsearchDatabase();
        settings = new HashMap<>();
    }

    @After
    public void deleteModels() {
        Query.from(SearchIndexModel.class).deleteAllImmediately();
        Query.from(ElasticModel.class).deleteAllImmediately();
    }

    private void put(String path, Object value) {
        CollectionUtils.putByPath(settings, path, value);
    }

    @Test
    public void embeddedElastic() {
        String nodeHost = "http://localhost:9200/";
        assertThat(ElasticsearchDatabase.checkAlive(nodeHost), is(true));

        String elasticCluster = ElasticsearchDatabase.getClusterName(nodeHost);
        assertThat(elasticCluster, is(notNullValue()));
        if (ElasticDBSuite.ElasticTests.getIsEmbedded()) {
            Node node = EmbeddedElasticsearchServer.getNode();
            assertThat(node, is(notNullValue()));
        }

        Settings.setOverride(ElasticDBSuite.ElasticTests.DEFAULT_DATABASE_NAME, ElasticDBSuite.ElasticTests.DATABASE_NAME);

        put(ElasticDBSuite.ElasticTests.DEFAULT_DATABASE_NAME, ElasticDBSuite.ElasticTests.DATABASE_NAME);
        put(ElasticsearchDatabase.INDEX_NAME_SUB_SETTING + "class", ElasticsearchDatabase.class.getName());
        put(ElasticsearchDatabase.CLUSTER_NAME_SUB_SETTING, elasticCluster);
        put(ElasticsearchDatabase.INDEX_NAME_SUB_SETTING, "index1");
        put("1/" + ElasticsearchDatabase.CLUSTER_PORT_SUB_SETTING, "9300");
        put("1/" + ElasticsearchDatabase.CLUSTER_REST_PORT_SUB_SETTING, "9200");
        put("1/" + ElasticsearchDatabase.HOSTNAME_SUB_SETTING, "localhost");
        put(ElasticsearchDatabase.SUBQUERY_RESOLVE_LIMIT_SETTING, "1000");
        database.initialize("", settings);
        assertThat(database.getIndexName(), is("index1"));
        assertThat(database.getClusterName(), is(elasticCluster));
        assertThat(database.getClusterNodes(), hasSize(1));
        List<ElasticsearchNode> n = database.getClusterNodes();
        assertThat(n.get(0).getHostname(), is("localhost"));
        assertThat(n.get(0).getPort(), is(9300));
        assertThat(n.get(0).getRestPort(), is(9200));
    }

    @Test(expected = NullPointerException.class)
    public void testMissingSettings() {
        put(ElasticsearchDatabase.CLUSTER_NAME_SUB_SETTING, "foo");
        database.initialize("", settings);
    }

    @Test
    public void testReadAllAt2() throws Exception {

        Settings.setOverride(ElasticDBSuite.ElasticTests.SETTING_KEY_PREFIX + "searchMaxRows", "2");

        for (int i = 0; i < 50; i++) {
            SearchIndexModel model = new SearchIndexModel();
            model.f = (float) i;
            model.save();
        }

        List<SearchIndexModel> fooResult = Query
                .from(SearchIndexModel.class)
                .sortAscending("f")
                .selectAll();

        assertThat("check size", fooResult, hasSize(50));

        Settings.setOverride(ElasticDBSuite.ElasticTests.SETTING_KEY_PREFIX + "searchMaxRows", "1000");
    }

    @Test
    public void testPainless() {
        ElasticsearchDatabase db = Database.Static.getFirst(ElasticsearchDatabase.class);
        assertThat(db.isModuleInstalled("lang-painless", "org.elasticsearch.painless.PainlessPlugin"), Matchers.is(true));
    }

    @Test
    public void testReIndex() {
        SearchIndexModel search = new SearchIndexModel();
        search.eid = "939393";
        search.name = "Bill";
        search.message = "tough";
        search.save();

        Date firstUpdate = Query.from(SearchIndexModel.class).where("eid = 939393").lastUpdate();
        search.getState().index();
        Date newUpdate = Query.from(SearchIndexModel.class).where("eid = 939393").lastUpdate();

        assertThat(newUpdate.getTime(), is(greaterThan(firstUpdate.getTime())));
    }

    @Test
    public void testScoreNormalizedScore()  {
        SearchIndexModel search = new SearchIndexModel();
        search.eid = "939393";
        search.name = "Bill";
        search.message = "tough";
        search.save();

        List<SearchIndexModel> fooResult = Query
                .from(SearchIndexModel.class)
                .where("eid matches ?", "939393")
                .sortRelevant(1.0, "eid matches ?", "939393")
                .selectAll();

        assertThat(fooResult, hasSize(1));

        if (Database.Static.getDefault().getName().equals(ElasticDBSuite.ElasticTests.DATABASE_NAME)) {
            assertThat(fooResult.get(0).getState().getExtras().size(), Matchers.is(4));
        }

        Float score = ObjectUtils.to(Float.class, fooResult.get(0).getExtra(ElasticsearchDatabase.SCORE_EXTRA));
        assertThat(score, Matchers.is(lessThan(.4f)));

        Float normalizedScore =  ObjectUtils.to(Float.class, fooResult.get(0).getExtra(ElasticsearchDatabase.NORMALIZED_SCORE_EXTRA));
        assertThat(normalizedScore, Matchers.is (1.0f));
    }

    @Test
    public void testLargeField() {
        StringBuilder msg = new StringBuilder();
        for (int i = 0; i < 2000; i++) {
            msg = msg.append("a");
        }
        String message = msg.toString();

        SearchIndexModel search = new SearchIndexModel();
        search.eid = "939393";
        search.name = "Bill";
        search.message = message;
        search.save();

        List<SearchIndexModel> fooResult = Query
                .from(SearchIndexModel.class)
                .where("eid matches ?", "939393")
                .selectAll();
        assertThat("max length", fooResult.get(0).getMessage().length(), Matchers.is(equalTo(2000)));
        List<SearchIndexModel> fooResult1 = Query
                .from(SearchIndexModel.class)
                .where("message startswith ?", message.substring(0, 255))
                .selectAll();
        assertThat("check database size", fooResult1, hasSize(0));

        // redo under 512
        SearchIndexModel search1 = new SearchIndexModel();
        search1.eid = "939393";
        search1.name = "Bill";
        search1.message = message.substring(0, 511);
        search1.save();
        List<SearchIndexModel> fooResult2 = Query
                .from(SearchIndexModel.class)
                .where("message startswith ?", message.substring(0, 511))
                .selectAll();
        assertThat("after the limit", fooResult2, hasSize(1));
    }

    @Test
    public void testLabelInsensitive() {
        SearchIndexModel s = new SearchIndexModel();
        s.setOne("ABOUT");
        s.setD(3d);
        s.save();
        // _label is "about"

        String internalName = "_label";

        List<SearchIndexModel> res1 = Query.from(SearchIndexModel.class).where(internalName + " " + PredicateParser.EQUALS_ANY_OPERATOR + " ?", "about").selectAll();
        assertThat(res1, hasSize(1));

        res1 = Query.from(SearchIndexModel.class).where(internalName + " " + PredicateParser.NOT_EQUALS_ALL_OPERATOR + " ?", "about").selectAll();
        assertThat(res1, hasSize(0));

        res1 = Query.from(SearchIndexModel.class).where(internalName + " " + PredicateParser.LESS_THAN_OPERATOR + " ?", "about").selectAll();
        assertThat(res1, hasSize(0));

        res1 = Query.from(SearchIndexModel.class).where(internalName + " " + PredicateParser.LESS_THAN_OR_EQUALS_OPERATOR + " ?", "about").selectAll();
        assertThat(res1, hasSize(1));

        res1 = Query.from(SearchIndexModel.class).where(internalName + " " + PredicateParser.GREATER_THAN_OPERATOR + " ?", "about").selectAll();
        assertThat(res1, hasSize(0));

        res1 = Query.from(SearchIndexModel.class).where(internalName + " " + PredicateParser.GREATER_THAN_OR_EQUALS_OPERATOR + " ?", "about").selectAll();
        assertThat(res1, hasSize(1));

        res1 = Query.from(SearchIndexModel.class).where(internalName + " " + PredicateParser.STARTS_WITH_OPERATOR + " ?", "about").selectAll();
        assertThat(res1, hasSize(1));

        res1 = Query.from(SearchIndexModel.class).where(internalName + " " + PredicateParser.CONTAINS_OPERATOR + " ?", "about").selectAll();
        assertThat(res1, hasSize(1));

        res1 = Query.from(SearchIndexModel.class).where(internalName + " " + PredicateParser.MATCHES_ANY_OPERATOR + " ?", "about").selectAll();
        assertThat(res1, hasSize(1));

        res1 = Query.from(SearchIndexModel.class).where(internalName + " " + PredicateParser.MATCHES_ALL_OPERATOR + " ?", "about").selectAll();
        assertThat(res1, hasSize(1));
    }

    @Test
    public void testPredicates() {
        SearchIndexModel s = new SearchIndexModel();
        s.setOne("3");
        s.setD(3d);
        s.save();
        // _label is "3"

        List<String> internalNames = new ArrayList<>();
        internalNames.add(ElasticsearchDatabase.ANY_FIELD);
        internalNames.add(Query.LABEL_KEY);

        for (String internalName : internalNames) {

            ElasticsearchDatabase e = Database.Static.getFirst(ElasticsearchDatabase.class);
            Query q = Query.from(SearchIndexModel.class).using(e).where(internalName + " " + PredicateParser.EQUALS_ANY_OPERATOR + " ?", "3");
            QueryBuilder x = e.predicateToQueryBuilder(null, q.getPredicate(), q);
            Assert.assertTrue(x.toString().contains(internalName));

            q = Query.from(SearchIndexModel.class).using(e).where(internalName + " " + PredicateParser.NOT_EQUALS_ALL_OPERATOR + " ?", "3");
            x = e.predicateToQueryBuilder(null, q.getPredicate(), q);
            Assert.assertTrue(x.toString().contains(internalName));

            q = Query.from(SearchIndexModel.class).using(e).where(internalName + " " + PredicateParser.LESS_THAN_OPERATOR + " ?", 3d);
            x = e.predicateToQueryBuilder(null, q.getPredicate(), q);
            Assert.assertTrue(x.toString().contains(internalName));

            q = Query.from(SearchIndexModel.class).using(e).where(internalName + " " + PredicateParser.LESS_THAN_OR_EQUALS_OPERATOR + " ?", 3d);
            x = e.predicateToQueryBuilder(null, q.getPredicate(), q);
            Assert.assertTrue(x.toString().contains(internalName));

            q = Query.from(SearchIndexModel.class).using(e).where(internalName + " " + PredicateParser.GREATER_THAN_OPERATOR + " ?", 3d);
            x = e.predicateToQueryBuilder(null, q.getPredicate(), q);
            Assert.assertTrue(x.toString().contains(internalName));

            q = Query.from(SearchIndexModel.class).using(e).where(internalName + " " + PredicateParser.GREATER_THAN_OR_EQUALS_OPERATOR + " ?", 3d);
            x = e.predicateToQueryBuilder(null, q.getPredicate(), q);
            Assert.assertTrue(x.toString().contains(internalName));

            q = Query.from(SearchIndexModel.class).using(e).where(internalName + " " + PredicateParser.STARTS_WITH_OPERATOR + " ?", "3");
            x = e.predicateToQueryBuilder(null, q.getPredicate(), q);
            Assert.assertTrue(x.toString().contains(internalName));

            if (ElasticsearchDatabase.ANY_FIELD.equals(internalName)) {
                try {
                    q = Query.from(SearchIndexModel.class).using(e).where(internalName + " " + PredicateParser.CONTAINS_OPERATOR + " ?", "3");
                    e.predicateToQueryBuilder(null, q.getPredicate(), q);
                    Assert.fail(internalName + " should not be allowed");
                } catch (java.lang.IllegalArgumentException error) {
                    // squash;
                }
            } else {
                q = Query.from(SearchIndexModel.class).using(e).where(internalName + " " + PredicateParser.CONTAINS_OPERATOR + " ?", "3");
                x = e.predicateToQueryBuilder(null, q.getPredicate(), q);
                Assert.assertTrue(x.toString().contains(internalName));
            }

            q = Query.from(SearchIndexModel.class).using(e).where(internalName + " " + PredicateParser.MATCHES_ANY_OPERATOR + " ?", "3");
            x = e.predicateToQueryBuilder(null, q.getPredicate(), q);
            Assert.assertTrue(x.toString().contains(internalName));

            q = Query.from(SearchIndexModel.class).using(e).where(internalName + " " + PredicateParser.MATCHES_ALL_OPERATOR + " ?", "3");
            x = e.predicateToQueryBuilder(null, q.getPredicate(), q);
            Assert.assertTrue(x.toString().contains(internalName));
        }
    }

    @Test
    public void testStringNormalizedScore()  {
        SearchIndexModel search = new SearchIndexModel();
        search.eid = "939393";
        search.name = "Bill Rick Smith";
        search.message = "tough";
        search.save();

        SearchIndexModel search1 = new SearchIndexModel();
        search1.eid = "939394";
        search1.name = "Bill Joseph";
        search1.message = "easy";
        search1.save();

        List<SearchIndexModel> fooResult = Query
                .from(SearchIndexModel.class)
                .where("name matches ?", "Bill")
                .sortRelevant(10.0, "one matches ?", "Bill")
                .selectAll();

        assertThat(fooResult, hasSize(2));

        assertThat(fooResult.get(0).getState().getExtras().size(), Matchers.is(4));

        Float score = ObjectUtils.to(Float.class, fooResult.get(0).getExtra(ElasticsearchDatabase.SCORE_EXTRA));
        assertThat(score, Matchers.is(lessThan(.3f)));
        Float normalizedScore =  ObjectUtils.to(Float.class, fooResult.get(0).getExtra(ElasticsearchDatabase.NORMALIZED_SCORE_EXTRA));
        assertThat(normalizedScore, Matchers.is (1.0f));

        Float score1 = ObjectUtils.to(Float.class, fooResult.get(1).getExtra(ElasticsearchDatabase.SCORE_EXTRA));
        assertThat(score1, Matchers.is(lessThan(score)));
        Float normalizedScore1 =  ObjectUtils.to(Float.class, fooResult.get(1).getExtra(ElasticsearchDatabase.NORMALIZED_SCORE_EXTRA));
        assertThat(normalizedScore1, Matchers.is (lessThan(1.0f)));
        assertThat(normalizedScore1, Matchers.is (lessThan(normalizedScore)));
    }

    @Test(expected = com.psddev.dari.db.DatabaseException.class)
    public void testTypeAheadErrorField() {
        ElasticTypeModel search = new ElasticTypeModel();
        search.name = "Mickey Mouse";
        search.desc = "Leader of the pack";
        search.fromTypeAhead = "Disney Club";
        search.save();
    }

    @Test(expected = com.psddev.dari.db.DatabaseException.class)
    public void testTypeAheadErrorField2() {
        ElasticTypeModel search = new ElasticTypeModel();
        search.fromTypeAhead = "Disney Club";
        search.save();
    }

    @Test
    public void testTypeAhead() {
        ElasticModel search = new ElasticModel();
        search.name = "Mickey Mouse";
        search.desc = "Leader of the pack";
        search.fromTypeAhead = "Disney Club";
        search.save();

        ElasticModel search1 = new ElasticModel();
        search1.name = "Donald Duck";
        search1.desc = "Happy lead";
        search1.fromTypeAhead = "Disney Movies";
        search1.save();

        List<ElasticModel> fooResult = Query
                .from(ElasticModel.class)
                .where("suggestField matches ?", "donald du")
                .selectAll();
        assertThat(fooResult, hasSize(1));

        List<ElasticModel> fooResult1 = Query
                .from(ElasticModel.class)
                .where("suggestField matches ?", "du donald")
                .selectAll();
        assertThat(fooResult1, hasSize(1));

        List<ElasticModel> fooResult2 = Query
                .from(ElasticModel.class)
                .where("suggestField matches ?", "lea")
                .selectAll();
        assertThat(fooResult2, hasSize(2));

        List<ElasticModel> fooResult3 = Query
                .from(ElasticModel.class)
                .where("typeAhead matches ?", "Dis")
                .selectAll();
        assertThat(fooResult3, hasSize(2));
    }

    @Test
    public void testExclude() {
        ElasticModel search = new ElasticModel();
        search.name = "Mickey Mouse";
        search.desc = "Indexed but not in Any";
        search.save();

        List<ElasticModel> fooResult = Query
                .from(ElasticModel.class)
                .where("name matches ?", "Mouse")
                .selectAll();
        assertThat(fooResult, hasSize(1));

        List<ElasticModel> fooResult1 = Query
                .from(ElasticModel.class)
                .where("_any matches ?", "Mouse")
                .selectAll();
        assertThat(fooResult1, hasSize(1));

        List<ElasticModel> fooResult2 = Query
                .from(ElasticModel.class)
                .where("desc matches ?", "Indexed")
                .selectAll();
        assertThat(fooResult2, hasSize(1));

        List<ElasticModel> fooResult3 = Query
                .from(ElasticModel.class)
                .where("_any matches ?", "Indexed")
                .selectAll();
        assertThat(fooResult3, hasSize(0));
    }

    @Test
    public void deDupe() {
        String val = ElasticsearchDatabase.deDup("There cat true true true west is cat is cat is cat is cat west west", 2L);
        String[] v = val.split(" ");
        Arrays.sort(v);
        assertThat("val0", v[0], is("cat"));
        assertThat("val1", v[1], is("cat"));
        assertThat("val2", v[2], is("is"));
        assertThat("val3", v[3], is("is"));
        assertThat("val4", v[4], is("there"));
        assertThat("val5", v[5], is("true"));
        assertThat("val6", v[6], is("west"));
        assertThat("val7", v[7], is("west"));
    }

    @Test
    public void testFacetFieldElastic() throws Exception {
        ElasticsearchDatabase db = Database.Static.getFirst(ElasticsearchDatabase.class);

        SearchIndexModel search = new SearchIndexModel();
        search.name = "Bill";
        search.num = 1;
        search.save();

        SearchIndexModel search1 = new SearchIndexModel();
        search1.name = "Joe";
        search1.num = 2;
        search1.save();

        SearchIndexModel search2 = new SearchIndexModel();
        search2.name = "Tom";
        search2.num = 5;
        search2.save();

        SearchIndexModel search3 = new SearchIndexModel();
        search3.name = "Bill";
        search3.num = 10;
        search3.save();

        Query<SearchIndexModel> q = Query
                .from(SearchIndexModel.class)
                .facetedField("name", null);
        List<SearchIndexModel> fooResult = q.selectAll();
        assertThat(fooResult, hasSize(4));

        Query q1 = Query
                .from(SearchIndexModel.class)
                .facetedField("name", null);
        Object e = db.readPartial(q1, 0L, 100);
        if (e instanceof ElasticPaginatedResult) {
            ElasticPaginatedResult<SearchIndexModel> e1 = (ElasticPaginatedResult) e;
            assertThat(e1.getFacetedFields(), hasSize(1));
            List<ElasticPaginatedResult.DariFacetField> dariFacetField = e1.getFacetedFields();
            assertThat(dariFacetField.get(0).getName(), is("name"));
            assertThat(dariFacetField.get(0).getCount(), is(3L));
            Map<String, Long> terms = dariFacetField.get(0).getTermValue();
            assertThat(terms.get("Bill"), is(2L));
            assertThat(terms.get("Tom"), is(1L));
            assertThat(terms.get("Joe"), is(1L));
        }

    }

    @Test
    public void testFacetUUIDElastic() throws Exception {
        ElasticsearchDatabase db = Database.Static.getFirst(ElasticsearchDatabase.class);
        UUID u = UUID.randomUUID();

        SearchIndexModel search = new SearchIndexModel();
        search.name = "Bill";
        search.num = 1;
        search.idBox = u;
        search.save();

        SearchIndexModel search1 = new SearchIndexModel();
        search1.name = "Joe";
        search1.num = 2;
        search1.idBox = u;
        search1.save();

        SearchIndexModel search2 = new SearchIndexModel();
        search2.name = "Tom";
        search2.num = 5;
        search2.idBox = UUID.randomUUID();
        search2.save();

        SearchIndexModel search3 = new SearchIndexModel();
        search3.name = "Bill";
        search3.num = 10;
        search3.idBox = UUID.randomUUID();
        search3.save();

        Query q1 = Query
                .from(SearchIndexModel.class)
                .facetedField("idBox", null);
        Object e = db.readPartial(q1, 0L, 100);
        if (e instanceof ElasticPaginatedResult) {
            ElasticPaginatedResult<SearchIndexModel> e1 = (ElasticPaginatedResult) e;
            assertThat(e1.getFacetedFields(), hasSize(1));
            List<ElasticPaginatedResult.DariFacetField> dariFacetField = e1.getFacetedFields();
            assertThat(dariFacetField.get(0).getName(), is("idBox"));
            assertThat(dariFacetField.get(0).getCount(), is(3L));
            List<SearchIndexModel> extraObjects = dariFacetField.get(0).getObjects();
            for (SearchIndexModel s : extraObjects) {
                if (s.getId().equals(search.getId())) {
                    assertThat(s.getState().getExtra("count"), is(2L));
                }
                if (s.getId().equals(search1.getId())) {
                    assertThat(s.getState().getExtra("count"), is(2L));
                }
                if (s.getId().equals(search2.getId())) {
                    assertThat(s.getState().getExtra("count"), is(1L));
                }
                if (s.getId().equals(search3.getId())) {
                    assertThat(s.getState().getExtra("count"), is(1L));
                }
            }
        }

    }

    @Test
    public void testFacetFieldElasticFilter() throws Exception {
        ElasticsearchDatabase db = Database.Static.getFirst(ElasticsearchDatabase.class);

        SearchIndexModel search = new SearchIndexModel();
        search.name = "Bill";
        search.num = 1;
        search.save();

        SearchIndexModel search1 = new SearchIndexModel();
        search1.name = "Joe";
        search1.num = 2;
        search1.save();

        SearchIndexModel search2 = new SearchIndexModel();
        search2.name = "Tom";
        search2.num = 5;
        search2.save();

        SearchIndexModel search3 = new SearchIndexModel();
        search3.name = "Bill";
        search3.num = 10;
        search3.save();

        // this is like a fq in SOLR for facets
        Query<SearchIndexModel> q = Query
                .from(SearchIndexModel.class)
                .facetedField("name", "Bill");
        List<SearchIndexModel> fooResult = q.selectAll();
        assertThat(fooResult, hasSize(2));

        Query<SearchIndexModel> q1 = Query
                .from(SearchIndexModel.class)
                .facetedField("name", null);
        List<SearchIndexModel> fooResult2 = q1.selectAll();
        assertThat(fooResult2, hasSize(4));

        Object e = db.readPartial(q1, 0L, 100);
        if (e instanceof ElasticPaginatedResult) {
            ElasticPaginatedResult<SearchIndexModel> e1 = (ElasticPaginatedResult) e;
            assertThat(e1.getFacetedFields(), hasSize(1));
            List<ElasticPaginatedResult.DariFacetField> dariFacetField = e1.getFacetedFields();
            assertThat(dariFacetField.get(0).getName(), is("name"));
            assertThat(dariFacetField.get(0).getCount(), is(3L));
            Map<String, Long> terms = dariFacetField.get(0).getTermValue();
            assertThat(terms.get("Bill"), is(2L));
        }
    }

    @Test
    public void testFacetRangeElastic() throws Exception {
        ElasticsearchDatabase db = Database.Static.getFirst(ElasticsearchDatabase.class);

        SearchIndexModel search = new SearchIndexModel();
        search.name = "Bill";
        search.num = 1;
        search.save();

        SearchIndexModel search1 = new SearchIndexModel();
        search1.name = "Joe";
        search1.num = 2;
        search1.save();

        SearchIndexModel search2 = new SearchIndexModel();
        search2.name = "Tom";
        search2.num = 5;
        search2.save();

        SearchIndexModel search3 = new SearchIndexModel();
        search3.name = "Bill";
        search3.num = 10;
        search3.save();

        Query<SearchIndexModel> q1 = Query
                .from(SearchIndexModel.class)
                .facetedRange("num", 0d, 3d, 1d);

        List<SearchIndexModel> fooResult2 = q1.selectAll();
        assertThat(fooResult2, hasSize(4));

        Object e = db.readPartial(q1, 0L, 100);
        if (e instanceof ElasticPaginatedResult) {
            ElasticPaginatedResult<SearchIndexModel> e1 = (ElasticPaginatedResult) e;
            assertThat(e1.getRangeFacets(), hasSize(1));
            List<ElasticPaginatedResult.DariRangeFacet> dariRangeFacet = e1.getRangeFacets();
            assertThat(dariRangeFacet.get(0).getName(), is("num"));
            assertThat(dariRangeFacet.get(0).getRangeFacet(), notNullValue());
            Map<String, Long> range = dariRangeFacet.get(0).getRangeValues();
            assertThat(range.get("0.0-1.0"), is(0L));
            assertThat(range.get("1.0-2.0"), is(1L));
            assertThat(range.get("2.0-3.0"), is(1L));
        }
    }

    @Test
    public void testFacetQueryElastic() throws Exception {
        ElasticsearchDatabase db = Database.Static.getFirst(ElasticsearchDatabase.class);

        SearchIndexModel search = new SearchIndexModel();
        search.name = "Bill";
        search.num = 1;
        search.save();

        SearchIndexModel search1 = new SearchIndexModel();
        search1.name = "Joe";
        search1.num = 2;
        search1.save();

        SearchIndexModel search2 = new SearchIndexModel();
        search2.name = "Tom";
        search2.num = 5;
        search2.save();

        SearchIndexModel search3 = new SearchIndexModel();
        search3.name = "Bill";
        search3.num = 10;
        search3.save();

        Query q1 = Query.from(SearchIndexModel.class).where("name = ?", "Joe");
        Query<SearchIndexModel> q2 = Query
                .from(SearchIndexModel.class)
                .facetQuery(q1);

        List<SearchIndexModel> fooResult3 = q2.selectAll();
        assertThat(fooResult3, hasSize(4));

        Object e = db.readPartial(q2, 0L, 100);
        if (e instanceof ElasticPaginatedResult) {
            ElasticPaginatedResult<SearchIndexModel> e1 = (ElasticPaginatedResult) e;
            assertThat(e1.getQueryFacet(), notNullValue());
            Long count = e1.getQueryFacetCount();
            assertThat(count, is(1L));
        }
    }

}
