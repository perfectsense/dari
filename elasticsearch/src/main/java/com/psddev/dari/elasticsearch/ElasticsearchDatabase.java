package com.psddev.dari.elasticsearch;

import com.google.common.base.Preconditions;
import com.psddev.dari.db.AbstractDatabase;
import com.psddev.dari.db.ComparisonPredicate;
import com.psddev.dari.db.CompoundPredicate;
import com.psddev.dari.db.Location;
import com.psddev.dari.db.Predicate;
import com.psddev.dari.db.PredicateParser;
import com.psddev.dari.db.Query;
import com.psddev.dari.db.Sorter;
import com.psddev.dari.db.State;
import com.psddev.dari.db.UnsupportedIndexException;
import com.psddev.dari.db.UnsupportedPredicateException;
import com.psddev.dari.util.ObjectUtils;
import com.psddev.dari.util.PaginatedResult;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.lucene.search.function.CombineFunction;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.GeoDistanceSortBuilder;
import org.elasticsearch.search.sort.ScoreSortBuilder;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.BiFunction;
import java.util.function.Function;

import static org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders.weightFactorFunction;
import static org.elasticsearch.search.sort.SortOrder.ASC;
import static org.elasticsearch.search.sort.SortOrder.DESC;

//Note: http://elasticsearch-users.115913.n3.nabble.com/What-is-your-best-practice-to-access-a-cluster-by-a-Java-client-td4015311.html
// Decided to implement a Singleton

public class ElasticsearchDatabase extends AbstractDatabase<TransportClient> {

    public class Node {
        public String hostname;
        public int port;
    }

    private static final String DATABASE_NAME = "elasticsearch";
    public static final String SETTING_KEY_PREFIX = "dari/database/" + DATABASE_NAME + "/";
    public static final String CLUSTER_NAME_SUB_SETTING = "clusterName";
    public static final String CLUSTER_PORT_SUB_SETTING = "clusterPort";
    public static final String HOSTNAME_SUB_SETTING = "clusterHostname";
    public static final String SEARCH_TIMEOUT = "searchTimeout";
    public static final String INDEX_NAME_SUB_SETTING = "indexName";

    public static final String ID_FIELD = "_uid";  // special for aggregations
    public static final String TYPE_ID_FIELD = "_type";
    public static final String ALL_FIELD = "_all";

    private static final Logger LOGGER = LoggerFactory.getLogger(ElasticsearchDatabase.class);

    private List<Node> clusterNodes = new ArrayList<>();

    private String clusterName;
    private String indexName;

    private int searchTimeout;

    private transient Settings nodeSettings;
    private transient TransportClient client;

    public String getIndexName() {
        return indexName;
    }

    public void setIndexName(String indexName) {
        this.indexName = indexName;
    }

    @Override
    public TransportClient openConnection() {

        if (this.client != null && isAlive(this.client)) {
            return this.client;
        }
        try {
            this.client = ElasticsearchDatabaseConnection.getClient(nodeSettings, this.clusterNodes);
            return this.client;
        } catch (Exception error) {
            LOGGER.info(
                    String.format("ELK openConnection Cannot open ES Exception [%s: %s]",
                            error.getClass().getName(),
                            error.getMessage()),
                    error);
        }
        LOGGER.info("ELK openConnection doWrites return null");
        return null;
    }

    @Override
    public void closeConnection(TransportClient client) {
        //client.close();
    }

    @Override
    protected void doInitialize(String settingsKey, Map<String, Object> settings) {

        String clusterName = ObjectUtils.to(String.class, settings.get(CLUSTER_NAME_SUB_SETTING));

        if (clusterName == null) {
            Preconditions.checkNotNull(clusterName);
        }

        String clusterPort = ObjectUtils.to(String.class, settings.get(CLUSTER_PORT_SUB_SETTING));

        if (clusterPort == null) {
            Preconditions.checkNotNull(clusterPort);
        }

        String clusterHostname = ObjectUtils.to(String.class, settings.get(HOSTNAME_SUB_SETTING));

        if (clusterHostname == null) {
            Preconditions.checkNotNull(clusterHostname);
        }

        String indexName = ObjectUtils.to(String.class, settings.get(INDEX_NAME_SUB_SETTING));

        if (indexName == null) {
            Preconditions.checkNotNull(indexName);
        }

        String clusterTimeout = ObjectUtils.to(String.class, settings.get(SEARCH_TIMEOUT));

        if (clusterTimeout == null) {
            this.searchTimeout = 50000; //ms
        } else {
            this.searchTimeout = Integer.parseInt(clusterTimeout);
        }

        this.clusterName = clusterName;

        Node n = new Node();
        n.hostname = clusterHostname;
        n.port = Integer.parseInt(clusterPort);

        this.clusterNodes.add(n);

        this.indexName = indexName;

        this.nodeSettings = Settings.builder()
                .put("cluster.name", this.clusterName)
                .put("client.transport.sniff", true).build();

    }

    @Override
    public Date readLastUpdate(Query<?> query) {
        return null;
    }

    public boolean isAlive(TransportClient client) {
        if (client != null) {
            List<DiscoveryNode> nodes = client.connectedNodes();
            if (!nodes.isEmpty()) {
                return true;
            }
        }
        return false;
    }

    public static String getVersion(String nodeHost) {
        try {
            HttpClient httpClient = HttpClientBuilder.create().build();

            HttpGet getRequest = new HttpGet(nodeHost);
            getRequest.addHeader("accept", "application/json");
            HttpResponse response = httpClient.execute(getRequest);
            String json;
            json = EntityUtils.toString(response.getEntity());
            JSONObject j = new JSONObject(json);
            if (j != null) {
                if (j.get("version") != null) {
                    if (j.getJSONObject("version") != null) {
                        JSONObject jo = j.getJSONObject("version");
                        String version = jo.getString("number");
                        if (!version.equals("5.2.0")) {
                            LOGGER.warn("Warning: ELK {} version is not 5.2.0", version);
                        }
                        return version;
                    }
                }
            }
        } catch (Exception e) {
            LOGGER.warn("Warning: ELK cannot get version");
            e.printStackTrace();
        }
        return null;
    }

    public static String getClusterName(String nodeHost) {
        try {
            HttpClient httpClient = HttpClientBuilder.create().build();

            HttpGet getRequest = new HttpGet(nodeHost);
            getRequest.addHeader("accept", "application/json");
            HttpResponse response = httpClient.execute(getRequest);
            String json;
            json = EntityUtils.toString(response.getEntity());
            JSONObject j = new JSONObject(json);
            if (j != null) {
                if (j.get("cluster_name") != null) {
                    return j.getString("cluster_name");
                }
            }
        } catch (Exception e) {
            LOGGER.warn("Warning: ELK cannot get cluster_name");
            e.printStackTrace();
        }
        return null;
    }

    public static boolean checkAlive(String nodeHost) {
        try {
            HttpClient httpClient = HttpClientBuilder.create().build();

            HttpGet getRequest = new HttpGet(nodeHost);
            getRequest.addHeader("accept", "application/json");
            HttpResponse response = httpClient.execute(getRequest);
            String json;
            json = EntityUtils.toString(response.getEntity());
            JSONObject j = new JSONObject(json);
            if (j != null) {
                if (j.get("cluster_name") != null) {
                    return true;
                }
            }
        } catch (Exception e) {
            LOGGER.warn("Warning: ELK is not already running");
        }
        return false;
    }

    public boolean isAlive() {
        TransportClient client = openConnection();
        if (client != null) {
            List<DiscoveryNode> nodes = client.connectedNodes();
            closeConnection(client);
            if (!nodes.isEmpty()) {
                return true;
            }
        }
        return false;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> PaginatedResult<T> readPartial(Query<T> query, long offset, int limit) {
        LOGGER.info("ELK PaginatedResult readPartial query.getPredicate() [{}]", query.getPredicate());

        TransportClient client = openConnection();
        if (client == null || !isAlive(client)) {
            return null;
        }
        List<T> items = new ArrayList<>();

        //try {
            Set<UUID> typeIds = query.getConcreteTypeIds(this);

            if (query.getGroup() != null && typeIds.size() == 0) {
                // should limit by the type
                LOGGER.info("ELK PaginatedResult readPartial the call is to limit by from() but did not load typeIds! [{}]", query.getGroup());
            }
            String[] typeIdStrings = typeIds.size() == 0
                    ? new String[]{ "_all" }
                    : typeIds.stream().map(UUID::toString).toArray(String[]::new);

            SearchResponse response;
            if (typeIds.size() == 0) {
                SearchRequestBuilder srb = client.prepareSearch(getIndexName())
                        .setFetchSource(!query.isReferenceOnly())
                        .setTimeout(new TimeValue(this.searchTimeout))
                        .setQuery(predicateToQueryBuilder(query.getPredicate()))
                        .setFrom((int) offset)
                        .setSize(limit);
                for (SortBuilder sb : predicateToSortBuilder(query.getSorters(), predicateToQueryBuilder(query.getPredicate()), query, srb)) {
                    srb = srb.addSort(sb);
                }
                LOGGER.info("ELK srb [{}]", srb.toString());
                response = srb.execute().actionGet();
            } else {
                SearchRequestBuilder srb = client.prepareSearch(getIndexName())
                        .setFetchSource(!query.isReferenceOnly())
                        .setTypes(typeIdStrings)
                        .setQuery(predicateToQueryBuilder(query.getPredicate()))
                        .setFrom((int) offset)
                        .setSize(limit);
                for (SortBuilder sb : predicateToSortBuilder(query.getSorters(), predicateToQueryBuilder(query.getPredicate()), query, srb)) {
                    srb.addSort(sb);
                }
                LOGGER.info("ELK srb typeIds [{}] - [{}]",  typeIdStrings, srb.toString());
                response = srb.execute().actionGet();
            }
            SearchHits hits = response.getHits();

            LOGGER.info("ELK PaginatedResult readPartial hits [{}]", hits.getTotalHits());

            for (SearchHit hit : hits.getHits()) {

                items.add(createSavedObjectWithHit(hit, query));

            }

            PaginatedResult<T> p = new PaginatedResult<>(offset, limit, hits.getTotalHits(), items);
            return p;
    /*     } catch (Exception error) {
            LOGGER.info(
                    String.format("ELK PaginatedResult readPartial Exception [%s: %s]",
                            error.getClass().getName(),
                            error.getMessage()),
                    error);
            throw error;
        } */
        //if certain errors: return new PaginatedResult<>(offset, limit, 0, items);
    }

    private <T> T createSavedObjectWithHit(SearchHit hit, Query<T> query) {
        T object = createSavedObject(hit.getType(), hit.getId(), query);

        State objectState = State.getInstance(object);

        if (!objectState.isReferenceOnly()) {
            objectState.setValues(hit.getSource());
        }

        return swapObjectType(query, object);
    }

    private final Map<Query.MappedKey, String> specialFields; {
        Map<Query.MappedKey, String> m = new HashMap<>();
        m.put(Query.MappedKey.ID, ID_FIELD);
        m.put(Query.MappedKey.TYPE, TYPE_ID_FIELD);
        m.put(Query.MappedKey.ANY, ALL_FIELD);
        specialFields = m;
    }

    private Query.MappedKey mapFullyDenormalizedKey(Query<?> query, String key) {
        Query.MappedKey mappedKey = query.mapDenormalizedKey(getEnvironment(), key);
        if (mappedKey.hasSubQuery()) {
            throw new Query.NoFieldException(query.getGroup(), key);
        } else {
            return mappedKey;
        }
    }

    private String getElkField(String internalType) {
        return "";
        // how do we handle internal types? Solr does it with a sortPrefix.
        // Reserved:  `_uid`, `_id`, `_type`, `_source`, `_all`, `_parent`, `_field_names`, `_routing`, `_index`, `_size`, `_timestamp`, and `_ttl`
    }

    // use the _mapping, so that we have field.raw set for sorting.
    private List<SortBuilder> predicateToSortBuilder(List<Sorter> sorters, QueryBuilder orig, Query<?> query, SearchRequestBuilder srb) {
        List<SortBuilder> list = new ArrayList<>();
        if (sorters == null || sorters.size() == 0) {
            list.add(new ScoreSortBuilder());
        } else {

            for (Sorter sorter : sorters) {
                String operator = sorter.getOperator();
                if (Sorter.ASCENDING_OPERATOR.equals(operator) || Sorter.DESCENDING_OPERATOR.equals(operator)) {
                    boolean isAscending = Sorter.ASCENDING_OPERATOR.equals(operator);
                    String queryKey = (String) sorter.getOptions().get(0);
                    Query.MappedKey mappedKey = mapFullyDenormalizedKey(query, queryKey);
                    String elkField = specialFields.get(mappedKey);

                    if (elkField == null) {
                        String internalType = mappedKey.getInternalType();
                        if (internalType != null) {
                            if (internalType.equals("text")) {
                                elkField = queryKey + ".raw";
                            }
                        }
                    }

                    if (elkField == null) {
                        throw new UnsupportedIndexException(this, queryKey);
                    }
                    list.add(new FieldSortBuilder(elkField).order(isAscending ? ASC : DESC));
                } else if (Sorter.OLDEST_OPERATOR.equals(operator) || Sorter.NEWEST_OPERATOR.equals(operator)) {
                    // OLDEST_OPERATOR, NEWEST_OPERATOR -- date ones
                } else if (Sorter.FARTHEST_OPERATOR.equals(operator) || Sorter.CLOSEST_OPERATOR.equals(operator)) {
                    if (sorter.getOptions().size() < 2) {
                        throw new IllegalArgumentException(operator + "requires Location");
                    }
                    boolean isClosest = Sorter.CLOSEST_OPERATOR.equals(operator);
                    String queryKey = (String) sorter.getOptions().get(0);
                    Query.MappedKey mappedKey = mapFullyDenormalizedKey(query, queryKey);
                    String elkField = specialFields.get(mappedKey);
                    if (elkField == null) {
                        String internalType = mappedKey.getInternalType();
                        if (internalType != null) {
                            if (internalType.equals("location")) {
                                elkField = queryKey + "._location";
                            }
                        }
                    }
                    if (!(sorter.getOptions().get(1) instanceof Location)) {
                        throw new IllegalArgumentException(operator + "requires Location");
                    }
                    Location sort = (Location) sorter.getOptions().get(1);
                    list.add(new GeoDistanceSortBuilder(elkField, new GeoPoint(sort.getX(), sort.getY()))
                            .order(isClosest ? SortOrder.ASC : SortOrder.DESC));
                } else if (Sorter.RELEVANT_OPERATOR.equals(operator)) {
                    list.add(new ScoreSortBuilder());
                    Predicate sortPredicate;
                    Object predicateObject = sorter.getOptions().get(1);
                    Object boostObject = sorter.getOptions().get(0);
                    String boostStr = boostObject.toString();
                    Float boost = Float.valueOf(boostStr);
                    if (predicateObject instanceof Predicate) {
                        sortPredicate = (Predicate) predicateObject;
                        FunctionScoreQueryBuilder.FilterFunctionBuilder[] functions = {
                                new FunctionScoreQueryBuilder.FilterFunctionBuilder(
                                        predicateToQueryBuilder(sortPredicate),
                                        weightFactorFunction(boost))
                        };

                        QueryBuilder qb = QueryBuilders.functionScoreQuery(orig, functions)
                                .boostMode(CombineFunction.MULTIPLY)
                                .boost(boost)
                                .maxBoost(1000.0f);
                        srb.setQuery(qb);
                    }
                }
            }
        }
        return list;

    }

    // must override since MAXIMUM_LIMIT is not good for ES
    @Override
    public <T> List<T> readAll(Query<T> query) {
        return readPartial(query, 0L, 1000).getItems();
    }

    // Used to convert the query to ELK
    private QueryBuilder predicateToQueryBuilder(Predicate predicate) {
        if (predicate == null) {
            return QueryBuilders.matchAllQuery();
        }
        if (predicate instanceof CompoundPredicate) {
            CompoundPredicate compound = (CompoundPredicate) predicate;
            List<Predicate> children = compound.getChildren();

            switch (compound.getOperator()) {
                case PredicateParser.AND_OPERATOR :
                    return combine(compound.getOperator(), children, BoolQueryBuilder::must, this::predicateToQueryBuilder);

                case PredicateParser.OR_OPERATOR :
                    return combine(compound.getOperator(), children, BoolQueryBuilder::should, this::predicateToQueryBuilder);

                case PredicateParser.NOT_OPERATOR :
                    return combine(compound.getOperator(), children, BoolQueryBuilder::mustNot, this::predicateToQueryBuilder);

                default :
                    break;
            }

        } else if (predicate instanceof ComparisonPredicate) {
            ComparisonPredicate comparison = (ComparisonPredicate) predicate;
            String key = "_any".equals(comparison.getKey()) ? "_all" : comparison.getKey();
            List<Object> values = comparison.getValues();

            String operator = comparison.getOperator();

            switch (operator) {
                case PredicateParser.EQUALS_ANY_OPERATOR :
                    // could be 'missing'
                    for (Object v : values) {
                        if (v == null) {
                            throw new IllegalArgumentException(PredicateParser.EQUALS_ANY_OPERATOR + " requires value");
                        }
                    }

                    return combine(operator, values, BoolQueryBuilder::should, v -> Query.MISSING_VALUE.equals(v)
                           // ? QueryBuilders.boolQuery().mustNot(QueryBuilders.existsQuery(key))
                            ? QueryBuilders.existsQuery(key)
                            : (v instanceof Location ? QueryBuilders.boolQuery().must(QueryBuilders.termQuery(key + ".x", ((Location) v).getX()))
                                                                                .must(QueryBuilders.termQuery(key + ".y", ((Location) v).getY()))
                                                     : QueryBuilders.termQuery(key, v)));

                case PredicateParser.NOT_EQUALS_ALL_OPERATOR :
                    for (Object v : values) {
                        if (v == null) {
                            throw new IllegalArgumentException(PredicateParser.NOT_EQUALS_ALL_OPERATOR + " requires value");
                        }
                    }
                    return combine(operator, values, BoolQueryBuilder::mustNot, v -> Query.MISSING_VALUE.equals(v)
                           // ? QueryBuilders.boolQuery().must(QueryBuilders.existsQuery(key))
                            ? QueryBuilders.existsQuery(key)
                            : (v instanceof Location ? QueryBuilders.boolQuery().must(QueryBuilders.termQuery(key + ".x", ((Location) v).getX()))
                                                                                .must(QueryBuilders.termQuery(key + ".y", ((Location) v).getY()))
                                                     : QueryBuilders.termQuery(key, v)));
                    //return combine(values, BoolQueryBuilder::mustNot, v -> QueryBuilders.termQuery(key, v));

                case PredicateParser.LESS_THAN_OPERATOR :
                    for (Object v : values) {
                        if (v == null) {
                            throw new IllegalArgumentException(PredicateParser.LESS_THAN_OPERATOR + " requires value");
                        }
                    }
                    return combine(operator, values, BoolQueryBuilder::must, v -> QueryBuilders.rangeQuery(key).lt(v));

                case PredicateParser.LESS_THAN_OR_EQUALS_OPERATOR :
                    for (Object v : values) {
                        if (v == null) {
                            throw new IllegalArgumentException(PredicateParser.LESS_THAN_OR_EQUALS_OPERATOR + " requires value");
                        }
                    }
                    return combine(operator, values, BoolQueryBuilder::must, v -> QueryBuilders.rangeQuery(key).lte(v));

                case PredicateParser.GREATER_THAN_OPERATOR :
                    for (Object v : values) {
                        if (v == null) {
                            throw new IllegalArgumentException(PredicateParser.GREATER_THAN_OPERATOR + " requires value");
                        }
                    }
                    return combine(operator, values, BoolQueryBuilder::must, v -> QueryBuilders.rangeQuery(key).gt(v));

                case PredicateParser.GREATER_THAN_OR_EQUALS_OPERATOR :
                    for (Object v : values) {
                        if (v == null) {
                            throw new IllegalArgumentException(PredicateParser.GREATER_THAN_OR_EQUALS_OPERATOR + " requires value");
                        }
                    }
                    return combine(operator, values, BoolQueryBuilder::must, v -> QueryBuilders.rangeQuery(key).gte(v));

                case PredicateParser.STARTS_WITH_OPERATOR :
                    for (Object v : values) {
                        if (v == null) {
                            throw new IllegalArgumentException(PredicateParser.STARTS_WITH_OPERATOR + " requires value");
                        }
                    }
                    return combine(operator, values, BoolQueryBuilder::should, v -> QueryBuilders.prefixQuery(key, v.toString()));

                case PredicateParser.CONTAINS_OPERATOR :
                case PredicateParser.MATCHES_ANY_OPERATOR :
                    return combine(operator, values, BoolQueryBuilder::should, v -> "*".equals(v)
                            ? QueryBuilders.matchAllQuery()
                            : QueryBuilders.matchPhrasePrefixQuery(key, v));

                case PredicateParser.MATCHES_ALL_OPERATOR :
                    return combine(operator, values, BoolQueryBuilder::must, v -> "*".equals(v)
                            ? QueryBuilders.matchAllQuery()
                            : QueryBuilders.matchPhrasePrefixQuery(key, v));

                case PredicateParser.MATCHES_EXACT_ANY_OPERATOR :
                case PredicateParser.MATCHES_EXACT_ALL_OPERATOR :
                default :
                    break;
            }
        }

        throw new UnsupportedPredicateException(this, predicate);
    }

    @SuppressWarnings("unchecked")
    private <T> QueryBuilder combine(String operatorType,
            Collection<T> items,
            BiFunction<BoolQueryBuilder, QueryBuilder, BoolQueryBuilder> operator,
            Function<T, QueryBuilder> itemFunction) {

        BoolQueryBuilder builder = QueryBuilders.boolQuery();

        for (T item : items) {
            if (item instanceof java.util.UUID) {
                item = (T) item.toString();
            }
            if (!Query.MISSING_VALUE.equals(item)) {
                builder = operator.apply(builder, itemFunction.apply(item));
            } else {
                if (operatorType.equals(PredicateParser.EQUALS_ANY_OPERATOR)) {
                    operator = BoolQueryBuilder::mustNot;
                }
                if (operatorType.equals(PredicateParser.NOT_EQUALS_ALL_OPERATOR)) {
                    operator = BoolQueryBuilder::must;
                }
                builder = operator.apply(builder, itemFunction.apply(item));
            }
        }

        if (builder.hasClauses()) {
            //LOGGER.info("ELK combine predicate [{}]", builder.toString());
            return builder;
        } else {
            //LOGGER.info("ELK combine predicate default [{}]", QueryBuilders.matchAllQuery());
            return QueryBuilders.matchAllQuery();
        }
    }

    public void saveJson(String json, String typeId, String id) {

        TransportClient client = openConnection();

        try {
            BulkRequestBuilder bulk = client.prepareBulk();
            String indexName = getIndexName();

            bulk.add(client
                        .prepareIndex(indexName, typeId, id)
                        .setSource(json));
            BulkResponse bulkResponse = bulk.get();
            if (bulkResponse.hasFailures()) {
                LOGGER.info("ELK saveJson Save Json hasFailures()");
            }

        } finally {
            closeConnection(client);
        }
    }

    @Override
    protected void commitTransaction(TransportClient client, boolean isImmediate) throws Exception {
        if (client != null) {
            if (isImmediate) {
                client.admin().indices().prepareFlush(this.indexName).get();
            }
            client.admin().indices().prepareRefresh(this.indexName).get();
        }
    }

    @SuppressWarnings("unchecked")
    private static void convertLocationToName(Map<String, Object> map, String name) {

        Iterator<Map.Entry<String, Object>> it = map.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, Object> pair = it.next();
            String key = pair.getKey();
            Object value = pair.getValue();

            if (value instanceof Map) {
                Map<String, Object> valueMap = (Map<String, Object>) value;
                if (valueMap.size() == 2) {
                    if (valueMap.get("x") != null && valueMap.get("y") != null) {
                        valueMap.put(name, valueMap.get("x") + "," + valueMap.get("y"));
                    }
                }
                convertLocationToName((Map<String, Object>) value, name);

            } else if (value instanceof List) {
                for (Object item : (List<?>) value) {
                    if (item instanceof Map) {
                        Map<String, Object> valueMap = (Map<String, Object>) item;
                        if (valueMap.size() == 2) {
                            if (valueMap.get("x") != null && valueMap.get("y") != null) {
                                valueMap.put(name, valueMap.get("x") + "," + valueMap.get("y"));
                            }
                        }
                        convertLocationToName((Map<String, Object>) item, name);
                    }
                }
            }
        }
    }

    @Override
    protected void doWrites(TransportClient client, boolean isImmediate, List<State> saves, List<State> indexes, List<State> deletes) throws Exception {
        try {
            BulkRequestBuilder bulk = client.prepareBulk(); // this forces .setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL);

            String indexName = getIndexName();

            if (saves != null) {
                    for (State state : saves) {
                        try {
                            String documentType = state.getTypeId().toString();
                            String documentId = state.getId().toString();

                            Map<String, Object> t = state.getSimpleValues();
                            t.remove("_id");
                            t.remove("_type");
                            LOGGER.info("ELK doWrites saving _type [{}] and _id [{}]",
                                    documentType, documentId);

                            convertLocationToName(t, "_location");

                            LOGGER.info("ELK doWrites saving _type [{}] and _id [{}] = [{}]",
                                    documentType, documentId, t.toString());
                            bulk.add(client.prepareIndex(indexName, documentType, documentId).setSource(t));
                        } catch (Exception error) {
                            LOGGER.info(
                                    String.format("ELK doWrites saves Exception [%s: %s]",
                                            error.getClass().getName(),
                                            error.getMessage()),
                                    error);
                        }
                    }
            }

            if (deletes != null) {
                for (State state : deletes) {
                    LOGGER.info("ELK doWrites deleting _type [{}] and _id [{}]",
                            state.getId().toString(), state.getTypeId().toString());
                    try {
                        bulk.add(client
                                .prepareDelete(indexName, state.getTypeId().toString(), state.getId().toString()));
                    } catch (Exception error) {
                        LOGGER.info(
                                String.format("ELK doWrites saves Exception [%s: %s]",
                                        error.getClass().getName(),
                                        error.getMessage()),
                                error);

                    }
                }
            }
            LOGGER.info("ELK Writing [{}]", bulk.request().requests().toString());
            bulk.execute().actionGet();
        } catch (Exception error) {
            LOGGER.info(
                    String.format("ELK doWrites Exception [%s: %s]",
                            error.getClass().getName(),
                            error.getMessage()),
                    error);

        }
    }
}
