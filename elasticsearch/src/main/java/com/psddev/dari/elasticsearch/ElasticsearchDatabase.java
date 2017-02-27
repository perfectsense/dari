package com.psddev.dari.elasticsearch;

import com.google.common.base.Preconditions;
import com.psddev.dari.db.AbstractDatabase;
import com.psddev.dari.db.ComparisonPredicate;
import com.psddev.dari.db.CompoundPredicate;
import com.psddev.dari.db.Location;
import com.psddev.dari.db.Predicate;
import com.psddev.dari.db.PredicateParser;
import com.psddev.dari.db.Query;
import com.psddev.dari.db.Record;
import com.psddev.dari.db.Region;
import com.psddev.dari.db.Sorter;
import com.psddev.dari.db.State;
import com.psddev.dari.db.StateSerializer;
import com.psddev.dari.db.UnsupportedIndexException;
import com.psddev.dari.db.UnsupportedPredicateException;
import com.psddev.dari.util.ObjectUtils;
import com.psddev.dari.util.PaginatedResult;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.ShapeRelation;
import com.vividsolutions.jts.geom.Coordinate;
import org.elasticsearch.common.geo.builders.ShapeBuilders;
import org.elasticsearch.common.lucene.search.function.CombineFunction;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.GeoShapeQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders;
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
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

/**
 * ElasticsearchDatabase for Elastic Search
 * Note: http://elasticsearch-users.115913.n3.nabble.com/What-is-your-best-practice-to-access-a-cluster-by-a-Java-client-td4015311.html
 */
public class ElasticsearchDatabase extends AbstractDatabase<TransportClient> {

    public class Node {
        public String hostname;
        public int port;
    }

    public static final String DEFAULT_DATABASE_NAME = "dari/defaultDatabase";
    public static final String DATABASE_NAME = "elasticsearch";
    public static final String SETTING_KEY_PREFIX = "dari/database/" + DATABASE_NAME + "/";
    public static final String CLUSTER_NAME_SUB_SETTING = "clusterName";
    public static final String CLUSTER_PORT_SUB_SETTING = "clusterPort";
    public static final String HOSTNAME_SUB_SETTING = "clusterHostname";
    public static final String INDEX_NAME_SUB_SETTING = "indexName";
    public static final String SEARCH_TIMEOUT_SETTING = "searchTimeout";
    public static final String SEARCH_MAX_ROWS_SETTING = "searchMaxRows";

    public static final String ID_FIELD = "_uid";  // special for aggregations
    public static final String TYPE_ID_FIELD = "_type";
    public static final String ALL_FIELD = "_all";
    public static final int MAX_ROWS = 1000;
    public static final int TIMEOUT = 500000;

    public static final String LOCATION_FIELD = "_location";
    public static final String REGION_FIELD = "_polygon";

    private static final Logger LOGGER = LoggerFactory.getLogger(ElasticsearchDatabase.class);

    private List<Node> clusterNodes = new ArrayList<>();
    private String clusterName;
    private String indexName;
    private int searchTimeout = TIMEOUT;
    private int searchMaxRows = MAX_ROWS;
    private transient Settings nodeSettings;
    private transient TransportClient client;

    /**
     * The amount of rows per each call to Elastic Search
     *
     * @param searchMaxRows
     */
    public void setSearchMaxRows(int searchMaxRows) {

        this.searchMaxRows = searchMaxRows;
    }

    /**
     *
     * @return
     */
    public int getSearchMaxRows() {

        return searchMaxRows;
    }

    /**
     *
     * @param searchTimeout
     */
    public void setSearchTimeout(int searchTimeout) {

        this.searchTimeout = searchTimeout;
    }

    /**
     *
     * @return
     */
    public int getSearchTimeout() {

        return searchTimeout;
    }

    /**
     *
     * @param clusterName
     */
    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    /**
     *
     * @return
     */
    public String getClusterName() {
        return clusterName;
    }

    /**
     *
     * @return
     */
    public String getIndexName() {
        return indexName;
    }

    /**
     *
     * @param indexName
     */
    public void setIndexName(String indexName) {
        this.indexName = indexName;
    }

    /**
     *
     * @return TransportClient
     */
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

    /**
     *
     * @param client
     */
    @Override
    public void closeConnection(TransportClient client) {
        //client.close();
    }

    /**
     *
     * @param settingsKey
     * @param settings
     */
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

        String searchMaxRows = ObjectUtils.to(String.class, settings.get(SEARCH_MAX_ROWS_SETTING));

        if (searchMaxRows == null) {
            this.searchMaxRows = MAX_ROWS;
        } else {
            this.searchMaxRows = Integer.parseInt(searchMaxRows);

        }

        String clusterTimeout = ObjectUtils.to(String.class, settings.get(SEARCH_TIMEOUT_SETTING));

        if (clusterTimeout == null) {
            this.searchTimeout = TIMEOUT;
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

    /**
     *
     * @param query
     * @return
     */
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

    /**
     *
     * @param nodeHost
     * @return
     */
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
        } catch (Exception error) {
            LOGGER.warn(
                    String.format("Warning: ELK cannot get version [%s: %s]",
                            error.getClass().getName(),
                            error.getMessage()),
                    error);
        }
        return null;
    }

    /**
     *
     * @param nodeHost
     * @return
     */
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
        } catch (Exception error) {
            LOGGER.warn(
                    String.format("Warning: ELK cannot get cluster_name [%s: %s]",
                            error.getClass().getName(),
                            error.getMessage()),
                    error);
        }
        return null;
    }

    /**
     *
     * @param nodeHost
     * @return
     */
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

    /**
     *
     * @return
     */
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

    /**
     *
     * @param query
     * @param offset
     * @param limit
     * @param <T>
     * @return
     */
    @Override
    @SuppressWarnings("unchecked")
    public <T> PaginatedResult<T> readPartial(Query<T> query, long offset, int limit) {
        LOGGER.info("ELK PaginatedResult readPartial query.getPredicate() [{}]", query.getPredicate());

        TransportClient client = openConnection();
        if (client == null || !isAlive(client)) {
            return null;
        }
        List<T> items = new ArrayList<>();

        Set<UUID> typeIds = query.getConcreteTypeIds(this);

        if (query.getGroup() != null && typeIds.size() == 0) {
            // should limit by the type
            LOGGER.info("ELK PaginatedResult readPartial the call is to limit by from() but did not load typeIds! [{}]", query.getGroup());
        }
        String[] typeIdStrings = typeIds.size() == 0
                ? new String[]{ "_all" }
                : typeIds.stream().map(UUID::toString).toArray(String[]::new);

        SearchResponse response;
        QueryBuilder qb = predicateToQueryBuilder(query.getPredicate(), query);
        SearchRequestBuilder srb;
        if (typeIds.size() == 0) {
            srb = client.prepareSearch(getIndexName())
                    .setFetchSource(!query.isReferenceOnly())
                    .setTimeout(new TimeValue(this.searchTimeout))
                    .setQuery(qb)
                    .setFrom((int) offset)
                    .setSize(limit);
            //if (query.getGroup() != null) {
            //    srb.addAggregation(AggregationBuilders.terms(query.getGroup() + "_aggs").field(query.getGroup() + ".raw").size(100));
            //}
            for (SortBuilder sb : predicateToSortBuilder(query.getSorters(), qb, query, srb, null)) {
                srb = srb.addSort(sb);
            }
        } else {
            srb = client.prepareSearch(getIndexName())
                    .setFetchSource(!query.isReferenceOnly())
                    .setTimeout(new TimeValue(this.searchTimeout))
                    .setTypes(typeIdStrings)
                    .setQuery(qb)
                    .setFrom((int) offset)
                    .setSize(limit);
            //if (query.getGroup() != null) {
            //    srb.addAggregation(AggregationBuilders.terms(query.getGroup() + "_aggs").field(query.getGroup() + ".raw").size(100));
            //}
            for (SortBuilder sb : predicateToSortBuilder(query.getSorters(), qb, query, srb, typeIdStrings)) {
                srb.addSort(sb);
            }

        }
        LOGGER.debug("ELK srb typeIds [{}] - [{}]", (typeIdStrings.length == 0 ? "" : typeIdStrings), srb.toString());
        response = srb.execute().actionGet();
        SearchHits hits = response.getHits();

        for (SearchHit hit : hits.getHits()) {
            items.add(createSavedObjectWithHit(hit, query));
        }

        LOGGER.info("ELK PaginatedResult readPartial hits [{} of {}]", items.size(), hits.getTotalHits());

        PaginatedResult<T> p = new PaginatedResult<>(offset, limit, hits.getTotalHits(), items);
        return p;
    }

    /**
     *
     * @param hit
     * @param query
     * @param <T>
     * @return
     */
    private <T> T createSavedObjectWithHit(SearchHit hit, Query<T> query) {
        T object = createSavedObject(hit.getType(), hit.getId(), query);

        State objectState = State.getInstance(object);

        if (!objectState.isReferenceOnly()) {
            objectState.setValues(hit.getSource());
        }

        return swapObjectType(query, object);
    }

    /**
     *
     */
    private final Map<Query.MappedKey, String> specialFields; {
        Map<Query.MappedKey, String> m = new HashMap<>();
        m.put(Query.MappedKey.ID, ID_FIELD);
        m.put(Query.MappedKey.TYPE, TYPE_ID_FIELD);
        m.put(Query.MappedKey.ANY, ALL_FIELD);
        specialFields = m;
    }

    /**
     *
     * @param query
     * @param key
     * @return
     */
    private Query.MappedKey mapFullyDenormalizedKey(Query<?> query, String key) {
        Query.MappedKey mappedKey = query.mapDenormalizedKey(getEnvironment(), key);
        if (mappedKey.hasSubQuery()) {
            throw new Query.NoFieldException(query.getGroup(), key);
        } else {
            return mappedKey;
        }
    }

    /**
     *
     * @param properties
     * @param key
     * @param length
     * @return
     */
    private boolean findElasticMap(Map<String, Object> properties, List<String> key, int length) {
        if (properties != null) {
            if (length < key.size()) {
                /* check fields separate */
                if (properties.get("fields") != null) {
                    Map<String, Object> fields = (Map<String, Object>) properties.get("fields");
                    if (fields.get(key.get(length)) != null) {
                        if (length == key.size() - 1) {
                            return true;
                        }
                    }
                }
                if (properties.get("properties") != null) {
                    Map<String, Object> p = (Map<String, Object>) properties.get("properties");
                    return findElasticMap(p, key, length);
                } else if (properties.get(key.get(length)) != null) {
                    if (length == key.size() - 1) {
                        return true;
                    }
                    if (properties.get(key.get(length)) instanceof Map) {
                        return findElasticMap((Map<String, Object>) properties.get(key.get(length)), key, length + 1);
                    }
                }
            }
        }
        return false;
    }

    /**
     // how do we handle internal types? Solr does it with a sortPrefix.
     // Reserved:  `_uid`, `_id`, `_type`, `_source`, `_all`, `_parent`, `_field_names`, `_routing`, `_index`, `_size`, `_timestamp`, and `_ttl`
     * @param typeIds
     * @param field
     * @return
     * @throws IOException
     */
    private boolean checkElasticMappingField(String[] typeIds, String field) throws IOException {

        GetMappingsResponse response = client.admin().indices()
                .prepareGetMappings(indexName)
                .setTypes(typeIds)
                .execute().actionGet();

        for (String typeId : typeIds) {
            Map<String, Object> source = response.getMappings().get(indexName).get(typeId).sourceAsMap();
            Map<String, Object> properties = (Map<String, Object>) source.get("properties");
            List<String> items = Arrays.asList(field.split("\\."));
            if (findElasticMap(properties, items, 0) == false) {
                return false;
            }
        }
        return true;
    }

    /**
     *
     * @param sorters
     * @param orig
     * @param query
     * @param srb
     * @param typeIds
     * @return
     */
    private List<SortBuilder> predicateToSortBuilder(List<Sorter> sorters, QueryBuilder orig, Query<?> query, SearchRequestBuilder srb, String[] typeIds) {
        List<SortBuilder> list = new ArrayList<>();
        if (sorters == null || sorters.size() == 0) {
            list.add(new ScoreSortBuilder());
        } else {
            List<FunctionScoreQueryBuilder.FilterFunctionBuilder> filterFunctionBuilders = new ArrayList<>();
            for (Sorter sorter : sorters) {
                String operator = sorter.getOperator();
                if (Sorter.ASCENDING_OPERATOR.equals(operator) || Sorter.DESCENDING_OPERATOR.equals(operator)) {
                    boolean isAscending = Sorter.ASCENDING_OPERATOR.equals(operator);
                    String queryKey = (String) sorter.getOptions().get(0);

                    String elkField = convertAscendingElkField(queryKey, query, typeIds);

                    if (elkField == null) {
                        throw new UnsupportedIndexException(this, queryKey);
                    }
                    list.add(new FieldSortBuilder(elkField).order(isAscending ? ASC : DESC));
                } else if (Sorter.OLDEST_OPERATOR.equals(operator) || Sorter.NEWEST_OPERATOR.equals(operator)) {
                    // OLDEST_OPERATOR, NEWEST_OPERATOR these are just boosts per Solr
                    // weight, key
                    if (sorter.getOptions().size() < 2) {
                        throw new IllegalArgumentException(operator + " requires Date field");
                    }
                    boolean isOldest = Sorter.OLDEST_OPERATOR.equals(operator);
                    String queryKey = (String) sorter.getOptions().get(1);
                    Query.MappedKey mappedKey = mapFullyDenormalizedKey(query, queryKey);
                    String elkField = specialFields.get(mappedKey);

                    if (elkField == null) {
                        String internalType = mappedKey.getInternalType();
                        if (internalType != null) {
                            // only date can boost this way
                            if (internalType.equals("date")) {
                                elkField = queryKey;
                            } else {
                                throw new IllegalArgumentException();
                            }
                        }
                    }

                    if (elkField == null) {
                        throw new UnsupportedIndexException(this, queryKey);
                    }

                    float boost = ObjectUtils.to(float.class, sorter.getOptions().get(0));
                    if (boost == 0) {
                        boost = 1.0f;
                    }
                    boost = .1f * boost;

                    QueryBuilder qb;
                    long scale = 1000L * 60L * 60L * 24L * 30L * 12L * 5L; // 5 years scaling
                    if (!isOldest) {
                        filterFunctionBuilders.add(
                                new FunctionScoreQueryBuilder.FilterFunctionBuilder(ScoreFunctionBuilders.exponentialDecayFunction(elkField, new Date().getTime(), scale, 0, .1).setWeight(boost))
                        );
                        // Solr: recip(x,m,a,b) implementing a/(m*x+b)
                        // boostFunctionBuilder.append(String.format("{!boost b=recip(ms(NOW/HOUR,%s),3.16e-11,%s,%s)}", solrField, boost, boost));
                    } else {
                        filterFunctionBuilders.add(
                                new FunctionScoreQueryBuilder.FilterFunctionBuilder(ScoreFunctionBuilders.exponentialDecayFunction(elkField, DateUtils.addYears(new java.util.Date(), -5).getTime(), scale, 0, .1).setWeight(boost))
                        );
                        // Solr: linear(x,2,4) returns 2*x+4
                        // boostFunctionBuilder.append(String.format("{!boost b=linear(ms(NOW/HOUR,%s),3.16e-11,%s)}", solrField, boost));
                    }

                } else if (Sorter.FARTHEST_OPERATOR.equals(operator) || Sorter.CLOSEST_OPERATOR.equals(operator)) {
                    if (sorter.getOptions().size() < 2) {
                        throw new IllegalArgumentException(operator + " requires Location");
                    }
                    boolean isClosest = Sorter.CLOSEST_OPERATOR.equals(operator);
                    String queryKey = (String) sorter.getOptions().get(0);

                    String elkField = convertFarthestElkField(queryKey, query, typeIds);

                    if (!(sorter.getOptions().get(1) instanceof Location)) {
                        throw new IllegalArgumentException(operator + " requires Location");
                    }
                    Location sort = (Location) sorter.getOptions().get(1);
                    list.add(new GeoDistanceSortBuilder(elkField, new GeoPoint(sort.getX(), sort.getY()))
                            .order(isClosest ? SortOrder.ASC : SortOrder.DESC));
                } else if (Sorter.RELEVANT_OPERATOR.equals(operator)) {
                    Predicate sortPredicate;
                    Object predicateObject = sorter.getOptions().get(1);
                    Object boostObject = sorter.getOptions().get(0);
                    String boostStr = boostObject.toString();
                    Float boost = Float.valueOf(boostStr);
                    if (predicateObject instanceof Predicate) {
                        sortPredicate = (Predicate) predicateObject;
                        filterFunctionBuilders.add(
                                new FunctionScoreQueryBuilder.FilterFunctionBuilder(
                                        predicateToQueryBuilder(sortPredicate, query),
                                        weightFactorFunction(boost)));
                    } else {
                        list.add(new ScoreSortBuilder());
                    }
                } else {
                    throw new UnsupportedOperationException(operator + " not supported");
                }
            }
            if (filterFunctionBuilders.size() > 0) {
                list.add(new ScoreSortBuilder());
                FunctionScoreQueryBuilder.FilterFunctionBuilder[] functions = new FunctionScoreQueryBuilder.FilterFunctionBuilder[filterFunctionBuilders.size()];
                for (int i = 0; i < filterFunctionBuilders.size(); i++) {
                    functions[i] = filterFunctionBuilders.get(i);
                }
                orig = QueryBuilders.functionScoreQuery(orig, functions)
                        .boostMode(CombineFunction.MULTIPLY)
                        .boost(1.0f)
                        .maxBoost(1000.0f);
                srb.setQuery(orig);
            }
        }
        return list;

    }

    /**
     *
     * @param queryKey
     * @param query
     * @return
     */
    private boolean isReference(String queryKey, Query<?> query) {
        Query.MappedKey mappedKey = query.mapDenormalizedKey(getEnvironment(), queryKey);
        if (mappedKey != null) {
            if (mappedKey.getField() != null) {
                if (mappedKey.getField().getState() != null && mappedKey.getField().getState() instanceof Map) {
                    Map<String, Object> itemMap = mappedKey.getField().getState().getSimpleValues();
                    if (itemMap != null) {
                        if (itemMap.get("valueTypes") != null && itemMap.get("valueTypes") instanceof List) {
                            List l = (List) itemMap.get("valueTypes");
                            if (l.size() == 1 && l.get(0) != null && l.get(0) instanceof Map) {
                                Map<String, Object> o = (Map<String, Object>) l.get(0);
                                if (o.get(StateSerializer.REFERENCE_KEY) != null) {
                                    return true;
                                }
                            }
                        }
                    }
                }

            }
        }
        return false;
    }

    /**
     *
     * @param queryKey
     * @param query
     * @param typeIds
     * @param <T>
     * @return
     */
    private <T> String convertAscendingElkField(String queryKey,  Query<T> query, String[] typeIds) {
        String elkField = null;

        int slash = queryKey.indexOf('/');
        // if ref drop it, if embedded put "."
        if (slash != -1) {
            List<String> keyArr = Arrays.asList(queryKey.split("/"));
            String newKey = null;
            for (String k : keyArr) {
                if (!isReference(k, query)) {
                    Query.MappedKey mappedKey = mapFullyDenormalizedKey(query, k);
                    elkField = specialFields.get(mappedKey);
                    if (elkField == null) {
                        String internalType = mappedKey.getInternalType();
                        if (internalType != null) {
                            if (internalType.equals("text")) {
                                elkField = queryKey + ".raw";
                            } else if (internalType.equals("location")) {
                                elkField = k + "." + LOCATION_FIELD;
                                throw new IllegalArgumentException(elkField + " cannot sort Location on Ascending");
                            } else if (internalType.equals("region")) {
                                elkField = k + "." + REGION_FIELD;
                                throw new IllegalArgumentException(elkField + " cannot sort GeoJSON in Elastic Search");
                            }
                        }
                        if (elkField == null) {
                            elkField = k;
                        }
                    }
                    if (!mappedKey.hasSubQuery() && mappedKey.getInternalType() != null) {
                        newKey = ((newKey == null) ? elkField : (newKey + "." + elkField));
                    }
                }
            }
            elkField = (newKey == null ? queryKey : newKey);
        } else {
            int dot = queryKey.lastIndexOf('.');
            String typeKey = queryKey;
            if (dot != -1) {
                typeKey = queryKey.substring(dot + 1);
            }
            // check the type of the ending field
            Query.MappedKey mappedKey = mapFullyDenormalizedKey(query, typeKey);

            elkField = specialFields.get(mappedKey);

            /* skip for special */
            if (elkField == null) {
                String internalType = mappedKey.getInternalType();
                if (internalType != null) {
                    if (internalType.equals("text")) {
                        elkField = queryKey + ".raw";
                    } else if (internalType.equals("location")) {
                        elkField = queryKey + "." + LOCATION_FIELD;
                        // not sure what to do with lat,long and sort?
                        throw new IllegalArgumentException(elkField + " cannot sort Location on Ascending");
                    } else if (internalType.equals("region")) {
                        elkField = queryKey + "." + REGION_FIELD;
                        throw new IllegalArgumentException(elkField + " cannot sort GeoJSON in Elastic Search");
                    }
                }
                if (elkField == null) {
                    elkField = queryKey;
                }
                if (typeIds != null && elkField != null) {
                    try {
                        if (!checkElasticMappingField(typeIds, elkField)) {
                            throw new UnsupportedIndexException(this, queryKey);
                        }
                    } catch (IOException e) {
                        throw new UnsupportedIndexException(this, queryKey);
                    }
                }
            }
        }
        return elkField;
    }

    /**
     *
     * @param queryKey
     * @param query
     * @param typeIds
     * @param <T>
     * @return
     */
    private <T> String convertFarthestElkField(String queryKey,  Query<T> query, String[] typeIds) {
        String elkField = null;

        int slash = queryKey.indexOf('/');
        // if ref drop it, if embedded put "."
        if (slash != -1) {
            List<String> keyArr = Arrays.asList(queryKey.split("/"));
            String newKey = null;
            for (String k : keyArr) {
                if (!isReference(k, query)) {
                    Query.MappedKey mappedKey = mapFullyDenormalizedKey(query, k);
                    elkField = specialFields.get(mappedKey);
                    if (elkField == null) {
                        String internalType = mappedKey.getInternalType();
                        if (internalType != null) {
                            if (internalType.equals("location")) {
                                elkField = k + "." + LOCATION_FIELD;
                            }
                            if (internalType.equals("region")) {
                                elkField = k + "." + REGION_FIELD;
                                throw new IllegalArgumentException(elkField + " cannot sort GeoJSON in Elastic Search");
                            }
                        }
                        if (elkField == null) {
                            elkField = k;
                        }
                    }
                    if (!mappedKey.hasSubQuery() && mappedKey.getInternalType() != null) {
                        newKey = ((newKey == null) ? elkField : (newKey + "." + elkField));
                    }
                }
            }
            //queryKey = queryKey.substring(slash + 1);
            elkField = (newKey == null ? queryKey : newKey);
        } else {
            int dot = queryKey.lastIndexOf('.');
            String typeKey = queryKey;
            if (dot != -1) {
                typeKey = queryKey.substring(dot + 1);
            }
            // check the type of the ending field
            Query.MappedKey mappedKey = mapFullyDenormalizedKey(query, typeKey);
            elkField = specialFields.get(mappedKey);
            if (elkField == null) {
                String internalType = mappedKey.getInternalType();
                if (internalType != null) {
                    if (internalType.equals("location")) {
                        elkField = queryKey + "." + LOCATION_FIELD;
                    }
                    if (internalType.equals("region")) {
                        elkField = queryKey + "." + REGION_FIELD;
                        throw new IllegalArgumentException(elkField + " cannot sort GeoJSON in Elastic Search");
                    }
                }
                if (elkField == null) {
                    elkField = queryKey;
                }
                if (typeIds != null && elkField != null) {
                    try {
                        if (!checkElasticMappingField(typeIds, elkField)) {
                            throw new UnsupportedIndexException(this, queryKey);
                        }
                    } catch (IOException e) {
                        throw new UnsupportedIndexException(this, queryKey);
                    }
                }
            }
        }

        return elkField;
    }

    /**
     *
     * @param key
     * @param query
     * @param <T>
     * @return
     */
    private <T> List<String> referenceSwitcher(String key, Query<T> query) {
        String[] keyArr = key.split("/");
        List<String> allids = new ArrayList<String>();
        List<?> list = new ArrayList<T>();
        String lastKey = null;
        // Go until last - since the user might want something besides != missing...
        if (keyArr.length > 0) {
            for (int i = 0; i < keyArr.length - 1; i++) {
                lastKey = keyArr[i];
                if (allids.size() == 0) {
                    list = Query.from(query.getObjectClass()).where(keyArr[i] + " != missing").selectAll();
                } else {
                    list = Query.from(query.getObjectClass()).where(keyArr[i] + " != missing").and("_id contains ?", allids).selectAll();
                }
                if (list.size() > (MAX_ROWS - 1)) {
                    LOGGER.warn("reference join in ELK is > " + (MAX_ROWS - 1) + " which will limit results");
                    throw new IllegalArgumentException(key + " / joins > " + (MAX_ROWS - 1) + " not allowed");
                }
                allids = new ArrayList<String>();
                for (int j = 0; j < list.size(); j++) {
                    if (list.get(j) instanceof Record) {
                        Map<String, Object> itemMap = ((Record) list.get(j)).getState().getSimpleValues(false);
                        if (itemMap.get(keyArr[i]) instanceof Map && itemMap.get(keyArr[i]) != null) {
                            Map<String, Object> o = (Map<String, Object>) itemMap.get(keyArr[i]);
                            if (o.get(StateSerializer.REFERENCE_KEY) != null) {
                                allids.add((String) o.get(StateSerializer.REFERENCE_KEY));
                            }
                        } else if (itemMap.get(keyArr[i]) instanceof List && itemMap.get(keyArr[i]) != null) {
                            List<Object> subList = (List<Object>) itemMap.get(keyArr[i]);
                            for (Object sub : subList) {
                                Map<String, Object> s = (Map<String, Object>) sub;
                                if (s.get(StateSerializer.REFERENCE_KEY) != null) {
                                    allids.add((String) s.get(StateSerializer.REFERENCE_KEY));
                                }
                            }
                        }
                    }
                }
            }
        }
        if (list.size() > 0) {
            return allids;
        } else {
            return null;
        }
    }

    /**
     * must override since MAXIMUM_LIMIT is not good for ES
     *
     * @param query
     *        Can't be {@code null}.
     *
     * @param <T>
     * @return
     */
    @Override
    public <T> List<T> readAll(Query<T> query) {
        List<T> listFinal = new ArrayList<T>();
        long row = 0L;
        boolean done = false;
        while (!done) {
            List<T> partial = readPartial(query, row, this.searchMaxRows).getItems();
            // most queries will be handled immediately
            if (partial.size() < this.searchMaxRows && row == 0) {
                return partial;
            }
            if (partial != null && partial.size() > 0) {
                listFinal.addAll(partial);
                row = row + partial.size();
            } else {
                done = true;
            }
        }
        return listFinal;
    }

    public String getGeoJson(List<Region.Circle> circles, Region.MultiPolygon polygons) {
        List<Map<String, Object>> features = new ArrayList<Map<String, Object>>();

        Map<String, Object> featureCollection = new HashMap<String, Object>();
        featureCollection.put("type", "geometrycollection");
        featureCollection.put("geometries", features);

        if (circles != null && circles.size() > 0) {

            for (Region.Circle circle : circles) {
                Map<String, Object> geometry = new HashMap<String, Object>();
                geometry.put("type", "circle");
                geometry.put("coordinates", circle.getGeoJsonArray().get(0)); // required for ELK
                geometry.put("radius", circle.getRadius() + "m");

                features.add(geometry);
            }
        }

        if (polygons != null && polygons.size() > 0) {
            Map<String, Object> geometry = new HashMap<String, Object>();
            geometry.put("type", "multipolygon");
            geometry.put("coordinates", polygons);

            features.add(geometry);
        }

        return ObjectUtils.toJson(featureCollection);
    }

    /**
     *
     * @param v
     * @param type
     * @param key
     * @return
     */
    private QueryBuilder geoLocation(Object v, String type, String key, ShapeRelation sr) {

        if (type != null && type.equals("location")) {
            if (v instanceof Location) {
                return QueryBuilders.boolQuery().must(QueryBuilders.termQuery(key + ".x", ((Location) v).getX()))
                    .must(QueryBuilders.termQuery(key + ".y", ((Location) v).getY()));
            } else if (v instanceof Region) {
                return QueryBuilders.geoDistanceQuery(key + "." + LOCATION_FIELD).point(((Region) v).getX(), ((Region) v).getY())
                        .distance(Region.degreesToKilometers(((Region) v).getRadius()), DistanceUnit.KILOMETERS);
            }
        } else if (type != null && type.equals("polygon")) {
            if (v instanceof Location) {
                return QueryBuilders.boolQuery().must(geoShape(key + "." + REGION_FIELD, ((Location) v).getX(), ((Location) v).getY()));

            } else if (v instanceof Region) {
                // required to fix array issue on Circles and capitals
                Region region = (Region) v;
                String geoJson = getGeoJson(region.getCircles(), region.getPolygons());

                String shapeJson = "{" + "\"shape\":" + geoJson + ",\"relation\": \"" + sr + "\"}";
                String nameJson = "{" + "\"" + key + "." + REGION_FIELD + "\":" + shapeJson + "}";
                String json = "{" + "\"geo_shape\":" + nameJson + "}";
                return QueryBuilders.boolQuery().must(QueryBuilders.wrapperQuery(json));
            }
        }
        return QueryBuilders.termQuery(key, v);
    }

    private GeoShapeQueryBuilder geoShapeIntersects(String key, double x, double y) {
        try {
            return QueryBuilders
                    .geoShapeQuery(key, ShapeBuilders.newPoint(new Coordinate(x, y))).relation(ShapeRelation.INTERSECTS);
        } catch (Exception error) {
            LOGGER.warn(
                    String.format("geoShapeIntersects threw Exception [%s: %s]",
                            error.getClass().getName(),
                            error.getMessage()),
                    error);
        }
        return null;
    }

    private GeoShapeQueryBuilder geoShape(String key, double x, double y) {
        try {
            return QueryBuilders
                    .geoShapeQuery(key, ShapeBuilders.newPoint(new Coordinate(x, y))).relation(ShapeRelation.CONTAINS);
        } catch (Exception error) {
            LOGGER.warn(
                    String.format("geoShape threw Exception [%s: %s]",
                            error.getClass().getName(),
                            error.getMessage()),
                    error);
        }
        return null;
    }

    private GeoShapeQueryBuilder geoCircle(String key, double x, double y, double r) {
        try {
            return QueryBuilders
                    .geoShapeQuery(key, ShapeBuilders.newCircleBuilder().center(x,y).radius(r, DistanceUnit.KILOMETERS)).relation(ShapeRelation.CONTAINS);
        } catch (Exception error) {
            LOGGER.warn(
                    String.format("geoCircle threw Exception [%s: %s]",
                            error.getClass().getName(),
                            error.getMessage()),
                    error);
        }
        return null;
    }

    /**
     *
     * @param predicate
     * @param query
     * @return
     */
    private QueryBuilder predicateToQueryBuilder(Predicate predicate, Query<?> query) {
        if (predicate == null) {
            return QueryBuilders.matchAllQuery();
        }
        if (predicate instanceof CompoundPredicate) {
            CompoundPredicate compound = (CompoundPredicate) predicate;
            List<Predicate> children = compound.getChildren();

            switch (compound.getOperator()) {
                case PredicateParser.AND_OPERATOR :
                    return combine(compound.getOperator(), children, BoolQueryBuilder::must, (predicate1) -> predicateToQueryBuilder(predicate1, query));

                case PredicateParser.OR_OPERATOR :
                    return combine(compound.getOperator(), children, BoolQueryBuilder::should, (predicate1) -> predicateToQueryBuilder(predicate1, query));

                case PredicateParser.NOT_OPERATOR :
                    return combine(compound.getOperator(), children, BoolQueryBuilder::mustNot, (predicate1) -> predicateToQueryBuilder(predicate1, query));

                default :
                    break;
            }

        } else if (predicate instanceof ComparisonPredicate) {
            ComparisonPredicate comparison = (ComparisonPredicate) predicate;
            String pKey = "_any".equals(comparison.getKey()) ? "_all" : comparison.getKey();
            int slash = pKey.lastIndexOf('/');

            List<Object> values = comparison.getValues();

            String operator = comparison.getOperator();

            // this specific one needs to be reduced */
            if (pKey.startsWith("com.psddev.dari.db.ObjectType/")) {
                pKey = pKey.substring(slash + 1) + ".raw";
            } else {
                if (pKey.indexOf('/') != -1) {
                    // ELK does not support joins in 5.2. Might be memory issue and slow!
                    // to do this requires query, take results and send to other query. Sample tests do this.
                    LOGGER.info(pKey + " / joins could be slow in Elastic - do it in app code");
                    List<String> ids = referenceSwitcher(pKey, query);
                    if (ids != null && ids.size() > 0) {
                        pKey = pKey.substring(slash + 1);
                        ComparisonPredicate nComparison = new ComparisonPredicate(comparison.getOperator(),
                                comparison.isIgnoreCase(), pKey, comparison.getValues());
                        Query n = Query.fromAll().where(nComparison).and("_id contains ?", ids);
                        return predicateToQueryBuilder(n.getPredicate(), query);
                    }
                }
            }

            String key = pKey;

            switch (operator) {
                case PredicateParser.EQUALS_ANY_OPERATOR :
                    String geoType = null;
                    for (Object v : values) {
                        if (v == null) {
                            throw new IllegalArgumentException(operator + " requires value");
                        } else if (!(v instanceof String)) {
                            Query.MappedKey mappedKey = mapFullyDenormalizedKey(query, key);
                            String checkField = specialFields.get(mappedKey);
                            if (checkField == null) {
                                String internalType = mappedKey.getInternalType();
                                if (v instanceof Boolean) {
                                    if (internalType != null && internalType.equals("location")) {
                                        throw new IllegalArgumentException(key + " boolean cannot be location");
                                    }
                                    if (internalType != null && internalType.equals("region")) {
                                        throw new IllegalArgumentException(key + " boolean cannot be region");
                                    }
                                } else if (internalType != null && internalType.equals("region")) {
                                    geoType = "polygon";
                                } else if (internalType != null && internalType.equals("location")) {
                                    geoType = "location";
                                }
                            }
                        }
                    }

                    String finalGeoType = geoType;
                    String finalKey = key;
                    return combine(operator, values, BoolQueryBuilder::should, v -> Query.MISSING_VALUE.equals(v)
                            ? QueryBuilders.existsQuery(finalKey)
                            : geoLocation(v, finalGeoType, finalKey, ShapeRelation.WITHIN));

                case PredicateParser.NOT_EQUALS_ALL_OPERATOR :
                    String geoTypeNotEq = null;
                    for (Object v : values) {
                        if (v == null) {
                            throw new IllegalArgumentException(operator + " requires value");
                        } else if (!(v instanceof String)) {
                            Query.MappedKey mappedKey = mapFullyDenormalizedKey(query, key);
                            String checkField = specialFields.get(mappedKey);
                            if (checkField == null) {
                                String internalType = mappedKey.getInternalType();
                                if (v instanceof Boolean) {
                                    if (internalType != null && internalType.equals("location")) {
                                        throw new IllegalArgumentException(key + " boolean cannot be location");
                                    }
                                    if (internalType != null && internalType.equals("region")) {
                                        throw new IllegalArgumentException(key + " boolean cannot be region");
                                    }
                                } else if (internalType != null && internalType.equals("region")) {
                                    geoTypeNotEq = "polygon";
                                } else if (internalType != null && internalType.equals("location")) {
                                    geoTypeNotEq = "location";
                                }
                            }
                        }
                    }

                    String finalGeoTypeNotEq = geoTypeNotEq;
                    String finalKeyNotEq = key;
                    return combine(operator, values, BoolQueryBuilder::mustNot, v -> Query.MISSING_VALUE.equals(v)
                            ? QueryBuilders.existsQuery(finalKeyNotEq)
                            : geoLocation(v, finalGeoTypeNotEq, finalKeyNotEq, ShapeRelation.WITHIN));

                case PredicateParser.LESS_THAN_OPERATOR :
                    for (Object v : values) {
                        if (v == null) {
                            throw new IllegalArgumentException(operator + " requires value");
                        } else if (v instanceof Boolean) {
                            throw new IllegalArgumentException(operator + " cannot be boolean");
                        }
                        if (v != null && Query.MISSING_VALUE.equals(v)) {
                            throw new IllegalArgumentException(operator + " missing not allowed");
                        }
                    }
                    Query.MappedKey mappedKey = mapFullyDenormalizedKey(query, key);
                    String checkField = specialFields.get(mappedKey);
                    if (checkField == null) {
                        String internalType = mappedKey.getInternalType();
                        if (internalType != null && internalType.equals("location")) {
                            throw new IllegalArgumentException(operator + " cannot be location");
                        }
                        if (internalType != null && internalType.equals("region")) {
                            throw new IllegalArgumentException(key + " cannot be region");
                        }
                    }
                    return combine(operator, values, BoolQueryBuilder::must, v ->
                              (v instanceof Location
                                      ? QueryBuilders.boolQuery().must(QueryBuilders.rangeQuery(key + ".x").lt(((Location) v).getX()))
                                        .must(QueryBuilders.rangeQuery(key + ".y").lt(((Location) v).getY()))
                                      : QueryBuilders.rangeQuery(key).lt(v)));

                case PredicateParser.LESS_THAN_OR_EQUALS_OPERATOR :
                    for (Object v : values) {
                        if (v == null) {
                            throw new IllegalArgumentException(operator + " requires value");
                        } else if (v instanceof Boolean) {
                            throw new IllegalArgumentException(operator + " cannot be boolean");
                        }
                        if (v != null && Query.MISSING_VALUE.equals(v)) {
                            throw new IllegalArgumentException(operator + " missing not allowed");
                        }
                    }
                    mappedKey = mapFullyDenormalizedKey(query, key);
                    checkField = specialFields.get(mappedKey);
                    if (checkField == null) {
                        String internalType = mappedKey.getInternalType();
                        if (internalType != null && internalType.equals("location")) {
                            throw new IllegalArgumentException(operator + " cannot be location");
                        }
                        if (internalType != null && internalType.equals("region")) {
                            throw new IllegalArgumentException(key + " cannot be region");
                        }
                    }
                    return combine(operator, values, BoolQueryBuilder::must, v ->
                            (v instanceof Location
                                    ? QueryBuilders.boolQuery().must(QueryBuilders.rangeQuery(key + ".x").lte(((Location) v).getX()))
                                    .must(QueryBuilders.rangeQuery(key + ".y").lte(((Location) v).getY()))
                                    : QueryBuilders.rangeQuery(key).lte(v)));

                case PredicateParser.GREATER_THAN_OPERATOR :
                    for (Object v : values) {
                        if (v == null) {
                            throw new IllegalArgumentException(operator + " requires value");
                        } else if (v instanceof Boolean) {
                            throw new IllegalArgumentException(operator + " cannot be boolean");
                        }
                        if (v != null && Query.MISSING_VALUE.equals(v)) {
                            throw new IllegalArgumentException(operator + " missing not allowed");
                        }

                    }
                    mappedKey = mapFullyDenormalizedKey(query, key);
                    checkField = specialFields.get(mappedKey);
                    if (checkField == null) {
                        String internalType = mappedKey.getInternalType();
                        if (internalType != null && internalType.equals("location")) {
                            throw new IllegalArgumentException(operator + " cannot be location");
                        }
                        if (internalType != null && internalType.equals("region")) {
                            throw new IllegalArgumentException(operator + " cannot be region");
                        }
                    }
                    return combine(operator, values, BoolQueryBuilder::must, v ->
                            (v instanceof Location
                                    ? QueryBuilders.boolQuery().must(QueryBuilders.rangeQuery(key + ".x").gt(((Location) v).getX()))
                                    .must(QueryBuilders.rangeQuery(key + ".y").gt(((Location) v).getY()))
                                    : QueryBuilders.rangeQuery(key).gt(v)));

                case PredicateParser.GREATER_THAN_OR_EQUALS_OPERATOR :
                    for (Object v : values) {
                        if (v == null) {
                            throw new IllegalArgumentException(operator + " requires value");
                        } else if (v instanceof Boolean) {
                            throw new IllegalArgumentException(operator + " cannot be boolean");
                        }
                        if (v != null && Query.MISSING_VALUE.equals(v)) {
                            throw new IllegalArgumentException(operator + " missing not allowed");
                        }
                    }
                    mappedKey = mapFullyDenormalizedKey(query, key);
                    checkField = specialFields.get(mappedKey);
                    if (checkField == null) {
                        String internalType = mappedKey.getInternalType();
                        if (internalType != null && internalType.equals("location")) {
                            throw new IllegalArgumentException(operator + " cannot be location");
                        }
                        if (internalType != null && internalType.equals("region")) {
                            throw new IllegalArgumentException(key + " cannot be region");
                        }
                    }
                    return combine(operator, values, BoolQueryBuilder::must, v ->
                            (v instanceof Location
                                    ? QueryBuilders.boolQuery().must(QueryBuilders.rangeQuery(key + ".x").gte(((Location) v).getX()))
                                    .must(QueryBuilders.rangeQuery(key + ".y").gte(((Location) v).getY()))
                                    : QueryBuilders.rangeQuery(key).gte(v)));

                case PredicateParser.STARTS_WITH_OPERATOR :
                    for (Object v : values) {
                        if (v == null) {
                            throw new IllegalArgumentException(operator + " requires value");
                        }
                        if (v != null && Query.MISSING_VALUE.equals(v)) {
                            throw new IllegalArgumentException(operator + " missing not allowed");
                        }
                        if (v != null && v instanceof Location) {
                            throw new IllegalArgumentException(operator + " location not allowed");
                        }
                        if (v != null && v instanceof Region) {
                            throw new IllegalArgumentException(operator + " region not allowed");
                        }
                    }
                    return combine(operator, values, BoolQueryBuilder::should, v -> QueryBuilders.prefixQuery(key, v.toString()));

                case PredicateParser.CONTAINS_OPERATOR :
                case PredicateParser.MATCHES_ANY_OPERATOR :
                    String internalType = null;
                    if (!key.equals("_any") && !key.equals("_all")) {
                        mappedKey = mapFullyDenormalizedKey(query, key);
                        checkField = specialFields.get(mappedKey);
                        if (checkField == null) {
                            internalType = mappedKey.getInternalType();
                            if (internalType.equals("location")) {
                                throw new IllegalArgumentException(operator + " cannot be location");
                            }
                        }
                    }
                    for (Object v : values) {
                        if (v == null) {
                            throw new IllegalArgumentException(operator + " requires value");
                        }
                        if (v != null && Query.MISSING_VALUE.equals(v)) {
                            throw new IllegalArgumentException(operator + " missing not allowed");
                        }
                        if (v != null && v instanceof Boolean) {
                            if (internalType != null && internalType.equals("region")) {
                                throw new IllegalArgumentException(operator + " region with boolean not allowed");
                            }
                        }
                        if (v != null && v instanceof Location) {
                            if (internalType == null) {
                                throw new IllegalArgumentException(operator + " location not allowed");
                            } else if (!internalType.equals("region") && !internalType.equals("location")) {
                                throw new IllegalArgumentException(operator + " location not allowed except for region/location");
                            }
                        }
                    }

                    String geoType1 = null;
                    if (internalType != null && internalType.equals("region")) {
                        geoType1 = "polygon";
                    } else if (internalType != null && internalType.equals("location")) {
                        geoType1 = "location";
                    }

                    String finalGeoType1 = geoType1;
                    String finalKey1 = key;
                    if (internalType != null && internalType.equals("region")) {
                        return combine(operator, values, BoolQueryBuilder::should, v -> "*".equals(v)
                                ? QueryBuilders.matchAllQuery()
                                : (v instanceof Location
                                    ? QueryBuilders.boolQuery().must(geoShapeIntersects(key + "." + REGION_FIELD, ((Location) v).getX(), ((Location) v).getY()))
                                    : (v instanceof Region
                                        ? QueryBuilders.boolQuery().must(geoLocation(v, finalGeoType1, finalKey1, ShapeRelation.CONTAINS))
                                        : QueryBuilders.matchPhrasePrefixQuery(key, v))));
                    } else {
                        return combine(operator, values, BoolQueryBuilder::should, v -> "*".equals(v)
                                ? QueryBuilders.matchAllQuery()
                                : QueryBuilders.matchPhrasePrefixQuery(key, v));
                    }

                case PredicateParser.MATCHES_ALL_OPERATOR :
                    for (Object v : values) {
                        if (v != null) {
                            throw new IllegalArgumentException(operator + " requires value");
                        }
                        if (v != null && Query.MISSING_VALUE.equals(v)) {
                            throw new IllegalArgumentException(operator + " missing not allowed");
                        }
                    }
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

    /**
     *
     * @param operatorType
     * @param items
     * @param operator
     * @param itemFunction
     * @param <T>
     * @return
     */
    @SuppressWarnings("unchecked")
    private <T> QueryBuilder combine(String operatorType,
            Collection<T> items,
            BiFunction<BoolQueryBuilder, QueryBuilder, BoolQueryBuilder> operator,
            Function<T, QueryBuilder> itemFunction) {

        BoolQueryBuilder builder = QueryBuilders.boolQuery();
        if (items.size() == 0) {
            return QueryBuilders.boolQuery().mustNot(QueryBuilders.matchAllQuery());
        }
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
            return builder;
        } else {
            return QueryBuilders.matchAllQuery();
        }
    }

    /**
     *
     * @param json
     * @param typeId
     * @param id
     */
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

    /**
     *
     * @param client
     * @param isImmediate If {@code true}, the saved data must be
     * @throws Exception
     */
    @Override
    protected void commitTransaction(TransportClient client, boolean isImmediate) throws Exception {
        if (client != null) {
            if (isImmediate) {
                client.admin().indices().prepareFlush(this.indexName).get();
            }
            client.admin().indices().prepareRefresh(this.indexName).get();
        }
    }

    /**
     *
     */
    public void defaultMap() {
        String json = "{\n" +
                "      \"dynamic_templates\": [\n" +
                "        {\n" +
                "          \"locationgeo\": {\n" +
                "            \"match\": \"" + LOCATION_FIELD + "\",\n" +
                "            \"match_mapping_type\": \"string\",\n" +
                "            \"mapping\": {\n" +
                "              \"type\": \"geo_point\"\n" +
                "            }\n" +
                "          }\n" +
                "        },\n" +
                "        {\n" +
                "          \"shapegeo\": {\n" +
                "            \"match\": \"" + REGION_FIELD + "\",\n" +
                "            \"match_mapping_type\": \"object\",\n" +
                "            \"mapping\": {\n" +
                "              \"type\": \"geo_shape\"\n" +
                "            }\n" +
                "          }\n" +
                "        }," +
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
                "    }\n";

        if (client != null) {
            CreateIndexRequestBuilder cirb = client.admin().indices().prepareCreate(this.indexName).addMapping("_default_", json);
            CreateIndexResponse createIndexResponse = cirb.execute().actionGet();

            client.admin().cluster().health(new ClusterHealthRequest(indexName).waitForYellowStatus());
            // need to set environment.
        }

    }

    /**
     *
     */
    public void deleteIndex() {
        if (client != null) {
            IndicesExistsRequest existsRequest = client.admin().indices().prepareExists(indexName).request();
            if (client.admin().indices().exists(existsRequest).actionGet().isExists()) {
                LOGGER.info("index %s exists... deleting!", indexName);
                DeleteIndexResponse response = client.admin().indices().delete(new DeleteIndexRequest(indexName)).actionGet();
                if (!response.isAcknowledged()) {
                    LOGGER.error("Failed to delete elastic search index named %s", indexName);
                }
            }
            client.close();
            client = null;
            this.client = openConnection();
        }
    }

    /**
     *
     * @param valueMap
     */
    private static void convertToGeometryCollection(Map<String, Object> valueMap, String name) {

        if (valueMap.size() > 2) {
            if (valueMap.containsKey("polygons") && valueMap.containsKey("circles") && valueMap.containsKey("radius")) {
                Map<String, Object> newValueMap = new HashMap();
                newValueMap.put("type", "geometrycollection");

                List newGeometries = new ArrayList();
                if (valueMap.get("polygons") != null && valueMap.get("polygons") instanceof List) {
                    List polygons = (List) valueMap.get("polygons");
                    if (polygons.size() > 0) {
                        Map<String, Object> newObject = new HashMap<String, Object>();
                        newObject.put("type", "multipolygon");
                        List newPolygons = new ArrayList();
                        for (Object p : polygons) {
                            List newPolygon = new ArrayList();
                            if (p instanceof List) {
                                for (Object ring : (List) p) {
                                    List<List> newRing = new ArrayList();
                                    for (Object latlon : (List) ring) {
                                        List<Double> newLatLon = new ArrayList();

                                        Double lat = (Double) ((List) latlon).get(0);
                                        Double lon = (Double) ((List) latlon).get(1);
                                        newLatLon.add(lat);
                                        newLatLon.add(lon);
                                        newRing.add(newLatLon);
                                    }
                                    newPolygon.add(newRing);
                                }
                                newPolygons.add(newPolygon);
                            }
                        }
                        newObject.put("coordinates", newPolygons);
                        newGeometries.add(newObject);
                    }
                }

                if (valueMap.get("circles") != null && valueMap.get("circles") instanceof List) {
                    List circles = (List) valueMap.get("circles");
                    if (circles.size() > 0) {
                        for (Object c : circles) {
                            if (c instanceof List) {
                                Map<String, Object> newGeometry = new HashMap<String, Object>();
                                List<Double> newCircle = new ArrayList();

                                newGeometry.put("type", "circle");
                                Double lat = (Double) ((List) c).get(0);
                                Double lon = (Double) ((List) c).get(1);
                                Double r = (Double) ((List) c).get(2);

                                newCircle.add(lat);
                                newCircle.add(lon);
                                newGeometry.put("coordinates", newCircle);
                                newGeometry.put("radius", Region.degreesToMeters(r) + "m");
                                newGeometries.add(newGeometry);
                            }
                        }
                    }
                }
                newValueMap.put("geometries", newGeometries);
                if (valueMap.containsKey("x")) {
                    valueMap.remove("x");
                }
                if (valueMap.containsKey("y")) {
                    valueMap.remove("y");
                }
                if (valueMap.containsKey("radius")) {
                    valueMap.remove("radius");
                }
                if (valueMap.containsKey("circles")) {
                    valueMap.remove("circles");
                }
                if (valueMap.containsKey("polygons")) {
                    valueMap.remove("polygons");
                }
                valueMap.put(name, newValueMap);
            }
        }
    }

    /**
     * @param map
     * @param name
     */
    @SuppressWarnings("unchecked")
    private static void convertRegionToName(Map<String, Object> map, String name) throws IOException {

        Iterator<Map.Entry<String, Object>> it = map.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, Object> pair = it.next();
            String key = pair.getKey();
            Object value = pair.getValue();

            if (value instanceof Map) {
                Map<String, Object> valueMap = (Map<String, Object>) value;

                convertToGeometryCollection(valueMap, name);
                convertRegionToName((Map<String, Object>) value, name);
            } else if (value instanceof List) {
                for (Object item : (List<?>) value) {
                    if (item instanceof Map) {
                        Map<String, Object> valueMap = (Map<String, Object>) item;

                        convertToGeometryCollection(valueMap, name);
                        convertRegionToName((Map<String, Object>) item, name);
                    }
                }
            }
        }
    }

    /**
     *
     * @param map
     * @param name
     */
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

    /**
     *
     * @param client
     * @param isImmediate
     * @param saves
     * @param indexes
     * @param deletes
     * @throws Exception
     */
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
                            // Elastic requires us to remove the 2
                            t.remove("_id");
                            t.remove("_type");
                            LOGGER.info("ELK doWrites saving _type [{}] and _id [{}]",
                                    documentType, documentId);

                            convertLocationToName(t, LOCATION_FIELD);
                            convertRegionToName(t, REGION_FIELD);

                            LOGGER.debug("ELK doWrites saving _type [{}] and _id [{}] = [{}]",
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
