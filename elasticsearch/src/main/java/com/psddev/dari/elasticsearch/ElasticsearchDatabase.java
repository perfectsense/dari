package com.psddev.dari.elasticsearch;

import com.google.common.base.Preconditions;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import com.psddev.dari.db.*;
import com.psddev.dari.util.ObjectUtils;
import com.psddev.dari.util.PaginatedResult;
import com.psddev.dari.util.gson.Gson;
import com.psddev.dari.util.gson.JsonObject;
import com.psddev.dari.util.gson.JsonParser;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
//import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.node.Node;
//import org.elasticsearch.node.NodeBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.client.*;
import org.elasticsearch.search.internal.InternalSearchHit;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Constructor;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.text.DateFormat;
import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Function;


public class ElasticsearchDatabase extends AbstractDatabase<TransportClient> {

    public static final String CLUSTER_NAME_SUB_SETTING = "clusterName";
    public static final String CLUSTER_PORT_SUB_SETTING = "clusterPort";
    public static final String HOSTNAME_SUB_SETTING = "clusterHostname";
    public static final String INDEX_NAME_SUB_SETTING = "indexName";

    private static final Logger LOGGER = LoggerFactory.getLogger(ElasticsearchDatabase.class);

    private static final String DATABASE_NAME = "elasticsearch";
    private static final String SETTING_KEY_PREFIX = "dari/database/" + DATABASE_NAME + "/";

    private String indexName;
    private String clusterName;
    private int clusterPort;
    private String clusterHostname;

    private static final String name = "ElasticsearchDatabase";

    //private transient Node node;
    private transient Settings nodeSettings;
    private transient TransportClient client;

    public String getIndexName() {
        return indexName;
    }

    public void setIndexName(String indexName) {
        this.indexName = indexName;
    }

    @Override
    public String toString() {
        return name;
    }


    @Override
    public TransportClient openConnection() {

        if (this.client != null && isAlive(this.client)) {
            return this.client;
        }
        TransportClient client;
        try {
            if (nodeSettings == null) {
                LOGGER.warn("ELK openConnection No nodeSettings");
                nodeSettings = Settings.builder()
                        .put("client.transport.sniff", true).build();
            }
             client = new PreBuiltTransportClient(nodeSettings)
                    .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(this.clusterHostname), this.clusterPort));
             if (!isAlive(client)) {
                 LOGGER.warn("ELK openConnection Not Alive!");
                 return null;
             }
             this.client = client;
             return client;
        } catch (Exception e) {
            StringWriter errors = new StringWriter();
            e.printStackTrace(new PrintWriter(errors));
            LOGGER.info("Cannot open ES Exception [{}]", errors.toString());

        }
        LOGGER.info("doWrites return null");
        return null;
    }

    @Override
    public void closeConnection(TransportClient client) {
        client.close();
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

        this.clusterName = clusterName;
        this.clusterPort = Integer.parseInt(clusterPort);
        this.clusterHostname = clusterHostname;
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
        List<DiscoveryNode> nodes = client.connectedNodes();
        if (nodes.isEmpty()) {
            return false;
        } else {
            return true;
        }
    }

    public boolean isAlive() {
        TransportClient client = openConnection();
        List<DiscoveryNode> nodes = client.connectedNodes();
        if (nodes.isEmpty()) {
            closeConnection(client);
            return false;
        } else {
            closeConnection(client);
            return true;
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> PaginatedResult<T> readPartial(Query<T> query, long offset, int limit) {
        LOGGER.info("ELK readPartial query.getPredicate() [{}]", query.getPredicate());

        TransportClient client = openConnection();
        if (client == null || !isAlive(client)) {
            return null;
        }
        List<T> items = new ArrayList<>();

        try {
            Set<UUID> typeIds = query.getConcreteTypeIds(this);
            if (query.getGroup() != null && typeIds.size() == 0) {
                // should limit by the type
                LOGGER.info("ELK readPartial the call is to limit by from() but did not load typeIds! [{}]", query.getGroup());
            }
            String[] typeIdStrings = typeIds.size() == 0
                    ? new String[]{ "_all" }
                    : typeIds.stream().map(UUID::toString).toArray(String[]::new);

            SearchResponse response;
            if (typeIds.size() == 0) {
                response = client.prepareSearch(getIndexName())
                        .setFetchSource(!query.isReferenceOnly())
                        .setQuery(predicateToQueryBuilder(query.getPredicate()))
                        .setFrom((int) offset)
                        .setSize(limit)
                        .execute()
                        .actionGet();
            } else {
                response = client.prepareSearch(getIndexName())
                        .setFetchSource(!query.isReferenceOnly())
                        .setTypes(typeIdStrings)
                        .setQuery(predicateToQueryBuilder(query.getPredicate()))
                        .setFrom((int) offset)
                        .setSize(limit)
                        .execute()
                        .actionGet();
            }

            SearchHits hits = response.getHits();

            LOGGER.info("ELK readPartial hits [{}]", hits.getTotalHits());

            for (SearchHit hit : hits.getHits()) {

                items.add(createSavedObjectWithHit(hit, query));

            }

            PaginatedResult<T> p = new PaginatedResult<>(offset, limit, hits.getTotalHits(), items);
            return p;
        } catch (Exception e) {
            StringWriter errors = new StringWriter();
            e.printStackTrace(new PrintWriter(errors));
            LOGGER.info("readPartial Exception [{}]", errors.toString());
        } /* finally {
            closeConnection(client);
        } */
        return new PaginatedResult<>(offset, limit, 0, items);
    }

    private <T> T createSavedObjectWithHit(SearchHit hit, Query<T> query) {
        T object = createSavedObject(hit.getType(), hit.getId(), query);

        State objectState = State.getInstance(object);

        if (!objectState.isReferenceOnly()) {
            objectState.setValues(hit.getSource());
        }

        return swapObjectType(query, object);
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
                    return combine(children, BoolQueryBuilder::must, this::predicateToQueryBuilder);

                case PredicateParser.OR_OPERATOR :
                    return combine(children, BoolQueryBuilder::should, this::predicateToQueryBuilder);

                case PredicateParser.NOT_OPERATOR :
                    return combine(children, BoolQueryBuilder::mustNot, this::predicateToQueryBuilder);

                default :
                    break;
            }

        } else if (predicate instanceof ComparisonPredicate) {
            ComparisonPredicate comparison = (ComparisonPredicate) predicate;
            String key = "_any".equals(comparison.getKey()) ? "_all" : comparison.getKey();
            List<Object> values = comparison.getValues();

            switch (comparison.getOperator()) {
                case PredicateParser.EQUALS_ANY_OPERATOR :
                    return combine(values, BoolQueryBuilder::should, v -> QueryBuilders.termQuery(key, v));

                case PredicateParser.NOT_EQUALS_ALL_OPERATOR :
                    return combine(values, BoolQueryBuilder::mustNot, v -> QueryBuilders.termQuery(key, v));

                case PredicateParser.LESS_THAN_OPERATOR :
                    return combine(values, BoolQueryBuilder::must, v -> QueryBuilders.rangeQuery(key).lt(v));

                case PredicateParser.LESS_THAN_OR_EQUALS_OPERATOR :
                    return combine(values, BoolQueryBuilder::must, v -> QueryBuilders.rangeQuery(key).lte(v));

                case PredicateParser.GREATER_THAN_OPERATOR :
                    return combine(values, BoolQueryBuilder::must, v -> QueryBuilders.rangeQuery(key).gt(v));

                case PredicateParser.GREATER_THAN_OR_EQUALS_OPERATOR :
                    return combine(values, BoolQueryBuilder::must, v -> QueryBuilders.rangeQuery(key).gte(v));

                case PredicateParser.STARTS_WITH_OPERATOR :
                    return combine(values, BoolQueryBuilder::should, v -> QueryBuilders.prefixQuery(key, v.toString()));

                case PredicateParser.CONTAINS_OPERATOR :
                case PredicateParser.MATCHES_ANY_OPERATOR :
                    return combine(values, BoolQueryBuilder::should, v -> "*".equals(v)
                            ? QueryBuilders.matchAllQuery()
                            : QueryBuilders.matchPhrasePrefixQuery(key, v));

                case PredicateParser.MATCHES_ALL_OPERATOR :
                    return combine(values, BoolQueryBuilder::must, v -> "*".equals(v)
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
    private <T> QueryBuilder combine(
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
            }
        }

        if (builder.hasClauses()) {
            LOGGER.info("ELK predicate [{}]", builder.toString());
            return builder;
        } else {
            LOGGER.info("ELK predicate default [{}]", QueryBuilders.matchAllQuery());
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
                LOGGER.info("saveJson: Save Json not working");
            }

        } finally {
            closeConnection(client);
        }


    }
    @Override
    protected void doWrites(TransportClient client, boolean isImmediate, List<State> saves, List<State> indexes, List<State> deletes) throws Exception {
        try {
            BulkRequestBuilder bulk = client.prepareBulk();

            String indexName = getIndexName();

            if (saves != null) {
                    for (State state : saves) {
                        LOGGER.info("ELK saving _id [{}] and _type [{}]",
                                state.getId().toString(),
                                state.getTypeId().toString());
                        try {
                            // before saving might want to theck if there
                            Map<String, Object> t = state.getSimpleValues();
                            t.remove("_id");
                            t.remove("_type");
                            bulk.add(client
                                    .prepareIndex(indexName, state.getTypeId().toString(), state.getId().toString())
                                    .setSource(t));
                        } catch (Exception e) {
                            StringWriter errors = new StringWriter();
                            e.printStackTrace(new PrintWriter(errors));
                            LOGGER.info("doWrites saves Exception [{}]", errors.toString());
                        }
                    }
            }

            if (deletes != null) {
                for (State state : deletes) {
                    LOGGER.info("ELK deleting _id [{}] and _type [{}]",
                            state.getId().toString(),
                            state.getTypeId().toString());
                    try {
                        bulk.add(client
                                .prepareDelete(indexName, state.getTypeId().toString(), state.getId().toString()));
                    } catch (Exception e) {
                        StringWriter errors = new StringWriter();
                        e.printStackTrace(new PrintWriter(errors));
                        LOGGER.info("doWrites deletes Exception [{}]", errors.toString());

                    }
                }

            }

            bulk.execute().actionGet();
        } catch (Exception e) {
            StringWriter errors = new StringWriter();
            e.printStackTrace(new PrintWriter(errors));
            LOGGER.info("doWrites Exception [{}]", errors.toString());

        }
    }
}
