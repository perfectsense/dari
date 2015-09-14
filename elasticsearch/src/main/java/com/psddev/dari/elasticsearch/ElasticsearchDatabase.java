package com.psddev.dari.elasticsearch;

import com.google.common.base.Preconditions;
import com.psddev.dari.db.AbstractDatabase;
import com.psddev.dari.db.ComparisonPredicate;
import com.psddev.dari.db.CompoundPredicate;
import com.psddev.dari.db.Predicate;
import com.psddev.dari.db.PredicateParser;
import com.psddev.dari.db.Query;
import com.psddev.dari.db.State;
import com.psddev.dari.db.UnsupportedPredicateException;
import com.psddev.dari.util.ObjectUtils;
import com.psddev.dari.util.PaginatedResult;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.BiFunction;
import java.util.function.Function;

public class ElasticsearchDatabase extends AbstractDatabase<Client> {

    public static final String CLUSTER_NAME_SUB_SETTING = "clusterName";
    public static final String INDEX_NAME_SUB_SETTING = "indexName";
    public static final String TYPE_NAME_SUB_SETTING = "typeName";

    private String indexName;
    private String typeName;

    private transient Node node;

    public String getIndexName() {
        return indexName;
    }

    public void setIndexName(String indexName) {
        this.indexName = indexName;
    }

    public String getTypeName() {
        return !ObjectUtils.isBlank(typeName) ? typeName : "dari";
    }

    public void setTypeName(String typeName) {
        this.typeName = typeName;
    }

    @Override
    public Client openConnection() {
        return node.client();
    }

    @Override
    public void closeConnection(Client client) {
        client.close();
    }

    @Override
    protected void doInitialize(String settingsKey, Map<String, Object> settings) {
        String clusterName = ObjectUtils.to(String.class, settings.get(CLUSTER_NAME_SUB_SETTING));

        Preconditions.checkNotNull(clusterName);

        String indexName = ObjectUtils.to(String.class, settings.get(INDEX_NAME_SUB_SETTING));

        Preconditions.checkNotNull(indexName);

        String typeName = ObjectUtils.to(String.class, settings.get(TYPE_NAME_SUB_SETTING));

        this.indexName = indexName;
        this.typeName = typeName;
        this.node = NodeBuilder.nodeBuilder()
                .clusterName(clusterName)
                .client(true)
                .node();
    }

    @Override
    public Date readLastUpdate(Query<?> query) {
        return null;
    }

    @Override
    public <T> PaginatedResult<T> readPartial(Query<T> query, long offset, int limit) {
        Client client = openConnection();

        try {
            Set<UUID> typeIds = query.getConcreteTypeIds(this);
            String[] typeIdStrings = typeIds.size() == 0
                    ? new String[] { "_all" }
                    : typeIds.stream().map(UUID::toString).toArray(String[]::new);

            SearchResponse response = client.prepareSearch(getIndexName())
                    .setFetchSource(!query.isReferenceOnly())
                    .setTypes(typeIdStrings)
                    .setQuery(predicateToQueryBuilder(query.getPredicate()))
                    .setFrom((int) offset)
                    .setSize(limit)
                    .execute()
                    .actionGet();

            SearchHits hits = response.getHits();
            List<T> items = new ArrayList<>();

            for (SearchHit hit : hits.getHits()) {
                items.add(createSavedObjectWithHit(hit, query));
            }

            return new PaginatedResult<>(offset, limit, hits.getTotalHits(), items);

        } finally {
            closeConnection(client);
        }
    }

    private QueryBuilder predicateToQueryBuilder(Predicate predicate) {
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
            if (!Query.MISSING_VALUE.equals(item)) {
                builder = operator.apply(builder, itemFunction.apply(item));
            }
        }

        return builder.hasClauses() ? builder : QueryBuilders.matchAllQuery();
    }

    private <T> T createSavedObjectWithHit(SearchHit hit, Query<T> query) {
        T object = createSavedObject(hit.getType(), hit.getId(), query);
        State objectState = State.getInstance(object);

        if (!objectState.isReferenceOnly()) {
            objectState.setValues(hit.getSource());
        }

        return swapObjectType(query, object);
    }

    @Override
    protected void doWrites(Client client, boolean isImmediate, List<State> saves, List<State> indexes, List<State> deletes) throws Exception {
        BulkRequestBuilder bulk = client.prepareBulk();
        String indexName = getIndexName();

        if (saves != null) {
            for (State state : saves) {
                bulk.add(client
                        .prepareIndex(indexName, state.getTypeId().toString(), state.getId().toString())
                        .setSource(state.getSimpleValues()));
            }
        }

        if (deletes != null) {
            for (State state : deletes) {
                bulk.add(client
                        .prepareDelete(indexName, state.getTypeId().toString(), state.getId().toString()));
            }
        }

        bulk.execute().actionGet();
    }
}
