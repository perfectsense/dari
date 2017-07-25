package com.psddev.dari.db;

import java.io.IOException;
import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.FacetField;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.RangeFacet;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.MoreLikeThisParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.psddev.dari.util.Lazy;
import com.psddev.dari.util.ObjectUtils;
import com.psddev.dari.util.PaginatedResult;
import com.psddev.dari.util.Profiler;
import com.psddev.dari.util.Settings;
import com.psddev.dari.util.SettingsException;
import com.psddev.dari.util.Stats;
import com.psddev.dari.util.UuidUtils;

/**
 * Database backed by a
 * <a href="http://lucene.apache.org/solr/">Apache Solr</a> server.
 */
public class SolrDatabase extends AbstractDatabase<SolrServer> {

    public static final String DEFAULT_COMMIT_WITHIN_SETTING = "dari/solrDefaultCommitWithin";

    public static final String SERVER_URL_SUB_SETTING = "serverUrl";
    public static final String READ_SERVER_URL_SUB_SETTING = "readServerUrl";
    public static final String TENANT_SUB_SETTING = "tenant";
    public static final String COMMIT_WITHIN_SUB_SETTING = "commitWithin";
    public static final String VERSION_SUB_SETTING = "version";
    public static final String SAVE_DATA_SUB_SETTING = "saveData";
    public static final String AUTO_COMMIT_SUB_SETTING = "autoCommit";

    public static final double DEFAULT_COMMIT_WITHIN = 0.0;

    public static final String TENANT_FIELD = "_s__tenant";
    public static final String ID_FIELD = "id";
    public static final String TYPE_ID_FIELD = "typeId";
    public static final String DATA_FIELD = "data";
    public static final String FIELDS_FIELD = "_t__fields";
    public static final String ALL_FIELD = "all";
    public static final String SCORE_FIELD = "score";

    public static final String SUGGESTION_FIELD = "_e_suggestField";

    public static final String SCORE_EXTRA = "solr.score";
    public static final String NORMALIZED_SCORE_EXTRA = "solr.normalizedScore";

    private static final int INITIAL_FETCH_SIZE = 100;
    private static final Logger LOGGER = LoggerFactory.getLogger(SolrDatabase.class);
    private static final Pattern UUID_PATTERN = Pattern.compile("([A-Fa-f0-9]{8})-([A-Fa-f0-9]{4})-([A-Fa-f0-9]{4})-([A-Fa-f0-9]{4})-([A-Fa-f0-9]{12})");

    private static final String SHORT_NAME = "Solr";
    private static final Stats STATS = new Stats(SHORT_NAME);
    private static final String ADD_STATS_OPERATION = "Add";
    private static final String COMMIT_STATS_OPERATION = "Commit";
    private static final String DELETE_STATS_OPERATION = "Delete";
    private static final String QUERY_STATS_OPERATION = "Query";
    private static final String ADD_PROFILER_EVENT = SHORT_NAME + " " + ADD_STATS_OPERATION;
    private static final String COMMIT_PROFILER_EVENT = SHORT_NAME + " " + COMMIT_STATS_OPERATION;
    private static final String DELETE_PROFILER_EVENT = SHORT_NAME + " " + DELETE_STATS_OPERATION;
    private static final String QUERY_PROFILER_EVENT = SHORT_NAME + " " + QUERY_STATS_OPERATION;
    private static final int MAX_BINARY_FIELD_LENGTH = 500;
    private static final Set<String> TRUNCATE_FIELD_PREFIXES = new HashSet<>();

    static {
        TRUNCATE_FIELD_PREFIXES.add("_sl_");
        TRUNCATE_FIELD_PREFIXES.add("_ss_");
    }

    private volatile SolrServer server;
    private volatile SolrServer readServer;
    private volatile String tenant;
    private volatile Double commitWithin;
    private volatile String version;
    private volatile boolean saveData = true;
    private volatile boolean autoCommit;

    /** Returns the underlying Solr server. */
    public SolrServer getServer() {
        return server;
    }

    /** Sets the underlying Solr server. */
    public void setServer(SolrServer server) {
        this.server = server;
        schema.reset();
    }

    /** Returns the underlying Solr read server. */
    public SolrServer getReadServer() {
        return readServer;
    }

    /** Sets the underlying Solr read server. */
    public void setReadServer(SolrServer readServer) {
        this.readServer = readServer;
        schema.reset();
    }

    public String getTenant() {
        return tenant;
    }

    public void setTenant(String tenant) {
        this.tenant = tenant;
    }

    public Double getCommitWithin() {
        return commitWithin;
    }

    public double getEffectiveCommitWithin() {
        Double commitWithin = getCommitWithin();
        if (commitWithin == null) {
            commitWithin = Settings.get(Double.class, DEFAULT_COMMIT_WITHIN_SETTING);
            if (commitWithin == null) {
                commitWithin = DEFAULT_COMMIT_WITHIN;
            }
        }
        return commitWithin;
    }

    public void setCommitWithin(Double newCommitWithin) {
        commitWithin = newCommitWithin;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public boolean isSaveData() {
        return saveData;
    }

    public void setSaveData(boolean saveData) {
        this.saveData = saveData;
    }

    public boolean isAutoCommit() {
        return autoCommit;
    }

    public void setAutoCommit(boolean autoCommit) {
        this.autoCommit = autoCommit;
    }

    private static class SolrSchema {

        public final int version;

        private SolrField defaultType;
        private final Map<String, SolrField> types = new HashMap<String, SolrField>();

        public SolrSchema(int version) {
            this.version = version;
        }

        public void setDefaultField(SolrField defaultType) {
            this.defaultType = defaultType;
        }

        public void mapFields(SolrField type, String... internalTypes) {
            if (internalTypes != null) {
                for (String internalType : internalTypes) {
                    types.put(internalType, type);
                }
            }
        }

        public SolrField getField(String internalType) {
            SolrField type = types.get(internalType);
            return type != null ? type : defaultType;
        }
    }

    private static class SolrField {

        public final String facetPrefix;
        public final String searchPrefix;
        public final String sortPrefix;
        public final Set<String> addPrefixes;
        public final Set<String> setPrefixes;

        public SolrField(String facetPrefix, String searchPrefix, String sortPrefix, boolean allMultiValued) {
            this.facetPrefix = facetPrefix;
            this.searchPrefix = searchPrefix;
            this.sortPrefix = sortPrefix;

            Set<String> addPrefixes = new HashSet<String>();
            addPrefixes.add(facetPrefix);
            addPrefixes.add(searchPrefix);

            Set<String> setPrefixes = new HashSet<String>();
            setPrefixes.add(sortPrefix);

            if (allMultiValued) {
                setPrefixes.removeAll(addPrefixes);
            } else {
                addPrefixes.removeAll(setPrefixes);
            }

            this.addPrefixes = Collections.unmodifiableSet(addPrefixes);
            this.setPrefixes = Collections.unmodifiableSet(setPrefixes);
        }

        public SolrField(String facetPrefix, String searchPrefix, String sortPrefix) {
            this(facetPrefix, searchPrefix, sortPrefix, false);
        }
    }

    private final transient Lazy<SolrSchema> schema = new Lazy<SolrSchema>() {

        @Override
        protected SolrSchema create() {
            Exception error = query("*:*");

            if (error != null) {
                throw new IllegalStateException("Solr server isn't available!", error);
            }

            SolrSchema schema;

            if (query("_e_test:1") == null) {
                schema = new SolrSchema(10);

                schema.setDefaultField(new SolrField("_sl_", "_t_", "_ss_"));
                schema.mapFields(new SolrField("_b_", "_b_", "_bs_"), ObjectField.BOOLEAN_TYPE);
                schema.mapFields(new SolrField("_d_", "_d_", "_ds_"), ObjectField.NUMBER_TYPE);
                schema.mapFields(new SolrField("_l_", "_l_", "_ls_"), ObjectField.DATE_TYPE);
                schema.mapFields(new SolrField("_u_", "_u_", "_us_"), ObjectField.RECORD_TYPE, ObjectField.UUID_TYPE);
                schema.mapFields(new SolrField("_g_", "_g_", "_g_"), ObjectField.LOCATION_TYPE);

            } else if (query("_query_:\"{!geofilt pt=0.0,0.0 sfield=_g_test d=1}\"") == null) {
                schema = new SolrSchema(9);

                schema.setDefaultField(new SolrField("_sl_", "_t_", "_ss_"));
                schema.mapFields(new SolrField("_b_", "_b_", "_bs_"), ObjectField.BOOLEAN_TYPE);
                schema.mapFields(new SolrField("_d_", "_d_", "_ds_"), ObjectField.NUMBER_TYPE);
                schema.mapFields(new SolrField("_l_", "_l_", "_ls_"), ObjectField.DATE_TYPE);
                schema.mapFields(new SolrField("_u_", "_u_", "_us_"), ObjectField.RECORD_TYPE, ObjectField.UUID_TYPE);
                schema.mapFields(new SolrField("_g_", "_g_", "_g_"), ObjectField.LOCATION_TYPE);

            } else if (query("_sl_test:1") == null) {
                schema = new SolrSchema(8);

                schema.setDefaultField(new SolrField("_sl_", "_t_", "_ss_"));
                schema.mapFields(new SolrField("_b_", "_b_", "_bs_"), ObjectField.BOOLEAN_TYPE);
                schema.mapFields(new SolrField("_d_", "_d_", "_ds_"), ObjectField.NUMBER_TYPE);
                schema.mapFields(new SolrField("_l_", "_l_", "_ls_"), ObjectField.DATE_TYPE);
                schema.mapFields(new SolrField("_u_", "_u_", "_us_"), ObjectField.RECORD_TYPE, ObjectField.UUID_TYPE);

            } else if (query("_ss_test:1") == null) {
                schema = new SolrSchema(7);

                schema.setDefaultField(new SolrField("_s_", "_t_", "_ss_"));
                schema.mapFields(new SolrField("_b_", "_b_", "_bs_"), ObjectField.BOOLEAN_TYPE);
                schema.mapFields(new SolrField("_d_", "_d_", "_ds_"), ObjectField.NUMBER_TYPE);
                schema.mapFields(new SolrField("_l_", "_l_", "_ls_"), ObjectField.DATE_TYPE);
                schema.mapFields(new SolrField("_u_", "_u_", "_us_"), ObjectField.RECORD_TYPE, ObjectField.UUID_TYPE);

            } else {
                schema = new SolrSchema(6);

                schema.setDefaultField(new SolrField("_s_", "_t_", "_s_"));
                schema.mapFields(new SolrField("_b_", "_b_", "_b_", true), ObjectField.BOOLEAN_TYPE);
                schema.mapFields(new SolrField("_d_", "_d_", "_d_", true), ObjectField.NUMBER_TYPE);
                schema.mapFields(new SolrField("_l_", "_l_", "_l_", true), ObjectField.DATE_TYPE);
                schema.mapFields(new SolrField("_u_", "_u_", "_u_", true), ObjectField.RECORD_TYPE, ObjectField.UUID_TYPE);
            }

            LOGGER.info("Using Solr schema version [{}]", schema.version);
            return schema;
        }

        private Exception query(String query) {
            SolrServer server = openReadConnection();
            SolrQuery solrQuery = new SolrQuery(query);

            solrQuery.setRows(0);

            try {
                server.query(solrQuery);
                return null;

            } catch (SolrServerException error) {
                return error;
            }
        }
    };

    private SolrField getSolrField(String internalType) {
        return schema.get().getField(internalType);
    }

    private Query.MappedKey mapFullyDenormalizedKey(Query<?> query, String key) {
        Query.MappedKey mappedKey = query.mapDenormalizedKey(getEnvironment(), key);
        if (mappedKey.hasSubQuery()) {
            throw new Query.NoFieldException(query.getGroup(), key);
        } else {
            return mappedKey;
        }
    }

    private SolrQuery addComment(SolrQuery solrQuery, Query<?> query) {
        if (solrQuery != null && query != null) {
            String comment = query.getComment();

            if (!ObjectUtils.isBlank(comment)) {
                solrQuery.set("dari.queryComment", comment);
            }
        }

        return solrQuery;
    }

    public SolrQuery buildQueryFacetByField(Query<?> query, String field) {
        SolrQuery solrQuery = buildQuery(query);
        Query.MappedKey mappedKey = mapFullyDenormalizedKey(query, field);
        String solrField = specialFields.get(mappedKey);

        if (solrField == null) {
            String internalType = mappedKey.getInternalType();
            if (internalType != null) {
                solrField = getSolrField(internalType).searchPrefix + mappedKey.getIndexKey(null);
            }
        }

        if (solrField == null) {
            throw new UnsupportedIndexException(this, field);
        }

        solrQuery.setFacet(true);
        solrQuery.addFacetField(solrField);

        return addComment(solrQuery, query);
    }

    public SolrQuery buildQueryNumericRangeFacet(Query<?> query, String field, Number start, Number end, Number gap) {
        SolrQuery solrQuery = buildQuery(query);
        Query.MappedKey mappedKey = mapFullyDenormalizedKey(query, field);
        String solrField = specialFields.get(mappedKey);

        if (solrField == null) {
            String internalType = mappedKey.getInternalType();
            if (internalType != null) {
                solrField = getSolrField(internalType).searchPrefix + mappedKey.getIndexKey(null);
            }
        }

        if (solrField == null) {
            throw new UnsupportedIndexException(this, field);
        }

        solrQuery.addNumericRangeFacet(solrField, start, end, gap);

        return addComment(solrQuery, query);
    }

    /** Builds a Solr query based on the given {@code query}. */
    public SolrQuery buildQuery(Query<?> query) {
        SolrQuery solrQuery = new SolrQuery();
        StringBuilder queryBuilder = new StringBuilder();

        Predicate predicate = query.getPredicate();
        if (predicate != null) {
            appendPredicate(query, queryBuilder, predicate);
        }

        Query<?> facetedQuery = query.getFacetQuery();
        if (facetedQuery != null) {
            StringBuilder fq = new StringBuilder();
            appendPredicate(facetedQuery, fq, query.getPredicate());
            solrQuery.addFacetQuery(fq.toString());
            solrQuery.setFacetMinCount(1);
            solrQuery.setFacet(true);
        }

        Map<String, Object> facetedFields = query.getFacetedFields();
        if (!facetedFields.isEmpty()) {
            boolean facet = false;
            for (Map.Entry<String, Object> entry : facetedFields.entrySet()) {
                String field = entry.getKey();
                Object value = entry.getValue();
                if (value != null) {
                    Predicate p = new ComparisonPredicate(PredicateParser.EQUALS_ANY_OPERATOR, false, field, ObjectUtils.to(Iterable.class, value));
                    StringBuilder filter = new StringBuilder();
                    appendPredicate(query, filter, p);
                    solrQuery.addFilterQuery(filter.toString());
                } else {
                    Query.MappedKey mappedKey = mapFullyDenormalizedKey(query, field);
                    solrQuery.addFacetField(getSolrField(mappedKey.getInternalType()).facetPrefix + mappedKey.getIndexKey(null));
                    facet = true;
                }
            }

            if (facet) {
                solrQuery.setFacetMinCount(1);
                solrQuery.setFacet(true);
            }
        }

        Map<String, Object> facetedRanges = query.getFacetedRanges();
        if (!facetedRanges.isEmpty()) {
            for (Map.Entry<String, Object> entry : facetedRanges.entrySet()) {
                String field = entry.getKey();
                if (entry.getValue() instanceof Map) {
                    Map<String, Object> facetedRangeMap = (Map) entry.getValue();
                    Query.MappedKey mappedKey = mapFullyDenormalizedKey(query, field);
                    field = getSolrField(mappedKey.getInternalType()).facetPrefix + mappedKey.getIndexKey(null);
                    solrQuery.addNumericRangeFacet(field, (Number) facetedRangeMap.get(Query.RANGE_START), (Number) facetedRangeMap.get(Query.RANGE_END), (Number) facetedRangeMap.get(Query.RANGE_GAP));
                }
            }
        }

        if (!query.isFromAll()) {
            Set<ObjectType> types = query.getConcreteTypes(getEnvironment());

            if (!isAllTypes(types)) {
                if (types.isEmpty()) {
                    if (queryBuilder.length() > 0) {
                        queryBuilder.insert(0, '(');
                        queryBuilder.append(") && ");
                    }
                    queryBuilder.append("-*:*");

                } else {
                    if (queryBuilder.length() > 0) {
                        queryBuilder.insert(0, '(');
                        queryBuilder.append(") && ");
                    }
                    queryBuilder.append("typeId:(");
                    for (UUID typeId : query.getConcreteTypeIds(this)) {
                        queryBuilder.append(Static.escapeValue(typeId));
                        queryBuilder.append(" || ");
                    }
                    queryBuilder.setLength(queryBuilder.length() - 4);
                    queryBuilder.append(')');
                }
            }
        }

        if (queryBuilder.length() < 1) {
            queryBuilder.append("*:*");
        }

        StringBuilder sortBuilder = new StringBuilder();
        StringBuilder boostFunctionBuilder = new StringBuilder();
        for (Sorter sorter : query.getSorters()) {
            String operator = sorter.getOperator();
            boolean isAscending = Sorter.ASCENDING_OPERATOR.equals(operator);

            if (isAscending || Sorter.DESCENDING_OPERATOR.equals(operator)) {
                String queryKey = (String) sorter.getOptions().get(0);
                Query.MappedKey mappedKey = mapFullyDenormalizedKey(query, queryKey);
                String solrField = specialFields.get(mappedKey);

                if (solrField == null) {
                    String internalType = mappedKey.getInternalType();
                    if (internalType != null) {
                        solrField = getSolrField(internalType).sortPrefix + mappedKey.getIndexKey(null);
                    }
                }

                if (solrField == null) {
                    throw new UnsupportedIndexException(this, queryKey);
                }

                solrQuery.addSortField(solrField, isAscending ? SolrQuery.ORDER.asc : SolrQuery.ORDER.desc);
                continue;

            } else if (Sorter.RELEVANT_OPERATOR.equals(operator)) {

                Predicate sortPredicate = null;

                Object predicateObject = sorter.getOptions().get(1);
                if (predicateObject instanceof Predicate) {
                    sortPredicate = (Predicate) predicateObject;

                } else {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> simpleValues = (Map<String, Object>) predicateObject;

                    ObjectType type = ObjectType.getInstance(ObjectUtils.to(UUID.class, simpleValues.get(StateSerializer.TYPE_KEY)));
                    Object object = type.createObject(ObjectUtils.to(UUID.class, simpleValues.get(StateSerializer.ID_KEY)));
                    State state = State.getInstance(object);
                    state.putAll(simpleValues);

                    if (object instanceof Predicate) {
                        sortPredicate = (Predicate) object;
                    }
                }
                double boost = ObjectUtils.to(double.class, sorter.getOptions().get(0));
                if (boost < 0.0) {
                    boost = -boost;
                    sortPredicate = new CompoundPredicate(PredicateParser.NOT_OPERATOR, Arrays.asList(sortPredicate));
                }

                solrQuery.addSortField(SCORE_FIELD, SolrQuery.ORDER.desc);
                sortBuilder.append('(');
                appendPredicate(query, sortBuilder, sortPredicate);
                sortBuilder.append(")^");
                sortBuilder.append(boost);
                sortBuilder.append(" || ");
                continue;

            } else if (Sorter.NEWEST_OPERATOR.equals(operator) || Sorter.OLDEST_OPERATOR.equals(operator)) {
                String queryKey = (String) sorter.getOptions().get(1);
                Query.MappedKey mappedKey = mapFullyDenormalizedKey(query, queryKey);
                String solrField = specialFields.get(mappedKey);

                if (solrField == null) {
                    String internalType = mappedKey.getInternalType();
                    if (internalType != null) {
                        solrField = getSolrField(internalType).sortPrefix + mappedKey.getIndexKey(null);
                    }
                }

                if (solrField == null) {
                    throw new UnsupportedIndexException(this, queryKey);
                }

                double boost = ObjectUtils.to(double.class, sorter.getOptions().get(0));
                if (boost == 0) {
                    boost = 1.0;
                }
                boost = 1 / boost;

                if (Sorter.NEWEST_OPERATOR.equals(operator)) {
                    boostFunctionBuilder.append(String.format("{!boost b=recip(ms(NOW/HOUR,%s),3.16e-11,%s,%s)}", solrField, boost, boost));
                } else {
                    boostFunctionBuilder.append(String.format("{!boost b=linear(ms(NOW/HOUR,%s),3.16e-11,%s)}", solrField, boost));
                }
                continue;

            } else if (schema.get().version >= 9) {
                boolean closest = Sorter.CLOSEST_OPERATOR.equals(operator);

                if (closest || Sorter.FARTHEST_OPERATOR.equals(operator)) {
                    Location location = (Location) sorter.getOptions().get(1);
                    Query.MappedKey mappedKey = mapFullyDenormalizedKey(query, (String) sorter.getOptions().get(0));
                    String internalType = mappedKey.getInternalType();

                    String oldField = solrQuery.get("sfield");
                    String newField = getSolrField(internalType).sortPrefix + mappedKey.getIndexKey(null);
                    if (oldField == null) {
                        solrQuery.set("sfield", newField);
                    } else if (!oldField.equals(newField)) {
                        throw new IllegalArgumentException("Can't query against more than one location at a time!");
                    }

                    StringBuilder geoBuilder = new StringBuilder();
                    geoBuilder.append("geodist(");
                    geoBuilder.append(location.getX());
                    geoBuilder.append(',');
                    geoBuilder.append(location.getY());
                    geoBuilder.append(')');

                    solrQuery.setSortField(geoBuilder.toString(), closest ? SolrQuery.ORDER.asc : SolrQuery.ORDER.desc);
                    continue;
                }
            }

            throw new UnsupportedSorterException(this, sorter);
        }

        if (sortBuilder.length() > 0) {
            queryBuilder.insert(0, "(");
            queryBuilder.append(") && (");
            queryBuilder.append(sortBuilder);
            queryBuilder.append("*:*)");
        }

        String tenant = getTenant();

        if (tenant != null) {
            queryBuilder.insert(0, '(');
            queryBuilder.append(") && ");
            queryBuilder.append(TENANT_FIELD);
            queryBuilder.append(':');
            queryBuilder.append(tenant);
        }

        if (boostFunctionBuilder.length() > 0) {
            queryBuilder.insert(0, boostFunctionBuilder.toString());
        }

        solrQuery.setQuery(queryBuilder.toString());
        return addComment(solrQuery, query);
    }

    /**
     * Transforms and appends the given {@code predicate} to the given
     * {@code queryBuilder}.
     */
    private void appendPredicate(Query<?> query, StringBuilder queryBuilder, Predicate predicate) {

        if (predicate instanceof CompoundPredicate) {
            CompoundPredicate compoundPredicate = (CompoundPredicate) predicate;

            String operator = compoundPredicate.getOperator();
            CompoundOperator solrOperator = compoundOperators.get(operator);

            if (solrOperator != null) {
                List<Predicate> children = compoundPredicate.getChildren();
                if (children.isEmpty()) {
                    queryBuilder.append("*:*");
                } else {
                    solrOperator.appendCompound(this, query, queryBuilder, children);
                }
                return;
            }

        } else if (predicate instanceof ComparisonPredicate) {
            ComparisonPredicate comparisonPredicate = (ComparisonPredicate) predicate;

            String queryKey = comparisonPredicate.getKey();
            Query.MappedKey mappedKey = query.mapDenormalizedKey(getEnvironment(), queryKey);
            String solrField = specialFields.get(mappedKey);

            if (solrField == null) {
                String internalType = mappedKey.getInternalType();
                if (internalType != null) {
                    solrField = getSolrField(internalType).searchPrefix + mappedKey.getIndexKey(null);
                }
            }

            if (solrField == null) {
                throw new UnsupportedIndexException(this, queryKey);
            }

            String operator = comparisonPredicate.getOperator();
            ComparisonOperator solrOperator = comparisonOperators.get(operator);

            if (solrOperator != null) {
                List<Object> values;
                Query<?> valueQuery = mappedKey.getSubQueryWithComparison(comparisonPredicate);

                if (valueQuery == null) {
                    values = comparisonPredicate.resolveValues(this);

                } else {
                    values = new ArrayList<Object>();
                    for (Object item : readPartial(
                            valueQuery, 0, Settings.getOrDefault(int.class, "dari/subQueryResolveLimit", 100))
                            .getItems()) {
                        values.add(State.getInstance(item).getId());
                    }
                }

                solrOperator.appendComparison(queryBuilder, solrField, values);
                return;
            }
        }

        throw new UnsupportedPredicateException(this, predicate);
    }

    private final Map<String, CompoundOperator> compoundOperators; {
        Map<String, CompoundOperator> m = new HashMap<String, CompoundOperator>();

        m.put(PredicateParser.AND_OPERATOR, new StandardCompoundOperator("&&"));
        m.put(PredicateParser.OR_OPERATOR, new StandardCompoundOperator("||"));

        m.put(PredicateParser.NOT_OPERATOR, new CompoundOperator() {
            @Override
            protected void appendCompound(
                    SolrDatabase database,
                    Query<?> query,
                    StringBuilder queryBuilder,
                    List<Predicate> children) {

                queryBuilder.append("(*:*");
                for (Predicate child : children) {
                    queryBuilder.append(" && -(");
                    database.appendPredicate(query, queryBuilder, child);
                    queryBuilder.append(')');
                }
                queryBuilder.append(')');
            }
        });

        compoundOperators = m;
    }

    private abstract class CompoundOperator {

        protected abstract void appendCompound(
                SolrDatabase database,
                Query<?> query,
                StringBuilder queryBuilder,
                List<Predicate> children);
    }

    private class StandardCompoundOperator extends CompoundOperator {

        private final String join;

        public StandardCompoundOperator(String join) {
            this.join = join;
        }

        protected void appendCompound(
                SolrDatabase database,
                Query<?> query,
                StringBuilder queryBuilder,
                List<Predicate> children) {

            for (Predicate child : children) {
                queryBuilder.append('(');
                database.appendPredicate(query, queryBuilder, child);
                queryBuilder.append(") ");
                queryBuilder.append(join);
                queryBuilder.append(' ');
            }

            queryBuilder.setLength(queryBuilder.length() - join.length() - 2);
        }
    }

    private final Map<Query.MappedKey, String> specialFields; {
        Map<Query.MappedKey, String> m = new HashMap<Query.MappedKey, String>();
        m.put(Query.MappedKey.ID, ID_FIELD);
        m.put(Query.MappedKey.TYPE, TYPE_ID_FIELD);
        m.put(Query.MappedKey.ANY, ALL_FIELD);
        specialFields = m;
    }

    private final Map<String, ComparisonOperator> comparisonOperators; {
        Map<String, ComparisonOperator> m = new HashMap<String, ComparisonOperator>();

        m.put(PredicateParser.EQUALS_ANY_OPERATOR, new ExactMatchOperator(false, true));
        m.put(PredicateParser.NOT_EQUALS_ALL_OPERATOR, new ExactMatchOperator(true, false));
        m.put(PredicateParser.MATCHES_ANY_OPERATOR, new FullTextMatchOperator(false, true, false));
        m.put(PredicateParser.MATCHES_ALL_OPERATOR, new FullTextMatchOperator(false, false, false));
        m.put(PredicateParser.MATCHES_EXACT_ANY_OPERATOR, new FullTextMatchOperator(false, true, true));
        m.put(PredicateParser.MATCHES_EXACT_ALL_OPERATOR, new FullTextMatchOperator(false, false, true));

        m.put(PredicateParser.STARTS_WITH_OPERATOR, new MatchOperator(false, true) {
            @Override
            protected void addNonMissingValue(StringBuilder comparisonBuilder, String solrField, Object value) {
                comparisonBuilder.append(escapeValue(value));
                comparisonBuilder.append('*');
            }
        });

        m.put(PredicateParser.LESS_THAN_OPERATOR, new RangeOperator("{* TO ", "}"));
        m.put(PredicateParser.LESS_THAN_OR_EQUALS_OPERATOR, new RangeOperator("[* TO ", "]"));
        m.put(PredicateParser.GREATER_THAN_OPERATOR, new RangeOperator("{", " TO *}"));
        m.put(PredicateParser.GREATER_THAN_OR_EQUALS_OPERATOR, new RangeOperator("[", " TO *]"));

        comparisonOperators = m;
    }

    private abstract class ComparisonOperator {

        private final boolean isNegate;
        private final boolean isAny;
        private final String join;

        public ComparisonOperator(boolean isNegate, boolean isAny, String join) {
            this.isNegate = isNegate;
            this.isAny = isAny;
            this.join = join;
        }

        public void appendComparison(
                StringBuilder queryBuilder,
                String solrField,
                List<Object> values) {

            StringBuilder comparisonBuilder = new StringBuilder();
            for (Object value : values) {
                if (ObjectUtils.isBlank(value)) {
                    comparisonBuilder.append("(*:* && -*:*)");

                } else if (value instanceof QueryPhrase) {
                    QueryPhrase phrase = (QueryPhrase) value;

                    comparisonBuilder.append('"');
                    comparisonBuilder.append(escapeValue(phrase.getPhrase()));
                    comparisonBuilder.append('"');

                    int proximity = phrase.getProximity();

                    if (proximity > 0) {
                        comparisonBuilder.append('~');
                        comparisonBuilder.append(proximity);
                    }

                    double weight = phrase.getWeight();

                    if (weight > 0.0) {
                        comparisonBuilder.append('^');
                        comparisonBuilder.append(weight);
                    }

                } else {
                    addValue(comparisonBuilder, solrField, value);
                }
                comparisonBuilder.append(join);
            }

            if (comparisonBuilder.length() > 0) {
                comparisonBuilder.setLength(comparisonBuilder.length() - 4);

                if (isNegate) {
                    queryBuilder.append("(*:* && -");
                }

                // field:(...)
                queryBuilder.append(changeSolrField(solrField));
                queryBuilder.append(":(");
                queryBuilder.append(comparisonBuilder);
                queryBuilder.append(')');

                if (isNegate) {
                    queryBuilder.append(')');
                }

            } else if (isAny) {
                queryBuilder.append("(*:* && -*:*)");

            } else {
                queryBuilder.append("*:*");
            }
        }

        protected abstract void addValue(StringBuilder comparisonBuilder, String solrField, Object value);

        protected String changeSolrField(String solrField) {
            return solrField.startsWith("_t_")
                    ? (schema.get().version >= 8 ? "_sl_" : "_s_") + solrField.substring(3)
                    : solrField;
        }

        protected String escapeValue(Object value) {
            if (value != null && schema.get().version > 8) {
                return Static.escapeValue(value.toString().trim().toLowerCase(Locale.ENGLISH));

            } else {
                return Static.escapeValue(value);
            }
        }
    }

    private abstract class MatchOperator extends ComparisonOperator {

        public MatchOperator(boolean isNegate, boolean isAny) {
            super(isNegate, isAny, isNegate ? (isAny ? " && " : " || ") : (isAny ? " || " : " && "));
        }

        @Override
        protected void addValue(StringBuilder comparisonBuilder, String solrField, Object value) {
            if (value == Query.MISSING_VALUE) {
                if (solrField.startsWith("_g_")) {
                    comparisonBuilder.append("(*:* && -[-90,-180 TO 90,180])");
                } else {
                    comparisonBuilder.append("(*:* && -[* TO *])");
                }
            } else {
                addNonMissingValue(comparisonBuilder, solrField, value);
            }
        }

        protected abstract void addNonMissingValue(StringBuilder comparisonBuilder, String solrField, Object value);
    }

    private class ExactMatchOperator extends MatchOperator {

        public ExactMatchOperator(boolean isNegate, boolean isAny) {
            super(isNegate, isAny);
        }

        @Override
        protected void addNonMissingValue(StringBuilder comparisonBuilder, String solrField, Object value) {
            if (value instanceof Region) {
                List<Location> locations = ((Region) value).getLocations();
                double minX = Double.POSITIVE_INFINITY;
                double minY = Double.POSITIVE_INFINITY;
                double maxX = Double.NEGATIVE_INFINITY;
                double maxY = Double.NEGATIVE_INFINITY;

                for (Location location : locations) {
                    double x = location.getX();
                    double y = location.getY();

                    if (minX > x) {
                        minX = x;
                    } else if (maxX < x) {
                        maxX = x;
                    }

                    if (minY > y) {
                        minY = y;
                    } else if (maxY < y) {
                        maxY = y;
                    }
                }

                comparisonBuilder.append('[');
                comparisonBuilder.append(minX);
                comparisonBuilder.append(',');
                comparisonBuilder.append(minY);
                comparisonBuilder.append(" TO ");
                comparisonBuilder.append(maxX);
                comparisonBuilder.append(',');
                comparisonBuilder.append(maxY);
                comparisonBuilder.append(']');

            } else {
                comparisonBuilder.append(escapeValue(value));
            }
        }
    }

    private static final char[] UUID_WORD_CHARS = new char[] {
            'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l',
            'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x',
            'y', 'z' };

    private static String uuidToWord(UUID uuid) {
        if (uuid == null) {
            return null;
        }

        byte[] bytes = UuidUtils.toBytes(uuid);
        int bytesLength = bytes.length;
        int wordLast = bytesLength * 2;
        byte currentByte;
        char[] word = new char[wordLast + 1];

        for (int byteIndex = 0, hexIndex = 0;
                byteIndex < bytesLength;
                ++ byteIndex, ++ hexIndex) {

            currentByte = bytes[byteIndex];
            word[hexIndex] = UUID_WORD_CHARS[(currentByte & 0xf0) >> 4];
            ++ hexIndex;
            word[hexIndex] = UUID_WORD_CHARS[(currentByte & 0x0f)];
        }

        word[wordLast] = 'z';
        return new String(word);
    }

    private class FullTextMatchOperator extends MatchOperator {

        private final boolean isExact;

        public FullTextMatchOperator(boolean isNegate, boolean isAny, boolean isExact) {
            super(isNegate, isAny);

            this.isExact = isExact;
        }

        @Override
        protected void addNonMissingValue(StringBuilder comparisonBuilder, String solrField, Object value) {
            if (ALL_FIELD.equals(solrField)) {
                if ("*".equals(value)) {
                    comparisonBuilder.append("*:*");
                    return;

                } else {
                    UUID valueUuid = ObjectUtils.to(UUID.class, value);
                    if (valueUuid != null) {
                        comparisonBuilder.append(uuidToWord(valueUuid));
                        return;
                    }
                }
            }

            String valueString = value.toString();
            int valueStringLength = valueString.length();

            if (valueStringLength == 0) {
                comparisonBuilder.append("(*:* && -*:*)");

            } else {
                String escapedValue = escapeValue(valueString).toLowerCase(Locale.ENGLISH);

                if (valueString.length() == 1) {
                    comparisonBuilder.append(escapedValue);

                } else {
                    comparisonBuilder.append('(');
                    comparisonBuilder.append(escapedValue);

                    if (!isExact) {
                        comparisonBuilder.append(" || ");
                        comparisonBuilder.append(escapedValue);
                        comparisonBuilder.append("*");
                    }

                    comparisonBuilder.append(")");
                }
            }
        }

        @Override
        protected String changeSolrField(String solrField) {
            return solrField;
        }
    }

    private class RangeOperator extends ComparisonOperator {

        private final String prefix;
        private final String suffix;

        public RangeOperator(String prefix, String suffix) {
            super(false, true, " || ");
            this.prefix = prefix;
            this.suffix = suffix;
        }

        @Override
        protected void addValue(StringBuilder comparisonBuilder, String solrField, Object value) {
            String valueString = value.toString();
            if (schema.get().version >= 8) {
                valueString = valueString.trim().toLowerCase(Locale.ENGLISH);
            }

            comparisonBuilder.append(prefix);
            comparisonBuilder.append('"');
            comparisonBuilder.append(valueString.replaceAll("([\\\\\"])", "\\\\$1"));
            comparisonBuilder.append('"');
            comparisonBuilder.append(suffix);
        }
    }

    public SolrQuery buildSimilarQuery(Object object) {
        State state = State.getInstance(object);
        SolrQuery solrQuery = new SolrQuery();

        solrQuery.setQueryType("/mlt");

        solrQuery.set(MoreLikeThisParams.SIMILARITY_FIELDS, ALL_FIELD);
        solrQuery.set(MoreLikeThisParams.MIN_WORD_LEN, 2);
        solrQuery.set(MoreLikeThisParams.BOOST, true);

        StringBuilder streamBody = new StringBuilder();

        addToStreamBody(streamBody, state.getSimpleValues());

        solrQuery.set(CommonParams.STREAM_BODY, streamBody.toString());
        solrQuery.add(CommonParams.FQ, "-id:" + state.getId());

        return solrQuery;
    }

    private void addToStreamBody(StringBuilder streamBody, Object value) {
        if (value == null
                || value instanceof Boolean) {
            // No need to search against null or a boolean.

        } else if (value instanceof Iterable) {
            for (Object item : (Iterable<?>) value) {
                addToStreamBody(streamBody, item);
            }

        } else if (value instanceof Map) {
            for (Object item : ((Map<?, ?>) value).values()) {
                addToStreamBody(streamBody, item);
            }

        } else {
            streamBody.append(value);
            streamBody.append(' ');
        }
    }

    /**
     * Queries the underlying Solr server with the given {@code query}
     * and options from the given {@code query}.
     */
    public QueryResponse queryWithOptions(SolrQuery solrQuery, Query<?> query) {
        if (query != null && query.isReferenceOnly()) {
            solrQuery.setFields(ID_FIELD, TYPE_ID_FIELD);
        } else {
            solrQuery.setFields("*", SCORE_FIELD);
        }

        Stats.Timer timer = STATS.startTimer();
        Profiler.Static.startThreadEvent(QUERY_PROFILER_EVENT);

        try {
            return openQueryConnection(query).query(solrQuery, SolrRequest.METHOD.POST);

        } catch (SolrServerException ex) {
            throw new DatabaseException(this, String.format(
                    "Unable to read from the Solr server using [%s]!",
                    solrQuery), ex);

        } finally {
            double duration = timer.stop(QUERY_STATS_OPERATION);
            Profiler.Static.stopThreadEvent(solrQuery);

            LOGGER.debug(
                    "Read from the Solr server using [{}] in [{}]ms",
                    solrQuery, duration);
        }
    }

    /**
     * Queries the underlying Solr server with the given
     * {@code solrQuery}.
     */
    public QueryResponse query(SolrQuery solrQuery) {
        return queryWithOptions(solrQuery, null);
    }

    /**
     * Queries the underlying Solr server for a partial list of objects
     * that match the given {@code solrQuery} with the options from
     * the given {@code query}.
     */
    public <T> SolrPaginatedResult<T> queryPartialWithOptions(SolrQuery solrQuery, Query<T> query) {
        QueryResponse response = queryWithOptions(solrQuery, query);
        SolrDocumentList documents = response.getResults();

        long count = 0;
        List<T> objects = new ArrayList<T>();
        if (documents != null) {
            count = documents.getNumFound();
            for (SolrDocument document : documents) {
                objects.add(createSavedObjectWithDocument(document, documents, query));
            }
        }

        return new SolrPaginatedResult<T>(
                solrQuery.getStart(),
                solrQuery.getRows(),
                count,
                objects,
                response.getLimitingFacets(),
                response.getFacetRanges(),
                query != null ? query.getClass() : null,
                Settings.isDebug() ? solrQuery : null);
    }

    /**
     * Queries the underlying Solr server for a partial list of objects
     * that match the given {@code solrQuery}.
     */
    public SolrPaginatedResult<Object> queryPartial(SolrQuery solrQuery) {
        return queryPartialWithOptions(solrQuery, null);
    }

    /**
     * Creates a previously saved object using the given {@code document}.
     */
    private <T> T createSavedObjectWithDocument(
            SolrDocument document,
            SolrDocumentList documents,
            Query<T> query) {

        T object = createSavedObject(document.get(TYPE_ID_FIELD), document.get(ID_FIELD), query);
        State objectState = State.getInstance(object);

        if (!objectState.isReferenceOnly()) {
            String data = (String) document.get(DATA_FIELD);

            if (ObjectUtils.isBlank(data)) {
                Object original = objectState.getDatabase().readFirst(Query.from(Object.class).where("_id = ?", objectState.getId()));
                if (original != null) {
                    objectState.setValues(State.getInstance(original).getSimpleValues());
                }

            } else {
                @SuppressWarnings("unchecked")
                Map<String, Object> values = (Map<String, Object>) ObjectUtils.fromJson(data);
                objectState.setValues(values);
            }
        }

        Map<String, Object> extras = objectState.getExtras();
        Object score = document.get(SCORE_FIELD);
        extras.put(SCORE_EXTRA, score);

        Float maxScore = documents.getMaxScore();
        if (maxScore != null && score instanceof Number) {
            extras.put(NORMALIZED_SCORE_EXTRA, ((Number) score).floatValue() / maxScore);
        }

        return swapObjectType(query, object);
    }

    /** Commits all pending writes in the underlying Solr server. */
    public void commit() {
        doCommit(openConnection());
    }

    private void doCommit(SolrServer server) {
        if (isAutoCommit()) {
            return;
        }

        Throwable error = null;

        try {
            Stats.Timer timer = STATS.startTimer();
            Profiler.Static.startThreadEvent(COMMIT_PROFILER_EVENT);

            try {
                server.commit();

            } finally {
                double duration = timer.stop(COMMIT_STATS_OPERATION);
                Profiler.Static.stopThreadEvent();

                LOGGER.debug("Solr commit time: [{}]ms", duration);
            }

        } catch (IOException e) {
            error = e;
        } catch (SolrServerException e) {
            error = e;
        }

        if (error != null) {
            throw new DatabaseException(this, "Can't commit to Solr!", error);
        }
    }

    // --- AbstractDatabase support ---

    @Override
    protected void doInitialize(String settingsKey, Map<String, Object> settings) {
        String url = ObjectUtils.to(String.class, settings.get(SERVER_URL_SUB_SETTING));
        if (ObjectUtils.isBlank(url)) {
            throw new SettingsException(
                    settingsKey + "/" + SERVER_URL_SUB_SETTING,
                    "No Solr server URL!");
        }

        try {
            setServer(new HttpSolrServer(url));

            String readUrl = ObjectUtils.to(String.class, settings.get(READ_SERVER_URL_SUB_SETTING));
            if (!ObjectUtils.isBlank(readUrl)) {
                setReadServer(new HttpSolrServer(readUrl));
            }

        } catch (Exception ex) {
            throw new SettingsException(
                    settingsKey + "/" + SERVER_URL_SUB_SETTING,
                    String.format("[%s] is not a valid URL!", url));
        }

        setTenant(ObjectUtils.to(String.class, settings.get(TENANT_SUB_SETTING)));
        setCommitWithin(ObjectUtils.to(Double.class, settings.get(COMMIT_WITHIN_SUB_SETTING)));
        setVersion(ObjectUtils.to(String.class, settings.get(VERSION_SUB_SETTING)));

        Boolean saveData = ObjectUtils.to(Boolean.class, settings.get(SAVE_DATA_SUB_SETTING));
        if (saveData != null) {
            setSaveData(saveData);
        }

        Boolean autoCommit = ObjectUtils.to(Boolean.class, settings.get(AUTO_COMMIT_SUB_SETTING));

        if (autoCommit != null) {
            setAutoCommit(autoCommit);
        }
    }

    @Override
    public SolrServer openConnection() {
        return getServer();
    }

    @Override
    protected SolrServer doOpenReadConnection() {
        SolrServer server = getReadServer();
        return server != null ? server : getServer();
    }

    @Override
    public void closeConnection(SolrServer server) {
    }

    @Override
    public <T> List<T> readAll(Query<T> query) {

        // Solr sometimes throws an OutOfMemoryError when the limit
        // is too large, so read all using 2 separate queries.
        SolrQuery solrQuery = buildQuery(query);
        solrQuery.setStart(0);
        solrQuery.setRows(INITIAL_FETCH_SIZE);

        PaginatedResult<T> result = queryPartialWithOptions(solrQuery, query);
        int count = (int) result.getCount();
        List<T> all = new ArrayList<T>(count);
        all.addAll(result.getItems());

        if (count > INITIAL_FETCH_SIZE) {
            solrQuery.setStart(INITIAL_FETCH_SIZE);
            solrQuery.setRows(count - INITIAL_FETCH_SIZE);
            all.addAll(queryPartialWithOptions(solrQuery, query).getItems());
        }

        return all;
    }

    @Override
    public long readCount(Query<?> query) {
        SolrQuery solrQuery = buildQuery(query);
        solrQuery.setStart(0);
        solrQuery.setRows(0);
        SolrDocumentList documents = queryWithOptions(solrQuery, query).getResults();
        return documents != null ? documents.getNumFound() : 0L;
    }

    @Override
    public <T> T readFirst(Query<T> query) {
        SolrQuery solrQuery = buildQuery(query);
        solrQuery.setStart(0);
        solrQuery.setRows(1);
        SolrDocumentList documents = queryWithOptions(solrQuery, query).getResults();
        if (documents != null) {
            for (SolrDocument document : documents) {
                return createSavedObjectWithDocument(document, documents, query);
            }
        }
        return null;
    }

    @Override
    public Date readLastUpdate(Query<?> query) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> PaginatedResult<T> readPartial(Query<T> query, long offset, int limit) {
        SolrQuery solrQuery = buildQuery(query);
        solrQuery.setStart((int) offset);
        solrQuery.setRows(limit);
        return queryPartialWithOptions(solrQuery, query);
    }

    @Override
    public <T> PaginatedResult<Grouping<T>> readPartialGrouped(Query<T> query, long offset, int limit, String... fields) {
        if (fields == null || fields.length != 1) {
            return super.readPartialGrouped(query, offset, limit, fields);
        }

        List<Grouping<T>> groupings = new ArrayList<Grouping<T>>();

        Matcher groupingMatcher = Query.RANGE_PATTERN.matcher(fields[0]);
        if (groupingMatcher.find()) {
            String field = groupingMatcher.group(1);
            Double start = ObjectUtils.to(Double.class, groupingMatcher.group(2).trim());
            Double end   = ObjectUtils.to(Double.class, groupingMatcher.group(3).trim());
            Double gap   = ObjectUtils.to(Double.class, groupingMatcher.group(4).trim());

            SolrQuery solrQuery = buildQueryNumericRangeFacet(query, field, start, end, gap);
            solrQuery.setStart(0);
            solrQuery.setRows(0);
            QueryResponse response = queryWithOptions(solrQuery, query);

            for (RangeFacet rangeFacet : response.getFacetRanges()) {
                List<RangeFacet.Count> counts = rangeFacet.getCounts();

                if (counts != null) {
                    for (RangeFacet.Count count : counts) {
                        if (count.getCount() > 0) {
                            Object key = count.getValue();
                            groupings.add(new SolrGrouping<T>(Arrays.asList(key), query, fields, count.getCount()));
                        }
                    }
                }
            }

        } else {
            SolrQuery solrQuery = buildQueryFacetByField(query, fields[0]);
            solrQuery.setStart(0);
            solrQuery.setRows(0);
            solrQuery.setFacetMinCount(1);

            QueryResponse response = queryWithOptions(solrQuery, query);

            for (FacetField facetField : response.getFacetFields()) {
                List<FacetField.Count> values = facetField.getValues();
                if (values == null) {
                    continue;
                }

                for (FacetField.Count value : facetField.getValues()) {
                    Object key = value.getName();
                    ObjectField field = mapFullyDenormalizedKey(query, fields[0]).getField();
                    if (field != null) {
                        key = StateValueUtils.toJavaValue(query.getDatabase(), null, field, field.getInternalItemType(), key);
                    }
                    groupings.add(new SolrGrouping<T>(Arrays.asList(key), query, fields, value.getCount()));
                }
            }
        }

        return new PaginatedResult<Grouping<T>>(offset, limit, groupings);
    }

    /** Solr-specific implementation of {@link Grouping}. */
    private static class SolrGrouping<T> extends AbstractGrouping<T> {

        private final long count;

        public SolrGrouping(List<Object> keys, Query<T> query, String[] fields, long count) {
            super(keys, query, fields);
            this.count = count;
        }

        // --- AbstractGrouping support ---

        @Override
        protected Aggregate createAggregate(String field) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getCount() {
            return count;
        }
    }

    @Override
    public void deleteByQuery(Query<?> query) {
        Throwable error = null;

        try {
            SolrServer server = openConnection();
            server.deleteByQuery(buildQuery(query).getQuery());
            doCommit(server);

        } catch (IOException e) {
            error = e;
        } catch (SolrServerException e) {
            error = e;
        }

        if (error != null) {
            throw new DatabaseException(this, String.format(
                    "Can't delete documents matching [%s] from Solr!",
                    query), error);
        }
    }

    @Override
    protected void commitTransaction(SolrServer server, boolean isImmediate) {
        if (isImmediate && getEffectiveCommitWithin() <= 0.0) {
            doCommit(openConnection());
        }
    }

    private void processUpdate(
            SolrServer server,
            UpdateRequest update,
            boolean isImmediate)
            throws IOException, SolrServerException {

        if (isImmediate) {
            double commitWithin = getEffectiveCommitWithin();
            if (commitWithin > 0.0) {
                update.setCommitWithin((int) (commitWithin * 1000));
            }
        }

        update.process(server);
    }

    @Override
    protected void doSaves(SolrServer server, boolean isImmediate, List<State> states) {
        Set<String> databaseGroups = getGroups();
        List<SolrInputDocument> documents = new ArrayList<SolrInputDocument>();
        String tenant = getTenant();

        for (State state : states) {
            ObjectType type = state.getType();

            if (type != null) {
                boolean savable = false;

                for (String typeGroup : type.getGroups()) {
                    if (databaseGroups.contains(typeGroup)) {
                        savable = true;
                        break;
                    }
                }

                if (!savable) {
                    continue;
                }
            } else {
                // skip processing States with no typeId
                continue;
            }

            Map<String, Object> stateValues = state.getSimpleValues();
            SolrInputDocument document = new SolrInputDocument();
            StringBuilder allBuilder = new StringBuilder();

            documents.add(document);
            document.setField(ID_FIELD, state.getId());
            document.setField(TYPE_ID_FIELD, state.getVisibilityAwareTypeId());

            if (isSaveData()) {
                document.setField(DATA_FIELD, ObjectUtils.toJson(stateValues));
            }

            if (schema.get().version >= 10) {
                Set<String> typeAheadFields = type.as(TypeModification.class).getTypeAheadFields();
                Map<String, List<String>> typeAheadFieldsMap = type.as(TypeModification.class).getTypeAheadFieldsMap();

                if (!typeAheadFields.isEmpty()) {
                    for (String typeAheadField : typeAheadFields) {
                        String value = ObjectUtils.to(String.class, state.getByPath(typeAheadField));

                        // Hack for a client.
                        if (!ObjectUtils.isBlank(value)) {
                            value = value.replaceAll("\\{", "").replaceAll("\\}", "");
                            document.setField(SUGGESTION_FIELD, value);
                        }
                    }
                }

                if (!typeAheadFieldsMap.isEmpty()) {
                    for (Map.Entry<String, List<String>> entry : typeAheadFieldsMap.entrySet()) {
                        String typeAheadField = entry.getKey();
                        List<String> targetFields = entry.getValue();
                        String value = ObjectUtils.to(String.class, state.getByPath(typeAheadField));

                        if (!ObjectUtils.isBlank(targetFields)) {
                            for (String targetField : targetFields) {
                                if (!ObjectUtils.isBlank(value)) {
                                    value = value.replaceAll("\\{", "").replaceAll("\\}", "");
                                    document.setField("_e_" + targetField, value);
                                }
                            }
                        }
                    }
                }
            }

            for (Map.Entry<String, Object> entry : stateValues.entrySet()) {
                String fieldName = entry.getKey();

                ObjectField field = state.getField(fieldName);
                if (field == null) {
                    continue;
                }

                String uniqueName = field.getUniqueName();
                addDocumentValues(
                        document,
                        allBuilder,
                        true,
                        field,
                        uniqueName,
                        entry.getValue());
            }

            List<ObjectMethod> methods = new ArrayList<>(state.getType().getMethods());
            methods.addAll(getEnvironment().getMethods());
            for (ObjectMethod method : methods) {
                addDocumentValues(
                        document,
                        allBuilder,
                        true,
                        method,
                        method.getUniqueName(),
                        Static.getStateMethodValue(state, method)
                        );
            }

            document.setField(ALL_FIELD, allBuilder.toString());

            SolrField labelField = schema.get().getField(ObjectField.TEXT_TYPE);
            String label = state.getLabel().trim().toLowerCase(Locale.ENGLISH);

            for (String prefix : labelField.addPrefixes) {
                document.addField(prefix + Query.LABEL_KEY, label);
            }

            for (String prefix : labelField.setPrefixes) {
                document.setField(prefix + Query.LABEL_KEY, label);
            }

            if (tenant != null) {
                document.setField(TENANT_FIELD, tenant);
            }
        }

        int documentsSize = documents.size();
        if (documentsSize == 0) {
            return;
        }

        Throwable error = null;

        try {
            Stats.Timer timer = STATS.startTimer();
            Profiler.Static.startThreadEvent(ADD_PROFILER_EVENT, documentsSize);

            try {
                UpdateRequest update = new UpdateRequest();
                update.add(documents);
                processUpdate(server, update, isImmediate);

            } finally {
                double duration = timer.stop(ADD_STATS_OPERATION);
                Profiler.Static.stopThreadEvent();

                LOGGER.debug("Solr add: [{}], Time: [{}]ms", documentsSize, duration);
            }

        } catch (IOException e) {
            error = e;
        } catch (SolrServerException e) {
            error = e;
        }

        if (error != null) {
            throw new DatabaseException(this, String.format(
                    "Can't add [%s] documents to Solr!",
                    documentsSize), error);
        }
    }

    // Pass through distinct set of States to doSaves.
    @Override
    protected void doWriteRecalculations(SolrServer server, boolean isImmediate, Map<ObjectIndex, List<State>> recalculations) throws Exception {
        if (recalculations != null) {
            Set<State> states = new HashSet<State>();
            for (Map.Entry<ObjectIndex, List<State>> entry : recalculations.entrySet()) {
                states.addAll(entry.getValue());
            }
            doSaves(server, isImmediate, new ArrayList<State>(states));
        }
    }

    // Pass through to doWriteRecalculations.
    @Override
    protected void doRecalculations(SolrServer server, boolean isImmediate, ObjectIndex index, List<State> states) throws Exception {
        Map<ObjectIndex, List<State>> recalculations = new HashMap<ObjectIndex, List<State>>();
        recalculations.put(index, states);
        doWriteRecalculations(server, isImmediate, recalculations);
    }

    // Adds all items within the given {@code value} to the given
    // {@code document} at the given {@code name}.
    private void addDocumentValues(
            SolrInputDocument document,
            StringBuilder allBuilder,
            boolean includeInAny,
            ObjectField field,
            String name,
            Object value) {

        if (value == null) {
            return;
        }

        if (value instanceof Iterable) {
            for (Object item : (Iterable<?>) value) {
                addDocumentValues(document, allBuilder, includeInAny, field, name, item);
            }
            return;
        }

        if (includeInAny) {
            includeInAny = field == null
                    || !field.as(FieldData.class).isExcludeFromAny();
        }

        if (value instanceof Recordable) {
            value = ((Recordable) value).getState().getSimpleValues();
        }

        if (value instanceof Map) {
            Map<?, ?> valueMap = (Map<?, ?>) value;

            if (schema.get().version >= 9 && field.getInternalItemType().equals(ObjectField.LOCATION_TYPE)) {
                if (valueMap.containsKey("x") && valueMap.containsKey("y")) {
                    value = valueMap.get("x") + "," + valueMap.get("y");
                } else {
                    return;
                }

            } else {
                UUID valueTypeId = ObjectUtils.to(UUID.class, valueMap.get(StateSerializer.TYPE_KEY));

                if (valueTypeId == null) {
                    if (field != null && ObjectField.RECORD_TYPE.equals(field.getInternalItemType())) {
                        for (Object item : valueMap.values()) {
                            if (item instanceof Map) {
                                addDocumentValues(document, allBuilder, includeInAny, field, name, item);
                            }
                        }

                    } else {
                        for (Object item : valueMap.values()) {
                            addDocumentValues(document, allBuilder, includeInAny, field, name, item);
                        }
                    }
                    return;

                } else {
                    UUID valueId = ObjectUtils.to(UUID.class, valueMap.get(StateSerializer.REFERENCE_KEY));

                    if (valueId == null) {
                        if (includeInAny) {
                            allBuilder.append(valueTypeId).append(' ');
                        }

                        ObjectType valueType = getEnvironment().getTypeById(valueTypeId);

                        if (valueType != null) {
                            State valueState = null;
                            if (!valueType.getMethods().isEmpty()) {
                                valueState = new State();
                                valueState.setType(valueType);
                                valueState.setId(ObjectUtils.to(UUID.class, valueMap.get(StateSerializer.ID_KEY)));
                            }
                            for (Map.Entry<?, ?> entry : valueMap.entrySet()) {
                                String subName = entry.getKey().toString();
                                ObjectField subField = valueType.getField(subName);

                                if (subField != null) {
                                    addDocumentValues(
                                            document,
                                            allBuilder,
                                            includeInAny,
                                            subField,
                                            name + "/" + subName,
                                            entry.getValue());
                                }
                                if (valueState != null) {
                                    valueState.putByPath(subName, entry.getValue());
                                }
                            }
                            if (valueState != null) {
                                for (ObjectMethod method : valueType.getMethods()) {
                                    addDocumentValues(
                                            document,
                                            allBuilder,
                                            includeInAny,
                                            method,
                                            name + "/" + method.getInternalName(),
                                            Static.getStateMethodValue(valueState, method)
                                    );
                                }
                            }
                        }
                        return;

                    } else {
                        value = valueId;
                        Set<ObjectField> denormFields = field.getEffectiveDenormalizedFields(getEnvironment().getTypeById(valueTypeId));

                        if (denormFields != null) {
                            State valueState = State.getInstance(Query.from(Object.class).where("_id = ?", valueId).first());
                            if (valueState != null) {
                                Map<String, Object> valueValues = valueState.getSimpleValues();
                                for (ObjectField denormField : denormFields) {
                                    String denormFieldName = denormField.getInternalName();
                                    addDocumentValues(
                                            document,
                                            allBuilder,
                                            includeInAny,
                                            denormField,
                                            name + "/" + denormFieldName,
                                            valueValues.get(denormFieldName));
                                }
                            }
                        }
                    }
                }
            }
        }

        String trimmed = value.toString().trim();
        Matcher uuidMatcher = UUID_PATTERN.matcher(trimmed);
        int uuidLast = 0;

        while (uuidMatcher.find()) {
            if (includeInAny) {
                allBuilder.append(trimmed.substring(uuidLast, uuidMatcher.start()));
            }

            uuidLast = uuidMatcher.end();
            String word = uuidToWord(ObjectUtils.to(UUID.class, uuidMatcher.group(0)));

            if (includeInAny && word != null) {
                allBuilder.append(word);
            }
        }

        if (includeInAny) {
            allBuilder.append(trimmed.substring(uuidLast));
            allBuilder.append(' ');
        }

        if (value instanceof String && schema.get().version >= 8) {
            value = ((String) value).trim().toLowerCase(Locale.ENGLISH);
        }

        Object truncatedValue = value;
        if (value instanceof String) {
            if (((String) value).length() > MAX_BINARY_FIELD_LENGTH) {
                truncatedValue = ((String) value).substring(0, MAX_BINARY_FIELD_LENGTH);
            }
        }

        SolrField solrField = getSolrField(field.getInternalItemType());
        for (String prefix : solrField.addPrefixes) {
            document.addField(prefix + name, TRUNCATE_FIELD_PREFIXES.contains(prefix) ? truncatedValue : value);
        }
        for (String prefix : solrField.setPrefixes) {
            document.setField(prefix + name, TRUNCATE_FIELD_PREFIXES.contains(prefix) ? truncatedValue : value);
        }
    }

    @Override
    protected void doDeletes(SolrServer server, boolean isImmediate, List<State> states) {
        List<String> idStrings = new ArrayList<String>();
        for (State state : states) {
            idStrings.add(state.getId().toString());
        }

        int statesSize = states.size();
        Throwable error = null;

        try {
            Stats.Timer timer = STATS.startTimer();
            Profiler.Static.startThreadEvent(DELETE_PROFILER_EVENT, statesSize);

            try {
                UpdateRequest update = new UpdateRequest();
                update.deleteById(idStrings);
                processUpdate(openConnection(), update, isImmediate);

            } finally {
                double duration = timer.stop(DELETE_STATS_OPERATION);
                Profiler.Static.stopThreadEvent();

                LOGGER.debug("Solr delete: [{}], Time: [{}]ms", statesSize, duration);
            }

        } catch (IOException e) {
            error = e;
        } catch (SolrServerException e) {
            error = e;
        }

        if (error != null) {
            throw new DatabaseException(this, String.format(
                    "Can't delete [%s] documents from Solr!",
                    statesSize), error);
        }
    }

    /** {@link SolrDatabase} utility methods. */
    public static final class Static {

        private static final Pattern ESCAPE_PATTERN = Pattern.compile("([-+&|!(){}\\[\\]^\"~*?:\\\\\\s/])");

        /**
         * Escapes the given {@code value} so that it's safe to use
         * in a Solr query.
         *
         * @param value If {@code null}, returns {@code null}.
         */
        public static final String escapeValue(Object value) {
            return value != null ? ESCAPE_PATTERN.matcher(value.toString()).replaceAll("\\\\$1") : null;
        }

        /**
         * Returns the Solr search result score associated with the given
         * {@code object}.
         *
         * @return May be {@code null} if the score isn't available.
         */
        public static Float getScore(Object object) {
            return (Float) State.getInstance(object).getExtra(SCORE_EXTRA);
        }

        /**
         * Returns the normalized Solr search result score, in a scale of
         * {@code 0.0} to {@code 1.0}, associated with the given
         * {@code object}.
         *
         * @return May be {@code null} if the score isn't available.
         */
        public static Float getNormalizedScore(Object object) {
            return (Float) State.getInstance(object).getExtra(NORMALIZED_SCORE_EXTRA);
        }

        /**
         * Execute an ObjectMethod on the given State and return the result as a value or reference.
         */
        private static Object getStateMethodValue(State state, ObjectMethod method) {
            Object methodResult = state.getByPath(method.getInternalName());
            return State.toSimpleValue(methodResult, method.isEmbedded(), false);
        }
    }

    /**
     * Specifies all fields that are stored for type-ahead from an
     * instance of the target type.
     */
    @Documented
    @Inherited
    @ObjectType.AnnotationProcessorClass(TypeAheadFieldsProcessor.class)
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.TYPE)
    public @interface TypeAheadFields {
        String[] value() default { };
        TypeAheadFieldsMapping[] mappings() default { };
    }

    public @interface TypeAheadFieldsMapping {
        String field();
        String[] solrFields();
    }

    private static class TypeAheadFieldsProcessor implements ObjectType.AnnotationProcessor<TypeAheadFields> {
        @Override
        public void process(ObjectType type, TypeAheadFields annotation) {
            Map<String, List<String>> typeAheadFieldsMap = new HashMap<String, List<String>>();

            for (TypeAheadFieldsMapping mapping : annotation.mappings()) {
                List<String> fields = Arrays.asList(mapping.solrFields());
                typeAheadFieldsMap.put(mapping.field(), fields);
            }

            Collections.addAll(type.as(TypeModification.class).getTypeAheadFields(), annotation.value());
            type.as(TypeModification.class).setTypeAheadFieldsMap(typeAheadFieldsMap);
        }
    }

    @Documented
    @Inherited
    @ObjectField.AnnotationProcessorClass(ExcludeFromAnyProcessor.class)
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.FIELD)
    public @interface ExcludeFromAny {
        boolean value() default true;
    }

    private static class ExcludeFromAnyProcessor implements ObjectField.AnnotationProcessor<ExcludeFromAny> {
        @Override
        public void process(ObjectType type, ObjectField field, ExcludeFromAny annotation) {
            field.as(FieldData.class).setExcludeFromAny(annotation.value());
        }
    }

    @TypeModification.FieldInternalNamePrefix("solr.")
    public static class TypeModification extends Modification<ObjectType> {

        private Set<String> typeAheadFields;
        private Map<String, List<String>> typeAheadFieldsMap;

        public Set<String> getTypeAheadFields() {
            if (typeAheadFields == null) {
                typeAheadFields = new HashSet<String>();
            }
            return typeAheadFields;
        }

        public void setTypeAheadFields(Set<String> typeAheadFields) {
            this.typeAheadFields = typeAheadFields;
        }

        public Map<String, List<String>> getTypeAheadFieldsMap() {
            if (null == typeAheadFieldsMap) {
                typeAheadFieldsMap = new HashMap<String, List<String>>();
            }
            return typeAheadFieldsMap;
        }

        public void setTypeAheadFieldsMap(Map<String, List<String>> typeAheadFieldsMap) {
            this.typeAheadFieldsMap = typeAheadFieldsMap;
        }
    }

    @Modification.FieldInternalNamePrefix("solr.")
    public static class FieldData extends Modification<ObjectField> {

        private boolean excludeFromAny;

        public boolean isExcludeFromAny() {
            return excludeFromAny;
        }

        public void setExcludeFromAny(boolean excludeFromAny) {
            this.excludeFromAny = excludeFromAny;
        }
    }

    // --- Deprecated ---

    /** @deprecated Use the auto commit feature native to Solr instead. */
    @Deprecated
    public static final String MAXIMUM_DOCUMENTS_SETTING = "maximumDocuments";

    /** @deprecated Use the auto commit feature native to Solr instead. */
    @Deprecated
    public static final String MAXIMUM_TIME_SETTING = "maximumTime";

    /** @deprecated Use {@link #SERVER_URL_SUB_SETTING} instead. */
    @Deprecated
    public static final String SERVER_URL_SETTING = SERVER_URL_SUB_SETTING;

    /** @deprecated Use {@link #READ_SERVER_URL_SUB_SETTING} instead. */
    @Deprecated
    public static final String READ_SERVER_URL_SETTING = READ_SERVER_URL_SUB_SETTING;

    /** @deprecated Use {@link Static#escapeValue} instead. */
    @Deprecated
    public static final String quoteValue(Object value) {
        return Static.escapeValue(String.valueOf(value));
    }

    /** @deprecated Use {@link Static#getScore} instead. */
    @Deprecated
    public static Float getScore(Object object) {
        return (Float) State.getInstance(object).getExtra(SCORE_EXTRA);
    }

    /** @deprecated Use {@link Static#getNormalizedScore} instead. */
    @Deprecated
    public static Float getNormalizedScore(Object object) {
        return (Float) State.getInstance(object).getExtra(NORMALIZED_SCORE_EXTRA);
    }

    /** @deprecated Use the auto commit feature native to Solr instead. */
    @Deprecated
    public Long getMaximumDocuments() {
        return null;
    }

    /** @deprecated Use the auto commit feature native to Solr instead. */
    @Deprecated
    public void setMaximumDocuments(Long maximumDocuments) {
    }

    /** @deprecated Use the auto commit feature native to Solr instead. */
    @Deprecated
    public Long getMaximumTime() {
        return null;
    }

    /** @deprecated Use the auto commit feature native to Solr instead. */
    @Deprecated
    public void setMaximumTime(Long maximumTime) {
    }

    /** @deprecated Use {@link #buildQuery} instead. */
    @Deprecated
    public SolrQuery buildSolrQuery(Query<?> query) {
        return buildQuery(query);
    }

    /** @deprecated Use {@link #commit} instead. */
    @Deprecated
    public void commitImmediately() {
        commit();
    }

    /** @deprecated Use the auto commit feature native to Solr instead. */
    @Deprecated
    public void commitEventually() {
    }
}
