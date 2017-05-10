package com.psddev.dari.mysql;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.psddev.dari.db.CompoundPredicate;
import com.psddev.dari.db.MetricAccessDatabase;
import com.psddev.dari.db.ObjectType;
import com.psddev.dari.db.Predicate;
import com.psddev.dari.db.PredicateParser;
import com.psddev.dari.db.Query;
import com.psddev.dari.db.Singleton;
import com.psddev.dari.db.SqlVendor;
import com.psddev.dari.db.State;
import com.psddev.dari.db.StateSerializer;
import com.psddev.dari.sql.AbstractSqlDatabase;
import com.psddev.dari.sql.SqlDatabaseException;
import com.psddev.dari.util.CompactMap;
import com.psddev.dari.util.ObjectUtils;
import com.psddev.dari.util.Profiler;
import com.psddev.dari.util.UuidUtils;
import org.jooq.Converter;
import org.jooq.DataType;
import org.jooq.SQLDialect;
import org.jooq.conf.ParamType;
import org.jooq.impl.DSL;
import org.jooq.util.mysql.MySQLDataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

/**
 * Database implementation for use with MySQL.
 *
 * @see <a href="http://www.mysql.com/">MySQL</a>
 */
public class MySQLDatabase extends AbstractSqlDatabase implements MetricAccessDatabase {

    /**
     * Sub-setting name for specifying whether the replication caching should
     * be enabled.
     *
     * @see #isEnableReplicationCache()
     * @see #setEnableReplicationCache(boolean)
     */
    public static final String ENABLE_REPLICATION_CACHE_SUB_SETTING = "enableReplicationCache";

    /**
     * Sub-setting name for specifying the MySQL host used by replication
     * caching.
     *
     * @see #getReplicationCacheHost()
     * @see #setReplicationCacheHost(String)
     */
    public static final String REPLICATION_CACHE_HOST_SUB_SETTING = "replicationCacheHost";

    /**
     * Sub-setting name for specifying the MySQL port used by replication
     * caching.
     *
     * @see #getReplicationCachePort()
     * @see #setReplicationCachePort(Integer)
     */
    public static final String REPLICATION_CACHE_PORT_SUB_SETTING = "replicationCachePort";

    /**
     * Sub-setting name for specifying the MySQL schema used by replication
     * caching.
     *
     * @see #getReplicationCacheSchema()
     * @see #setReplicationCacheSchema(String)
     */
    public static final String REPLICATION_CACHE_SCHEMA_SUB_SETTING = "replicationCacheSchema";

    /**
     * Sub-setting name for specifying the MySQL username used by replication
     * caching.
     *
     * @see #getReplicationCacheUsername()
     * @see #setReplicationCacheUsername(String)
     */
    public static final String REPLICATION_CACHE_USERNAME_SUB_SETTING = "replicationCacheUsername";

    /**
     * Sub-setting name for specifying the MySQL password used by replication
     * caching.
     *
     * @see #getReplicationCachePassword()
     * @see #setReplicationCachePassword(String)
     */
    public static final String REPLICATION_CACHE_PASSWORD_SUB_SETTING = "replicationCachePassword";

    /**
     * Sub-setting name for specifying the maximum number of items to hold in
     * the replication cache.
     *
     * @see #getReplicationCacheMaximumSize()
     * @see #setReplicationCacheMaximumSize(long)
     */
    public static final String REPLICATION_CACHE_SIZE_SUB_SETTING = "replicationCacheSize";

    private static final Logger LOGGER = LoggerFactory.getLogger(MySQLDatabase.class);

    private static final DataType<UUID> UUID_TYPE = MySQLDataType.BINARY.asConvertedDataType(new Converter<byte[], UUID>() {

        @Override
        public UUID from(byte[] bytes) {
            return bytes != null ? UuidUtils.fromBytes(bytes) : null;
        }

        @Override
        public byte[] to(UUID uuid) {
            return uuid != null ? UuidUtils.toBytes(uuid) : null;
        }

        @Override
        public Class<byte[]> fromType() {
            return byte[].class;
        }

        @Override
        public Class<UUID> toType() {
            return UUID.class;
        }
    });

    private volatile boolean enableReplicationCache;
    private volatile String replicationCacheHost;
    private volatile Integer replicationCachePort;
    private volatile String replicationCacheSchema;
    private volatile String replicationCacheUsername;
    private volatile String replicationCachePassword;
    private volatile long replicationCacheMaximumSize;

    private volatile Cache<UUID, Object[]> replicationCache;
    private volatile MySQLBinaryLogReader binaryLogReader;
    private volatile boolean binlogFormatStatement;

    private final ConcurrentMap<Class<?>, UUID> singletonIds = new ConcurrentHashMap<>();

    /**
     * Returns {@code true} if the replication caching should be enabled.
     *
     * @see #ENABLE_REPLICATION_CACHE_SUB_SETTING
     */
    public boolean isEnableReplicationCache() {
        return enableReplicationCache;
    }

    /**
     * Sets whether the replication caching should be enabled.
     *
     * @see #ENABLE_REPLICATION_CACHE_SUB_SETTING
     */
    public void setEnableReplicationCache(boolean enableReplicationCache) {
        this.enableReplicationCache = enableReplicationCache;
    }

    /**
     * Returns the MySQL host used by replication caching.
     *
     * @see #REPLICATION_CACHE_HOST_SUB_SETTING
     */
    public String getReplicationCacheHost() {
        return replicationCacheHost;
    }

    /**
     * Sets the MySQL host used by replication caching.
     *
     * @param replicationCacheHost Nullable.
     * @see #REPLICATION_CACHE_HOST_SUB_SETTING
     */
    public void setReplicationCacheHost(String replicationCacheHost) {
        this.replicationCacheHost = replicationCacheHost;
    }

    /**
     * Returns the MySQL port used by replication caching.
     *
     * @see #REPLICATION_CACHE_PORT_SUB_SETTING
     */
    public Integer getReplicationCachePort() {
        return replicationCachePort;
    }

    /**
     * Sets the MySQL port used by replication caching.
     *
     * @param replicationCachePort Nullable.
     * @see #REPLICATION_CACHE_PORT_SUB_SETTING
     */
    public void setReplicationCachePort(Integer replicationCachePort) {
        this.replicationCachePort = replicationCachePort;
    }

    /**
     * Returns the MySQL schema used by replication caching.
     *
     * @see #REPLICATION_CACHE_SCHEMA_SUB_SETTING
     */
    public String getReplicationCacheSchema() {
        return replicationCacheSchema;
    }

    /**
     * Sets the MySQL schema used by replication caching.
     *
     * @param replicationCacheSchema Nullable.
     * @see #REPLICATION_CACHE_SCHEMA_SUB_SETTING
     */
    public void setReplicationCacheSchema(String replicationCacheSchema) {
        this.replicationCacheSchema = replicationCacheSchema;
    }

    /**
     * Returns the MySQL username used by replication caching.
     *
     * @see #REPLICATION_CACHE_USERNAME_SUB_SETTING
     */
    public String getReplicationCacheUsername() {
        return replicationCacheUsername;
    }

    /**
     * Sets the MySQL username used by replication caching.
     *
     * @param replicationCacheUsername Nullable.
     * @see #REPLICATION_CACHE_USERNAME_SUB_SETTING
     */
    public void setReplicationCacheUsername(String replicationCacheUsername) {
        this.replicationCacheUsername = replicationCacheUsername;
    }

    /**
     * Returns the MySQL password used by replication caching.
     *
     * @see #REPLICATION_CACHE_PASSWORD_SUB_SETTING
     */
    public String getReplicationCachePassword() {
        return replicationCachePassword;
    }

    /**
     * Sets the MySQL password used by replication caching.
     *
     * @param replicationCachePassword Nullable.
     * @see #REPLICATION_CACHE_PASSWORD_SUB_SETTING
     */
    public void setReplicationCachePassword(String replicationCachePassword) {
        this.replicationCachePassword = replicationCachePassword;
    }

    /**
     * Returns the maximum number of items to hold in the replication cache.
     *
     * @see #REPLICATION_CACHE_SIZE_SUB_SETTING
     */
    public void setReplicationCacheMaximumSize(long replicationCacheMaximumSize) {
        this.replicationCacheMaximumSize = replicationCacheMaximumSize;
    }

    /**
     * Sets the maximum number of items to hold in the replication cache.
     *
     * @see #REPLICATION_CACHE_SIZE_SUB_SETTING
     */
    public long getReplicationCacheMaximumSize() {
        return this.replicationCacheMaximumSize;
    }

    @Override
    protected SQLDialect getDialect() {
        return SQLDialect.MYSQL;
    }

    @Override
    protected DataType<UUID> uuidType() {
        return UUID_TYPE;
    }

    @Override
    protected void prepareConnection(Connection connection, boolean readOnly) throws SQLException {
        if (!binlogFormatStatement) {
            super.prepareConnection(connection, readOnly);
        }
    }

    @Override
    public void invalidateCaches() {
        super.invalidateCaches();
        replicationCache.invalidateAll();
    }

    @Override
    protected void doInitialize(String settingsKey, Map<String, Object> settings) {
        super.doInitialize(settingsKey, settings);

        // Initialize replication caching?
        setEnableReplicationCache(ObjectUtils.to(boolean.class, settings.get(ENABLE_REPLICATION_CACHE_SUB_SETTING)));
        setReplicationCacheHost(ObjectUtils.to(String.class, settings.get(REPLICATION_CACHE_HOST_SUB_SETTING)));
        setReplicationCachePort(ObjectUtils.to(Integer.class, settings.get(REPLICATION_CACHE_PORT_SUB_SETTING)));
        setReplicationCacheSchema(ObjectUtils.to(String.class, settings.get(REPLICATION_CACHE_SCHEMA_SUB_SETTING)));
        setReplicationCacheUsername(ObjectUtils.to(String.class, settings.get(REPLICATION_CACHE_USERNAME_SUB_SETTING)));
        setReplicationCachePassword(ObjectUtils.to(String.class, settings.get(REPLICATION_CACHE_PASSWORD_SUB_SETTING)));
        setReplicationCacheMaximumSize(ObjectUtils.firstNonNull(ObjectUtils.to(Long.class, settings.get(REPLICATION_CACHE_SIZE_SUB_SETTING)), 10000L));

        if (isEnableReplicationCache()
                && (binaryLogReader == null
                || !binaryLogReader.isRunning())) {

            replicationCache = CacheBuilder.newBuilder().maximumSize(getReplicationCacheMaximumSize()).build();

            try {
                LOGGER.info("Starting MySQL binary log reader");
                binaryLogReader = new MySQLBinaryLogReader(this, replicationCache, getReadDataSource(), recordTable.getName());
                binaryLogReader.start();

            } catch (IllegalArgumentException error) {
                setEnableReplicationCache(false);
                LOGGER.warn("Can't start MySQL binary log reader!", error);
            }
        }

        // Verify that binlog format is set correctly.
        Connection connection = openConnection();

        try {
            try (Statement statement = connection.createStatement();
                 ResultSet result = statement.executeQuery("SHOW VARIABLES WHERE variable_name IN ('log_bin', 'binlog_format')")) {

                boolean logBin = false;

                while (result.next()) {
                    String name = result.getString(1);
                    String value = result.getString(2);

                    if ("binlog_format".equalsIgnoreCase(name)) {
                        binlogFormatStatement = "STATEMENT".equalsIgnoreCase(value);

                    } else if ("log_bin".equalsIgnoreCase(name)) {
                        logBin = !"OFF".equalsIgnoreCase(value);
                    }
                }

                binlogFormatStatement = logBin && Boolean.TRUE.equals(binlogFormatStatement);

                if (binlogFormatStatement) {
                    LOGGER.warn("Can't set transaction isolation to"
                            + " READ COMMITTED because binlog_format"
                            + " is set to STATEMENT. Please set it to"
                            + " MIXED (my.cnf: binlog_format = mixed)"
                            + " to prevent reduced performance under"
                            + " load.");
                }
            }

        } catch (SQLException error) {
            throw new SqlDatabaseException(this, "Can't read MySQL variables!", error);

        } finally {
            closeConnection(connection);
        }
    }

    @Override
    public void close() throws Exception {
        super.close();

        if (binaryLogReader != null) {
            LOGGER.info("Stopping MySQL binary log reader");
            binaryLogReader.stop();
            binaryLogReader = null;
        }
    }

    @Override
    protected <T> T createSavedObjectUsingResultSet(ResultSet resultSet, Query<T> query) throws SQLException {
        T object = super.createSavedObjectUsingResultSet(resultSet, query);

        if (object instanceof Singleton) {
            State objectState = State.getInstance(object);
            ObjectType objectType = objectState.getType();

            if (objectType != null) {
                Class<?> objectClass = objectType.getObjectClass();

                if (objectClass != null) {
                    singletonIds.put(objectClass, objectState.getId());
                }
            }
        }

        return object;
    }

    // Creates a previously saved object from the replication cache.
    <T> T createSavedObjectFromReplicationCache(UUID id, byte[] data, Map<String, Object> dataJson, Query<T> query) {
        UUID typeId = ObjectUtils.to(UUID.class, dataJson.get(StateSerializer.TYPE_KEY));
        T object = createSavedObject(typeId, id, query);
        State state = State.getInstance(object);
        @SuppressWarnings("unchecked")
        Map<String, Object> dataJsonClone = (Map<String, Object>) cloneJson(dataJson);

        state.setValues(dataJsonClone);

        if (query != null && ObjectUtils.to(boolean.class, query.getOptions().get(RETURN_ORIGINAL_DATA_QUERY_OPTION))) {
            state.getExtras().put(ORIGINAL_DATA_EXTRA, data);
        }

        return swapObjectType(query, object);
    }

    // Clones the object so that it's safe to use in multiple threads.
    private static Object cloneJson(Object object) {
        if (object instanceof Map) {
            @SuppressWarnings("unchecked")
            Map<String, Object> map = (Map<String, Object>) object;
            Map<String, Object> clone = new CompactMap<>(map.size());

            for (Map.Entry<?, ?> entry : map.entrySet()) {
                clone.put((String) entry.getKey(), cloneJson(entry.getValue()));
            }

            return clone;

        } else if (object instanceof List) {
            @SuppressWarnings("unchecked")
            List<Object> objectList = (List<Object>) object;

            return objectList.stream()
                    .map(MySQLDatabase::cloneJson)
                    .collect(Collectors.toList());

        } else {
            return object;
        }
    }

    @Override
    public <T> List<T> readAll(Query<T> query) {
        if (checkReplicationCache(query)) {
            List<Object> ids = query.findIdOnlyQueryValues();

            if (ids != null && !ids.isEmpty()) {
                List<T> objects = findObjectsFromReplicationCache(ids, query);

                return objects != null ? objects : new ArrayList<>();
            }
        }

        return super.readAll(query);
    }

    // Checks whether the replication cache should be used.
    private boolean checkReplicationCache(Query<?> query) {
        return query.isCache()
                && isEnableReplicationCache()
                && binaryLogReader != null
                && binaryLogReader.isConnected();
    }

    // Tries to find objects from the replication cache, and if not found,
    // executes the query to populate it.
    private <T> List<T> findObjectsFromReplicationCache(List<Object> ids, Query<T> query) {
        List<T> objects = null;
        List<UUID> missingIds = null;

        Profiler.Static.startThreadEvent("MySQL: Replication Cache Get");

        try {
            for (Object idObject : ids) {
                UUID id = ObjectUtils.to(UUID.class, idObject);

                if (id == null) {
                    continue;
                }

                Object[] value = replicationCache.getIfPresent(id);

                if (value == null) {
                    if (missingIds == null) {
                        missingIds = new ArrayList<>();
                    }

                    missingIds.add(id);
                    continue;
                }

                UUID typeId = ObjectUtils.to(UUID.class, value[0]);
                byte[] data = (byte[]) value[1];
                @SuppressWarnings("unchecked")
                Map<String, Object> dataJson = (Map<String, Object>) value[2];

                objects = createReplicationCacheObjects(
                        objects,
                        typeId,
                        id,
                        data,
                        dataJson,
                        query);
            }

        } finally {
            Profiler.Static.stopThreadEvent((objects != null ? objects.size() : 0) + " Objects");
        }

        if (missingIds != null && !missingIds.isEmpty()) {
            Profiler.Static.startThreadEvent("MySQL: Replication Cache Put");

            try {
                String sqlQuery = DSL.using(getDialect())
                        .select(recordIdField, recordDataField)
                        .from(recordTable)
                        .where(recordIdField.in(missingIds))
                        .getSQL(ParamType.INLINED);

                List<T> selectObjects = objects;

                objects = select(sqlQuery, query, result -> {
                    List<T> resultObjects = selectObjects;

                    while (result.next()) {
                        UUID id = ObjectUtils.to(UUID.class, result.getBytes(1));

                        if (id == null) {
                            continue;
                        }

                        byte[] data = result.getBytes(2);
                        Map<String, Object> dataJson = StateSerializer.deserialize(data);
                        UUID typeId = ObjectUtils.to(UUID.class, dataJson.get(StateSerializer.TYPE_KEY));

                        if (!UuidUtils.ZERO_UUID.equals(typeId)) {
                            replicationCache.put(id, new Object[] { UuidUtils.toBytes(typeId), data, dataJson });
                        }

                        resultObjects = createReplicationCacheObjects(
                                resultObjects,
                                typeId,
                                id,
                                data,
                                dataJson,
                                query);
                    }

                    return resultObjects;
                });

            } finally {
                Profiler.Static.stopThreadEvent(missingIds.size() + " Objects");
            }
        }

        return objects;
    }

    // Creates objects that can go into the replication cache.
    private <T> List<T> createReplicationCacheObjects(List<T> objects, UUID typeId, UUID id, byte[] data, Map<String, Object> dataJson, Query<T> query) {
        if (typeId != null && query != null) {
            ObjectType type = ObjectType.getInstance(typeId);

            if (type != null) {
                Class<?> queryObjectClass = query.getObjectClass();

                if (queryObjectClass != null && !query.getObjectClass().isAssignableFrom(type.getObjectClass())) {
                    return objects;
                }

                String queryGroup = query.getGroup();

                if (queryGroup != null && !type.getGroups().contains(queryGroup)) {
                    return objects;
                }
            }
        }

        T object = createSavedObjectFromReplicationCache(id, data, dataJson, query);

        if (object != null) {
            if (objects == null) {
                objects = new ArrayList<>();
            }

            objects.add(object);
        }

        return objects;
    }

    @Override
    public <T> T readFirst(Query<T> query) {
        if (query.getSorters().isEmpty()) {

            Predicate predicate = query.getPredicate();
            if (predicate instanceof CompoundPredicate) {

                CompoundPredicate compoundPredicate = (CompoundPredicate) predicate;
                if (PredicateParser.OR_OPERATOR.equals(compoundPredicate.getOperator())) {

                    for (Predicate child : compoundPredicate.getChildren()) {
                        Query<T> childQuery = query.clone();
                        childQuery.setPredicate(child);

                        T first = readFirst(childQuery);
                        if (first != null) {
                            return first;
                        }
                    }

                    return null;
                }
            }
        }

        if (checkReplicationCache(query)) {
            Class<?> objectClass = query.getObjectClass();
            List<Object> ids;

            if (objectClass != null
                    && Singleton.class.isAssignableFrom(objectClass)
                    && query.getPredicate() == null) {

                UUID id = singletonIds.get(objectClass);
                ids = id != null ? Collections.singletonList(id) : null;

            } else {
                ids = query.findIdOnlyQueryValues();
            }

            if (ids != null && !ids.isEmpty()) {
                List<T> objects = findObjectsFromReplicationCache(ids, query);

                return objects == null || objects.isEmpty() ? null : objects.get(0);
            }
        }

        return super.readFirst(query);
    }

    @Override
    protected boolean shouldUseSavepoint() {
        return false;
    }

    @Override
    public SqlVendor getMetricVendor() {
        return new SqlVendor.MySQL();
    }

    @Override
    public String getMetricCatalog() {
        return null;
    }

    @Override
    public int getSymbolId(String symbol) {
        return findSymbolId(symbol, true);
    }

    @Override
    public ResultSet executeQueryBeforeTimeout(Statement statement, String sqlQuery, int timeout) throws SQLException {
        if (timeout > 0) {
            statement.setQueryTimeout(timeout);
        }

        return statement.executeQuery(sqlQuery);
    }
}
