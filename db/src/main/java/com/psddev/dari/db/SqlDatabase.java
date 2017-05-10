package com.psddev.dari.db;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.ref.WeakReference;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.SQLRecoverableException;
import java.sql.SQLTimeoutException;
import java.sql.Savepoint;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;

import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;

import com.zaxxer.hikari.HikariDataSource;
import org.iq80.snappy.Snappy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.psddev.dari.util.CompactMap;
import com.psddev.dari.util.Lazy;
import com.psddev.dari.util.ObjectUtils;
import com.psddev.dari.util.PaginatedResult;
import com.psddev.dari.util.PeriodicValue;
import com.psddev.dari.util.Profiler;
import com.psddev.dari.util.Settings;
import com.psddev.dari.util.SettingsException;
import com.psddev.dari.util.Stats;
import com.psddev.dari.util.StringUtils;
import com.psddev.dari.util.TypeDefinition;
import com.psddev.dari.util.UuidUtils;

/** Database backed by a SQL engine. */
public class SqlDatabase extends AbstractDatabase<Connection> implements MetricAccessDatabase {

    public static final String DATA_SOURCE_SETTING = "dataSource";
    public static final String DATA_SOURCE_JNDI_NAME_SETTING = "dataSourceJndiName";
    public static final String JDBC_DRIVER_CLASS_SETTING = "jdbcDriverClass";
    public static final String JDBC_URL_SETTING = "jdbcUrl";
    public static final String JDBC_USER_SETTING = "jdbcUser";
    public static final String JDBC_PASSWORD_SETTING = "jdbcPassword";
    public static final String JDBC_POOL_SIZE_SETTING = "jdbcPoolSize";

    public static final String READ_DATA_SOURCE_SETTING = "readDataSource";
    public static final String READ_DATA_SOURCE_JNDI_NAME_SETTING = "readDataSourceJndiName";
    public static final String READ_JDBC_DRIVER_CLASS_SETTING = "readJdbcDriverClass";
    public static final String READ_JDBC_URL_SETTING = "readJdbcUrl";
    public static final String READ_JDBC_USER_SETTING = "readJdbcUser";
    public static final String READ_JDBC_PASSWORD_SETTING = "readJdbcPassword";
    public static final String READ_JDBC_POOL_SIZE_SETTING = "readJdbcPoolSize";

    public static final String CATALOG_SUB_SETTING = "catalog";
    public static final String METRIC_CATALOG_SUB_SETTING = "metricCatalog";
    public static final String VENDOR_CLASS_SETTING = "vendorClass";
    public static final String COMPRESS_DATA_SUB_SETTING = "compressData";

    @Deprecated
    public static final String CACHE_DATA_SUB_SETTING = "cacheData";

    @Deprecated
    public static final String DATA_CACHE_SIZE_SUB_SETTING = "dataCacheSize";

    public static final String ENABLE_REPLICATION_CACHE_SUB_SETTING = "enableReplicationCache";
    public static final String ENABLE_FUNNEL_CACHE_SUB_SETTING = "enableFunnelCache";
    public static final String REPLICATION_CACHE_SIZE_SUB_SETTING = "replicationCacheSize";
    public static final String INDEX_SPATIAL_SUB_SETTING = "indexSpatial";

    public static final String RECORD_TABLE = "Record";
    public static final String RECORD_UPDATE_TABLE = "RecordUpdate";
    public static final String SYMBOL_TABLE = "Symbol";
    public static final String ID_COLUMN = "id";
    public static final String TYPE_ID_COLUMN = "typeId";
    public static final String IN_ROW_INDEX_COLUMN = "inRowIndex";
    public static final String DATA_COLUMN = "data";
    public static final String SYMBOL_ID_COLUMN = "symbolId";
    public static final String UPDATE_DATE_COLUMN = "updateDate";
    public static final String VALUE_COLUMN = "value";

    public static final String CONNECTION_QUERY_OPTION = "sql.connection";
    public static final String EXTRA_COLUMNS_QUERY_OPTION = "sql.extraColumns";
    public static final String EXTRA_JOINS_QUERY_OPTION = "sql.extraJoins";
    public static final String EXTRA_WHERE_QUERY_OPTION = "sql.extraWhere";
    public static final String EXTRA_HAVING_QUERY_OPTION = "sql.extraHaving";
    public static final String MYSQL_INDEX_HINT_QUERY_OPTION = "sql.mysqlIndexHint";
    public static final String RETURN_ORIGINAL_DATA_QUERY_OPTION = "sql.returnOriginalData";
    public static final String USE_JDBC_FETCH_SIZE_QUERY_OPTION = "sql.useJdbcFetchSize";
    public static final String USE_READ_DATA_SOURCE_QUERY_OPTION = "sql.useReadDataSource";
    public static final String DISABLE_REPLICATION_CACHE_QUERY_OPTION = "sql.disableReplicationCache";
    public static final String SKIP_INDEX_STATE_EXTRA = "sql.skipIndex";

    public static final String INDEX_TABLE_INDEX_OPTION = "sql.indexTable";

    public static final String EXTRA_COLUMN_EXTRA_PREFIX = "sql.extraColumn.";
    public static final String ORIGINAL_DATA_EXTRA = "sql.originalData";

    public static final String SUB_DATA_COLUMN_ALIAS_PREFIX = "subData_";

    private static final Logger LOGGER = LoggerFactory.getLogger(SqlDatabase.class);
    private static final String SHORT_NAME = "SQL";
    private static final Stats STATS = new Stats(SHORT_NAME);
    private static final String CONNECTION_ERROR_STATS_OPERATION = "Connection Error";
    private static final String QUERY_STATS_OPERATION = "Query";
    private static final String UPDATE_STATS_OPERATION = "Update";
    private static final String QUERY_PROFILER_EVENT = SHORT_NAME + " " + QUERY_STATS_OPERATION;
    private static final String UPDATE_PROFILER_EVENT = SHORT_NAME + " " + UPDATE_STATS_OPERATION;
    private static final String REPLICATION_CACHE_GET_PROFILER_EVENT = SHORT_NAME + " Replication Cache Get";
    private static final String REPLICATION_CACHE_PUT_PROFILER_EVENT = SHORT_NAME + " Replication Cache Put";
    private static final String FUNNEL_CACHE_GET_PROFILER_EVENT = SHORT_NAME + " Funnel Cache Get";
    private static final String FUNNEL_CACHE_PUT_PROFILER_EVENT = SHORT_NAME + " Funnel Cache Put";
    private static final long NOW_EXPIRATION_SECONDS = 300;
    public static final long DEFAULT_REPLICATION_CACHE_SIZE = 10000L;
    public static final long DEFAULT_DATA_CACHE_SIZE = 10000L;

    private static final List<SqlDatabase> INSTANCES = new ArrayList<SqlDatabase>();

    {
        INSTANCES.add(this);
    }

    private volatile DataSource dataSource;
    private volatile DataSource readDataSource;
    private volatile String catalog;
    private volatile String metricCatalog;
    private transient volatile String defaultCatalog;
    private volatile SqlVendor vendor;
    private volatile boolean compressData;
    private volatile boolean enableReplicationCache;
    private volatile boolean enableFunnelCache;
    private volatile long replicationCacheMaximumSize;
    private volatile boolean indexSpatial;

    private final transient ConcurrentMap<Class<?>, UUID> singletonIds = new ConcurrentHashMap<>();
    private transient volatile Cache<UUID, Object[]> replicationCache;
    private transient volatile MySQLBinaryLogReader mysqlBinaryLogReader;
    private transient volatile FunnelCache<SqlDatabase> funnelCache;
    private final List<UpdateNotifier<?>> updateNotifiers = new ArrayList<>();

    /**
     * Quotes the given {@code identifier} so that it's safe to use
     * in a SQL query.
     */
    public static String quoteIdentifier(String identifier) {
        return "\"" + StringUtils.replaceAll(identifier, "\\\\", "\\\\\\\\", "\"", "\"\"") + "\"";
    }

    /**
     * Quotes the given {@code value} so that it's safe to use
     * in a SQL query.
     */
    public static String quoteValue(Object value) {
        if (value == null) {
            return "NULL";
        } else if (value instanceof Number) {
            return value.toString();
        } else if (value instanceof byte[]) {
            return "X'" + StringUtils.hex((byte[]) value) + "'";
        } else {
            return "'" + value.toString().replace("'", "''").replace("\\", "\\\\") + "'";
        }
    }

    /** Closes all resources used by all instances. */
    public static void closeAll() {
        for (SqlDatabase database : INSTANCES) {
            database.close();
        }
        INSTANCES.clear();
    }

    /**
     * Creates an {@link SqlDatabaseException} that occurred during
     * an execution of a query.
     */
    private SqlDatabaseException createQueryException(
            SQLException error,
            String sqlQuery,
            Query<?> query) {

        String message = error.getMessage();
        if (error instanceof SQLTimeoutException || message.contains("timeout")) {
            return new SqlDatabaseException.ReadTimeout(this, error, sqlQuery, query);
        } else {
            return new SqlDatabaseException(this, error, sqlQuery, query);
        }
    }

    /** Returns the JDBC data source used for general database operations. */
    public DataSource getDataSource() {
        return dataSource;
    }

    private static final Map<String, Class<? extends SqlVendor>> VENDOR_CLASSES; static {
        Map<String, Class<? extends SqlVendor>> m = new HashMap<String, Class<? extends SqlVendor>>();
        m.put("H2", SqlVendor.H2.class);
        m.put("MySQL", SqlVendor.MySQL.class);
        m.put("PostgreSQL", SqlVendor.PostgreSQL.class);
        m.put("EnterpriseDB", SqlVendor.PostgreSQL.class);
        m.put("Oracle", SqlVendor.Oracle.class);
        VENDOR_CLASSES = m;
    }

    /** Sets the JDBC data source used for general database operations. */
    public void setDataSource(DataSource dataSource) {
        this.dataSource = dataSource;
        if (dataSource == null) {
            return;
        }

        synchronized (this) {
            try {
                boolean writable = false;

                if (vendor == null) {
                    Connection connection;

                    try {
                        connection = openConnection();
                        writable = true;

                    } catch (DatabaseException error) {
                        LOGGER.debug("Can't read vendor information from the writable server!", error);
                        connection = openReadConnection();
                    }

                    try {
                        defaultCatalog = connection.getCatalog();
                        DatabaseMetaData meta = connection.getMetaData();
                        String vendorName = meta.getDatabaseProductName();
                        Class<? extends SqlVendor> vendorClass = VENDOR_CLASSES.get(vendorName);

                        LOGGER.info(
                                "Initializing SQL vendor for [{}]: [{}] -> [{}]",
                                new Object[] { getName(), vendorName, vendorClass });

                        vendor = vendorClass != null ? TypeDefinition.getInstance(vendorClass).newInstance() : new SqlVendor();
                        vendor.setDatabase(this);

                    } finally {
                        closeConnection(connection);
                    }
                }

                tableColumnNames.refresh();
                symbols.reset();

                if (writable) {
                    vendor.setUp(this);
                    tableColumnNames.refresh();
                    symbols.reset();
                }

            } catch (IOException error) {
                throw new IllegalStateException(error);

            } catch (SQLException error) {
                throw new SqlDatabaseException(this, "Can't check for required tables!", error);
            }
        }
    }

    /** Returns the JDBC data source used exclusively for read operations. */
    public DataSource getReadDataSource() {
        return this.readDataSource;
    }

    /** Sets the JDBC data source used exclusively for read operations. */
    public void setReadDataSource(DataSource readDataSource) {
        this.readDataSource = readDataSource;
    }

    public String getCatalog() {
        return catalog;
    }

    public void setCatalog(String catalog) {
        this.catalog = catalog;

        try {
            getVendor().setUp(this);
            tableColumnNames.refresh();
            symbols.reset();

        } catch (IOException error) {
            throw new IllegalStateException(error);

        } catch (SQLException error) {
            throw new SqlDatabaseException(this, "Can't check for required tables!", error);
        }
    }

    /** Returns the catalog that contains the Metric table.
     *
     * @return May be {@code null}.
     *
     **/
    @Override
    public String getMetricCatalog() {
        return metricCatalog;
    }

    public void setMetricCatalog(String metricCatalog) {
        if (ObjectUtils.isBlank(metricCatalog)) {
            this.metricCatalog = null;

        } else {
            StringBuilder str = new StringBuilder();

            vendor.appendIdentifier(str, metricCatalog);
            str.append(".");
            vendor.appendIdentifier(str, MetricAccess.METRIC_TABLE);

            if (getVendor().checkTableExists(str.toString())) {
                this.metricCatalog = metricCatalog;

            } else {
                LOGGER.error("SqlDatabase#setMetricCatalog error: " + str.toString() + " does not exist or is not accessible. Falling back to default catalog.");
                this.metricCatalog = null;
            }
        }
    }

    /** Returns the vendor-specific SQL engine information. */
    public SqlVendor getVendor() {
        return vendor;
    }

    @Override
    public SqlVendor getMetricVendor() {
        return getVendor();
    }

    /** Sets the vendor-specific SQL engine information. */
    public void setVendor(SqlVendor vendor) {
        this.vendor = vendor;
    }

    /** Returns {@code true} if the data should be compressed. */
    public boolean isCompressData() {
        return compressData;
    }

    /** Sets whether the data should be compressed. */
    public void setCompressData(boolean compressData) {
        this.compressData = compressData;
    }

    @Deprecated
    public boolean isCacheData() {
        return false;
    }

    @Deprecated
    public void setCacheData(boolean cacheData) {
    }

    @Deprecated
    public long getDataCacheMaximumSize() {
        return 0L;
    }

    @Deprecated
    public void setDataCacheMaximumSize(long dataCacheMaximumSize) {
    }

    public boolean isEnableReplicationCache() {
        return enableReplicationCache;
    }

    public void setEnableReplicationCache(boolean enableReplicationCache) {
        this.enableReplicationCache = enableReplicationCache;
    }

    public boolean isEnableFunnelCache() {
        return enableFunnelCache;
    }

    public void setEnableFunnelCache(boolean enableFunnelCache) {
        this.enableFunnelCache = enableFunnelCache;
    }

    public void setReplicationCacheMaximumSize(long replicationCacheMaximumSize) {
        this.replicationCacheMaximumSize = replicationCacheMaximumSize;
    }

    public long getReplicationCacheMaximumSize() {
        return this.replicationCacheMaximumSize;
    }

    public boolean isIndexSpatial() {
        return indexSpatial;
    }

    public void setIndexSpatial(boolean indexSpatial) {
        this.indexSpatial = indexSpatial;
    }

    /**
     * Returns {@code true} if the {@link #RECORD_TABLE} in this database
     * has the {@link #IN_ROW_INDEX_COLUMN}.
     */
    public boolean hasInRowIndex() {
        return hasInRowIndex;
    }

    /**
     * Returns {@code true} if all comparisons executed in this database
     * should ignore case by default.
     */
    public boolean comparesIgnoreCase() {
        return comparesIgnoreCase;
    }

    /**
     * Returns {@code true} if this database contains a table with
     * the given {@code name}.
     */
    public boolean hasTable(String name) {
        if (name == null) {
            return false;
        } else {
            Set<String> names = tableColumnNames.get().keySet();
            return names != null && names.contains(name.toLowerCase(Locale.ENGLISH));
        }
    }

    /**
     * Returns {@code true} if the given {@code table} in this database
     * contains the given {@code column}.
     *
     * @param table If {@code null}, always returns {@code false}.
     * @param column If {@code null}, always returns {@code false}.
     */
    public boolean hasColumn(String table, String column) {
        if (table == null || column == null) {
            return false;

        } else {
            Set<String> columnNames = tableColumnNames.get().get(table.toLowerCase(Locale.ENGLISH));

            return columnNames != null && columnNames.contains(column.toLowerCase(Locale.ENGLISH));
        }
    }

    private transient volatile boolean hasInRowIndex;
    private transient volatile boolean comparesIgnoreCase;

    private final transient PeriodicValue<Map<String, Set<String>>> tableColumnNames = new PeriodicValue<Map<String, Set<String>>>(0.0, 60.0) {

        @Override
        protected Map<String, Set<String>> update() {
            if (getDataSource() == null) {
                return Collections.emptyMap();
            }

            Connection connection;

            try {
                connection = openConnection();

            } catch (DatabaseException error) {
                LOGGER.debug("Can't read table names from the writable server!", error);
                connection = openReadConnection();
            }

            try {
                SqlVendor vendor = getVendor();
                String recordTable = null;
                int maxStringVersion = 0;
                Map<String, Set<String>> loweredNames = new HashMap<String, Set<String>>();

                for (String name : vendor.getTables(connection)) {
                    String loweredName = name.toLowerCase(Locale.ENGLISH);
                    Set<String> loweredColumnNames = new HashSet<String>();

                    for (String columnName : vendor.getColumns(connection, name)) {
                        loweredColumnNames.add(columnName.toLowerCase(Locale.ENGLISH));
                    }

                    loweredNames.put(loweredName, loweredColumnNames);

                    if ("record".equals(loweredName)) {
                        recordTable = name;

                    } else if (loweredName.startsWith("recordstring")) {
                        int version = ObjectUtils.to(int.class, loweredName.substring(12));
                        if (version > maxStringVersion) {
                            maxStringVersion = version;
                        }
                    }
                }

                if (recordTable != null) {
                    hasInRowIndex = vendor.hasInRowIndex(connection, recordTable);
                }

                comparesIgnoreCase = maxStringVersion >= 3;

                return loweredNames;

            } catch (SQLException error) {
                LOGGER.error("Can't query table names!", error);
                return get();

            } finally {
                closeConnection(connection);
            }
        }
    };

    /**
     * Returns an unique numeric ID for the given {@code symbol},
     * or {@code -1} if it's not available.
     */
    public int getReadSymbolId(String symbol) {
        Integer id = symbols.get().get(symbol);

        if (id == null) {
            Connection connection = openConnection();
            try {
                id = selectSymbolId(connection, symbol);
                if (id != null) {
                    symbols.get().put(symbol, id);
                }
            } finally {
                closeConnection(connection);
            }
            sqlQueryCache.invalidateAll();
        }

        return id != null ? id : -1;
    }

    /**
     * Returns an unique numeric ID for the given {@code symbol}.
     */
    @Override
    public int getSymbolId(String symbol) {
        Integer id = symbols.get().get(symbol);
        if (id == null) {

            SqlVendor vendor = getVendor();
            Connection connection = openConnection();

            try {
                List<Object> parameters = new ArrayList<Object>();
                StringBuilder insertBuilder = new StringBuilder();

                insertBuilder.append("INSERT /*! IGNORE */ INTO ");
                vendor.appendIdentifier(insertBuilder, SYMBOL_TABLE);
                insertBuilder.append(" (");
                vendor.appendIdentifier(insertBuilder, VALUE_COLUMN);
                insertBuilder.append(") VALUES (");
                vendor.appendBindValue(insertBuilder, symbol, parameters);
                insertBuilder.append(')');

                String insertSql = insertBuilder.toString();
                try {
                    Static.executeUpdateWithList(vendor, connection, insertSql, parameters);

                } catch (SQLException ex) {
                    if (!Static.isIntegrityConstraintViolation(ex)) {
                        throw createQueryException(ex, insertSql, null);
                    }
                }

                id = selectSymbolId(connection, symbol);
                symbols.get().put(symbol, id);

            } finally {
                closeConnection(connection);
            }
        }

        return id;
    }

    private Integer selectSymbolId(Connection connection, String symbol) {
        Integer id = null;

        try {
            StringBuilder selectBuilder = new StringBuilder();
            selectBuilder.append("SELECT ");
            vendor.appendIdentifier(selectBuilder, SYMBOL_ID_COLUMN);
            selectBuilder.append(" FROM ");
            vendor.appendIdentifier(selectBuilder, SYMBOL_TABLE);
            selectBuilder.append(" WHERE ");
            vendor.appendIdentifier(selectBuilder, VALUE_COLUMN);
            selectBuilder.append('=');
            vendor.appendValue(selectBuilder, symbol);

            String selectSql = selectBuilder.toString();
            Statement statement = null;
            ResultSet result = null;

            try {
                statement = connection.createStatement();
                result = statement.executeQuery(selectSql);
                if (result.next()) {
                    id = result.getInt(1);
                }

            } catch (SQLException ex) {
                throw createQueryException(ex, selectSql, null);

            } finally {
                closeResources(null, null, statement, result);
            }

        } finally {
            closeConnection(connection);
        }

        return id;
    }

    private final Supplier<Long> nowOffset = Suppliers.memoizeWithExpiration(new Supplier<Long>() {

        @Override
        public Long get() {
            String selectSql = getVendor().getSelectTimestampMillisSql();
            Connection connection;
            Statement statement = null;
            ResultSet result = null;
            Long nowOffsetMillis = 0L;

            if (selectSql != null) {
                try {
                    connection = openConnection();
                } catch (DatabaseException error) {
                    LOGGER.debug("Can't read timestamp from the writable server!", error);
                    connection = openReadConnection();
                }

                try {
                    statement = connection.createStatement();
                    result = statement.executeQuery(selectSql);
                    if (result.next()) {
                        nowOffsetMillis = System.currentTimeMillis() - result.getLong(1);
                    }
                } catch (SQLException ex) {
                    throw createQueryException(ex, selectSql, null);
                } finally {
                    closeResources(null, connection, statement, result);
                }
            }

            return nowOffsetMillis;
        }
    }, NOW_EXPIRATION_SECONDS, TimeUnit.SECONDS);

    @Override
    public long now() {
        return System.currentTimeMillis() - nowOffset.get();
    }

    // Cache of all internal symbols.
    private final transient Lazy<Map<String, Integer>> symbols = new Lazy<Map<String, Integer>>() {

        @Override
        protected Map<String, Integer> create() {
            SqlVendor vendor = getVendor();
            StringBuilder selectBuilder = new StringBuilder();
            selectBuilder.append("SELECT ");
            vendor.appendIdentifier(selectBuilder, SYMBOL_ID_COLUMN);
            selectBuilder.append(',');
            vendor.appendIdentifier(selectBuilder, VALUE_COLUMN);
            selectBuilder.append(" FROM ");
            vendor.appendIdentifier(selectBuilder, SYMBOL_TABLE);

            String selectSql = selectBuilder.toString();
            Connection connection;
            Statement statement = null;
            ResultSet result = null;

            try {
                connection = openConnection();

            } catch (DatabaseException error) {
                LOGGER.debug("Can't read symbols from the writable server!", error);
                connection = openReadConnection();
            }

            try {
                statement = connection.createStatement();
                result = statement.executeQuery(selectSql);

                Map<String, Integer> symbols = new ConcurrentHashMap<String, Integer>();
                while (result.next()) {
                    symbols.put(new String(result.getBytes(2), StandardCharsets.UTF_8), result.getInt(1));
                }

                return symbols;

            } catch (SQLException ex) {
                throw createQueryException(ex, selectSql, null);

            } finally {
                closeResources(null, connection, statement, result);
            }
        }
    };

    /**
     * Returns the underlying JDBC connection.
     *
     * @deprecated Use {@link #openConnection} instead.
     */
    @Deprecated
    public Connection getConnection() {
        return openConnection();
    }

    /** Closes any resources used by this database. */
    public void close() {
        DataSource dataSource = getDataSource();
        if (dataSource instanceof HikariDataSource) {
            LOGGER.info("Closing connection pool in {}", getName());
            ((HikariDataSource) dataSource).close();
        }

        DataSource readDataSource = getReadDataSource();
        if (readDataSource instanceof HikariDataSource) {
            LOGGER.info("Closing read connection pool in {}", getName());
            ((HikariDataSource) readDataSource).close();
        }

        setDataSource(null);
        setReadDataSource(null);

        if (mysqlBinaryLogReader != null) {
            LOGGER.info("Stopping MySQL binary log reader");
            mysqlBinaryLogReader.stop();
            mysqlBinaryLogReader = null;
        }
    }

    private String addComment(String sql, Query<?> query) {
        if (query != null) {
            String comment = query.getComment();

            if (!ObjectUtils.isBlank(comment)) {
                return "/*" + comment + "*/ " + sql;
            }
        }

        return sql;
    }

    /**
     * Builds an SQL statement that can be used to get a count of all
     * objects matching the given {@code query}.
     */
    public String buildCountStatement(Query<?> query) {
        return addComment(new SqlQuery(this, query).countStatement(), query);
    }

    /**
     * Builds an SQL statement that can be used to delete all rows
     * matching the given {@code query}.
     */
    public String buildDeleteStatement(Query<?> query) {
        return addComment(new SqlQuery(this, query).deleteStatement(), query);
    }

    /**
     * Builds an SQL statement that can be used to get all objects
     * grouped by the values of the given {@code groupFields}.
     */
    public String buildGroupStatement(Query<?> query, String... groupFields) {
        return addComment(new SqlQuery(this, query).groupStatement(groupFields), query);
    }

    public String buildGroupedMetricStatement(Query<?> query, String metricFieldName, String... groupFields) {
        return addComment(new SqlQuery(this, query).groupedMetricSql(metricFieldName, groupFields), query);
    }

    /**
     * Builds an SQL statement that can be used to get when the objects
     * matching the given {@code query} were last updated.
     */
    public String buildLastUpdateStatement(Query<?> query) {
        return addComment(new SqlQuery(this, query).lastUpdateStatement(), query);
    }

    /**
     * Maintains a cache of Querys to SQL select statements.
     */
    private final LoadingCache<Query<?>, String> sqlQueryCache = CacheBuilder
            .newBuilder()
            .maximumSize(5000)
            .concurrencyLevel(20)
            .build(new CacheLoader<Query<?>, String>() {
                @Override
                public String load(Query<?> query) throws Exception {
                    return new SqlQuery(SqlDatabase.this, query).selectStatement();
                }
            });

    /**
     * Builds an SQL statement that can be used to list all rows
     * matching the given {@code query}.
     */
    public String buildSelectStatement(Query<?> query) {
        try {
            Query<?> strippedQuery = query.clone();
            // Remove any possibility that multiple CachingDatabases will be cached in the sqlQueryCache.
            strippedQuery.setDatabase(this);
            strippedQuery.getOptions().remove(State.REFERENCE_RESOLVING_QUERY_OPTION);
            return addComment(sqlQueryCache.getUnchecked(strippedQuery), query);
        } catch (UncheckedExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof RuntimeException) {
                throw (RuntimeException) cause;
            } else {
                throw new DatabaseException(this, cause);
            }
        }
    }

    // Closes all the given SQL resources safely.
    protected void closeResources(Query<?> query, Connection connection, Statement statement, ResultSet result) {
        if (result != null) {
            try {
                result.close();
            } catch (SQLException error) {
                // Not likely and probably harmless.
            }
        }

        if (statement != null) {
            try {
                statement.close();
            } catch (SQLException error) {
                // Not likely and probably harmless.
            }
        }

        Object queryConnection;

        if (connection != null
                && (query == null
                || (queryConnection = query.getOptions().get(CONNECTION_QUERY_OPTION)) == null
                || !connection.equals(queryConnection))) {
            try {
                if (!connection.isClosed()) {
                    connection.close();
                }
            } catch (SQLException ex) {
                // Not likely and probably harmless.
            }
        }
    }

    private byte[] serializeState(State state) {
        Map<String, Object> values = state.getSimpleValues();

        for (Iterator<Map.Entry<String, Object>> i = values.entrySet().iterator(); i.hasNext();) {
            Map.Entry<String, Object> entry = i.next();
            ObjectField field = state.getField(entry.getKey());

            if (field != null) {
                if (field.as(FieldData.class).isIndexTableSourceFromAnywhere()) {
                    i.remove();
                }
            }
        }

        byte[] dataBytes = ObjectUtils.toJson(values).getBytes(StandardCharsets.UTF_8);

        if (isCompressData()) {
            byte[] compressed = new byte[Snappy.maxCompressedLength(dataBytes.length)];
            int compressedLength = Snappy.compress(dataBytes, 0, dataBytes.length, compressed, 0);

            dataBytes = new byte[compressedLength + 1];
            dataBytes[0] = 's';
            System.arraycopy(compressed, 0, dataBytes, 1, compressedLength);
        }

        return dataBytes;
    }

    private static byte[] decodeData(byte[] dataBytes) {
        char format;

        while (true) {
            format = (char) dataBytes[0];

            if (format == 's') {
                dataBytes = Snappy.uncompress(dataBytes, 1, dataBytes.length - 1);

            } else if (format == '{') {
                return dataBytes;

            } else {
                break;
            }
        }

        throw new IllegalStateException(String.format(
                "Unknown format! ([%s])",
                format));
    }

    @SuppressWarnings("unchecked")
    protected static Map<String, Object> unserializeData(byte[] dataBytes) {
        char format = '\0';

        while (true) {
            format = (char) dataBytes[0];

            if (format == 's') {
                dataBytes = Snappy.uncompress(dataBytes, 1, dataBytes.length - 1);

            } else if (format == '{') {
                return (Map<String, Object>) ObjectUtils.fromJson(dataBytes);

            } else {
                break;
            }
        }

        throw new IllegalStateException(String.format(
                "Unknown format! ([%s])", format));
    }

    private class ConnectionRef {

        private Connection connection;

        public Connection getOrOpen(Query<?> query) {
            if (connection == null) {
                connection = SqlDatabase.super.openQueryConnection(query);
            }
            return connection;
        }

        public void close() {
            if (connection != null) {
                try {
                    if (!connection.isClosed()) {
                        connection.close();
                    }
                } catch (SQLException error) {
                    // Not likely and probably harmless.
                }
            }
        }
    }

    // Creates a previously saved object using the given resultSet.
    private <T> T createSavedObjectWithResultSet(
            ResultSet resultSet,
            Query<T> query,
            ConnectionRef extraConnectionRef)
            throws SQLException {

        T object = createSavedObject(resultSet.getObject(2), resultSet.getObject(1), query);
        State objectState = State.getInstance(object);

        if (object instanceof Singleton) {
            ObjectType objectType = objectState.getType();

            if (objectType != null) {
                Class<?> objectClass = objectType.getObjectClass();

                if (objectClass != null) {
                    singletonIds.put(objectClass, objectState.getId());
                }
            }
        }

        if (!objectState.isReferenceOnly()) {
            byte[] data = resultSet.getBytes(3);

            if (data != null) {
                byte[] decodedData = decodeData(data);
                @SuppressWarnings("unchecked")
                Map<String, Object> unserializedData = (Map<String, Object>) ObjectUtils.fromJson(decodedData);

                objectState.setValues(unserializedData);
                objectState.getExtras().put(DATA_LENGTH_EXTRA, decodedData.length);
                Boolean returnOriginal = ObjectUtils.to(Boolean.class, query.getOptions().get(RETURN_ORIGINAL_DATA_QUERY_OPTION));
                if (returnOriginal == null) {
                    returnOriginal = Boolean.FALSE;
                }
                if (returnOriginal) {
                    objectState.getExtras().put(ORIGINAL_DATA_EXTRA, data);
                }
            }
        }

        ResultSetMetaData meta = resultSet.getMetaData();
        Object subId = null, subTypeId = null;
        byte[] subData;

        for (int i = 4, count = meta.getColumnCount(); i <= count; ++ i) {
            String columnName = meta.getColumnLabel(i);
            if (columnName.startsWith(SUB_DATA_COLUMN_ALIAS_PREFIX)) {
                if (columnName.endsWith("_" + ID_COLUMN)) {
                    subId = resultSet.getObject(i);
                } else if (columnName.endsWith("_" + TYPE_ID_COLUMN)) {
                    subTypeId = resultSet.getObject(i);
                } else if (columnName.endsWith("_" + DATA_COLUMN)) {
                    subData = resultSet.getBytes(i);
                    if (subId != null && subTypeId != null && subData != null && !subId.equals(objectState.getId())) {
                        Object subObject = createSavedObject(subTypeId, subId, query);
                        State subObjectState = State.getInstance(subObject);
                        subObjectState.setValues(unserializeData(subData));
                        subObject = swapObjectType(null, subObject);
                        subId = null;
                        subTypeId = null;
                        subData = null;
                        objectState.getExtras().put(State.SUB_DATA_STATE_EXTRA_PREFIX + subObjectState.getId(), subObject);
                    }
                }
            } else if (query.getExtraSourceColumns().containsKey(columnName)) {
                objectState.put(query.getExtraSourceColumns().get(columnName), resultSet.getObject(i));
            } else {
                objectState.getExtras().put(EXTRA_COLUMN_EXTRA_PREFIX + meta.getColumnLabel(i), resultSet.getObject(i));
            }
        }

        // Load some extra column from source index tables.
        @SuppressWarnings("unchecked")
        Set<UUID> unresolvedTypeIds = (Set<UUID>) query.getOptions().get(State.UNRESOLVED_TYPE_IDS_QUERY_OPTION);
        Set<ObjectType> queryTypes = query.getConcreteTypes(getEnvironment());
        ObjectType type = objectState.getType();
        HashSet<ObjectField> loadExtraFields = new HashSet<ObjectField>();

        if (type != null) {
            if ((unresolvedTypeIds == null || !unresolvedTypeIds.contains(type.getId()))
                    && !queryTypes.contains(type)) {
                for (ObjectField field : type.getFields()) {
                    SqlDatabase.FieldData fieldData = field.as(SqlDatabase.FieldData.class);

                    if (fieldData.isIndexTableSource() && !field.isMetric()) {
                        loadExtraFields.add(field);
                    }
                }
            }
        }

        if (!loadExtraFields.isEmpty()) {
            Connection connection = extraConnectionRef.getOrOpen(query);

            for (ObjectField field : loadExtraFields) {
                Statement extraStatement = null;
                ResultSet extraResult = null;

                try {
                    extraStatement = connection.createStatement();
                    extraResult = executeQueryBeforeTimeout(
                            extraStatement,
                            extraSourceSelectStatementById(field, objectState.getId(), objectState.getTypeId()),
                            getQueryReadTimeout(query));

                    if (extraResult.next()) {
                        meta = extraResult.getMetaData();

                        for (int i = 1, count = meta.getColumnCount(); i <= count; ++ i) {
                            objectState.put(meta.getColumnLabel(i), extraResult.getObject(i));
                        }
                    }

                } finally {
                    closeResources(null, null, extraStatement, extraResult);
                }
            }
        }

        return swapObjectType(query, object);
    }

    // Creates an SQL statement to return a single row from a FieldIndexTable
    // used as a source table.
    //
    // Maybe: move this to SqlQuery and use initializeClauses() and
    // needsRecordTable=false instead of passing id to this method. Needs
    // countperformance branch to do this.
    private String extraSourceSelectStatementById(ObjectField field, UUID id, UUID typeId) {
        FieldData fieldData = field.as(FieldData.class);
        ObjectType parentType = field.getParentType();
        StringBuilder keyName = new StringBuilder(parentType.getInternalName());

        keyName.append('/');
        keyName.append(field.getInternalName());

        Query<?> query = Query.fromType(parentType);
        Query.MappedKey key = query.mapEmbeddedKey(getEnvironment(), keyName.toString());
        ObjectIndex useIndex = null;

        for (ObjectIndex index : key.getIndexes()) {
            if (field.getInternalName().equals(index.getFields().get(0))) {
                useIndex = index;
                break;
            }
        }

        SqlIndex useSqlIndex = SqlIndex.Static.getByIndex(useIndex);
        SqlIndex.Table indexTable = useSqlIndex.getReadTable(this, useIndex);
        String sourceTableName = fieldData.getIndexTable();
        int symbolId = getReadSymbolId(key.getIndexKey(useIndex));
        StringBuilder sql = new StringBuilder();
        int fieldIndex = 0;

        sql.append("SELECT ");

        for (String indexFieldName : useIndex.getFields()) {
            String indexColumnName = indexTable.getValueField(this, useIndex, fieldIndex);

            ++ fieldIndex;

            vendor.appendIdentifier(sql, indexColumnName);
            sql.append(" AS ");
            vendor.appendIdentifier(sql, indexFieldName);
            sql.append(", ");
        }
        sql.setLength(sql.length() - 2);

        sql.append(" FROM ");
        vendor.appendIdentifier(sql, sourceTableName);
        sql.append(" WHERE ");
        vendor.appendIdentifier(sql, "id");
        sql.append(" = ");
        vendor.appendValue(sql, id);
        sql.append(" AND ");
        vendor.appendIdentifier(sql, "symbolId");
        sql.append(" = ");
        sql.append(symbolId);

        return sql.toString();
    }

    /**
     * Executes the given read {@code statement} (created from the given
     * {@code sqlQuery}) before the given {@code timeout} (in seconds).
     */
    @Override
    public ResultSet executeQueryBeforeTimeout(
            Statement statement,
            String sqlQuery,
            int timeout)
            throws SQLException {

        if (timeout > 0 && !(vendor instanceof SqlVendor.PostgreSQL)) {
            statement.setQueryTimeout(timeout);
        }

        Stats.Timer timer = STATS.startTimer();
        Profiler.Static.startThreadEvent(QUERY_PROFILER_EVENT);

        try {
            return statement.executeQuery(sqlQuery);

        } finally {
            double duration = timer.stop(QUERY_STATS_OPERATION);
            Profiler.Static.stopThreadEvent(sqlQuery);

            LOGGER.debug(
                    "Read from the SQL database using [{}] in [{}]ms",
                    sqlQuery, duration * 1000.0);
        }
    }

    // Creates a previously saved object from the replication cache.
    protected <T> T createSavedObjectFromReplicationCache(byte[] typeId, UUID id, byte[] data, Map<String, Object> dataJson, Query<T> query) {
        T object = createSavedObject(typeId, id, query);
        State objectState = State.getInstance(object);

        objectState.setValues(cloneDataJson(dataJson));

        Boolean returnOriginal = query != null ? ObjectUtils.to(Boolean.class, query.getOptions().get(RETURN_ORIGINAL_DATA_QUERY_OPTION)) : null;

        if (returnOriginal == null) {
            returnOriginal = Boolean.FALSE;
        }

        if (returnOriginal) {
            objectState.getExtras().put(ORIGINAL_DATA_EXTRA, data);
        }

        return swapObjectType(query, object);
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> cloneDataJson(Map<String, Object> dataJson) {
        return (Map<String, Object>) cloneDataJsonRecursively(dataJson);
    }

    private static Object cloneDataJsonRecursively(Object object) {
        if (object instanceof Map) {
            Map<?, ?> objectMap = (Map<?, ?>) object;
            int objectMapSize = objectMap.size();
            Map<String, Object> clone = objectMapSize <= 8
                    ? new CompactMap<String, Object>()
                    : new LinkedHashMap<String, Object>(objectMapSize);

            for (Map.Entry<?, ?> entry : objectMap.entrySet()) {
                clone.put((String) entry.getKey(), cloneDataJsonRecursively(entry.getValue()));
            }

            return clone;

        } else if (object instanceof List) {
            List<?> objectList = (List<?>) object;
            List<Object> clone = new ArrayList<Object>(objectList.size());

            for (Object item : objectList) {
                clone.add(cloneDataJsonRecursively(item));
            }

            return clone;

        } else {
            return object;
        }
    }

    // Tries to find objects by the given ids from the replication cache.
    // If not found, execute the given query to populate it.
    private <T> List<T> findObjectsFromReplicationCache(List<Object> ids, Query<T> query) {
        List<T> objects = null;

        if (ids == null || ids.isEmpty()) {
            return objects;
        }

        List<UUID> missingIds = null;

        Profiler.Static.startThreadEvent(REPLICATION_CACHE_GET_PROFILER_EVENT);

        String queryGroup = query != null ? query.getGroup() : null;
        Class queryObjectClass = query != null ? query.getObjectClass() : null;

        try {
            for (Object idObject : ids) {
                UUID id = ObjectUtils.to(UUID.class, idObject);

                if (id == null) {
                    continue;
                }

                Object[] value = replicationCache.getIfPresent(id);

                if (value == null) {
                    if (missingIds == null) {
                        missingIds = new ArrayList<UUID>();
                    }

                    missingIds.add(id);
                    continue;
                }

                UUID typeId = ObjectUtils.to(UUID.class, (byte[]) value[0]);

                ObjectType type = typeId != null ? ObjectType.getInstance(typeId) : null;

                // Restrict objects based on the class provided to the Query
                if (type != null && queryObjectClass != null && !query.getObjectClass().isAssignableFrom(type.getObjectClass())) {
                    continue;
                }

                // Restrict objects based on the group provided to the Query
                if (type != null && queryGroup != null && !type.getGroups().contains(queryGroup)) {
                    continue;
                }

                @SuppressWarnings("unchecked")
                T object = createSavedObjectFromReplicationCache((byte[]) value[0], id, (byte[]) value[1], (Map<String, Object>) value[2], query);

                if (object != null) {
                    if (objects == null) {
                        objects = new ArrayList<T>();
                    }

                    objects.add(object);
                }
            }

        } finally {
            Profiler.Static.stopThreadEvent((objects != null ? objects.size() : 0) + " Objects");
        }

        if (missingIds != null && !missingIds.isEmpty()) {
            Profiler.Static.startThreadEvent(REPLICATION_CACHE_PUT_PROFILER_EVENT);

            try {
                SqlVendor vendor = getVendor();
                StringBuilder sqlQuery = new StringBuilder();

                sqlQuery.append("SELECT ");
                vendor.appendIdentifier(sqlQuery, TYPE_ID_COLUMN);
                sqlQuery.append(", ");
                vendor.appendIdentifier(sqlQuery, DATA_COLUMN);
                sqlQuery.append(", ");
                vendor.appendIdentifier(sqlQuery, ID_COLUMN);
                sqlQuery.append(" FROM ");
                vendor.appendIdentifier(sqlQuery, RECORD_TABLE);
                sqlQuery.append(" WHERE ");
                vendor.appendIdentifier(sqlQuery, ID_COLUMN);
                sqlQuery.append(" IN (");

                for (UUID missingId : missingIds) {
                    vendor.appendUuid(sqlQuery, missingId);
                    sqlQuery.append(", ");
                }

                sqlQuery.setLength(sqlQuery.length() - 2);
                sqlQuery.append(")");

                Connection connection = null;
                ConnectionRef extraConnectionRef = new ConnectionRef();
                Statement statement = null;
                ResultSet result = null;

                try {
                    connection = extraConnectionRef.getOrOpen(query);
                    statement = connection.createStatement();
                    result = executeQueryBeforeTimeout(statement, sqlQuery.toString(), 0);

                    while (result.next()) {
                        UUID id = ObjectUtils.to(UUID.class, result.getBytes(3));
                        byte[] data = result.getBytes(2);
                        Map<String, Object> dataJson = unserializeData(data);
                        byte[] typeIdBytes = UuidUtils.toBytes(ObjectUtils.to(UUID.class, dataJson.get(StateSerializer.TYPE_KEY)));

                        if (!Arrays.equals(typeIdBytes, UuidUtils.ZERO_BYTES) && id != null) {
                            replicationCache.put(id, new Object[] { typeIdBytes, data, dataJson });
                        }

                        UUID typeId = ObjectUtils.to(UUID.class, typeIdBytes);

                        ObjectType type = typeId != null ? ObjectType.getInstance(typeId) : null;

                        // Restrict objects based on the class provided to the Query
                        if (type != null && queryObjectClass != null && !query.getObjectClass().isAssignableFrom(type.getObjectClass())) {
                            continue;
                        }

                        // Restrict objects based on the group provided to the Query
                        if (type != null && queryGroup != null && !type.getGroups().contains(queryGroup)) {
                            continue;
                        }

                        T object = createSavedObjectFromReplicationCache(typeIdBytes, id, data, dataJson, query);

                        if (object != null) {
                            if (objects == null) {
                                objects = new ArrayList<T>();
                            }

                            objects.add(object);
                        }
                    }

                } catch (SQLException error) {
                    throw createQueryException(error, sqlQuery.toString(), query);

                } finally {
                    closeResources(query, connection, statement, result);
                    extraConnectionRef.close();
                }

            } finally {
                Profiler.Static.stopThreadEvent(missingIds.size() + " Objects");
            }
        }

        return objects;
    }

    private <T> List<T> findObjectsFromFunnelCache(String sqlQuery, Query<T> query) {
        List<T> objects = new ArrayList<T>();
        Profiler.Static.startThreadEvent(FUNNEL_CACHE_GET_PROFILER_EVENT);
        try {
            List<FunnelCachedObject> cachedObjects = funnelCache.get(new FunnelCacheProducer(sqlQuery, query));
            if (cachedObjects != null) {
                for (FunnelCachedObject cachedObj : cachedObjects) {
                    T savedObj = createSavedObjectFromReplicationCache(UuidUtils.toBytes(cachedObj.getTypeId()), cachedObj.getId(), null, cachedObj.getValues(), query);
                    if (cachedObj.getExtras() != null && !cachedObj.getExtras().isEmpty()) {
                        State.getInstance(savedObj).getExtras().putAll(cachedObj.getExtras());
                    }
                    objects.add(savedObj);
                }
            }
            return objects;
        } finally {
            Profiler.Static.stopThreadEvent(objects);
        }
    }

    /**
     * Selects the first object that matches the given {@code sqlQuery}
     * with options from the given {@code query}.
     */
    public <T> T selectFirstWithOptions(String sqlQuery, Query<T> query) {
        sqlQuery = vendor.rewriteQueryWithLimitClause(sqlQuery, 1, 0);
        if (checkFunnelCache(query)) {
            List<T> objects = findObjectsFromFunnelCache(sqlQuery, query);
            if (objects != null) {
                return objects.isEmpty() ? null : objects.get(0);
            }
        }

        ConnectionRef extraConnectionRef = new ConnectionRef();
        Connection connection = null;
        Statement statement = null;
        ResultSet result = null;

        try {
            connection = openQueryConnection(query);
            statement = connection.createStatement();
            result = executeQueryBeforeTimeout(statement, sqlQuery, getQueryReadTimeout(query));
            return result.next() ? createSavedObjectWithResultSet(result, query, extraConnectionRef) : null;

        } catch (SQLException ex) {
            throw createQueryException(ex, sqlQuery, query);

        } finally {
            closeResources(query, connection, statement, result);
            extraConnectionRef.close();
        }
    }

    /**
     * Selects the first object that matches the given {@code sqlQuery}
     * without a timeout.
     */
    public Object selectFirst(String sqlQuery) {
        return selectFirstWithOptions(sqlQuery, null);
    }

    /**
     * Selects a list of objects that match the given {@code sqlQuery}
     * with options from the given {@code query}.
     */
    public <T> List<T> selectListWithOptions(String sqlQuery, Query<T> query) {
        if (checkFunnelCache(query)) {
            List<T> objects = findObjectsFromFunnelCache(sqlQuery, query);
            if (objects != null) {
                return objects;
            }
        }
        ConnectionRef extraConnectionRef = new ConnectionRef();
        Connection connection = null;
        Statement statement = null;
        ResultSet result = null;
        List<T> objects = new ArrayList<T>();
        int timeout = getQueryReadTimeout(query);

        try {
            connection = openQueryConnection(query);
            statement = connection.createStatement();
            result = executeQueryBeforeTimeout(statement, sqlQuery, timeout);
            while (result.next()) {
                objects.add(createSavedObjectWithResultSet(result, query, extraConnectionRef));
            }

            return objects;

        } catch (SQLException ex) {
            throw createQueryException(ex, sqlQuery, query);

        } finally {
            closeResources(query, connection, statement, result);
            extraConnectionRef.close();
        }
    }

    /**
     * Selects a list of objects that match the given {@code sqlQuery}
     * without a timeout.
     */
    public List<Object> selectList(String sqlQuery) {
        return selectListWithOptions(sqlQuery, null);
    }

    /**
     * Returns an iterable that selects all objects matching the given
     * {@code sqlQuery} with options from the given {@code query}.
     */
    public <T> Iterable<T> selectIterableWithOptions(
            final String sqlQuery,
            final int fetchSize,
            final Query<T> query) {

        return new Iterable<T>() {
            @Override
            public Iterator<T> iterator() {
                return new SqlIterator<T>(sqlQuery, fetchSize, query);
            }
        };
    }

    private class SqlIterator<T> implements java.io.Closeable, Iterator<T> {

        private final String sqlQuery;
        private final Query<T> query;
        private final ConnectionRef extraConnectionRef;

        private final Connection connection;
        private final Statement statement;
        private final ResultSet result;

        private boolean hasNext = true;

        public SqlIterator(String initialSqlQuery, int fetchSize, Query<T> initialQuery) {
            sqlQuery = initialSqlQuery;
            query = initialQuery;
            extraConnectionRef = new ConnectionRef();

            try {
                connection = openQueryConnection(query);
                statement = connection.createStatement();
                statement.setFetchSize(getVendor() instanceof SqlVendor.MySQL ? Integer.MIN_VALUE
                        : fetchSize <= 0 ? 200
                        : fetchSize);
                result = statement.executeQuery(sqlQuery);
                moveToNext();

            } catch (SQLException ex) {
                close();
                throw createQueryException(ex, sqlQuery, query);
            }
        }

        private void moveToNext() throws SQLException {
            if (hasNext) {
                hasNext = result.next();
                if (!hasNext) {
                    close();
                }
            }
        }

        @Override
        public void close() {
            hasNext = false;
            closeResources(query, connection, statement, result);
            extraConnectionRef.close();
        }

        @Override
        public boolean hasNext() {
            return hasNext;
        }

        @Override
        public T next() {
            if (!hasNext) {
                throw new NoSuchElementException();
            }

            try {
                T object = createSavedObjectWithResultSet(result, query, extraConnectionRef);
                moveToNext();
                return object;

            } catch (SQLException ex) {
                close();
                throw createQueryException(ex, sqlQuery, query);
            }
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

        @Override
        protected void finalize() {
            close();
        }
    }

    /**
     * Fills the placeholders in the given {@code sqlQuery} with the given
     * {@code parameters}.
     */
    private static String fillPlaceholders(String sqlQuery, Object... parameters) {
        StringBuilder filled = new StringBuilder();
        int prevPh = 0;
        for (int ph, index = 0; (ph = sqlQuery.indexOf('?', prevPh)) > -1; ++ index) {
            filled.append(sqlQuery.substring(prevPh, ph));
            prevPh = ph + 1;
            filled.append(quoteValue(parameters[index]));
        }
        filled.append(sqlQuery.substring(prevPh));
        return filled.toString();
    }

    /**
     * Executes the given write {@code sqlQuery} with the given
     * {@code parameters}.
     *
     * @deprecated Use {@link Static#executeUpdate} instead.
     */
    @Deprecated
    public int executeUpdate(String sqlQuery, Object... parameters) {
        try {
            return Static.executeUpdateWithArray(getVendor(), getConnection(), sqlQuery, parameters);
        } catch (SQLException ex) {
            throw createQueryException(ex, fillPlaceholders(sqlQuery, parameters), null);
        }
    }

    /**
     * Reads the given {@code resultSet} into a list of maps
     * and closes it.
     */
    public List<Map<String, Object>> readResultSet(ResultSet resultSet) throws SQLException {
        try {
            ResultSetMetaData meta = resultSet.getMetaData();
            List<String> columnNames = new ArrayList<String>();
            for (int i = 1, count = meta.getColumnCount(); i < count; ++ i) {
                columnNames.add(meta.getColumnName(i));
            }

            List<Map<String, Object>> maps = new ArrayList<Map<String, Object>>();
            while (resultSet.next()) {
                Map<String, Object> map = new LinkedHashMap<String, Object>();
                maps.add(map);
                for (int i = 0, size = columnNames.size(); i < size; ++ i) {
                    map.put(columnNames.get(i), resultSet.getObject(i + 1));
                }
            }

            return maps;

        } finally {
            resultSet.close();
        }
    }

    // --- AbstractDatabase support ---

    @Override
    public Connection openConnection() {
        DataSource dataSource = getDataSource();

        if (dataSource == null) {
            throw new SqlDatabaseException(this, "No SQL data source!");
        }

        try {
            Connection connection = getConnectionFromDataSource(dataSource);

            connection.setReadOnly(false);

            if (vendor != null) {
                vendor.setTransactionIsolation(connection);
            }

            return connection;

        } catch (SQLException error) {
            throw new SqlDatabaseException(this, "Can't connect to the SQL engine!", error);
        }
    }

    private Connection getConnectionFromDataSource(DataSource dataSource) throws SQLException {
        int limit = Settings.getOrDefault(int.class, "dari/sqlConnectionRetryLimit", 5);

        while (true) {
            try {
                Connection connection = dataSource.getConnection();
                String catalog = getCatalog();

                if (catalog != null) {
                    connection.setCatalog(catalog);
                }

                return connection;

            } catch (SQLException error) {
                if (error instanceof SQLRecoverableException) {
                    -- limit;

                    if (limit >= 0) {
                        Stats.Timer timer = STATS.startTimer();

                        try {
                            Thread.sleep(ObjectUtils.jitter(10L, 0.5));

                        } catch (InterruptedException ignore) {
                            // Ignore and keep retrying.

                        } finally {
                            timer.stop(CONNECTION_ERROR_STATS_OPERATION);
                        }

                        continue;
                    }
                }

                throw error;
            }
        }
    }

    @Override
    protected Connection doOpenReadConnection() {
        DataSource readDataSource = getReadDataSource();

        if (readDataSource == null) {
            readDataSource = getDataSource();
        }

        if (readDataSource == null) {
            throw new SqlDatabaseException(this, "No SQL data source!");
        }

        try {
            Connection connection = getConnectionFromDataSource(readDataSource);

            connection.setReadOnly(true);
            return connection;

        } catch (SQLException error) {
            throw new SqlDatabaseException(this, "Can't connect to the SQL engine!", error);
        }
    }

    @Override
    public Connection openQueryConnection(Query<?> query) {
        if (query != null) {
            Connection connection = (Connection) query.getOptions().get(CONNECTION_QUERY_OPTION);

            if (connection != null) {
                return connection;
            }

            Boolean useRead = ObjectUtils.to(Boolean.class, query.getOptions().get(USE_READ_DATA_SOURCE_QUERY_OPTION));

            if (useRead == null) {
                useRead = Boolean.TRUE;
            }

            if (!useRead) {
                return openConnection();
            }
        }

        return super.openQueryConnection(query);
    }

    @Override
    public void closeConnection(Connection connection) {
        if (connection != null) {
            try {
                if (defaultCatalog != null) {
                    String catalog = getCatalog();

                    if (catalog != null) {
                        connection.setCatalog(defaultCatalog);
                    }
                }

                if (!connection.isClosed()) {
                    connection.close();
                }

            } catch (SQLException error) {
                // Not likely and probably harmless.
            }
        }
    }

    @Override
    protected boolean isRecoverableError(Exception error) {
        if (error instanceof SQLException) {
            SQLException sqlError = (SQLException) error;
            return "40001".equals(sqlError.getSQLState());
        }

        return false;
    }

    @Override
    protected void doInitialize(String settingsKey, Map<String, Object> settings) {
        close();
        setReadDataSource(createDataSource(
                settings,
                READ_DATA_SOURCE_SETTING,
                READ_DATA_SOURCE_JNDI_NAME_SETTING,
                READ_JDBC_DRIVER_CLASS_SETTING,
                READ_JDBC_URL_SETTING,
                READ_JDBC_USER_SETTING,
                READ_JDBC_PASSWORD_SETTING,
                READ_JDBC_POOL_SIZE_SETTING));
        setDataSource(createDataSource(
                settings,
                DATA_SOURCE_SETTING,
                DATA_SOURCE_JNDI_NAME_SETTING,
                JDBC_DRIVER_CLASS_SETTING,
                JDBC_URL_SETTING,
                JDBC_USER_SETTING,
                JDBC_PASSWORD_SETTING,
                JDBC_POOL_SIZE_SETTING));

        setCatalog(ObjectUtils.to(String.class, settings.get(CATALOG_SUB_SETTING)));

        setMetricCatalog(ObjectUtils.to(String.class, settings.get(METRIC_CATALOG_SUB_SETTING)));

        String vendorClassName = ObjectUtils.to(String.class, settings.get(VENDOR_CLASS_SETTING));
        Class<?> vendorClass = null;

        if (vendorClassName != null) {
            vendorClass = ObjectUtils.getClassByName(vendorClassName);
            if (vendorClass == null) {
                throw new SettingsException(
                        VENDOR_CLASS_SETTING,
                        String.format("Can't find [%s]!",
                        vendorClassName));
            } else if (!SqlVendor.class.isAssignableFrom(vendorClass)) {
                throw new SettingsException(
                        VENDOR_CLASS_SETTING,
                        String.format("[%s] doesn't implement [%s]!",
                        vendorClass, Driver.class));
            }
        }

        if (vendorClass != null) {
            setVendor((SqlVendor) TypeDefinition.getInstance(vendorClass).newInstance());
        }

        Boolean compressData = ObjectUtils.firstNonNull(
                ObjectUtils.to(Boolean.class, settings.get(COMPRESS_DATA_SUB_SETTING)),
                Settings.get(Boolean.class, "dari/isCompressSqlData"));
        if (compressData != null) {
            setCompressData(compressData);
        }

        setEnableReplicationCache(ObjectUtils.to(boolean.class, settings.get(ENABLE_REPLICATION_CACHE_SUB_SETTING)));
        setEnableFunnelCache(ObjectUtils.to(boolean.class, settings.get(ENABLE_FUNNEL_CACHE_SUB_SETTING)));
        Long replicationCacheMaxSize = ObjectUtils.to(Long.class, settings.get(REPLICATION_CACHE_SIZE_SUB_SETTING));
        setReplicationCacheMaximumSize(replicationCacheMaxSize != null ? replicationCacheMaxSize : DEFAULT_REPLICATION_CACHE_SIZE);
        setIndexSpatial(ObjectUtils.firstNonNull(ObjectUtils.to(Boolean.class, settings.get(INDEX_SPATIAL_SUB_SETTING)), Boolean.TRUE));

        if (isEnableReplicationCache()
                && vendor instanceof SqlVendor.MySQL
                && (mysqlBinaryLogReader == null
                || !mysqlBinaryLogReader.isRunning())) {

            replicationCache = CacheBuilder.newBuilder().maximumSize(getReplicationCacheMaximumSize()).build();

            try {
                LOGGER.info("Starting MySQL binary log reader");
                mysqlBinaryLogReader = new MySQLBinaryLogReader(this, replicationCache, ObjectUtils.firstNonNull(getReadDataSource(), getDataSource()));
                mysqlBinaryLogReader.start();

            } catch (IllegalArgumentException error) {
                setEnableReplicationCache(false);
                LOGGER.warn("Can't start MySQL binary log reader!", error);
            }
        }

        if (isEnableFunnelCache()) {
            funnelCache = new FunnelCache<SqlDatabase>(this, settings);
        }
    }

    private static final Map<String, String> DRIVER_CLASS_NAMES; static {
        Map<String, String> m = new HashMap<String, String>();
        m.put("h2", "org.h2.Driver");
        m.put("jtds", "net.sourceforge.jtds.jdbc.Driver");
        m.put("mysql", "com.mysql.jdbc.Driver");
        m.put("postgresql", "org.postgresql.Driver");
        DRIVER_CLASS_NAMES = m;
    }

    private static final Set<WeakReference<Driver>> REGISTERED_DRIVERS = new HashSet<WeakReference<Driver>>();

    private DataSource createDataSource(
            Map<String, Object> settings,
            String dataSourceSetting,
            String dataSourceJndiNameSetting,
            String jdbcDriverClassSetting,
            String jdbcUrlSetting,
            String jdbcUserSetting,
            String jdbcPasswordSetting,
            String jdbcPoolSizeSetting) {

        Object dataSourceJndiName = settings.get(dataSourceJndiNameSetting);
        if (dataSourceJndiName instanceof String) {
            try {
                Object dataSourceObject = new InitialContext().lookup((String) dataSourceJndiName);
                if (dataSourceObject instanceof DataSource) {
                    return (DataSource) dataSourceObject;
                }
            } catch (NamingException e) {
                throw new SettingsException(dataSourceJndiNameSetting,
                        String.format("Can't find [%s]!",
                        dataSourceJndiName), e);
            }
        }

        Object dataSourceObject = settings.get(dataSourceSetting);
        if (dataSourceObject instanceof DataSource) {
            return (DataSource) dataSourceObject;

        } else {
            String url = ObjectUtils.to(String.class, settings.get(jdbcUrlSetting));
            if (ObjectUtils.isBlank(url)) {
                return null;

            } else {
                String driverClassName = ObjectUtils.to(String.class, settings.get(jdbcDriverClassSetting));
                Class<?> driverClass = null;

                if (driverClassName != null) {
                    driverClass = ObjectUtils.getClassByName(driverClassName);
                    if (driverClass == null) {
                        throw new SettingsException(
                                jdbcDriverClassSetting,
                                String.format("Can't find [%s]!",
                                driverClassName));
                    } else if (!Driver.class.isAssignableFrom(driverClass)) {
                        throw new SettingsException(
                                jdbcDriverClassSetting,
                                String.format("[%s] doesn't implement [%s]!",
                                driverClass, Driver.class));
                    }

                } else {
                    int firstColonAt = url.indexOf(':');
                    if (firstColonAt > -1) {
                        ++ firstColonAt;
                        int secondColonAt = url.indexOf(':', firstColonAt);
                        if (secondColonAt > -1) {
                            driverClass = ObjectUtils.getClassByName(DRIVER_CLASS_NAMES.get(url.substring(firstColonAt, secondColonAt)));
                        }
                    }
                }

                if (driverClass != null) {
                    Driver driver = null;
                    for (Enumeration<Driver> e = DriverManager.getDrivers(); e.hasMoreElements();) {
                        Driver d = e.nextElement();
                        if (driverClass.isInstance(d)) {
                            driver = d;
                            break;
                        }
                    }

                    if (driver == null) {
                        driver = (Driver) TypeDefinition.getInstance(driverClass).newInstance();
                        try {
                            LOGGER.info("Registering [{}]", driver);
                            DriverManager.registerDriver(driver);
                        } catch (SQLException ex) {
                            LOGGER.warn("Can't register [{}]!", driver);
                        }
                    }

                    if (driver != null) {
                        REGISTERED_DRIVERS.add(new WeakReference<Driver>(driver));
                    }
                }

                String user = ObjectUtils.to(String.class, settings.get(jdbcUserSetting));
                String password = ObjectUtils.to(String.class, settings.get(jdbcPasswordSetting));

                Integer poolSize = ObjectUtils.to(Integer.class, settings.get(jdbcPoolSizeSetting));
                if (poolSize == null || poolSize <= 0) {
                    poolSize = 24;
                }

                LOGGER.info(
                        "Automatically creating connection pool:"
                                + "\n\turl={}"
                                + "\n\tusername={}"
                                + "\n\tpoolSize={}",
                        new Object[] {
                                url,
                                user,
                                poolSize });

                HikariDataSource ds = new HikariDataSource();

                ds.setJdbcUrl(url);
                ds.setUsername(user);
                ds.setPassword(password);
                ds.setMaximumPoolSize(poolSize);

                return ds;
            }
        }
    }

    /** Returns the read timeout associated with the given {@code query}. */
    private int getQueryReadTimeout(Query<?> query) {
        if (query != null) {
            Double timeout = query.getTimeout();
            if (timeout == null) {
                timeout = getReadTimeout();
            }
            if (timeout > 0.0) {
                return (int) Math.round(timeout);
            }
        }
        return 0;
    }

    private boolean checkReplicationCache(Query<?> query) {
        return query.isCache()
                && !query.isMaster()
                && isEnableReplicationCache()
                && !Boolean.TRUE.equals(query.getOptions().get(DISABLE_REPLICATION_CACHE_QUERY_OPTION))
                && mysqlBinaryLogReader != null
                && mysqlBinaryLogReader.isConnected();
    }

    private boolean checkFunnelCache(Query<?> query) {
        return query.isCache()
                && !query.isReferenceOnly()
                && isEnableFunnelCache()
                && !Boolean.TRUE.equals(query.getOptions().get(Database.DISABLE_FUNNEL_CACHE_QUERY_OPTION))
                && funnelCache != null;
    }

    @Override
    public <T> List<T> readAll(Query<T> query) {
        if (checkReplicationCache(query)) {
            List<Object> ids = query.findIdOnlyQueryValues();

            if (ids != null && !ids.isEmpty()) {
                List<T> objects = findObjectsFromReplicationCache(ids, query);

                return objects != null ? objects : new ArrayList<T>();
            }
        }

        return selectListWithOptions(buildSelectStatement(query), query);
    }

    @Override
    public long readCount(Query<?> query) {
        String sqlQuery = buildCountStatement(query);
        Connection connection = null;
        Statement statement = null;
        ResultSet result = null;

        try {
            connection = openQueryConnection(query);
            statement = connection.createStatement();
            result = executeQueryBeforeTimeout(statement, sqlQuery, getQueryReadTimeout(query));

            if (result.next()) {
                Object countObj = result.getObject(1);
                if (countObj instanceof Number) {
                    return ((Number) countObj).longValue();
                }
            }

            return 0;

        } catch (SQLException ex) {
            throw createQueryException(ex, sqlQuery, query);

        } finally {
            closeResources(query, connection, statement, result);
        }
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

        return selectFirstWithOptions(buildSelectStatement(query), query);
    }

    @Override
    public <T> Iterable<T> readIterable(Query<T> query, int fetchSize) {
        Boolean useJdbc = ObjectUtils.to(Boolean.class, query.getOptions().get(USE_JDBC_FETCH_SIZE_QUERY_OPTION));
        if (useJdbc == null) {
            useJdbc = Boolean.TRUE;
        }
        if (useJdbc) {
            return selectIterableWithOptions(buildSelectStatement(query), fetchSize, query);
        } else {
            return new ByIdIterable<T>(query, fetchSize);
        }
    }

    private static class ByIdIterable<T> implements Iterable<T> {

        private final Query<T> query;
        private final int fetchSize;

        public ByIdIterable(Query<T> query, int fetchSize) {
            this.query = query;
            this.fetchSize = fetchSize;
        }

        @Override
        public Iterator<T> iterator() {
            return new ByIdIterator<T>(query, fetchSize);
        }
    }

    private static class ByIdIterator<T> implements Iterator<T> {

        private final Query<T> query;
        private final int fetchSize;
        private UUID lastTypeId;
        private UUID lastId;
        private List<T> items;
        private int index;

        public ByIdIterator(Query<T> query, int fetchSize) {
            if (!query.getSorters().isEmpty()) {
                throw new IllegalArgumentException("Can't iterate over a query that has sorters!");
            }

            this.query = query.clone().timeout(0.0).sortAscending("_type").sortAscending("_id");
            this.fetchSize = fetchSize > 0 ? fetchSize : 200;
        }

        @Override
        public boolean hasNext() {
            if (items != null && items.isEmpty()) {
                return false;
            }

            if (items == null || index >= items.size()) {
                Query<T> nextQuery = query.clone();
                if (lastTypeId != null) {
                    nextQuery.and("_type = ? and _id > ?", lastTypeId, lastId);
                }

                items = nextQuery.select(0, fetchSize).getItems();

                int size = items.size();
                if (size < 1) {
                    if (lastTypeId == null) {
                        return false;

                    } else {
                        nextQuery = query.clone().and("_type > ?", lastTypeId);
                        items = nextQuery.select(0, fetchSize).getItems();
                        size = items.size();

                        if (size < 1) {
                            return false;
                        }
                    }
                }

                State lastState = State.getInstance(items.get(size - 1));
                lastTypeId = lastState.getVisibilityAwareTypeId();
                lastId = lastState.getId();
                index = 0;
            }

            return true;
        }

        @Override
        public T next() {
            if (hasNext()) {
                T object = items.get(index);
                ++ index;
                return object;

            } else {
                throw new NoSuchElementException();
            }
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    @Override
    public Date readLastUpdate(Query<?> query) {
        String sqlQuery = buildLastUpdateStatement(query);
        Connection connection = null;
        Statement statement = null;
        ResultSet result = null;

        try {
            connection = openQueryConnection(query);
            statement = connection.createStatement();
            result = executeQueryBeforeTimeout(statement, sqlQuery, getQueryReadTimeout(query));

            if (result.next()) {
                Double date = ObjectUtils.to(Double.class, result.getObject(1));
                if (date != null) {
                    return new Date((long) (date * 1000L));
                }
            }

            return null;

        } catch (SQLException ex) {
            throw createQueryException(ex, sqlQuery, query);

        } finally {
            closeResources(query, connection, statement, result);
        }
    }

    @Override
    public <T> PaginatedResult<T> readPartial(final Query<T> query, long offset, int limit) {
        // Guard against integer overflow
        if (limit == Integer.MAX_VALUE) {
            limit --;
        }
        List<T> objects = selectListWithOptions(
                vendor.rewriteQueryWithLimitClause(buildSelectStatement(query), limit + 1, offset),
                query);

        int size = objects.size();
        if (size <= limit) {
            return new PaginatedResult<T>(offset, limit, offset + size, objects);

        } else {
            objects.remove(size - 1);
            return new PaginatedResult<T>(offset, limit, 0, objects) {

                private Long count;

                @Override
                public long getCount() {
                    if (count == null) {
                        count = readCount(query);
                    }
                    return count;
                }

                @Override
                public boolean hasNext() {
                    return true;
                }
            };
        }
    }

    @Override
    public <T> PaginatedResult<Grouping<T>> readPartialGrouped(Query<T> query, long offset, int limit, String... fields) {
        for (String field : fields) {
            Matcher groupingMatcher = Query.RANGE_PATTERN.matcher(field);
            if (groupingMatcher.find()) {
                throw new UnsupportedOperationException("SqlDatabase does not support group by numeric range");
            }
        }

        List<Grouping<T>> groupings = new ArrayList<Grouping<T>>();
        String sqlQuery = buildGroupStatement(query, fields);
        Connection connection = null;
        Statement statement = null;
        ResultSet result = null;

        try {
            connection = openQueryConnection(query);
            statement = connection.createStatement();
            result = executeQueryBeforeTimeout(statement, sqlQuery, getQueryReadTimeout(query));

            int fieldsLength = fields.length;
            int groupingsCount = 0;

            for (int i = 0, last = (int) offset + limit; result.next(); ++ i, ++ groupingsCount) {
                if (i < offset || i >= last) {
                    continue;
                }

                List<Object> keys = new ArrayList<Object>();

                SqlGrouping<T> grouping;
                ResultSetMetaData meta = result.getMetaData();
                String aggregateColumnName = meta.getColumnName(1);
                if ("_count".equals(aggregateColumnName)) {
                    long count = ObjectUtils.to(long.class, result.getObject(1));
                    for (int j = 0; j < fieldsLength; ++ j) {
                        keys.add(result.getObject(j + 2));
                    }
                    grouping = new SqlGrouping<T>(keys, query, fields, count, groupings);
                } else {
                    Double amount = ObjectUtils.to(Double.class, result.getObject(1));
                    for (int j = 0; j < fieldsLength; ++ j) {
                        keys.add(result.getObject(j + 3));
                    }
                    long count = 0L;
                    if (meta.getColumnName(2).equals("_count")) {
                        count = ObjectUtils.to(long.class, result.getObject(2));
                    }
                    grouping = new SqlGrouping<T>(keys, query, fields, count, groupings);
                    if (amount == null) {
                        amount = 0d;
                    }
                    grouping.setSum(aggregateColumnName, amount);
                }
                groupings.add(grouping);
            }

            int groupingsSize = groupings.size();
            List<Integer> removes = new ArrayList<Integer>();

            for (int i = 0; i < fieldsLength; ++ i) {
                Query.MappedKey key = query.mapEmbeddedKey(getEnvironment(), fields[i]);
                ObjectField field = key.getSubQueryKeyField();
                if (field == null) {
                    field = key.getField();
                }

                if (field != null) {
                    Map<String, Object> rawKeys = new HashMap<String, Object>();
                    for (int j = 0; j < groupingsSize; ++ j) {
                        rawKeys.put(String.valueOf(j), groupings.get(j).getKeys().get(i));
                    }

                    String itemType = field.getInternalItemType();
                    if (ObjectField.RECORD_TYPE.equals(itemType)) {
                        for (Map.Entry<String, Object> entry : rawKeys.entrySet()) {
                            Map<String, Object> ref = new HashMap<String, Object>();
                            ref.put(StateSerializer.REFERENCE_KEY, entry.getValue());
                            entry.setValue(ref);
                        }
                    }

                    Map<String, Object> rawKeysCopy = new HashMap<String, Object>(rawKeys);
                    Map<?, ?> convertedKeys = (Map<?, ?>) StateValueUtils.toJavaValue(query.getDatabase(), null, field, "map/" + itemType, rawKeys);

                    for (int j = 0; j < groupingsSize; ++ j) {
                        String jString = String.valueOf(j);
                        Object convertedKey = convertedKeys.get(jString);

                        if (convertedKey == null
                                && rawKeysCopy.get(jString) != null) {
                            removes.add(j - removes.size());
                        }

                        groupings.get(j).getKeys().set(i, convertedKey);
                    }
                }
            }

            for (Integer i : removes) {
                groupings.remove((int) i);
            }

            return new PaginatedResult<Grouping<T>>(offset, limit, groupingsCount - removes.size(), groupings);

        } catch (SQLException ex) {
            throw createQueryException(ex, sqlQuery, query);

        } finally {
            closeResources(query, connection, statement, result);
        }
    }

    /** SQL-specific implementation of {@link Grouping}. */
    private class SqlGrouping<T> extends AbstractGrouping<T> {

        private final long count;

        private final Map<String, Double> metricSums = new HashMap<String, Double>();

        private final List<Grouping<T>> groupings;

        public SqlGrouping(List<Object> keys, Query<T> query, String[] fields, long count, List<Grouping<T>> groupings) {
            super(keys, query, fields);
            this.count = count;
            this.groupings = groupings;
        }

        @Override
        public double getSum(String field) {
            Query.MappedKey mappedKey = this.query.mapEmbeddedKey(getEnvironment(), field);
            ObjectField sumField = mappedKey.getField();
            if (sumField.isMetric()) {
                if (!metricSums.containsKey(field)) {

                    String sqlQuery = buildGroupedMetricStatement(query, field, fields);
                    Connection connection = null;
                    Statement statement = null;
                    ResultSet result = null;
                    try {
                        connection = openQueryConnection(query);
                        statement = connection.createStatement();
                        result = executeQueryBeforeTimeout(statement, sqlQuery, getQueryReadTimeout(query));

                        if (this.getKeys().size() == 0) {
                            // Special case for .groupby() without any fields
                            if (this.groupings.size() != 1) {
                                throw new RuntimeException("There should only be one grouping when grouping by nothing. Something went wrong internally.");
                            }
                            if (result.next() && result.getBytes(1) != null) {
                                this.setSum(field, result.getDouble(1));
                            } else {
                                this.setSum(field, 0);
                            }
                        } else {
                            // Find the ObjectFields for the specified fields
                            List<ObjectField> objectFields = new ArrayList<ObjectField>();
                            for (String fieldName : fields) {
                                objectFields.add(query.mapEmbeddedKey(getEnvironment(), fieldName).getField());
                            }

                            // index the groupings by their keys
                            Map<List<Object>, SqlGrouping<T>> groupingMap = new HashMap<List<Object>, SqlGrouping<T>>();
                            for (Grouping<T> grouping : groupings) {
                                if (grouping instanceof SqlGrouping) {
                                    ((SqlGrouping<T>) grouping).setSum(field, 0);
                                    groupingMap.put(grouping.getKeys(), (SqlGrouping<T>) grouping);
                                }
                            }

                            // Find the sums and set them on each grouping
                            while (result.next()) {
                                // TODO: limit/offset
                                List<Object> keys = new ArrayList<Object>();
                                for (int j = 0; j < objectFields.size(); ++ j) {
                                    keys.add(StateValueUtils.toJavaValue(query.getDatabase(), null, objectFields.get(j), objectFields.get(j).getInternalItemType(), result.getObject(j + 3))); // 3 because _count and amount
                                }
                                if (groupingMap.containsKey(keys)) {
                                    if (result.getBytes(1) != null) {
                                        groupingMap.get(keys).setSum(field, result.getDouble(1));
                                    } else {
                                        groupingMap.get(keys).setSum(field, 0);
                                    }
                                }
                            }
                        }
                    } catch (SQLException ex) {
                        throw createQueryException(ex, sqlQuery, query);
                    } finally {
                        closeResources(query, connection, statement, result);
                    }
                }
                if (metricSums.containsKey(field)) {
                    return metricSums.get(field);
                } else {
                    return 0;
                }
            } else {
                // If it's not a MetricValue, we don't need to override it.
                return super.getSum(field);
            }
       }

        private void setSum(String field, double sum) {
            metricSums.put(field, sum);
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

    /**
     * Invalidates all entries in the replication cache.
     */
    public void invalidateReplicationCache() {
        replicationCache.invalidateAll();
    }

    @Override
    protected void beginTransaction(Connection connection, boolean isImmediate) throws SQLException {
        connection.setAutoCommit(false);
    }

    @Override
    protected void commitTransaction(Connection connection, boolean isImmediate) throws SQLException {
        connection.commit();
    }

    @Override
    protected void rollbackTransaction(Connection connection, boolean isImmediate) throws SQLException {
        connection.rollback();
    }

    @Override
    protected void endTransaction(Connection connection, boolean isImmediate) throws SQLException {
        connection.setAutoCommit(true);
    }

    @Override
    protected void doSaves(Connection connection, boolean isImmediate, List<State> states) throws SQLException {
        List<State> indexStates = null;
        for (State state1 : states) {
            if (Boolean.TRUE.equals(state1.getExtra(SKIP_INDEX_STATE_EXTRA))) {
                indexStates = new ArrayList<State>();
                for (State state2 : states) {
                    if (!Boolean.TRUE.equals(state2.getExtra(SKIP_INDEX_STATE_EXTRA))) {
                        indexStates.add(state2);
                    }
                }
                break;
            }
        }

        if (indexStates == null) {
            indexStates = states;
        }

        SqlIndex.Static.deleteByStates(this, connection, indexStates);
        Map<State, String> inRowIndexes = SqlIndex.Static.insertByStates(this, connection, indexStates);
        boolean hasInRowIndex = hasInRowIndex();
        SqlVendor vendor = getVendor();
        double now = System.currentTimeMillis() / 1000.0;

        for (State state : states) {
            boolean isNew = state.isNew();
            boolean saveInRowIndex = hasInRowIndex && !Boolean.TRUE.equals(state.getExtra(SKIP_INDEX_STATE_EXTRA));
            UUID id = state.getId();
            UUID typeId = state.getVisibilityAwareTypeId();
            byte[] dataBytes = null;
            String inRowIndex = inRowIndexes.get(state);
            byte[] inRowIndexBytes = inRowIndex != null ? inRowIndex.getBytes(StandardCharsets.UTF_8) : new byte[0];

            while (true) {
                if (isNew) {
                    try {
                        if (dataBytes == null) {
                            dataBytes = serializeState(state);
                        }

                        List<Object> parameters = new ArrayList<Object>();
                        StringBuilder insertBuilder = new StringBuilder();

                        insertBuilder.append("INSERT INTO ");
                        vendor.appendIdentifier(insertBuilder, RECORD_TABLE);
                        insertBuilder.append(" (");
                        vendor.appendIdentifier(insertBuilder, ID_COLUMN);
                        insertBuilder.append(',');
                        vendor.appendIdentifier(insertBuilder, TYPE_ID_COLUMN);
                        insertBuilder.append(',');
                        vendor.appendIdentifier(insertBuilder, DATA_COLUMN);

                        if (saveInRowIndex) {
                            insertBuilder.append(',');
                            vendor.appendIdentifier(insertBuilder, IN_ROW_INDEX_COLUMN);
                        }

                        insertBuilder.append(") VALUES (");
                        vendor.appendBindValue(insertBuilder, id, parameters);
                        insertBuilder.append(',');
                        vendor.appendBindValue(insertBuilder, typeId, parameters);
                        insertBuilder.append(',');
                        vendor.appendBindValue(insertBuilder, dataBytes, parameters);

                        if (saveInRowIndex) {
                            insertBuilder.append(',');
                            vendor.appendBindValue(insertBuilder, inRowIndexBytes, parameters);
                        }

                        insertBuilder.append(')');
                        Static.executeUpdateWithList(vendor, connection, insertBuilder.toString(), parameters);

                    } catch (SQLException ex) {
                        if (Static.isIntegrityConstraintViolation(ex)) {
                            isNew = false;
                            continue;
                        } else {
                            throw ex;
                        }
                    }

                } else {
                    List<AtomicOperation> atomicOperations = state.getAtomicOperations();
                    if (atomicOperations.isEmpty()) {
                        if (dataBytes == null) {
                            dataBytes = serializeState(state);
                        }

                        List<Object> parameters = new ArrayList<Object>();
                        StringBuilder updateBuilder = new StringBuilder();

                        updateBuilder.append("UPDATE ");
                        vendor.appendIdentifier(updateBuilder, RECORD_TABLE);
                        updateBuilder.append(" SET ");
                        vendor.appendIdentifier(updateBuilder, TYPE_ID_COLUMN);
                        updateBuilder.append('=');
                        vendor.appendBindValue(updateBuilder, typeId, parameters);
                        updateBuilder.append(',');

                        if (saveInRowIndex) {
                            vendor.appendIdentifier(updateBuilder, IN_ROW_INDEX_COLUMN);
                            updateBuilder.append('=');
                            vendor.appendBindValue(updateBuilder, inRowIndexBytes, parameters);
                            updateBuilder.append(',');
                        }

                        vendor.appendIdentifier(updateBuilder, DATA_COLUMN);
                        updateBuilder.append('=');
                        vendor.appendBindValue(updateBuilder, dataBytes, parameters);
                        updateBuilder.append(" WHERE ");
                        vendor.appendIdentifier(updateBuilder, ID_COLUMN);
                        updateBuilder.append('=');
                        vendor.appendBindValue(updateBuilder, id, parameters);

                        if (Static.executeUpdateWithList(vendor, connection, updateBuilder.toString(), parameters) < 1) {
                            isNew = true;
                            continue;
                        }

                    } else {
                        Object oldObject = Query
                                .from(Object.class)
                                .where("_id = ?", id)
                                .using(this)
                                .option(CONNECTION_QUERY_OPTION, connection)
                                .option(RETURN_ORIGINAL_DATA_QUERY_OPTION, Boolean.TRUE)
                                .option(USE_READ_DATA_SOURCE_QUERY_OPTION, Boolean.FALSE)
                                .first();
                        if (oldObject == null) {
                            retryWrites();
                            break;
                        }

                        State oldState = State.getInstance(oldObject);
                        UUID oldTypeId = oldState.getVisibilityAwareTypeId();
                        byte[] oldData = Static.getOriginalData(oldObject);

                        state.setValues(oldState.getValues());

                        for (AtomicOperation operation : atomicOperations) {
                            String field = operation.getField();
                            state.putByPath(field, oldState.getByPath(field));
                        }

                        for (AtomicOperation operation : atomicOperations) {
                            operation.execute(state);
                        }

                        dataBytes = serializeState(state);

                        List<Object> parameters = new ArrayList<Object>();
                        StringBuilder updateBuilder = new StringBuilder();

                        updateBuilder.append("UPDATE ");
                        vendor.appendIdentifier(updateBuilder, RECORD_TABLE);
                        updateBuilder.append(" SET ");
                        vendor.appendIdentifier(updateBuilder, TYPE_ID_COLUMN);
                        updateBuilder.append('=');
                        vendor.appendBindValue(updateBuilder, typeId, parameters);

                        if (saveInRowIndex) {
                            updateBuilder.append(',');
                            vendor.appendIdentifier(updateBuilder, IN_ROW_INDEX_COLUMN);
                            updateBuilder.append('=');
                            vendor.appendBindValue(updateBuilder, inRowIndexBytes, parameters);
                        }

                        updateBuilder.append(',');
                        vendor.appendIdentifier(updateBuilder, DATA_COLUMN);
                        updateBuilder.append('=');
                        vendor.appendBindValue(updateBuilder, dataBytes, parameters);
                        updateBuilder.append(" WHERE ");
                        vendor.appendIdentifier(updateBuilder, ID_COLUMN);
                        updateBuilder.append('=');
                        vendor.appendBindValue(updateBuilder, id, parameters);
                        updateBuilder.append(" AND ");
                        vendor.appendIdentifier(updateBuilder, TYPE_ID_COLUMN);
                        updateBuilder.append('=');
                        vendor.appendBindValue(updateBuilder, oldTypeId, parameters);
                        updateBuilder.append(" AND ");
                        vendor.appendIdentifier(updateBuilder, DATA_COLUMN);
                        updateBuilder.append('=');
                        vendor.appendBindValue(updateBuilder, oldData, parameters);

                        if (Static.executeUpdateWithList(vendor, connection, updateBuilder.toString(), parameters) < 1) {
                            retryWrites();
                            break;
                        }
                    }
                }

                break;
            }

            while (true) {
                if (isNew) {
                    List<Object> parameters = new ArrayList<Object>();
                    StringBuilder insertBuilder = new StringBuilder();

                    insertBuilder.append("INSERT INTO ");
                    vendor.appendIdentifier(insertBuilder, RECORD_UPDATE_TABLE);
                    insertBuilder.append(" (");
                    vendor.appendIdentifier(insertBuilder, ID_COLUMN);
                    insertBuilder.append(',');
                    vendor.appendIdentifier(insertBuilder, TYPE_ID_COLUMN);
                    insertBuilder.append(',');
                    vendor.appendIdentifier(insertBuilder, UPDATE_DATE_COLUMN);
                    insertBuilder.append(") VALUES (");
                    vendor.appendBindValue(insertBuilder, id, parameters);
                    insertBuilder.append(',');
                    vendor.appendBindValue(insertBuilder, typeId, parameters);
                    insertBuilder.append(',');
                    vendor.appendBindValue(insertBuilder, now, parameters);
                    insertBuilder.append(')');

                    try {
                        Static.executeUpdateWithList(vendor, connection, insertBuilder.toString(), parameters);

                    } catch (SQLException ex) {
                        if (Static.isIntegrityConstraintViolation(ex)) {
                            isNew = false;
                            continue;
                        } else {
                            throw ex;
                        }
                    }

                } else {
                    List<Object> parameters = new ArrayList<Object>();
                    StringBuilder updateBuilder = new StringBuilder();

                    updateBuilder.append("UPDATE ");
                    vendor.appendIdentifier(updateBuilder, RECORD_UPDATE_TABLE);
                    updateBuilder.append(" SET ");
                    vendor.appendIdentifier(updateBuilder, TYPE_ID_COLUMN);
                    updateBuilder.append('=');
                    vendor.appendBindValue(updateBuilder, typeId, parameters);
                    updateBuilder.append(',');
                    vendor.appendIdentifier(updateBuilder, UPDATE_DATE_COLUMN);
                    updateBuilder.append('=');
                    vendor.appendBindValue(updateBuilder, now, parameters);
                    updateBuilder.append(" WHERE ");
                    vendor.appendIdentifier(updateBuilder, ID_COLUMN);
                    updateBuilder.append('=');
                    vendor.appendBindValue(updateBuilder, id, parameters);

                    if (Static.executeUpdateWithList(vendor, connection, updateBuilder.toString(), parameters) < 1) {
                        isNew = true;
                        continue;
                    }
                }

                break;
            }
        }
    }

    @Override
    protected void doIndexes(Connection connection, boolean isImmediate, List<State> states) throws SQLException {
        SqlIndex.Static.deleteByStates(this, connection, states);
        Map<State, String> inRowIndexes = SqlIndex.Static.insertByStates(this, connection, states);

        if (!hasInRowIndex()) {
            return;
        }

        SqlVendor vendor = getVendor();
        for (Map.Entry<State, String> entry : inRowIndexes.entrySet()) {
            StringBuilder updateBuilder = new StringBuilder();
            updateBuilder.append("UPDATE ");
            vendor.appendIdentifier(updateBuilder, RECORD_TABLE);
            updateBuilder.append(" SET ");
            vendor.appendIdentifier(updateBuilder, IN_ROW_INDEX_COLUMN);
            updateBuilder.append('=');
            vendor.appendValue(updateBuilder, entry.getValue());
            updateBuilder.append(" WHERE ");
            vendor.appendIdentifier(updateBuilder, ID_COLUMN);
            updateBuilder.append('=');
            vendor.appendValue(updateBuilder, entry.getKey().getId());
            Static.executeUpdateWithArray(vendor, connection, updateBuilder.toString());
        }
    }

    @Override
    public void doRecalculations(Connection connection, boolean isImmediate, ObjectIndex index, List<State> states) throws SQLException {
        SqlIndex.Static.updateByStates(this, connection, index, states);
    }

    /** @deprecated Use {@link #index} instead. */
    @Deprecated
    public void fixIndexes(List<State> states) {
        Connection connection = openConnection();

        try {
            doIndexes(connection, true, states);

        } catch (SQLException ex) {
            List<UUID> ids = new ArrayList<UUID>();
            for (State state : states) {
                ids.add(state.getId());
            }
            throw new SqlDatabaseException(this, String.format(
                    "Can't index states! (%s)", ids));

        } finally {
            closeConnection(connection);
        }
    }

    @Override
    protected void doDeletes(Connection connection, boolean isImmediate, List<State> states) throws SQLException {
        SqlVendor vendor = getVendor();

        StringBuilder whereBuilder = new StringBuilder();
        whereBuilder.append(" WHERE ");
        vendor.appendIdentifier(whereBuilder, ID_COLUMN);
        whereBuilder.append(" IN (");

        for (State state : states) {
            vendor.appendValue(whereBuilder, state.getId());
            whereBuilder.append(',');
        }

        whereBuilder.setCharAt(whereBuilder.length() - 1, ')');

        StringBuilder deleteBuilder = new StringBuilder();
        deleteBuilder.append("DELETE FROM ");
        vendor.appendIdentifier(deleteBuilder, RECORD_TABLE);
        deleteBuilder.append(whereBuilder);
        Static.executeUpdateWithArray(vendor, connection, deleteBuilder.toString());

        SqlIndex.Static.deleteByStates(this, connection, states);

        StringBuilder updateBuilder = new StringBuilder();
        updateBuilder.append("UPDATE ");
        vendor.appendIdentifier(updateBuilder, RECORD_UPDATE_TABLE);
        updateBuilder.append(" SET ");
        vendor.appendIdentifier(updateBuilder, UPDATE_DATE_COLUMN);
        updateBuilder.append('=');
        vendor.appendValue(updateBuilder, System.currentTimeMillis() / 1000.0);
        updateBuilder.append(whereBuilder);
        Static.executeUpdateWithArray(vendor, connection, updateBuilder.toString());
    }

    @Override
    public void addUpdateNotifier(UpdateNotifier<?> notifier) {
        updateNotifiers.add(notifier);
    }

    @Override
    public void removeUpdateNotifier(UpdateNotifier<?> notifier) {
        updateNotifiers.remove(notifier);
    }

    protected void notifyUpdate(Object object) {
        NOTIFIER: for (UpdateNotifier<?> notifier : updateNotifiers) {
            for (Type notifierInterface : notifier.getClass().getGenericInterfaces()) {
                if (notifierInterface instanceof ParameterizedType) {
                    ParameterizedType pt = (ParameterizedType) notifierInterface;
                    Type rt = pt.getRawType();

                    if (rt instanceof Class
                            && UpdateNotifier.class.isAssignableFrom((Class<?>) rt)) {

                        Type[] args = pt.getActualTypeArguments();

                        if (args.length > 0) {
                            Type arg = args[0];

                            if (arg instanceof Class
                                    && !((Class<?>) arg).isInstance(object)) {
                                continue NOTIFIER;

                            } else {
                                break;
                            }
                        }
                    }
                }
            }

            @SuppressWarnings("unchecked")
            UpdateNotifier<Object> objectNotifier = (UpdateNotifier<Object>) notifier;

            try {
                objectNotifier.onUpdate(object);

            } catch (Exception error) {
                LOGGER.warn(
                        String.format(
                                "Can't notify [%s] of [%s] update!",
                                notifier,
                                State.getInstance(object).getId()),
                        error);
            }
        }
    }

    @FieldData.FieldInternalNamePrefix("sql.")
    public static class FieldData extends Modification<ObjectField> {

        private String indexTable;
        private String indexTableColumnName;
        private boolean indexTableReadOnly;
        private boolean indexTableSameColumnNames;
        private boolean indexTableSource;

        public String getIndexTable() {
            return indexTable;
        }

        public void setIndexTable(String indexTable) {
            this.indexTable = indexTable;
        }

        public boolean isIndexTableReadOnly() {
            return indexTableReadOnly;
        }

        public void setIndexTableReadOnly(boolean indexTableReadOnly) {
            this.indexTableReadOnly = indexTableReadOnly;
        }

        public boolean isIndexTableSameColumnNames() {
            return indexTableSameColumnNames;
        }

        public void setIndexTableSameColumnNames(boolean indexTableSameColumnNames) {
            this.indexTableSameColumnNames = indexTableSameColumnNames;
        }

        public void setIndexTableColumnName(String indexTableColumnName) {
            this.indexTableColumnName = indexTableColumnName;
        }

        public String getIndexTableColumnName() {
            return this.indexTableColumnName;
        }

        public boolean isIndexTableSource() {
            return indexTableSource;
        }

        public void setIndexTableSource(boolean indexTableSource) {
            this.indexTableSource = indexTableSource;
        }

        public boolean isIndexTableSourceFromAnywhere() {
            if (isIndexTableSource()) {
                return true;
            }

            ObjectField field = getOriginalObject();
            ObjectStruct parent = field.getParent();
            String fieldName = field.getInternalName();

            for (ObjectIndex index : parent.getIndexes()) {
                List<String> indexFieldNames = index.getFields();

                if (!indexFieldNames.isEmpty()
                        && indexFieldNames.contains(fieldName)) {
                    String firstIndexFieldName = indexFieldNames.get(0);

                    if (!fieldName.equals(firstIndexFieldName)) {
                        ObjectField firstIndexField = parent.getField(firstIndexFieldName);

                        if (firstIndexField != null) {
                            return firstIndexField.as(FieldData.class).isIndexTableSource();
                        }
                    }
                }
            }

            return false;
        }
    }

    /** Specifies the name of the table for storing target field values. */
    @Documented
    @ObjectField.AnnotationProcessorClass(FieldIndexTableProcessor.class)
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.FIELD)
    public @interface FieldIndexTable {
        String value();
        boolean readOnly() default false;
        boolean sameColumnNames() default false;
        boolean source() default false;
    }

    private static class FieldIndexTableProcessor implements ObjectField.AnnotationProcessor<FieldIndexTable> {
        @Override
        public void process(ObjectType type, ObjectField field, FieldIndexTable annotation) {
            FieldData data = field.as(FieldData.class);

            data.setIndexTable(annotation.value());
            data.setIndexTableSameColumnNames(annotation.sameColumnNames());
            data.setIndexTableSource(annotation.source());
            data.setIndexTableReadOnly(annotation.readOnly());
        }
    }

    private static final class FunnelCacheProducer implements FunnelCachedObjectProducer<SqlDatabase> {

        private final String sqlQuery;
        private final Query<?> query;

        private FunnelCacheProducer(String sqlQuery, Query<?> query) {
            this.query = query;
            this.sqlQuery = sqlQuery;
        }

        @Override
        public boolean equals(Object other) {
            if (!(other instanceof FunnelCacheProducer)) {
                return false;
            }
            FunnelCacheProducer otherProducer = (FunnelCacheProducer) other;
            return (otherProducer.sqlQuery + otherProducer.query.getOptions().get(RETURN_ORIGINAL_DATA_QUERY_OPTION))
                    .equals(sqlQuery + query.getOptions().get(RETURN_ORIGINAL_DATA_QUERY_OPTION));
        }

        @Override
        public int hashCode() {
            return sqlQuery.hashCode();
        }

        @Override
        public List<FunnelCachedObject> produce(SqlDatabase db) {
            ConnectionRef extraConnectionRef = db.new ConnectionRef();
            Connection connection = null;
            Statement statement = null;
            ResultSet result = null;
            List<FunnelCachedObject> objects = new ArrayList<FunnelCachedObject>();
            int timeout = db.getQueryReadTimeout(query);
            Profiler.Static.startThreadEvent(FUNNEL_CACHE_PUT_PROFILER_EVENT);

            try {
                connection = db.openQueryConnection(query);
                statement = connection.createStatement();
                result = db.executeQueryBeforeTimeout(statement, sqlQuery, timeout);
                while (result.next()) {
                    UUID id = ObjectUtils.to(UUID.class, result.getObject(1));
                    UUID typeId = ObjectUtils.to(UUID.class, result.getObject(2));
                    byte[] data = result.getBytes(3);
                    Map<String, Object> dataJson = unserializeData(data);
                    Map<String, Object> extras = null;
                    if (Boolean.TRUE.equals(ObjectUtils.to(Boolean.class, query.getOptions().get(RETURN_ORIGINAL_DATA_QUERY_OPTION)))) {
                        extras = new CompactMap<String, Object>();
                        extras.put(ORIGINAL_DATA_EXTRA, data);
                    }

                    objects.add(new FunnelCachedObject(id, typeId, dataJson, extras));
                }

                return objects;

            } catch (SQLException ex) {
                throw db.createQueryException(ex, sqlQuery, query);

            } finally {
                Profiler.Static.stopThreadEvent(objects);
                db.closeResources(query, connection, statement, result);
                extraConnectionRef.close();
            }
        }

        @Override
        public String toString() {
            return sqlQuery;
        }
    }

    /** {@link SqlDatabase} utility methods. */
    public static final class Static {

        public static List<SqlDatabase> getAll() {
            return INSTANCES;
        }

        public static void deregisterAllDrivers() {
            for (WeakReference<Driver> driverRef : REGISTERED_DRIVERS) {
                Driver driver = driverRef.get();
                if (driver != null) {
                    LOGGER.info("Deregistering [{}]", driver);
                    try {
                        DriverManager.deregisterDriver(driver);
                    } catch (SQLException ex) {
                        LOGGER.warn("Can't deregister [{}]!", driver);
                    }
                }
            }
        }

        /**
         * Log a batch update exception with values.
         */
        static void logBatchUpdateException(BatchUpdateException bue, String sqlQuery, List<? extends List<?>> parameters) {
            int i = 0;

            StringBuilder errorBuilder = new StringBuilder();
            for (int code : bue.getUpdateCounts()) {
                if (code == Statement.EXECUTE_FAILED) {
                    List<?> rowData = parameters.get(i);

                    errorBuilder.append("Batch update failed with query '");
                    errorBuilder.append(sqlQuery);
                    errorBuilder.append("' with values (");
                    int o = 0;
                    for (Object value : rowData) {
                        if (o++ != 0) {
                            errorBuilder.append(", ");
                        }

                        if (value instanceof byte[]) {
                            errorBuilder.append(StringUtils.hex((byte[]) value));
                        } else {
                            errorBuilder.append(value);
                        }
                    }
                    errorBuilder.append(')');
                }

                i++;
            }

            Exception ex = bue.getNextException() != null ? bue.getNextException() : bue;
            LOGGER.error(errorBuilder.toString(), ex);
        }

        static void logUpdateException(String sqlQuery, List<?> parameters) {
            int i = 0;

            StringBuilder errorBuilder = new StringBuilder();
            errorBuilder.append("Batch update failed with query '");
            errorBuilder.append(sqlQuery);
            errorBuilder.append("' with values (");
            for (Object value : parameters) {
                if (i++ != 0) {
                    errorBuilder.append(", ");
                }

                if (value instanceof byte[]) {
                    errorBuilder.append(StringUtils.hex((byte[]) value));
                } else {
                    errorBuilder.append(value);
                }
            }
            errorBuilder.append(')');

            LOGGER.error(errorBuilder.toString());
        }

        // Safely binds the given parameter to the given statement at the
        // given index.
        private static void bindParameter(PreparedStatement statement, int index, Object parameter) throws SQLException {
            if (parameter instanceof String) {
                parameter = ((String) parameter).getBytes(StandardCharsets.UTF_8);
            }

            if (parameter instanceof StringBuilder) {
                parameter = ((StringBuilder) parameter).toString();
            }

            if (parameter instanceof byte[]) {
                byte[] parameterBytes = (byte[]) parameter;
                int parameterBytesLength = parameterBytes.length;
                if (parameterBytesLength > 2000) {
                    statement.setBinaryStream(index, new ByteArrayInputStream(parameterBytes), parameterBytesLength);
                    return;
                }
            }

            statement.setObject(index, parameter);
        }

        /**
         * Executes the given batch update {@code sqlQuery} with the given
         * list of {@code parameters} within the given {@code connection}.
         *
         * @return Array of number of rows affected by the update query.
         */
        public static int[] executeBatchUpdate(
                Connection connection,
                String sqlQuery,
                List<? extends List<?>> parameters) throws SQLException {

            PreparedStatement prepared = connection.prepareStatement(sqlQuery);
            List<?> currentRow = null;

            try {
                for (List<?> row : parameters) {
                    currentRow = row;
                    int columnIndex = 1;

                    for (Object parameter : row) {
                        bindParameter(prepared, columnIndex, parameter);
                        columnIndex++;
                    }

                    prepared.addBatch();
                }

                int[] affected = null;
                Stats.Timer timer = STATS.startTimer();
                Profiler.Static.startThreadEvent(UPDATE_PROFILER_EVENT);

                try {
                    affected = prepared.executeBatch();

                    return affected;

                } finally {
                    double time = timer.stop(UPDATE_STATS_OPERATION);
                    Profiler.Static.stopThreadEvent(sqlQuery);

                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug(
                                "SQL batch update: [{}], Parameters: {}, Affected: {}, Time: [{}]ms",
                                new Object[] { sqlQuery, parameters, affected != null ? Arrays.toString(affected) : "[]", time * 1000.0 });
                    }
                }

            } catch (SQLException error) {
                logUpdateException(sqlQuery, currentRow);
                throw error;

            } finally {
                try {
                    prepared.close();
                } catch (SQLException error) {
                    // Not likely and probably harmless.
                }
            }
        }

        /**
         * Executes the given update {@code sqlQuery} with the given
         * {@code parameters} within the given {@code connection}.
         *
         * @return Number of rows affected by the update query.
         *
         * @deprecated Use {@link #executeUpdateWithList(SqlVendor, Connection, String, List)} instead.
         */
        @Deprecated
        public static int executeUpdateWithList(
                Connection connection,
                String sqlQuery,
                List<?> parameters)
                throws SQLException {

            return executeUpdateWithList(null, connection, sqlQuery, parameters);
        }

        /**
         * Executes the given update {@code sqlQuery} with the given
         * {@code parameters} within the given {@code connection}.
         *
         * @return Number of rows affected by the update query.
         */
        public static int executeUpdateWithList(
                SqlVendor vendor,
                Connection connection,
                String sqlQuery,
                List<?> parameters)
                throws SQLException {

            if (parameters == null) {
                return executeUpdateWithArray(vendor, connection, sqlQuery);

            } else {
                Object[] array = parameters.toArray(new Object[parameters.size()]);
                return executeUpdateWithArray(vendor, connection, sqlQuery, array);
            }
        }

        /**
         * Executes the given update {@code sqlQuery} with the given
         * {@code parameters} within the given {@code connection}.
         *
         * @return Number of rows affected by the update query.
         *
         * @deprecated Use {@link #executeUpdateWithArray(SqlVendor, Connection, String, Object...)} instead.
         */
        @Deprecated
        public static int executeUpdateWithArray(
                Connection connection,
                String sqlQuery,
                Object... parameters)
                throws SQLException {

            return executeUpdateWithArray(null, connection, sqlQuery, parameters);
        }

        /**
         * Executes the given update {@code sqlQuery} with the given
         * {@code parameters} within the given {@code connection}.
         *
         * @return Number of rows affected by the update query.
         */
        public static int executeUpdateWithArray(
                SqlVendor vendor,
                Connection connection,
                String sqlQuery,
                Object... parameters)
                throws SQLException {

            boolean hasParameters = parameters != null && parameters.length > 0;
            PreparedStatement prepared;
            Statement statement;

            if (hasParameters) {
                prepared = connection.prepareStatement(sqlQuery);
                statement = prepared;

            } else {
                prepared = null;
                statement = connection.createStatement();
            }

            try {
                if (hasParameters) {
                    for (int i = 0; i < parameters.length; i++) {
                        bindParameter(prepared, i + 1, parameters[i]);
                    }
                }

                Integer affected = null;
                Stats.Timer timer = STATS.startTimer();
                Profiler.Static.startThreadEvent(UPDATE_PROFILER_EVENT);
                Savepoint savePoint = null;

                try {
                    if ((vendor == null || vendor.useSavepoint()) && !connection.getAutoCommit()) {
                        savePoint = connection.setSavepoint();
                    }

                    affected = hasParameters
                            ? prepared.executeUpdate()
                            : statement.executeUpdate(sqlQuery);

                    return affected;
                } catch (SQLException sqlEx) {
                    if (savePoint != null) {
                        try {
                            connection.rollback(savePoint);

                        } catch (SQLException error) {
                            // Safe to ignore?
                        }
                    }

                    throw sqlEx;

                } finally {
                    if (savePoint != null) {
                        try {
                            connection.releaseSavepoint(savePoint);

                        } catch (SQLException error) {
                            // Safe to ignore?
                        }
                    }

                    double time = timer.stop(UPDATE_STATS_OPERATION);
                    Profiler.Static.stopThreadEvent(sqlQuery);

                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug(
                                "SQL update: [{}], Affected: [{}], Time: [{}]ms",
                                new Object[] { fillPlaceholders(sqlQuery, parameters), affected, time * 1000.0 });
                    }
                }

            } finally {
                try {
                    statement.close();
                } catch (SQLException error) {
                    // Not likely and probably harmless.
                }
            }
        }

        /**
         * Returns {@code true} if the given {@code error} looks like a
         * {@link SQLIntegrityConstraintViolationException}.
         */
        public static boolean isIntegrityConstraintViolation(SQLException error) {
            if (error instanceof SQLIntegrityConstraintViolationException) {
                return true;
            } else {
                String state = error.getSQLState();
                return state != null && state.startsWith("23");
            }
        }

        /**
         * Returns the name of the table for storing the values of the
         * given {@code index}.
         */
        public static String getIndexTable(ObjectIndex index) {
            return ObjectUtils.to(String.class, index.getOptions().get(INDEX_TABLE_INDEX_OPTION));
        }

        /**
         * Sets the name of the table for storing the values of the
         * given {@code index}.
         */
        public static void setIndexTable(ObjectIndex index, String table) {
            index.getOptions().put(INDEX_TABLE_INDEX_OPTION, table);
        }

        public static Object getExtraColumn(Object object, String name) {
            return State.getInstance(object).getExtra(EXTRA_COLUMN_EXTRA_PREFIX + name);
        }

        public static byte[] getOriginalData(Object object) {
            return (byte[]) State.getInstance(object).getExtra(ORIGINAL_DATA_EXTRA);
        }

        // --- Deprecated ---

        /** @deprecated Use {@link #executeUpdateWithArray} instead. */
        @Deprecated
        public static int executeUpdate(
                Connection connection,
                String sqlQuery,
                Object... parameters)
                throws SQLException {

            return executeUpdateWithArray(connection, sqlQuery, parameters);
        }
    }

    // --- Deprecated ---

    /** @deprecated No replacement. */
    @Deprecated
    public void beginThreadLocalReadConnection() {
    }

    /** @deprecated No replacement. */
    @Deprecated
    public void endThreadLocalReadConnection() {
    }
}
