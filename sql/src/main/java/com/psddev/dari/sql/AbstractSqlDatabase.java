package com.psddev.dari.sql;

import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.psddev.dari.db.AbstractDatabase;
import com.psddev.dari.db.AtomicOperation;
import com.psddev.dari.db.ComparisonPredicate;
import com.psddev.dari.db.Grouping;
import com.psddev.dari.db.ObjectField;
import com.psddev.dari.db.ObjectIndex;
import com.psddev.dari.db.Query;
import com.psddev.dari.db.Sorter;
import com.psddev.dari.db.State;
import com.psddev.dari.db.StateSerializer;
import com.psddev.dari.db.UpdateNotifier;
import com.psddev.dari.util.IoUtils;
import com.psddev.dari.util.Lazy;
import com.psddev.dari.util.ObjectUtils;
import com.psddev.dari.util.PaginatedResult;
import com.psddev.dari.util.Profiler;
import com.psddev.dari.util.Settings;
import com.psddev.dari.util.SettingsException;
import com.psddev.dari.util.Stats;
import com.psddev.dari.util.StringUtils;
import com.psddev.dari.util.TypeDefinition;
import org.jooq.BatchBindStep;
import org.jooq.Condition;
import org.jooq.Converter;
import org.jooq.DSLContext;
import org.jooq.DataType;
import org.jooq.DeleteConditionStep;
import org.jooq.Field;
import org.jooq.Param;
import org.jooq.Record;
import org.jooq.Record1;
import org.jooq.Record2;
import org.jooq.ResultQuery;
import org.jooq.SQLDialect;
import org.jooq.SortField;
import org.jooq.Table;
import org.jooq.conf.ParamType;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLRecoverableException;
import java.sql.SQLTimeoutException;
import java.sql.Savepoint;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.stream.Collectors;

/**
 * Abstract database implementation for use with JDBC.
 */
public abstract class AbstractSqlDatabase extends AbstractDatabase<Connection> {

    /**
     * Sub-setting name for specifying the JDBC data source used primarily for
     * writes and sometimes reads.
     *
     * @see #getDataSource()
     * @see #setDataSource(DataSource)
     */
    public static final String DATA_SOURCE_SUB_SETTING = "dataSource";

    /**
     * Sub-setting name for specifying the JNDI name that points to the JDBC
     * data source. used primarily for writes and sometimes reads.
     *
     * @see #getDataSource()
     * @see #setDataSource(DataSource)
     */
    public static final String DATA_SOURCE_JNDI_NAME_SUB_SETTING = "dataSourceJndiName";

    /**
     * Sub-setting name for specifying the JDBC data source used exclusively
     * for reads.
     *
     * @see #getReadDataSource()
     * @see #setReadDataSource(DataSource)
     */
    public static final String READ_DATA_SOURCE_SUB_SETTING = "readDataSource";

    /**
     * Sub-setting name for specifying the JNDI name that points to the JDBC
     * data source used exclusively for reads.
     *
     * @see #getReadDataSource()
     * @see #setReadDataSource(DataSource)
     */
    public static final String READ_DATA_SOURCE_JNDI_NAME_SUB_SETTING = "readDataSourceJndiName";

    /**
     * Sub-setting name for specifying the JDBC catalog where the data should
     * be stored.
     *
     * @see #getCatalog()
     * @see #setCatalog(String)
     */
    public static final String CATALOG_SUB_SETTING = "catalog";

    /**
     * Sub-setting name for specifying whether spatial data should be indexed.
     *
     * @see #isIndexSpatial()
     * @see #setIndexSpatial(boolean)
     */
    public static final String INDEX_SPATIAL_SUB_SETTING = "indexSpatial";

    public static final String CONNECTION_QUERY_OPTION = "sql.connection";
    public static final String RETURN_ORIGINAL_DATA_QUERY_OPTION = "sql.returnOriginalData";
    public static final String DISABLE_BY_ID_ITERATOR_OPTION = "sql.disableByIdIterator";

    public static final String SKIP_INDEX_STATE_EXTRA = "sql.skipIndex";
    public static final String ORIGINAL_DATA_EXTRA = "sql.originalData";

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractSqlDatabase.class);
    private static final Stats STATS = new Stats("SQL");

    private static final DataType<String> STRING_INDEX_TYPE = SQLDataType.LONGVARBINARY.asConvertedDataType(new Converter<byte[], String>() {

        private static final int MAX_STRING_INDEX_TYPE_LENGTH = 500;

        @Override
        public String from(byte[] bytes) {
            return bytes != null ? new String(bytes, StandardCharsets.UTF_8) : null;
        }

        @Override
        public byte[] to(String string) {
            if (string != null) {
                byte[] bytes = string.getBytes(StandardCharsets.UTF_8);

                if (bytes.length <= MAX_STRING_INDEX_TYPE_LENGTH) {
                    return bytes;

                } else {
                    byte[] shortened = new byte[MAX_STRING_INDEX_TYPE_LENGTH];
                    System.arraycopy(bytes, 0, shortened, 0, MAX_STRING_INDEX_TYPE_LENGTH);
                    return shortened;
                }

            } else {
                return null;
            }
        }

        @Override
        public Class<byte[]> fromType() {
            return byte[].class;
        }

        @Override
        public Class<String> toType() {
            return String.class;
        }
    });

    private volatile DataSource dataSource;
    private volatile DataSource readDataSource;
    private volatile String catalog;
    private volatile boolean indexSpatial;

    /**
     * jOOQ table that represents the {@code Record} table.
     *
     * <p>It contains the following fields:</p>
     *
     * <ul>
     *     <li>{@link #recordIdField}</li>
     *     <li>{@link #recordTypeIdField}</li>
     *     <li>{@link #recordDataField}</li>
     * </ul>
     */
    protected final Table<Record> recordTable = DSL.table(DSL.name("Record"));

    /**
     * jOOQ field that represents the {@code id} field in {@link #recordTable}.
     */
    protected final Field<UUID> recordIdField = DSL.field(DSL.name("id"), uuidType());

    /**
     * jOOQ field that represents the {@code typeId} field in
     * {@link #recordTable}.
     */
    protected final Field<UUID> recordTypeIdField = DSL.field(DSL.name("typeId"), uuidType());

    /**
     * jOOQ field that represents the {@code data} field in
     * {@link #recordTable}.
     */
    protected final Field<byte[]> recordDataField = DSL.field(DSL.name("data"), byteArrayType());

    /**
     * jOOQ table that represents the {@code RecordUpdate} table.
     *
     * <p>It contains the following fields:</p>
     *
     * <ul>
     *     <li>{@link #recordUpdateIdField}</li>
     *     <li>{@link #recordUpdateTypeIdField}</li>
     *     <li>{@link #recordUpdateDateField}</li>
     * </ul>
     */
    protected final Table<Record> recordUpdateTable = DSL.table(DSL.name("RecordUpdate"));

    /**
     * jOOQ field that represents the {@code id} field in
     * {@link #recordUpdateTable}.
     */
    protected final Field<UUID> recordUpdateIdField = DSL.field(DSL.name("id"), uuidType());

    /**
     * jOOQ field that represents the {@code typeId} field in
     * {@link #recordUpdateTable}.
     */
    protected final Field<UUID> recordUpdateTypeIdField = DSL.field(DSL.name("typeId"), uuidType());

    /**
     * jOOQ field that represents the {@code updateDate} field in
     * {@link #recordUpdateTable}.
     */
    protected final Field<Double> recordUpdateDateField = DSL.field(DSL.name("updateDate"), doubleType());

    /**
     * jOOQ table that represents the {@code Symbol} table.
     *
     * <p>It contains the following fields:</p>
     *
     * <ul>
     *     <li>{@link #symbolIdField}</li>
     *     <li>{@link #symbolValueField}</li>
     * </ul>
     */
    protected final Table<Record> symbolTable = DSL.table(DSL.name("Symbol"));

    /**
     * jOOQ field that represents the {@code symbolId} field in
     * {@link #symbolTable}.
     */
    protected final Field<Integer> symbolIdField = DSL.field(DSL.name("symbolId"), integerType());

    /**
     * jOOQ field that represents the {@code value} field in
     * {@link #symbolTable}.
     */
    protected final Field<String> symbolValueField = DSL.field(DSL.name("value"), stringIndexType());

    private volatile List<AbstractSqlIndex> locationSqlIndexes;
    private volatile List<AbstractSqlIndex> numberSqlIndexes;
    private volatile List<AbstractSqlIndex> regionSqlIndexes;
    private volatile List<AbstractSqlIndex> stringSqlIndexes;
    private volatile List<AbstractSqlIndex> uuidSqlIndexes;
    private volatile List<AbstractSqlIndex> deleteSqlIndexes;

    private final List<UpdateNotifier<?>> updateNotifiers = new ArrayList<>();

    // Cache that stores the difference between the local and the database
    // time. This is used to calculate a more accurate time in #now without
    // querying the database all the time.
    private final Supplier<Long> nowOffset = Suppliers.memoizeWithExpiration(() -> {
        try {
            Connection connection = openConnection();

            try (DSLContext context = openContext(connection)) {
                return System.currentTimeMillis() - context
                        .select(DSL.currentTimestamp())
                        .fetchOptional()
                        .map(r -> r.value1().getTime())
                        .orElse(0L);

            } finally {
                closeConnection(connection);
            }

        } catch (Exception error) {
            return 0L;
        }

    }, 5, TimeUnit.MINUTES);

    // Cache that stores all symbol IDs.
    private final Lazy<Map<String, Integer>> symbolIds = new Lazy<Map<String, Integer>>() {

        @Override
        protected Map<String, Integer> create() {
            Connection connection = openConnection();

            try (DSLContext context = openContext(connection)) {
                ResultQuery<Record2<Integer, String>> query = context
                        .select(symbolIdField, symbolValueField)
                        .from(symbolTable);

                try {
                    Map<String, Integer> symbolIds = new ConcurrentHashMap<>();
                    query.fetch().forEach(r -> symbolIds.put(r.value2(), r.value1()));
                    return symbolIds;

                } catch (DataAccessException error) {
                    throw convertJooqError(error, query);
                }

            } finally {
                closeConnection(connection);
            }
        }
    };

    /**
     * Returns the JDBC data source that should be used for writes and
     * possibly reads.
     *
     * @return Nullable.
     * @see #DATA_SOURCE_SUB_SETTING
     * @see #DATA_SOURCE_JNDI_NAME_SUB_SETTING
     */
    public DataSource getDataSource() {
        return dataSource;
    }

    /**
     * Sets the JDBC data source that should be used for writes and
     * possibly reads.
     *
     * @param dataSource Nullable.
     * @see #DATA_SOURCE_SUB_SETTING
     * @see #DATA_SOURCE_JNDI_NAME_SUB_SETTING
     */
    public void setDataSource(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    /**
     * Returns the JDBC data source that should be used for reads.
     *
     * <p>This may return a JDBC data source that can be used for writes,
     * if a read-only data source isn't available.</p>
     *
     * @return Nullable.
     * @see #READ_DATA_SOURCE_SUB_SETTING
     * @see #READ_DATA_SOURCE_JNDI_NAME_SUB_SETTING
     */
    public DataSource getReadDataSource() {
        return readDataSource != null ? readDataSource : getDataSource();
    }

    /**
     * Sets the JDBC data source that should be used for reads.
     *
     * @param readDataSource Nullable.
     * @see #READ_DATA_SOURCE_SUB_SETTING
     * @see #READ_DATA_SOURCE_JNDI_NAME_SUB_SETTING
     */
    public void setReadDataSource(DataSource readDataSource) {
        this.readDataSource = readDataSource;
    }

    /**
     * Returns the JDBC catalog where the data should be stored.
     *
     * @return Nullable.
     * @see #CATALOG_SUB_SETTING
     */
    public String getCatalog() {
        return catalog;
    }

    /**
     * Sets the JDBC catalog where the data should be stored.
     *
     * @param catalog Nullable.
     * @see #CATALOG_SUB_SETTING
     */
    public void setCatalog(String catalog) {
        this.catalog = catalog;
    }

    /**
     * Returns {@code true} if spatial data should be indexed.
     *
     * @see #INDEX_SPATIAL_SUB_SETTING
     */
    public boolean isIndexSpatial() {
        return indexSpatial;
    }

    /**
     * Sets whether spatial data should be indexed.
     *
     * @see #INDEX_SPATIAL_SUB_SETTING
     */
    public void setIndexSpatial(boolean indexSpatial) {
        this.indexSpatial = indexSpatial;
    }

    /**
     * Returns the jOOQ dialect that should be used to construct the SQL
     * statements.
     *
     * @return Nonnull.
     */
    protected abstract SQLDialect getDialect();

    /**
     * Returns the jOOQ data type that should be used to work with byte arrays.
     *
     * @return Nonnull.
     */
    protected DataType<byte[]> byteArrayType() {
        return SQLDataType.LONGVARBINARY;
    }

    /**
     * Returns the jOOQ data type that should be used to work with doubles.
     *
     * @return Nonnull.
     */
    protected DataType<Double> doubleType() {
        return SQLDataType.DOUBLE;
    }

    /**
     * Returns the jOOQ data type that should be used to work with integers.
     *
     * @return Nonnull.
     */
    protected DataType<Integer> integerType() {
        return SQLDataType.INTEGER;
    }

    /**
     * Returns the jOOQ data type that should be used to work with index
     * table strings.
     *
     * <p>The default implementation limits the length to 500 bytes.</p>
     *
     * @return Nonnull.
     */
    protected DataType<String> stringIndexType() {
        return STRING_INDEX_TYPE;
    }

    /**
     * Returns the jOOQ data type that should be used to work with UUIDs.
     *
     * @return Nonnull.
     */
    protected DataType<UUID> uuidType() {
        return SQLDataType.UUID;
    }

    /**
     * Returns a jOOQ field that represents {@code ST_Area(field)} function.
     *
     * @param field Nullable.
     * @return Nonnull.
     * @see <a href="http://www.postgis.org/docs/ST_Area.html">ST_Area</a>
     */
    protected Field<Double> stArea(Field<Object> field) {
        return DSL.field("ST_Area({0})", Double.class, field);
    }

    /**
     * Returns a jOOQ condition that represents {@code ST_Contains(x, y)}
     * function.
     *
     * @param x Nullable.
     * @param y Nullable.
     * @return Nonnull.
     * @see <a href="http://www.postgis.org/docs/ST_Contains.html">ST_Contains</a>
     */
    protected Condition stContains(Field<Object> x, Field<Object> y) {
        return DSL.condition("ST_Contains({0}, {1})", x, y);
    }

    /**
     * Returns a jOOQ field that represents {@code ST_GeomFromText(wkt)}
     * function.
     *
     * @param wkt Nullable.
     * @return Nonnull.
     * @see <a href="http://www.postgis.org/docs/ST_GeomFromText.html">ST_GeomFromText</a>
     */
    protected Field<Object> stGeomFromText(Field<String> wkt) {
        return DSL.field("ST_GeomFromText({0})", wkt);
    }

    /**
     * Returns a jOOQ field that represents {@code ST_Length(field)} function.
     *
     * @param field Nullable.
     * @return Nonnull.
     * @see <a href="http://www.postgis.org/docs/ST_Length.html">ST_Length</a>
     */
    protected Field<Double> stLength(Field<Object> field) {
        return DSL.field("ST_Length({0})", Double.class, field);
    }

    /**
     * Returns a jOOQ condition that represents the given {@code comparison}
     * using the given {@code options}.
     *
     * <p>This is used to override how a comparison should be handled in a
     * specific database implementation, and by default, it returns
     * {@code null} to indicate that it shouldn't do anything special.</p>
     *
     * @param options Nonnull.
     * @return Nullable.
     */
    protected Condition compare(ComparisonPredicate comparison, SqlCompareOptions options) {
        return null;
    }

    /**
     * Returns a jOOQ sort field that represents the given {@code sorter}
     * using the given {@code options}.
     *
     * <p>This is used ot override how a sorter should be handled in a specific
     * database implementation, and by default, it returns {@code null} to
     * indicate that it shouldn't do anything special.</p>
     *
     * @param options Nonnull.
     * @return Nullable.
     */
    protected SortField<?> sort(Sorter sorter, SqlSortOptions options) {
        return null;
    }

    /**
     * Returns a jOOQ field that represents {@code ST_MakeLine(x, y)} function.
     *
     * @param x Nullable.
     * @param y Nullable.
     * @return Nonnull.
     * @see <a href="http://www.postgis.org/docs/ST_MakeLine.html">ST_MakeLine</a>
     */
    protected Field<Object> stMakeLine(Field<Object> x, Field<Object> y) {
        return DSL.field("ST_MakeLine({0}, {1})", x, y);
    }

    @Override
    public Connection openConnection() {
        return getConnection(getDataSource(), false);
    }

    @Override
    protected Connection doOpenReadConnection() {
        return getConnection(getReadDataSource(), true);
    }

    // Returns the most appropriate and prepared connection from the data
    // source.
    private Connection getConnection(DataSource dataSource, boolean readOnly) {
        if (dataSource == null) {
            throw new SqlDatabaseException(this, "Can't get a connection without a data source!");
        }

        int retryLimit = Settings.getOrDefault(int.class, "dari/sqlConnectionRetryLimit", 5);

        while (true) {
            try {
                Connection connection = dataSource.getConnection();
                String catalog = getCatalog();

                if (catalog != null) {
                    connection.setCatalog(catalog);
                }

                connection.setReadOnly(readOnly);
                prepareConnection(connection, readOnly);

                return connection;

            } catch (SQLException error) {
                -- retryLimit;

                if (retryLimit <= 0 || !(error instanceof SQLRecoverableException)) {
                    throw new SqlDatabaseException(this, "Can't get a connection!", error);
                }

                Stats.Timer timer = STATS.startTimer();

                try {
                    Thread.sleep(ObjectUtils.jitter(10L, 0.5));

                } catch (InterruptedException ignore) {
                    // Ignore and keep retrying.

                } finally {
                    timer.stop("SQL: Connection Error");
                }
            }
        }
    }

    /**
     * Prepares the given {@code connection} after it's been opened.
     *
     * <p>The default implementation sets the transaction isolation to
     * {@link Connection#TRANSACTION_READ_COMMITTED}.</p>
     *
     * @param connection Nonnull.
     *
     * @param readOnly
     *        {@code true} if the given {@code connection} is read-only.
     *
     * @see #openConnection()
     * @see #openReadConnection()
     */
    protected void prepareConnection(Connection connection, boolean readOnly) throws SQLException {
        if (!readOnly) {
            connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
        }
    }

    // Opens a jOOQ context from the connection.
    private DSLContext openContext(Connection connection) {
        return DSL.using(connection, getDialect());
    }

    // Converts the jOOQ error to a more standard one.
    private SqlDatabaseException convertJooqError(DataAccessException error, org.jooq.Query query) {
        Throwable cause = error.getCause();
        SQLException sqlError = cause instanceof SQLException ? (SQLException) cause : null;

        if (sqlError != null) {
            String message = sqlError.getMessage();

            if (sqlError instanceof SQLTimeoutException
                    || (message != null
                    && message.contains("timeout"))) {

                return new SqlDatabaseException.ReadTimeout(this, sqlError, query != null ? query.getSQL() : null, null);
            }
        }

        return new SqlDatabaseException(this, sqlError, query != null ? query.getSQL() : null, null);
    }

    @Override
    public void closeConnection(Connection connection) {
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

    /**
     * Sets up the database.
     *
     * <p>This method should create all the necessary elements, such as tables,
     * that are required for proper operation.</p>
     *
     * <p>The default implementation executes all SQL statements from the
     * resource at {@link #getSetUpResourcePath()}, ignoring any errors as
     * indicated by {@link #shouldIgnoreSetUpError(DataAccessException)}.</p>
     */
    protected void setUp() {
        String resourcePath = getSetUpResourcePath();

        if (resourcePath == null) {
            return;
        }

        Connection connection = openConnection();

        try (DSLContext context = openContext(connection)) {

            // Skip set-up if the Record table already exists.
            if (context.meta().getTables().stream()
                    .filter(t -> t.getName().equals(recordTable.getName()))
                    .findFirst()
                    .isPresent()) {

                return;
            }

            String ddls;

            try (InputStream resourceInput = getClass().getResourceAsStream(resourcePath)) {
                ddls = IoUtils.toString(resourceInput, StandardCharsets.UTF_8);

            } catch (IOException error) {
                throw new IllegalStateException(
                        String.format("Can't read from [%s] to set up [%s] SQL database!", resourcePath, this),
                        error);
            }

            for (String ddl : ddls.trim().split("(?:\r\n?|\n){2,}")) {
                try {
                    context.execute(ddl);

                } catch (DataAccessException error) {
                    if (!shouldIgnoreSetUpError(error)) {
                        throw convertJooqError(error, null);
                    }
                }
            }

            invalidateCaches();

        } finally {
            closeConnection(connection);
        }
    }

    /**
     * Invalidates all caches.
     */
    public void invalidateCaches() {
        symbolIds.reset();
    }

    /**
     * Returns the path to the resource that contains the SQL statements to
     * be executed during {@link #setUp()}.
     *
     * <p>The default implementation returns {@code null} to signal that
     * there's nothing to do.</p>
     *
     * @return Nullable.
     */
    protected String getSetUpResourcePath() {
        return null;
    }

    /**
     * Returns {@code true} if the given {@code error} thrown in
     * {@link #setUp()} should be ignored.
     *
     * <p>This is for when the underlying database doesn't natively support a
     * specific capability (e.g. {@code CREATE TABLE IF NOT EXISTS}).</p>
     *
     * <p>The default implementation always returns {@code false} to indicate
     * that errors shouldn't be ignored.</p>
     *
     * @param error Nonnull.
     */
    protected boolean shouldIgnoreSetUpError(DataAccessException error) {
        return false;
    }

    @Override
    protected void doInitialize(String settingsKey, Map<String, Object> settings) {
        setDataSource(createDataSource(settings, DATA_SOURCE_JNDI_NAME_SUB_SETTING, DATA_SOURCE_SUB_SETTING));
        setReadDataSource(createDataSource(settings, READ_DATA_SOURCE_JNDI_NAME_SUB_SETTING, READ_DATA_SOURCE_SUB_SETTING));
        setCatalog(ObjectUtils.to(String.class, settings.get(CATALOG_SUB_SETTING)));
        setIndexSpatial(ObjectUtils.to(boolean.class, settings.get(INDEX_SPATIAL_SUB_SETTING)));

        setUp();

        // Cache of existing table names.
        Set<String> existingTables;
        Connection connection = openReadConnection();

        try (DSLContext context = openContext(connection)) {
            existingTables = context.meta().getTables().stream()
                    .map(t -> t.getName().toLowerCase(Locale.ENGLISH))
                    .collect(Collectors.toSet());

        } catch (DataAccessException error) {
            throw convertJooqError(error, null);

        } finally {
            closeConnection(connection);
        }

        // Which index tables to actually use?
        numberSqlIndexes = filterSqlIndexes(existingTables, new NumberSqlIndex(this, "RecordNumber", 3));
        stringSqlIndexes = filterSqlIndexes(existingTables, new StringSqlIndex(this, "RecordString", 4));
        uuidSqlIndexes = filterSqlIndexes(existingTables, new UuidSqlIndex(this, "RecordUuid", 3));

        if (isIndexSpatial()) {
            locationSqlIndexes = filterSqlIndexes(existingTables, new LocationSqlIndex(this, "RecordLocation", 3));
            regionSqlIndexes = filterSqlIndexes(existingTables, new RegionSqlIndex(this, "RecordRegion", 2));

        } else {
            locationSqlIndexes = Collections.emptyList();
            regionSqlIndexes = Collections.emptyList();
        }

        deleteSqlIndexes = ImmutableList.<AbstractSqlIndex>builder()
                .addAll(locationSqlIndexes)
                .addAll(numberSqlIndexes)
                .addAll(regionSqlIndexes)
                .addAll(stringSqlIndexes)
                .addAll(uuidSqlIndexes)
                .build();
    }

    // Creates a data source using the settings.
    private DataSource createDataSource(Map<String, Object> settings, String dataSourceJndiNameSetting, String dataSourceSetting) {

        // Data source at a non-standard location?
        String dataSourceJndiName = ObjectUtils.to(String.class, settings.get(dataSourceJndiNameSetting));
        Object dataSourceObject = null;

        if (dataSourceJndiName != null) {
            try {
                dataSourceObject = new InitialContext().lookup(dataSourceJndiName);

            } catch (NamingException error) {
                throw new SettingsException(
                        dataSourceJndiNameSetting,
                        String.format("Can't find [%s] via JNDI!", dataSourceJndiName),
                        error);
            }
        }

        // Data source at a standard location?
        if (dataSourceObject == null) {
            dataSourceObject = settings.get(dataSourceSetting);
        }

        // Really a data source?
        if (dataSourceObject != null) {
            if (dataSourceObject instanceof DataSource) {
                return (DataSource) dataSourceObject;

            } else if (dataSourceObject instanceof Map) {
                Map<?, ?> dataSourceMap = (Map<?, ?>) dataSourceObject;
                String className = ObjectUtils.to(String.class, dataSourceMap.get("class"));

                if (!StringUtils.isBlank(className)) {
                    Class<?> dataSourceClass = ObjectUtils.getClassByName(className);

                    if (dataSourceClass == null) {
                        throw new SettingsException(
                                dataSourceSetting + "/class",
                                String.format("[%s] isn't a valid class name!", className));
                    }

                    if (!(DataSource.class.isAssignableFrom(dataSourceClass))) {
                        throw new SettingsException(
                                dataSourceSetting + "/class",
                                String.format(
                                        "[%s] doesn't implement [%s]!",
                                        className,
                                        DataSource.class.getName()));
                    }

                    DataSource dataSource = (DataSource) TypeDefinition.getInstance(dataSourceClass).newInstance();
                    Map<String, Method> setters = new HashMap<>();

                    try {
                        for (PropertyDescriptor desc : Introspector.getBeanInfo(dataSourceClass).getPropertyDescriptors()) {
                            Method setter = desc.getWriteMethod();

                            if (setter != null) {
                                setters.put(desc.getName(), setter);
                            }
                        }

                    } catch (IntrospectionException error) {
                        throw new IllegalStateException(String.format(
                                "Can't introspect [%s]!",
                                className));
                    }

                    for (Map.Entry<?, ?> entry : dataSourceMap.entrySet()) {
                        String key = ObjectUtils.to(String.class, entry.getKey());
                        Object value = entry.getValue();

                        if (key != null && !"class".equals(key)) {
                            Method setter = setters.get(key);
                            boolean setterCalled = false;
                            Throwable setterError = null;

                            if (setter != null) {
                                try {
                                    setter.invoke(dataSource, ObjectUtils.to(setter.getGenericParameterTypes()[0], value));
                                    setterCalled = true;

                                } catch (IllegalAccessException error) {
                                    setterError = error;

                                } catch (InvocationTargetException error) {
                                    setterError = error.getCause();
                                }
                            }

                            if (!setterCalled) {
                                LOGGER.warn(
                                        String.format("Can't set [%s] to [%s] on [%s]!", key, value, dataSource),
                                        setterError);
                            }
                        }
                    }

                    return dataSource;
                }
            }

            throw new SettingsException(
                    dataSourceSetting,
                    String.format("[%s] isn't a data source!", dataSourceObject));
        }

        return null;
    }

    // Filters index tables that should be used based on existing tables.
    private List<AbstractSqlIndex> filterSqlIndexes(Set<String> existingTables, AbstractSqlIndex... sqlIndexes) {
        ImmutableList.Builder<AbstractSqlIndex> builder = ImmutableList.builder();
        boolean empty = true;

        for (AbstractSqlIndex sqlIndex : sqlIndexes) {
            if (existingTables.contains(sqlIndex.table.getName().toLowerCase(Locale.ENGLISH))) {
                builder.add(sqlIndex);
                empty = false;
            }
        }

        if (empty) {
            int length = sqlIndexes.length;

            if (length > 0) {
                builder.add(sqlIndexes[length - 1]);
            }
        }

        return builder.build();
    }

    @Override
    public long now() {
        return System.currentTimeMillis() - nowOffset.get();
    }

    @Override
    public Connection openQueryConnection(Query<?> query) {
        if (query != null) {
            Connection connection = (Connection) query.getOptions().get(CONNECTION_QUERY_OPTION);

            if (connection != null) {
                return connection;
            }
        }

        return super.openQueryConnection(query);
    }

    /**
     * Builds an SQL statement that can be used to select a subset of objects
     * matching the given {@code query}.
     *
     * @param query Nonnull.
     * @param offset Greater than or equal to {@code 0}.
     * @param limit Greater than {@code 0}.
     * @return Nonnull.
     */
    public String buildSelectStatement(Query<?> query, long offset, int limit) {
        Preconditions.checkNotNull(query);
        Preconditions.checkArgument(offset >= 0L);
        Preconditions.checkArgument(limit > 0);
        return addComment(new SqlQuery(this, query).select((int) offset, limit), query);
    }

    // Adds comment to the SQL to improve debugging.
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
     * Builds an SQL statement that can be used to select all objects
     * matching the given {@code query}.
     *
     * @param query Nonnull.
     * @return Nonnull.
     */
    public String buildSelectStatement(Query<?> query) {
        return buildSelectStatement(query, 0L, Integer.MAX_VALUE);
    }

    /**
     * Selects using the {@code sqlQuery} with the given {@code query} options,
     * and passes the result into the given {@code selectFunction}.
     *
     * @param sqlQuery Nonnull.
     * @param query Nullable.
     * @param selectFunction Nonnull.
     * @return Nullable.
     */
    public <R> R select(String sqlQuery, Query<?> query, SqlSelectFunction<R> selectFunction) {
        Preconditions.checkNotNull(sqlQuery);
        Preconditions.checkNotNull(selectFunction);

        Connection connection = openQueryConnection(query);

        try {
            double timeout = getReadTimeout();

            if (query != null) {
                Double queryTimeout = query.getTimeout();

                if (queryTimeout != null) {
                    timeout = queryTimeout;
                }
            }

            try (Statement statement = connection.createStatement()) {
                if (timeout > 0.0d) {
                    statement.setQueryTimeout((int) Math.ceil(timeout));
                }

                Stats.Timer timer = STATS.startTimer();
                Profiler.Static.startThreadEvent("SQL: Query");
                ResultSet result;

                try {
                    result = statement.executeQuery(sqlQuery);

                } finally {
                    double duration = timer.stop("SQL: Query");
                    Profiler.Static.stopThreadEvent(sqlQuery);

                    LOGGER.debug(
                            "Read from the SQL database using [{}] in [{}]ms",
                            sqlQuery, duration * 1000.0);
                }

                return selectFunction.apply(result);
            }

        } catch (SQLException error) {
            throw createSelectError(sqlQuery, query, error);

        } finally {
            closeResources(query, connection, null, null);
        }
    }

    // Creates an error that happened during #select.
    SqlDatabaseException createSelectError(String sqlQuery, Query<?> query, SQLException error) {
        String message;

        if (error instanceof SQLTimeoutException
                || ((message = error.getMessage()) != null
                && message.toLowerCase(Locale.ENGLISH).contains("timeout"))) {

            throw new SqlDatabaseException.ReadTimeout(this, error, sqlQuery, query);

        } else {
            throw new SqlDatabaseException(this, error, sqlQuery, query);
        }
    }

    // Closes all types of resources that were opened during a JDBC operation.
    void closeResources(Query<?> query, Connection connection, Statement statement, ResultSet result) {
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

    /**
     * Creates a previously saved object using the given {@code result},
     * and sets common state options based on the given {@code query}.
     *
     * @param result Nonnull.
     * @param query Nullable.
     * @return Nonnull.
     * @see #createSavedObject(Object, Object, Query)
     */
    protected <T> T createSavedObjectUsingResultSet(ResultSet result, Query<T> query) throws SQLException {
        T object = createSavedObject(result.getObject(2), result.getObject(1), query);
        State objectState = State.getInstance(object);

        if (!objectState.isReferenceOnly()) {
            byte[] data = result.getBytes(3);

            if (data != null) {
                objectState.setValues(StateSerializer.deserialize(data));
                objectState.getExtras().put(DATA_LENGTH_EXTRA, data.length);

                if (query != null
                        && ObjectUtils.to(boolean.class, query.getOptions().get(RETURN_ORIGINAL_DATA_QUERY_OPTION))) {

                    objectState.getExtras().put(ORIGINAL_DATA_EXTRA, data);
                }
            }
        }

        return swapObjectType(query, object);
    }

    /**
     * Selects a list of objects that match the given {@code sqlQuery} with
     * the given {@code query} options.
     *
     * @param sqlQuery Nonnull.
     * @param query Nullable.
     * @return Nonnull.
     */
    public <T> List<T> selectList(String sqlQuery, Query<T> query) {
        return select(sqlQuery, query, result -> {
            List<T> objects = new ArrayList<>();

            while (result.next()) {
                objects.add(createSavedObjectUsingResultSet(result, query));
            }

            return objects;
        });
    }

    @Override
    public <T> List<T> readAll(Query<T> query) {
        return selectList(buildSelectStatement(query), query);
    }

    /**
     * Builds an SQL statement that can be used to get a count of all
     * objects matching the given {@code query}.
     *
     * @param query Nonnull.
     * @return Nonnull.
     */
    public String buildCountStatement(Query<?> query) {
        Preconditions.checkNotNull(query);
        return addComment(new SqlQuery(this, query).countStatement(), query);
    }

    @Override
    public long readCount(Query<?> query) {
        String sqlQuery = buildCountStatement(query);

        return select(sqlQuery, query, result -> result.next()
                ? ObjectUtils.to(long.class, result.getObject(1))
                : 0L);
    }

    /**
     * Selects the first object that matches the given {@code sqlQuery},
     * which is executed with the given {@code query} options.
     *
     * @param sqlQuery Nonnull.
     * @param query Nullable.
     * @return Nullable.
     */
    public <T> T selectFirst(String sqlQuery, Query<T> query) {
        Preconditions.checkNotNull(sqlQuery);

        return select(sqlQuery, query, result -> result.next()
                ? createSavedObjectUsingResultSet(result, query)
                : null);
    }

    @Override
    public <T> T readFirst(Query<T> query) {
        return selectFirst(buildSelectStatement(query, 0L, 1), query);
    }

    /**
     * Selects an iterable of objects that match the given {@code sqlQuery},
     * which is executed with the given {@code query} options.
     *
     * @param sqlQuery Nonnull.
     * @param fetchSize Number of objects to fetch at a time.
     * @param query Nullable.
     * @return Nonnull.
     */
    public <T> Iterable<T> selectIterable(String sqlQuery, int fetchSize, Query<T> query) {
        return () -> new SqlIterator<>(this, sqlQuery, fetchSize, query);
    }

    @Override
    public <T> Iterable<T> readIterable(Query<T> query, int fetchSize) {
        if (query.getSorters().isEmpty()) {
            if (!ObjectUtils.to(boolean.class, query.getOptions().get(DISABLE_BY_ID_ITERATOR_OPTION))) {
                return ByIdIterator.iterable(query, fetchSize);
            }
        }

        return selectIterable(buildSelectStatement(query), fetchSize, query);
    }

    /**
     * Builds an SQL statement that can be used to get when any of the objects
     * matching the given {@code query} were last updated.
     *
     * @param query Nonnull.
     * @return Nonnull.
     */
    public String buildLastUpdateStatement(Query<?> query) {
        Preconditions.checkNotNull(query);
        return addComment(new SqlQuery(this, query).lastUpdateStatement(), query);
    }

    @Override
    public Date readLastUpdate(Query<?> query) {
        String sqlQuery = buildLastUpdateStatement(query);

        return select(sqlQuery, query, result -> result.next()
                ? new Date((long) (ObjectUtils.to(double.class, result.getObject(1)) * 1000L))
                : null);
    }

    @Override
    public <T> PaginatedResult<T> readPartial(Query<T> query, long offset, int limit) {

        // Efficiently determine whether there are more items by:
        // 1. Guard against integer overflow in step 2.
        if (limit == Integer.MAX_VALUE) {
            -- limit;
        }

        // 2. Select one more item than requested.
        String sqlQuery = buildSelectStatement(query, offset, limit + 1);
        List<T> items = selectList(sqlQuery, query);
        int size = items.size();

        // 3. If there are less items than the requested limit, there aren't
        // any more items after this result. For example, if there are 10 items
        // total matching the query, step 2 tries to fetch 11 items and would
        // trigger this.
        if (size <= limit) {
            return new PaginatedResult<>(offset, limit, offset + size, items);
        }

        // 4. Otherwise, there are more items, so remove the extra.
        items.remove(size - 1);

        // 5. And return a customized paginated result that calculates
        // the count on demand, as well as bypass the potentially expensive
        // #hasNext that uses #getCount.
        return new PaginatedResult<T>(offset, limit, 0, items) {

            private final Lazy<Long> count = new Lazy<Long>() {

                @Override
                protected Long create() {
                    return readCount(query);
                }
            };

            @Override
            public long getCount() {
                return count.get();
            }

            @Override
            public boolean hasNext() {
                return true;
            }
        };
    }

    /**
     * Builds an SQL statement that can be used to get all objects grouped by
     * the values of the given {@code fields}.
     *
     * @param query Nonnull.
     * @param fields Nonnull. Nonempty.
     * @return Nonnull.
     */
    public String buildGroupStatement(Query<?> query, String... fields) {
        Preconditions.checkNotNull(query);
        Preconditions.checkNotNull(fields);
        Preconditions.checkArgument(fields.length > 0);
        return addComment(new SqlQuery(this, query).groupStatement(fields), query);
    }

    @Override
    public <T> PaginatedResult<Grouping<T>> readPartialGrouped(Query<T> query, long offset, int limit, String... fields) {
        for (String field : fields) {
            Matcher groupingMatcher = Query.RANGE_PATTERN.matcher(field);
            if (groupingMatcher.find()) {
                throw new UnsupportedOperationException("SqlDatabase does not support group by numeric range");
            }
        }

        String sqlQuery = buildGroupStatement(query, fields);

        return select(sqlQuery, query, result -> {
            List<Grouping<T>> groupings = new ArrayList<>();
            int fieldsLength = fields.length;
            int groupingsCount = 0;

            for (int i = 0, last = (int) offset + limit; result.next(); ++ i, ++ groupingsCount) {
                if (i < offset || i >= last) {
                    continue;
                }

                List<Object> keys = new ArrayList<>();

                SqlGrouping<T> grouping;
                ResultSetMetaData meta = result.getMetaData();
                String aggregateColumnName = meta.getColumnName(1);
                if (SqlQuery.COUNT_ALIAS.equals(aggregateColumnName)) {
                    long count = ObjectUtils.to(long.class, result.getObject(1));
                    for (int j = 0; j < fieldsLength; ++ j) {
                        keys.add(result.getObject(j + 2));
                    }
                    grouping = new SqlGrouping<>(keys, query, fields, count);
                } else {
                    throw new UnsupportedOperationException();
                }
                groupings.add(grouping);
            }

            int groupingsSize = groupings.size();
            List<Integer> removes = new ArrayList<>();

            for (int i = 0; i < fieldsLength; ++ i) {
                Query.MappedKey key = query.mapEmbeddedKey(getEnvironment(), fields[i]);
                ObjectField field = key.getSubQueryKeyField();
                if (field == null) {
                    field = key.getField();
                }

                if (field != null) {
                    Map<String, Object> rawKeys = new HashMap<>();
                    for (int j = 0; j < groupingsSize; ++ j) {
                        rawKeys.put(String.valueOf(j), groupings.get(j).getKeys().get(i));
                    }

                    String itemType = field.getInternalItemType();
                    if (ObjectField.RECORD_TYPE.equals(itemType)) {
                        for (Map.Entry<String, Object> entry : rawKeys.entrySet()) {
                            Map<String, Object> ref = new HashMap<>();
                            ref.put(StateSerializer.REFERENCE_KEY, entry.getValue());
                            entry.setValue(ref);
                        }
                    }

                    Map<String, Object> rawKeysCopy = new HashMap<>(rawKeys);
                    Map<?, ?> convertedKeys = (Map<?, ?>) StateSerializer.toJavaValue(query.getDatabase(), null, field, "map/" + itemType, rawKeys);

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

            return new PaginatedResult<>(offset, limit, groupingsCount - removes.size(), groupings);
        });
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

    // Executes a write.
    private int execute(Connection connection, DSLContext context, org.jooq.Query query) throws SQLException {
        boolean useSavepoint = shouldUseSavepoint();
        Savepoint savepoint = null;
        Integer affected = null;

        Stats.Timer timer = STATS.startTimer();
        Profiler.Static.startThreadEvent("SQL: Update");

        try {
            if (useSavepoint && !connection.getAutoCommit()) {
                savepoint = connection.setSavepoint();
            }

            affected = query.execute();

            if (savepoint != null) {
                connection.releaseSavepoint(savepoint);
            }

            return affected;

        } catch (DataAccessException error) {
            if (savepoint != null) {
                connection.rollback(savepoint);
            }

            Throwables.propagateIfInstanceOf(error.getCause(), SQLException.class);
            throw error;

        } finally {
            double time = timer.stop("SQL: Update");
            java.util.function.Supplier<String> sqlSupplier = () -> context
                    .renderContext()
                    .paramType(ParamType.INLINED)
                    .render(query);

            Profiler.Static.stopThreadEvent(sqlSupplier);

            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(
                        "SQL update: [{}], Affected: [{}], Time: [{}]ms",
                        new Object[] {
                                sqlSupplier.get(),
                                affected,
                                time * 1000.0
                        });
            }
        }
    }

    /**
     * Returns {@code true} if the writes should use JDBC savepoints.
     */
    protected boolean shouldUseSavepoint() {
        return true;
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
    protected void doSaves(Connection connection, boolean isImmediate, List<State> states) throws SQLException {
        if (states == null || states.isEmpty()) {
            return;
        }

        List<State> indexStates = null;
        for (State state1 : states) {
            if (Boolean.TRUE.equals(state1.getExtra(SKIP_INDEX_STATE_EXTRA))) {
                indexStates = new ArrayList<>();
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

        try (DSLContext context = openContext(connection)) {

            // Save all indexes.
            if (!indexStates.isEmpty()) {
                deleteIndexes(context, null, indexStates);
                insertIndexes(context, null, indexStates);
            }

            double now = System.currentTimeMillis() / 1000.0;

            for (State state : states) {
                boolean isNew = state.isNew();
                UUID id = state.getId();
                UUID typeId = state.getVisibilityAwareTypeId();
                byte[] data = null;

                // Save data.
                while (true) {

                    // Looks like a new object so try to INSERT.
                    if (isNew) {
                        if (data == null) {
                            data = StateSerializer.serialize(state.getSimpleValues());
                        }

                        if (execute(connection, context, context
                                .insertInto(recordTable,
                                        recordIdField,
                                        recordTypeIdField,
                                        recordDataField)
                                .select(context.select(
                                        DSL.inline(id, uuidType()),
                                        DSL.inline(typeId, uuidType()),
                                        DSL.inline(data, byteArrayType()))
                                        .whereNotExists(context
                                                .selectOne()
                                                .from(recordTable)
                                                .where(recordIdField.eq(id))
                                                .and(recordTypeIdField.eq(typeId))))) < 1) {

                            // INSERT failed so retry with UPDATE.
                            isNew = false;
                            continue;
                        }

                    } else {
                        List<AtomicOperation> atomicOperations = state.getAtomicOperations();

                        // Normal update.
                        if (atomicOperations.isEmpty()) {
                            if (data == null) {
                                data = StateSerializer.serialize(state.getSimpleValues());
                            }

                            if (execute(connection, context, context
                                    .update(recordTable)
                                    .set(recordTypeIdField, typeId)
                                    .set(recordDataField, data)
                                    .where(recordIdField.eq(id))) < 1) {

                                // UPDATE failed so retry with INSERT.
                                isNew = true;
                                continue;
                            }

                        } else {

                            // Atomic operations requested, so find the old object.
                            Object oldObject = Query
                                    .from(Object.class)
                                    .where("_id = ?", id)
                                    .using(this)
                                    .option(CONNECTION_QUERY_OPTION, connection)
                                    .option(RETURN_ORIGINAL_DATA_QUERY_OPTION, Boolean.TRUE)
                                    .master()
                                    .noCache()
                                    .first();

                            if (oldObject == null) {
                                retryWrites();
                                break;
                            }

                            // Restore the data from the old object.
                            State oldState = State.getInstance(oldObject);
                            UUID oldTypeId = oldState.getVisibilityAwareTypeId();
                            byte[] oldData = (byte[]) oldState.getExtra(ORIGINAL_DATA_EXTRA);

                            state.setValues(oldState.getValues());

                            // Apply all the atomic operations.
                            for (AtomicOperation operation : atomicOperations) {
                                String field = operation.getField();
                                state.putByPath(field, oldState.getByPath(field));
                            }

                            for (AtomicOperation operation : atomicOperations) {
                                operation.execute(state);
                            }

                            data = StateSerializer.serialize(state.getSimpleValues());

                            if (execute(connection, context, context
                                    .update(recordTable)
                                    .set(recordTypeIdField, typeId)
                                    .set(recordDataField, data)
                                    .where(recordIdField.eq(id))
                                    .and(recordTypeIdField.eq(oldTypeId))
                                    .and(recordDataField.eq(oldData))) < 1) {

                                // UPDATE failed so start over.
                                retryWrites();
                                break;
                            }
                        }
                    }

                    // Success!
                    break;
                }

                // Save update date.
                while (true) {
                    if (isNew) {
                        if (execute(connection, context, context
                                .insertInto(recordUpdateTable,
                                        recordUpdateIdField,
                                        recordUpdateTypeIdField,
                                        recordUpdateDateField)
                                .select(context.select(
                                        DSL.inline(id, uuidType()),
                                        DSL.inline(typeId, uuidType()),
                                        DSL.inline(now, doubleType()))
                                        .whereNotExists(context
                                                .selectOne()
                                                .from(recordUpdateTable)
                                                .where(recordUpdateIdField.eq(id))))) < 1) {

                            // INSERT failed so retry with UPDATE.
                            isNew = false;
                            continue;
                        }

                    } else {
                        if (execute(connection, context, context
                                .update(recordUpdateTable)
                                .set(recordUpdateTypeIdField, typeId)
                                .set(recordUpdateDateField, now)
                                .where(recordUpdateIdField.eq(id))) < 1) {

                            // UPDATE failed so retry with INSERT.
                            isNew = true;
                            continue;
                        }
                    }

                    break;
                }
            }
        }
    }

    // Deletes all index data associated with the states.
    private void deleteIndexes(DSLContext context, ObjectIndex onlyIndex, List<State> states) throws SQLException {
        Set<UUID> stateIds = states.stream()
                .map(State::getId)
                .collect(Collectors.toSet());

        for (AbstractSqlIndex sqlIndex : deleteSqlIndexes) {
            try {
                DeleteConditionStep<Record> delete = context
                        .deleteFrom(sqlIndex.table)
                        .where(sqlIndex.idField.in(stateIds));

                if (onlyIndex != null) {
                    delete = delete.and(sqlIndex.symbolIdField.eq(findSymbolId(onlyIndex.getUniqueName(), false)));
                }

                context.execute(delete);

            } catch (DataAccessException error) {
                Throwables.propagateIfInstanceOf(error, SQLException.class);
                throw error;
            }
        }
    }

    /**
     * Find a unique ID associated with the given {@code symbol}, creating
     * one on demand if requested.
     *
     * @param symbol Nonnull.
     * @param create {@code true} to create the ID on demand.
     * @return {@code -1} if the given {@code symbol} isn't associated with
     *         an ID and its creation isn't requested.
     */
    protected int findSymbolId(String symbol, boolean create) {
        Preconditions.checkNotNull(symbol);

        // ID already cached?
        Integer cachedId = symbolIds.get().get(symbol);

        if (cachedId != null) {
            return cachedId;
        }

        // Try to find the ID from the database.
        Connection readConnection = openReadConnection();

        try {
            int id = readSymbolId(symbol, readConnection);

            if (id > 0 || !create) {
                return id;
            }

        } finally {
            closeConnection(readConnection);
        }

        // Create the ID and re-fetch it from the database to make sure that
        // it's correct in case of unique constraint violation.
        Connection connection = openConnection();

        try {
            try (DSLContext context = openContext(connection)) {
                org.jooq.Query createQuery = context
                        .insertInto(symbolTable, symbolValueField)
                        .select(context
                                .select(DSL.inline(symbol, stringIndexType()))
                                .whereNotExists(context
                                        .selectOne()
                                        .from(symbolTable)
                                        .where(symbolValueField.eq(symbol))));

                try {
                    createQuery.execute();

                } catch (DataAccessException error) {
                    throw convertJooqError(error, createQuery);
                }
            }

            return readSymbolId(symbol, connection);

        } finally {
            closeConnection(connection);
        }
    }

    // Reads the symbol ID from the database.
    private int readSymbolId(String symbol, Connection connection) {
        try (DSLContext context = openContext(connection)) {
            ResultQuery<Record1<Integer>> selectQuery = context
                    .select(symbolIdField)
                    .from(symbolTable)
                    .where(symbolValueField.eq(symbol));

            try {
                Integer id = selectQuery
                        .fetchOptional()
                        .map(Record1::value1)
                        .orElse(null);

                if (id != null) {
                    symbolIds.get().put(symbol, id);
                    return id;

                } else {
                    return -1;
                }

            } catch (DataAccessException error) {
                throw convertJooqError(error, selectQuery);
            }
        }
    }

    // Inserts all index data associated with the states.
    private void insertIndexes(DSLContext context, ObjectIndex onlyIndex, List<State> states) throws SQLException {
        Map<Table<Record>, BatchBindStep> batches = new HashMap<>();
        Map<Table<Record>, Set<Map<String, Object>>> bindValuesSets = new HashMap<>();

        for (State state : states) {
            UUID id = state.getId();
            UUID typeId = state.getVisibilityAwareTypeId();

            for (SqlIndexValue sqlIndexValue : SqlIndexValue.find(state)) {
                ObjectIndex index = sqlIndexValue.getIndex();

                if (onlyIndex != null && !onlyIndex.equals(index)) {
                    continue;
                }

                Object symbolId = findSymbolId(sqlIndexValue.getUniqueName(), true);

                for (AbstractSqlIndex sqlIndex : getSqlIndexes(index)) {
                    Table<Record> table = sqlIndex.table;
                    BatchBindStep batch = batches.get(table);
                    Param<UUID> idParam = sqlIndex.idParam;
                    Param<UUID> typeIdParam = sqlIndex.typeIdParam;
                    Param<Integer> symbolIdParam = sqlIndex.symbolIdParam;

                    if (batch == null) {
                        batch = context.batch(context.insertInto(table)
                                .set(sqlIndex.idField, idParam)
                                .set(sqlIndex.typeIdField, typeIdParam)
                                .set(sqlIndex.symbolIdField, symbolIdParam)
                                .set(sqlIndex.valueField, sqlIndex.valueParam()));
                    }

                    boolean bound = false;

                    for (Object[] valuesArray : sqlIndexValue.getValuesArray()) {
                        Map<String, Object> bindValues = sqlIndex.valueBindValues(index, valuesArray[0]);

                        if (bindValues != null) {
                            bindValues.put(idParam.getName(), id);
                            bindValues.put(typeIdParam.getName(), typeId);
                            bindValues.put(symbolIdParam.getName(), symbolId);

                            Set<Map<String, Object>> bindValuesSet = bindValuesSets.get(table);

                            if (bindValuesSet == null) {
                                bindValuesSet = new HashSet<>();
                                bindValuesSets.put(table, bindValuesSet);
                            }

                            if (!bindValuesSet.contains(bindValues)) {
                                batch = batch.bind(bindValues);
                                bound = true;
                                bindValuesSet.add(bindValues);
                            }
                        }
                    }

                    if (bound) {
                        batches.put(table, batch);
                    }
                }
            }
        }

        for (BatchBindStep batch : batches.values()) {
            try {
                batch.execute();

            } catch (DataAccessException error) {
                Throwables.propagateIfInstanceOf(error.getCause(), SQLException.class);
                throw error;
            }
        }
    }

    // Returns all tables that should be used to work with the index.
    List<AbstractSqlIndex> getSqlIndexes(ObjectIndex index) {
        List<String> fieldNames = index.getFields();
        ObjectField field = index.getParent().getField(fieldNames.get(0));

        switch (field != null ? field.getInternalItemType() : index.getType()) {
            case ObjectField.RECORD_TYPE :
            case ObjectField.UUID_TYPE :
                return uuidSqlIndexes;

            case ObjectField.DATE_TYPE :
            case ObjectField.NUMBER_TYPE :
                return numberSqlIndexes;

            case ObjectField.LOCATION_TYPE :
                return locationSqlIndexes;

            case ObjectField.REGION_TYPE :
                return regionSqlIndexes;

            default :
                return stringSqlIndexes;
        }
    }

    @Override
    protected void doIndexes(Connection connection, boolean isImmediate, List<State> states) throws SQLException {
        if (states == null || states.isEmpty()) {
            return;
        }

        try (DSLContext context = openContext(connection)) {
            deleteIndexes(context, null, states);
            insertIndexes(context, null, states);
        }
    }

    @Override
    protected void doRecalculations(Connection connection, boolean isImmediate, ObjectIndex index, List<State> states) throws SQLException {
        if (states == null || states.isEmpty()) {
            return;
        }

        try (DSLContext context = openContext(connection)) {
            deleteIndexes(context, index, states);
            insertIndexes(context, index, states);
        }
    }

    @Override
    protected void doDeletes(Connection connection, boolean isImmediate, List<State> states) throws SQLException {
        if (states == null || states.isEmpty()) {
            return;
        }

        try (DSLContext context = openContext(connection)) {

            // Delete all indexes.
            deleteIndexes(context, null, states);

            Set<UUID> stateIds = states.stream()
                    .map(State::getId)
                    .collect(Collectors.toSet());

            // Delete data.
            execute(connection, context, context
                    .delete(recordTable)
                    .where(recordIdField.in(stateIds)));

            // Save delete date.
            execute(connection, context, context
                    .update(recordUpdateTable)
                    .set(recordUpdateDateField, System.currentTimeMillis() / 1000.0)
                    .where(recordUpdateIdField.in(stateIds)));
        }
    }

    @Override
    public void addUpdateNotifier(UpdateNotifier<?> notifier) {
        updateNotifiers.add(notifier);
    }

    @Override
    public void removeUpdateNotifier(UpdateNotifier<?> notifier) {
        updateNotifiers.remove(notifier);
    }

    public void notifyUpdate(Object object) {
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
}
