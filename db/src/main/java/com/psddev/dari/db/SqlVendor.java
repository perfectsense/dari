package com.psddev.dari.db;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.psddev.dari.util.IoUtils;
import com.psddev.dari.util.StringUtils;
import com.psddev.dari.util.UuidUtils;

public class SqlVendor {

    public enum ColumnType {
        BYTES_LONG,
        BYTES_SHORT,
        DOUBLE,
        INTEGER,
        POINT,
        POLYGON,
        SERIAL,
        UUID;
    }

    public static final String RECORD_TABLE_NAME = "Record";
    public static final String RECORD_UPDATE_TABLE_NAME = "RecordUpdate";
    public static final String SYMBOL_TABLE_NAME = "Symbol";

    public static final int MAX_BYTES_SHORT_LENGTH = 500;

    private SqlDatabase database;

    public SqlDatabase getDatabase() {
        return database;
    }

    public void setDatabase(SqlDatabase database) {
        this.database = database;
    }

    /**
     * Returns the path to the resource that contains the SQL statements to
     * be executed during {@link #setUp}. The default implementation returns
     * {@code null} to signal that there's nothing to do.
     *
     * @return May be {@code null}.
     */
    protected String getSetUpResourcePath() {
        return null;
    }

    /**
     * Catches the given {@code error} thrown in {@link #setUp} to be
     * processed in vendor-specific way. Typically, this is used to ignore
     * errors when the vendor doesn't natively support that ability (e.g.
     * {@code CREATE TABLE IF NOT EXISTS}). The default implementation
     * always rethrows the error.
     *
     * @param error Can't be {@code null}.
     */
    protected void catchSetUpError(SQLException error) throws SQLException {
        throw error;
    }

    /**
     * Sets up the given {@code database}. This method should create all the
     * necessary elements, such as tables, that are required for proper
     * operation. The default implementation executes all SQL statements from
     * the resource at {@link #getSetUpResourcePath}, and processes the errors
     * using {@link #catchSetUpError}.
     *
     * @param database Can't be {@code null}.
     */
    public void setUp(SqlDatabase database) throws IOException, SQLException {
        String resourcePath = getSetUpResourcePath();

        if (resourcePath == null) {
            return;
        }

        InputStream resourceInput = getClass().getClassLoader().getResourceAsStream(resourcePath);

        if (resourceInput == null) {
            throw new IllegalArgumentException(String.format(
                    "Can't find [%s] using ClassLoader#getResourceAsStream!",
                    resourcePath));
        }

        Connection connection = database.openConnection();

        try {
            if (hasTable(connection, SqlDatabase.RECORD_TABLE)) {
                return;
            }

            for (String ddl : IoUtils.toString(resourceInput, StandardCharsets.UTF_8).trim().split("(?:\r\n?|\n){2,}")) {
                Statement statement = connection.createStatement();

                try {
                    statement.execute(ddl);

                } catch (SQLException error) {
                    catchSetUpError(error);

                } finally {
                    statement.close();
                }
            }

        } finally {
            database.closeConnection(connection);
        }
    }

    protected boolean hasTable(Connection connection, String tableName) throws SQLException {
        return getTables(connection).contains(tableName);
    }

    public void setTransactionIsolation(Connection connection) throws SQLException {
        connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
    }

    public Set<String> getTables(Connection connection) throws SQLException {
        Set<String> tableNames = new HashSet<String>();
        String catalog = connection.getCatalog();
        DatabaseMetaData meta = connection.getMetaData();
        ResultSet result = meta.getTables(catalog, null, null, null);

        try {
            while (result.next()) {
                String name = result.getString("TABLE_NAME");

                if (name != null) {
                    tableNames.add(name);
                }
            }

        } finally {
            result.close();
        }

        return tableNames;
    }

    public Set<String> getColumns(Connection connection, String table) throws SQLException {
        Set<String> columnNames = new HashSet<String>();
        String catalog = connection.getCatalog();
        DatabaseMetaData meta = connection.getMetaData();
        ResultSet result = null;

        try {
            result = meta.getColumns(catalog, null, table, null);
            while (result.next()) {
                String columnName = result.getString("COLUMN_NAME");
                if (columnName != null) {
                    columnNames.add(columnName);
                }
            }
        } finally {
            if (result != null) {
                result.close();
            }
        }

        return columnNames;
    }

    public boolean checkTableExists(String tableIdentifier) {
        Connection connection = database.openConnection();
        boolean tableExists = false;

        try {
            Statement statement = connection.createStatement();

            try {
                ResultSet result = statement.executeQuery("SELECT 'x' FROM " + tableIdentifier + " WHERE 0 = 1");

                try {
                    tableExists = true;

                } finally {
                    result.close();
                }

            } finally {
                statement.close();
            }

        } catch (SQLException error) {
            if ("42S02".equals(error.getSQLState())) {
                tableExists = false;
            }

        } finally {
            database.closeConnection(connection);
        }

        return tableExists;
    }

    public boolean hasInRowIndex(Connection connection, String recordTable) throws SQLException {
        boolean newHasInRowIndex = false;
        String catalog = connection.getCatalog();
        DatabaseMetaData meta = connection.getMetaData();
        ResultSet result = meta.getColumns(catalog, null, recordTable, null);

        try {
            while (result.next()) {
                String name = result.getString("COLUMN_NAME");

                if (name != null && name.equalsIgnoreCase(SqlDatabase.IN_ROW_INDEX_COLUMN)) {
                    newHasInRowIndex = true;
                    break;
                }
            }

        } finally {
            result.close();
        }

        return newHasInRowIndex;
    }

    public boolean supportsDistinctBlob() {
        return true;
    }

    public boolean useSavepoint() {
        return true;
    }

    public void appendIdentifier(StringBuilder builder, String identifier) {
        builder.append('"');
        builder.append(identifier.replace("\"", "\"\""));
        builder.append('"');
    }

    public void appendAlias(StringBuilder builder, String alias) {
        appendIdentifier(builder, alias);
    }

    public void appendBindLocation(StringBuilder builder, Location location, List<Object> parameters) {
        throw new UnsupportedOperationException("Location support is not implemented.");
    }

    public void appendBindRegion(StringBuilder builder, Region region, List<Object> parameters) {
        throw new UnsupportedOperationException("Region support is not implemented.");
    }

    public void appendBindUuid(StringBuilder builder, UUID uuid, List<Object> parameters) {
        builder.append('?');
        if (parameters != null) {
            parameters.add(uuid == null ? null : UuidUtils.toBytes(uuid));
        }
    }

    public void appendBindString(StringBuilder builder, String value, List<Object> parameters) {
        builder.append('?');
        if (parameters != null) {
            parameters.add(value == null ? null : value.getBytes(StandardCharsets.UTF_8));
        }
    }

    public void appendBindValue(StringBuilder builder, Object value, List<Object> parameters) {
        if (value instanceof Location) {
            appendBindLocation(builder, (Location) value, parameters);
        } else if (value instanceof Region) {
            appendBindRegion(builder, (Region) value, parameters);

        } else if (value instanceof UUID) {
            appendBindUuid(builder, (UUID) value, parameters);

        } else if (value instanceof String) {
            appendBindString(builder, (String) value, parameters);

        } else {
            builder.append('?');
            if (parameters != null) {
                parameters.add(value);
            }
        }
    }

    public void appendValue(StringBuilder builder, Object value) {
        if (value == null) {
            builder.append("NULL");

        } else if (value instanceof Number) {
            builder.append(value);

        } else if (value instanceof UUID) {
            appendUuid(builder, (UUID) value);

        } else if (value instanceof byte[]) {
            appendBytes(builder, (byte[]) value);

        } else if (value instanceof Location) {
            Location valueLocation = (Location) value;
            builder.append("GeomFromText('POINT(");
            builder.append(valueLocation.getX());
            builder.append(' ');
            builder.append(valueLocation.getY());
            builder.append(")')");

        } else if (value instanceof Region) {
            Region valueRegion = (Region) value;

            builder.append("GEOMFROMTEXT('MULTIPOLYGON((");
            for (Region.Polygon polygon : valueRegion.getPolygons()) {
                for (Region.LinearRing ring : polygon) {
                    builder.append("((");
                    for (Region.Coordinate coordinate : ring) {
                        builder.append(SqlDatabase.quoteValue(coordinate.getLatitude())); // Latitude
                        builder.append(' ');
                        builder.append(SqlDatabase.quoteValue(coordinate.getLongitude())); // Longitude
                        builder.append(", ");
                    }
                    builder.setLength(builder.length() - 2);
                    builder.append(")), ");
                }
            }

            for (Region.Circle circles : valueRegion.getCircles()) {
                for (Region.Polygon polygon : circles.getPolygons()) {
                    for (Region.LinearRing ring : polygon) {
                        builder.append("((");
                        for (Region.Coordinate coordinate : ring) {
                            builder.append(SqlDatabase.quoteValue(coordinate.getLatitude())); // Latitude
                            builder.append(' ');
                            builder.append(SqlDatabase.quoteValue(coordinate.getLongitude())); // Longitude
                            builder.append(", ");
                        }
                        builder.setLength(builder.length() - 2);
                        builder.append(")), ");
                    }
                }
            }

            builder.append("))')");
        } else {
            appendBytes(builder, value.toString().getBytes(StandardCharsets.UTF_8));
        }
    }

    protected void appendUuid(StringBuilder builder, UUID value) {
        builder.append('{');
        builder.append(value);
        builder.append('}');
    }

    protected void appendBytes(StringBuilder builder, byte[] value) {
        builder.append("X'");
        builder.append(StringUtils.hex(value));
        builder.append('\'');
    }

    protected void appendWhereRegion(StringBuilder builder, Region region, String field) {
        throw new UnsupportedOperationException("Region support is not implemented.");
    }

    protected void appendWhereLocation(StringBuilder builder, Location location, String field) {
        throw new UnsupportedOperationException("Region support is not implemented.");
    }

    protected void appendNearestLocation(
            StringBuilder orderbyBuilder,
            StringBuilder selectBuilder,
            Location location, String field) {
        throw new UnsupportedOperationException("Region support is not implemented.");
    }

    protected String rewriteQueryWithLimitClause(String query, int limit, long offset) {
        if (query.contains(getLimitOffsetPlaceholder())) {
            return String.format("%s LIMIT %d", query.replace(getLimitOffsetPlaceholder(), String.format(" LIMIT %d OFFSET %d ", limit, offset)), limit);
        } else {
            return String.format("%s LIMIT %d OFFSET %d", query, limit, offset);
        }
    }

    public String getLimitOffsetPlaceholder() {
        return "/*__LIMIT_OFFSET__*/";
    }

    /**
     * Creates a table using the given parameters.
     *
     * @deprecated Use {@link #setUp} instead.
     */
    @Deprecated
    public void createTable(
            SqlDatabase database,
            String tableName,
            Map<String, ColumnType> columns,
            List<String> primaryKeyColumns)
            throws SQLException {

        if (database.hasTable(tableName)) {
            return;
        }

        StringBuilder ddlBuilder = new StringBuilder();
        appendTablePrefix(ddlBuilder, tableName, columns);

        for (Map.Entry<String, ColumnType> entry : columns.entrySet()) {
            appendColumn(ddlBuilder, entry.getKey(), entry.getValue());
            ddlBuilder.append(", ");
        }

        if (primaryKeyColumns == null || primaryKeyColumns.isEmpty()) {
            ddlBuilder.setLength(ddlBuilder.length() - 2);
        } else {
            appendPrimaryKey(ddlBuilder, primaryKeyColumns);
        }

        appendTableSuffix(ddlBuilder, tableName, columns);
        executeDdl(database, ddlBuilder);
    }

    /**
     * Creates an index using the given parameters.
     *
     * @deprecated Use {@link #setUp} instead.
     */
    @Deprecated
    public void createIndex(
            SqlDatabase database,
            String tableName,
            List<String> columns,
            boolean isUnique)
            throws SQLException {

        StringBuilder ddlBuilder = new StringBuilder();
        appendIndexPrefix(ddlBuilder, tableName, columns, isUnique);

        appendIdentifier(ddlBuilder, columns.get(0));
        for (int i = 1, size = columns.size(); i < size; ++ i) {
            ddlBuilder.append(", ");
            appendIdentifier(ddlBuilder, columns.get(i));
        }

        appendIndexSuffix(ddlBuilder, tableName, columns, isUnique);
        executeDdl(database, ddlBuilder);
    }

    /**
     * @deprecated Use {@link #setUp} instead.
     */
    @Deprecated
    public void createRecord(SqlDatabase database) throws SQLException {
        if (database.hasTable(RECORD_TABLE_NAME)) {
            return;
        }

        Map<String, ColumnType> columns = new LinkedHashMap<String, ColumnType>();
        columns.put(SqlDatabase.ID_COLUMN, ColumnType.UUID);
        columns.put(SqlDatabase.TYPE_ID_COLUMN, ColumnType.UUID);
        columns.put(SqlDatabase.DATA_COLUMN, ColumnType.BYTES_LONG);

        createTable(database, RECORD_TABLE_NAME, columns, Arrays.asList(SqlDatabase.TYPE_ID_COLUMN, SqlDatabase.ID_COLUMN));
        createIndex(database, RECORD_TABLE_NAME, Arrays.asList(SqlDatabase.ID_COLUMN), true);
    }

    private static final Map<SqlIndex, ColumnType> INDEX_TYPES; static {
        Map<SqlIndex, ColumnType> m = new HashMap<SqlIndex, ColumnType>();
        m.put(SqlIndex.LOCATION, ColumnType.POINT);
        m.put(SqlIndex.NUMBER, ColumnType.DOUBLE);
        m.put(SqlIndex.REGION, ColumnType.POLYGON);
        m.put(SqlIndex.STRING, ColumnType.BYTES_SHORT);
        m.put(SqlIndex.UUID, ColumnType.UUID);
        INDEX_TYPES = m;
    }

    /**
     * @deprecated Use {@link #setUp} instead.
     */
    @Deprecated
    public void createRecordIndex(
            SqlDatabase database,
            String tableName,
            SqlIndex... types)
            throws SQLException {

        if (database.hasTable(tableName)) {
            return;
        }

        Map<String, ColumnType> columns = new LinkedHashMap<String, ColumnType>();
        columns.put(SqlDatabase.ID_COLUMN, ColumnType.UUID);
        columns.put(SqlDatabase.TYPE_ID_COLUMN, ColumnType.UUID);
        columns.put(SqlDatabase.SYMBOL_ID_COLUMN, ColumnType.INTEGER);
        for (int i = 0, length = types.length; i < length; ++ i) {
            columns.put(SqlDatabase.VALUE_COLUMN + (i == 0 ? "" : i + 1), INDEX_TYPES.get(types[i]));
        }

        List<String> primaryKeyColumns = new ArrayList<String>(columns.keySet());
        primaryKeyColumns.add(primaryKeyColumns.remove(0));

        createTable(database, tableName, columns, primaryKeyColumns);
        createIndex(database, tableName, Arrays.asList(SqlDatabase.ID_COLUMN), false);
    }

    /**
     * @deprecated Use {@link #setUp} instead.
     */
    @Deprecated
    public void createRecordUpdate(SqlDatabase database) throws SQLException {
        if (database.hasTable(RECORD_UPDATE_TABLE_NAME)) {
            return;
        }

        Map<String, ColumnType> columns = new LinkedHashMap<String, ColumnType>();
        columns.put(SqlDatabase.ID_COLUMN, ColumnType.UUID);
        columns.put(SqlDatabase.TYPE_ID_COLUMN, ColumnType.UUID);
        columns.put(SqlDatabase.UPDATE_DATE_COLUMN, ColumnType.DOUBLE);

        createTable(database, RECORD_UPDATE_TABLE_NAME, columns, Arrays.asList(SqlDatabase.ID_COLUMN));
        createIndex(database, RECORD_UPDATE_TABLE_NAME, Arrays.asList(SqlDatabase.TYPE_ID_COLUMN, SqlDatabase.UPDATE_DATE_COLUMN), false);
        createIndex(database, RECORD_UPDATE_TABLE_NAME, Arrays.asList(SqlDatabase.UPDATE_DATE_COLUMN), false);
    }

    /**
     * @deprecated Use {@link #setUp} instead.
     */
    @Deprecated
    public void createSymbol(SqlDatabase database) throws SQLException {
        if (database.hasTable(SYMBOL_TABLE_NAME)) {
            return;
        }

        Map<String, ColumnType> columns = new LinkedHashMap<String, ColumnType>();
        columns.put(SqlDatabase.SYMBOL_ID_COLUMN, ColumnType.SERIAL);
        columns.put(SqlDatabase.VALUE_COLUMN, ColumnType.BYTES_SHORT);

        createTable(database, SYMBOL_TABLE_NAME, columns, Arrays.asList(SqlDatabase.SYMBOL_ID_COLUMN));
        createIndex(database, SYMBOL_TABLE_NAME, Arrays.asList(SqlDatabase.VALUE_COLUMN), true);
    }

    protected void appendTablePrefix(
            StringBuilder builder,
            String name,
            Map<String, ColumnType> columns) {

        builder.append("CREATE TABLE ");
        appendIdentifier(builder, name);
        builder.append(" (");
    }

    protected void appendTableSuffix(
            StringBuilder builder,
            String name,
            Map<String, ColumnType> columns) {

        builder.append(')');
    }

    protected void appendColumn(StringBuilder builder, String name, ColumnType type) {
        appendColumnPrefix(builder, name);
        switch (type) {
            case BYTES_LONG :
                appendColumnTypeBytesLong(builder);
                break;
            case BYTES_SHORT :
                appendColumnTypeBytesShort(builder);
                break;
            case DOUBLE :
                appendColumnTypeDouble(builder);
                break;
            case INTEGER :
                appendColumnTypeInteger(builder);
                break;
            case POINT :
                appendColumnTypePoint(builder);
                break;
            case POLYGON :
                appendColumnTypePolygon(builder);
                break;
            case SERIAL :
                appendColumnTypeSerial(builder);
                break;
            case UUID :
                appendColumnTypeUuid(builder);
                break;
            default :
                throw new UnsupportedOperationException(String.format(
                        "Unknown column type! [%s]", type));
        }
    }

    protected void appendColumnPrefix(StringBuilder builder, String name) {
        appendIdentifier(builder, name);
        builder.append(' ');
    }

    protected void appendColumnTypeBytesLong(StringBuilder builder) {
        builder.append("BIT VARYING NOT NULL");
    }

    protected void appendColumnTypeBytesShort(StringBuilder builder) {
        builder.append("BIT VARYING(");
        builder.append(MAX_BYTES_SHORT_LENGTH * 8);
        builder.append(") NOT NULL");
    }

    protected void appendColumnTypeDouble(StringBuilder builder) {
        builder.append("DOUBLE NOT NULL");
    }

    protected void appendColumnTypeInteger(StringBuilder builder) {
        builder.append("INT NOT NULL");
    }

    protected void appendColumnTypePoint(StringBuilder builder) {
        builder.append("POINT NOT NULL");
    }

    protected void appendColumnTypePolygon(StringBuilder builder) {
        builder.append("POLYGON NOT NULL");
    }

    protected void appendColumnTypeSerial(StringBuilder builder) {
        builder.append("SERIAL NOT NULL");
    }

    protected void appendColumnTypeUuid(StringBuilder builder) {
        builder.append("UUID NOT NULL");
    }

    protected void appendPrimaryKey(StringBuilder builder, List<String> columns) {
        builder.append("PRIMARY KEY (");
        appendIdentifier(builder, columns.get(0));
        for (int i = 1, size = columns.size(); i < size; ++ i) {
            builder.append(", ");
            appendIdentifier(builder, columns.get(i));
        }
        builder.append(')');
    }

    protected void appendIndexPrefix(
            StringBuilder builder,
            String tableName,
            List<String> columns,
            boolean isUnique) {

        StringBuilder nameBuilder = new StringBuilder();
        nameBuilder.append("k_");
        nameBuilder.append(tableName);
        for (String column : columns) {
            nameBuilder.append('_');
            nameBuilder.append(column);
        }

        builder.append("CREATE");
        if (isUnique) {
            builder.append(" UNIQUE");
        }
        builder.append(" INDEX ");
        appendIdentifier(builder, nameBuilder.toString());
        builder.append(" ON ");
        appendIdentifier(builder, tableName);
        builder.append(" (");
    }

    protected void appendIndexSuffix(
            StringBuilder builder,
            String tableName,
            List<String> columns,
            boolean isUnique) {

        builder.append(')');
    }

    private void executeDdl(SqlDatabase database, StringBuilder ddlBuilder) throws SQLException {
        String ddl = ddlBuilder.toString();
        ddlBuilder.setLength(0);

        Connection connection = database.openConnection();
        try {
            Statement statement = connection.createStatement();
            try {
                statement.execute(ddl);
            } finally {
                statement.close();
            }

        } finally {
            database.closeConnection(connection);
        }
    }

    protected void appendSelectFields(StringBuilder builder, List<String> fields) {
        appendIdentifier(builder, SqlDatabase.DATA_COLUMN);
    }

    public boolean isDuplicateKeyException(SQLException ex) {
        return "23000".equals(ex.getSQLState());
    }

    public String convertRawToStringSql(String field) {
        return "CONVERT(" + field + " USING utf8)";
    }

    public UUID getUuid(ResultSet result, int col) throws SQLException {
        return UuidUtils.fromBytes(result.getBytes(col));
    }

    public String getSelectTimestampMillisSql() {
        // This should return the entire select statement, including the SELECT and FROM clauses, if necessary.
        return null;
    }

    /* ******************* METRICS ******************* */
    // These are all very vendor-specific.
    public void appendMetricUpdateDataSql(StringBuilder sql, String columnIdentifier, List<Object> parameters, double amount, long eventDate, boolean increment, boolean updateFuture) {
        // This DOES shift the decimal place and round to 6 places.
        // columnIdentifier is "`data`" - already quoted if it needs to be
        throw new UnsupportedOperationException("appendMetricUpdateDataSql: Metrics is not fully implemented for this vendor.");
    }
    public void appendMetricFixDataSql(StringBuilder sql, String columnIdentifier, List<Object> parameters, long eventDate, double cumulativeAmount, double amount) {
        // This DOES shift the decimal place and round to 6 places.
        // columnIdentifier is "`data`" - already quoted if it needs to be
        throw new UnsupportedOperationException("appendMetricFixDataSql: Metrics is not fully implemented for this vendor.");
    }
    public void appendMetricSelectAmountSql(StringBuilder str, String columnIdentifier, int position) {
        // This does NOT shift the decimal place or round to 6 places. Do it yourself AFTER any other arithmetic to avoid rounding errors.
        // position is 1 or 2 (use the MetricAccess.*_POSITION constants)
        // columnIdentifier is "`data`" or "MAX(`data`)" - already quoted if it needs to be
        throw new UnsupportedOperationException("appendMetricSelectAmountSql: Metrics is not fully implemented for this vendor.");
    }
    public void appendMetricSelectTimestampSql(StringBuilder str, String columnIdentifier) {
        // This does NOT shift the decimal place - the result will need to be multiplied
        // by MetricAccess.DATE_DECIMAL_SHIFT to get a timestamp in milliseconds.
        // columnIdentifier is "`data`" or "MAX(`data`)" - already escaped
        throw new UnsupportedOperationException("appendMetricSelectTimestampSql: Metrics is not fully implemented for this vendor.");
    }
    public void appendMetricDateFormatTimestampSql(StringBuilder str, String columnIdentifier, MetricInterval metricInterval) {
        // This DOES apply MetricAccess.DATE_DECIMAL_SHIFT and returns SQL to provide a string formatted according to MetricInterval.getSqlTruncatedDateFormat(SqlVendor)
        throw new UnsupportedOperationException("appendMetricDateFormatTimestampSql: Metrics is not fully implemented for this vendor.");
    }
    public void appendMetricEncodeTimestampSql(StringBuilder str, List<Object> parameters, long timestamp, Character rpadHexChar) {
        // This accepts a normal timestamp and DOES apply MetricAccess.DATE_DECIMAL_SHIFT
        throw new UnsupportedOperationException("appendMetricEncodeTimestampSql: Metrics is not fully implemented for this vendor.");
    }
    public void appendBindMetricBytes(StringBuilder str, byte[] bytes, List<Object> parameters) {
        appendBindValue(str, bytes, parameters);
    }
    public void appendMetricDataBytes(StringBuilder str, String columnIdentifier) {
        // columnIdentifier is "`data`" or "MAX(`data`)" - already escaped
        str.append(columnIdentifier);
    }

    /* ******************* METRICS ******************* */

    public static class H2 extends SqlVendor {

        @Override
        protected String getSetUpResourcePath() {
            return "h2/schema-12.sql";
        }

        @Override
        public void appendIdentifier(StringBuilder builder, String identifier) {
            builder.append(identifier);
        }

        @Override
        protected void appendUuid(StringBuilder builder, UUID value) {
            builder.append('\'');
            builder.append(value);
            builder.append('\'');
        }

        @Override
        protected void appendColumnTypeBytesLong(StringBuilder builder) {
            builder.append("LONGVARBINARY NOT NULL");
        }

        @Override
        protected void appendColumnTypeBytesShort(StringBuilder builder) {
            builder.append("VARBINARY(");
            builder.append(MAX_BYTES_SHORT_LENGTH);
            builder.append(") NOT NULL");
        }

        @Override
        protected void appendColumnTypePoint(StringBuilder builder) {
            builder.append("DOUBLE NOT NULL");
        }

        @Override
        protected void appendColumnTypePolygon(StringBuilder builder) {
            builder.append("DOUBLE NOT NULL");
        }

        @Override
        public boolean isDuplicateKeyException(SQLException ex) {
            return "23001".equals(ex.getSQLState()) || "23505".equals(ex.getSQLState());
        }

        @Override
        public String convertRawToStringSql(String field) {
            return "UTF8TOSTRING(" + field + ")";
        }

    }

    public static class MySQL extends SqlVendor {

        private volatile Boolean statementReplication;
        private volatile Boolean hasSTMethod;
        private volatile Boolean hasUdfGetFields;
        private volatile Boolean hasUdfIncrementMetric;

        private static final Logger LOGGER = LoggerFactory.getLogger(MySQL.class);

        @Override
        public void setTransactionIsolation(Connection connection) throws SQLException {
            if (statementReplication == null) {
                Statement statement = connection.createStatement();

                try {
                    ResultSet result = statement.executeQuery("SHOW VARIABLES WHERE variable_name IN ('log_bin', 'binlog_format')");

                    try {
                        boolean binLogEnabled = false;
                        while (result.next()) {
                            if ("binlog_format".equalsIgnoreCase(result.getString(1))) {
                                statementReplication = "STATEMENT".equalsIgnoreCase(result.getString(2));
                            } else if ("log_bin".equalsIgnoreCase(result.getString(1))) {
                                binLogEnabled = (!"OFF".equalsIgnoreCase(result.getString(2)));
                            }
                        }
                        // Server is using statement replication only if log_bin is not OFF and binlog_format is STATEMENT
                        statementReplication = (binLogEnabled && (statementReplication != null ? statementReplication : false));

                        if (statementReplication) {
                            LOGGER.warn(
                                    "Using REPEATABLE READ transaction isolation due to statement-based replication."
                                            + " This may cause reduced performance under load."
                                            + " Please use mixed-mode replication (Add 'binlog_format = mixed' to my.cnf).");
                        }

                    } finally {
                        result.close();
                    }

                } finally {
                    statement.close();
                }
            }

            if (!statementReplication) {
                connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
            }
        }

        @Override
        protected String getSetUpResourcePath() {
            return "mysql/schema-12.sql";
        }

        @Override
        public boolean useSavepoint() {
            return false;
        }

        @Override
        public void appendIdentifier(StringBuilder builder, String identifier) {
            builder.append('`');
            builder.append(identifier.replace("`", "``"));
            builder.append('`');
        }

        @Override
        protected void appendUuid(StringBuilder builder, UUID value) {
            appendBytes(builder, UuidUtils.toBytes(value));
        }

        @Override
        protected void appendTableSuffix(
                StringBuilder builder,
                String name,
                Map<String, ColumnType> columns) {

            builder.append(") ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_bin");
        }

        @Override
        protected void appendColumnTypeBytesLong(StringBuilder builder) {
            builder.append("LONGBLOB NOT NULL");
        }

        @Override
        protected void appendColumnTypeBytesShort(StringBuilder builder) {
            builder.append("VARBINARY(");
            builder.append(MAX_BYTES_SHORT_LENGTH);
            builder.append(") NOT NULL");
        }

        @Override
        protected void appendColumnTypeSerial(StringBuilder builder) {
            builder.append("INT NOT NULL AUTO_INCREMENT");
        }

        @Override
        protected void appendColumnTypeUuid(StringBuilder builder) {
            builder.append("BINARY(16) NOT NULL");
        }

        @Override
        protected void appendSelectFields(StringBuilder builder, List<String> fields) {
            SqlDatabase database = getDatabase();

            if (hasUdfGetFields == null) {
                Connection connection = database.openConnection();

                try {
                    Statement statement = connection.createStatement();
                    try {
                        ResultSet result = statement.executeQuery("SELECT dari_get_fields('{}', 'test')");
                        try {
                            hasUdfGetFields = true;
                        } finally {
                            result.close();
                        }
                    } finally {
                        statement.close();
                    }

                } catch (SQLException error) {
                    if ("42000".equals(error.getSQLState())) {
                        hasUdfGetFields = false;
                    } else {
                        LOGGER.error("Exception when checking for dari_get_fields()", error);
                        hasUdfGetFields = false;
                    }

                } finally {
                    database.closeConnection(connection);
                }
            }

            if (Boolean.TRUE.equals(hasUdfGetFields)) {
                builder.append("dari_get_fields(r.");
                appendIdentifier(builder, SqlDatabase.DATA_COLUMN);

                for (ObjectField field : database.getEnvironment().getFields()) {
                    builder.append(", ");
                    appendValue(builder, field.getInternalName());
                }

                for (String field : fields) {
                    builder.append(", ");
                    appendValue(builder, field);
                }

                builder.append(')');

            } else {
                builder.append("r.");
                appendIdentifier(builder, SqlDatabase.DATA_COLUMN);
            }
        }

        @Override
        public String getSelectTimestampMillisSql() {
            return "SELECT UNIX_TIMESTAMP()*1000";
        }

        /* Spatial Support */

        public String getGeometryContainsMethod() {
            SqlDatabase database = getDatabase();

            if (hasSTMethod == null) {
                Connection connection = database.openConnection();
                Statement statement = null;
                ResultSet result = null;

                try {
                    statement = connection.createStatement();
                    result = statement.executeQuery("SELECT ST_Contains(null, null)");
                    hasSTMethod = true;
                } catch (SQLException error) {
                    if ("42000".equals(error.getSQLState())) {
                        hasSTMethod = false;
                    } else {
                        LOGGER.error("Exception when checking for ST_Contains()", error);
                        hasSTMethod = false;
                    }
                } finally {
                    database.closeResources(null, connection, statement, result);
                }
            }

            if (Boolean.TRUE.equals(hasSTMethod)) {
                return "ST_Contains";
            }

            return "MBRContains";
        }

        public void appendBindLocation(StringBuilder builder, Location location, List<Object> parameters) {
            builder.append("GeomFromText(?)");
            if (parameters != null) {
                parameters.add(location == null ? null : "POINT(" + location.getX() + " " + location.getY() + ")");
            }
        }

        public void appendBindRegion(StringBuilder builder, Region region, List<Object> parameters) {
            builder.append("GeomFromText(?)");
            if (parameters != null) {
                StringBuilder b = new StringBuilder();

                b.append("MULTIPOLYGON(");
                for (Region.Polygon polygon : region.getPolygons()) {
                    for (Region.LinearRing ring : polygon) {
                        b.append("((");
                        for (Region.Coordinate coordinate : ring) {
                            b.append(SqlDatabase.quoteValue(coordinate.getLatitude()));
                            b.append(' ');
                            b.append(SqlDatabase.quoteValue(coordinate.getLongitude()));
                            b.append(", ");
                        }
                        b.setLength(b.length() - 2);
                        b.append(")), ");
                    }
                }

                for (Region.Circle circles : region.getCircles()) {
                    for (Region.Polygon polygon : circles.getPolygons()) {
                        for (Region.LinearRing ring : polygon) {
                            b.append("((");
                            for (Region.Coordinate coordinate : ring) {
                                b.append(SqlDatabase.quoteValue(coordinate.getLatitude()));
                                b.append(' ');
                                b.append(SqlDatabase.quoteValue(coordinate.getLongitude()));
                                b.append(", ");
                            }
                            b.setLength(b.length() - 2);
                            b.append(")), ");
                        }
                    }
                }

                b.setLength(b.length() - 2);
                b.append(")");

                parameters.add(b.toString());
            }
        }

        protected void appendWhereRegion(StringBuilder builder, Region region, String field) {
            List<Location> locations = region.getLocations();

            builder.append(getGeometryContainsMethod() + "(GEOMFROMTEXT('POLYGON((");
            for (Location location : locations) {
                builder.append(SqlDatabase.quoteValue(location.getX()));
                builder.append(' ');
                builder.append(SqlDatabase.quoteValue(location.getY()));
                builder.append(", ");
            }
            builder.setLength(builder.length() - 2);
            builder.append("))'), ");
            builder.append(field);
            builder.append(')');
        }

        protected void appendWhereLocation(StringBuilder builder, Location location, String field) {
            builder.append(getGeometryContainsMethod() + "(");
            builder.append(field);
            builder.append(", GEOMFROMTEXT('POINT(");
            builder.append(SqlDatabase.quoteValue(location.getX()));
            builder.append(' ');
            builder.append(SqlDatabase.quoteValue(location.getY()));
            builder.append(")'))");
        }

        protected void appendNearestLocation(
                StringBuilder orderbyBuilder,
                StringBuilder selectBuilder,
                Location location, String field) {

            StringBuilder builder = new StringBuilder();

            builder.append("GLENGTH(LINESTRING(GEOMFROMTEXT('POINT(");
            builder.append(location.getX());
            builder.append(' ');
            builder.append(location.getY());
            builder.append(")'), ");
            builder.append(field);
            builder.append("))");

            orderbyBuilder.append(builder);
            selectBuilder.append(builder);
        }

        /* ******************* METRICS ******************* */

        @Override
        public void appendMetricUpdateDataSql(StringBuilder sql, String columnIdentifier, List<Object> parameters, double amount, long eventDate, boolean increment, boolean updateFuture) {

            SqlDatabase database = getDatabase();

            if (hasUdfIncrementMetric == null) {
                Connection connection = database.openConnection();

                try {
                    Statement statement = connection.createStatement();
                    try {
                        ResultSet result = statement.executeQuery("SELECT dari_increment_metric(null, 0, 0)");
                        try {
                            hasUdfIncrementMetric = true;
                        } finally {
                            result.close();
                        }
                    } finally {
                        statement.close();
                    }

                } catch (SQLException error) {
                    if ("42000".equals(error.getSQLState())) {
                        hasUdfIncrementMetric = false;
                    } else {
                        LOGGER.error("Exception when checking for dari_increment_metric()", error);
                        hasUdfIncrementMetric = false;
                    }

                } finally {
                    database.closeConnection(connection);
                }
            }

            if (Boolean.TRUE.equals(hasUdfIncrementMetric)) {
                // dari_increment_metric() does NOT shift the decimal place for us.
                long adjustedAmount = (long) (amount * MetricAccess.AMOUNT_DECIMAL_SHIFT);
                sql.append("dari_increment_metric(");
                sql.append(columnIdentifier);
                sql.append(',');
                if (increment) { // increment
                    // cumulativeAmount should always be incremented
                    appendBindValue(sql, adjustedAmount, parameters);
                    sql.append(',');
                    if (updateFuture) {
                        // if we're updating future rows, only update the interval amount if it's the exact eventDate
                        sql.append("IF (");
                            appendIdentifier(sql, columnIdentifier);
                            sql.append(" <= ");
                            appendMetricEncodeTimestampSql(sql, parameters, eventDate, 'F');
                            sql.append(','); // if it's the exact date, then update the amount
                            appendBindValue(sql, adjustedAmount, parameters);
                            sql.append(','); // if it's a date in the future, leave the date alone
                            appendBindValue(sql, 0, parameters);
                        sql.append(')');
                    } else {
                        appendBindValue(sql, adjustedAmount, parameters);
                    }
                } else { // only used for set, not increment
                    appendBindValue(sql, amount, parameters);
                    sql.append(',');
                    appendBindValue(sql, amount, parameters);
                }
                sql.append(')');
            } else {
                sql.append(" UNHEX(");
                    sql.append("CONCAT(");
                        // timestamp
                        appendHexEncodeExistingTimestampSql(sql, columnIdentifier);
                        sql.append(',');
                        // cumulativeAmount and amount
                        if (increment) {
                            // cumulativeAmount should always be incremented
                            appendHexEncodeIncrementAmountSql(sql, parameters, columnIdentifier, MetricAccess.CUMULATIVEAMOUNT_POSITION, amount);
                            sql.append(',');
                            if (updateFuture) {
                                // if we're updating future rows, only update the interval amount if it's the exact eventDate
                                sql.append("IF (");
                                    appendIdentifier(sql, columnIdentifier);
                                    sql.append(" <= ");
                                    appendMetricEncodeTimestampSql(sql, parameters, eventDate, 'F');
                                    sql.append(','); // if it's the exact date, then update the amount
                                    appendHexEncodeIncrementAmountSql(sql, parameters, columnIdentifier, MetricAccess.AMOUNT_POSITION, amount);
                                    sql.append(','); // if it's a date in the future, leave the date alone
                                    appendHexEncodeIncrementAmountSql(sql, parameters, columnIdentifier, MetricAccess.AMOUNT_POSITION, 0);
                                sql.append(')');
                            } else {
                                appendHexEncodeIncrementAmountSql(sql, parameters, columnIdentifier, MetricAccess.AMOUNT_POSITION, amount);
                            }
                        } else {
                            appendHexEncodeSetAmountSql(sql, parameters, amount);
                            sql.append(',');
                            appendHexEncodeSetAmountSql(sql, parameters, amount);
                        }
                    sql.append(" )");
                sql.append(" )");
            }
        }

        @Override
        public void appendMetricFixDataSql(StringBuilder sql, String columnIdentifier, List<Object> parameters, long eventDate, double cumulativeAmount, double amount) {
            sql.append(" UNHEX(");
                sql.append("CONCAT(");
                    // timestamp
                    appendHexEncodeExistingTimestampSql(sql, columnIdentifier);
                    sql.append(',');
                    // cumulativeAmount
                    appendHexEncodeSetAmountSql(sql, parameters, cumulativeAmount);
                    sql.append(',');
                    // amount
                    appendHexEncodeSetAmountSql(sql, parameters, amount);
                sql.append(" )");
            sql.append(" )");
        }

        @Override
        public void appendMetricSelectAmountSql(StringBuilder str, String columnIdentifier, int position) {
            str.append("CONV(");
                str.append("HEX(");
                    str.append("SUBSTR(");
                        str.append(columnIdentifier);
                        str.append(',');
                        appendValue(str, 1 + MetricAccess.DATE_BYTE_SIZE + ((position - 1) * MetricAccess.AMOUNT_BYTE_SIZE));
                        str.append(',');
                        appendValue(str, MetricAccess.AMOUNT_BYTE_SIZE);
                    str.append(')');
                str.append(')');
            str.append(", 16, -10)");
        }

        @Override
        public void appendMetricEncodeTimestampSql(StringBuilder str, List<Object> parameters, long timestamp, Character rpadHexChar) {
            str.append("UNHEX(");
            appendHexEncodeTimestampSql(str, parameters, timestamp, rpadHexChar);
            str.append(')');
        }

        @Override
        public void appendMetricSelectTimestampSql(StringBuilder str, String columnIdentifier) {
            str.append("CONV(");
                str.append("HEX(");
                    str.append("SUBSTR(");
                        str.append(columnIdentifier);
                        str.append(',');
                        appendValue(str, 1);
                        str.append(',');
                        appendValue(str, MetricAccess.DATE_BYTE_SIZE);
                    str.append(')');
                str.append(')');
            str.append(", 16, 10)");
        }

        @Override
        public void appendMetricDateFormatTimestampSql(StringBuilder str, String columnIdentifier, MetricInterval metricInterval) {
            str.append("DATE_FORMAT(FROM_UNIXTIME(");
            appendMetricSelectTimestampSql(str, columnIdentifier);
            str.append('*');
            appendValue(str, (MetricAccess.DATE_DECIMAL_SHIFT / 1000L));
            str.append("),");
            appendValue(str, metricInterval.getSqlTruncatedDateFormat(this));
            str.append(')');
        }

        private void appendHexEncodeSetAmountSql(StringBuilder str, List<Object> parameters, double amount) {
            str.append("LPAD(");
                str.append("HEX(");
                    appendBindValue(str, (long) (amount * MetricAccess.AMOUNT_DECIMAL_SHIFT), parameters);
                str.append(" )");
            str.append(", " + (MetricAccess.AMOUNT_BYTE_SIZE * 2) + ", '0')");
        }

        private void appendHexEncodeExistingTimestampSql(StringBuilder str, String columnIdentifier) {
            // columnIdentifier is "data" or "max(`data`)" - already quoted
            str.append("HEX(");
                str.append("SUBSTR(");
                    str.append(columnIdentifier);
                    str.append(',');
                    appendValue(str, 1);
                    str.append(',');
                    appendValue(str, MetricAccess.DATE_BYTE_SIZE);
                str.append(')');
            str.append(')');
        }

        private void appendHexEncodeTimestampSql(StringBuilder str, List<Object> parameters, long timestamp, Character rpadChar) {
            if (rpadChar != null) {
                str.append("RPAD(");
            }
            str.append("LPAD(");
                str.append("HEX(");
                    if (parameters == null) {
                        appendValue(str, (int) (timestamp / MetricAccess.DATE_DECIMAL_SHIFT));
                    } else {
                        appendBindValue(str, (int) (timestamp / MetricAccess.DATE_DECIMAL_SHIFT), parameters);
                    }
                str.append(')');
            str.append(", " + (MetricAccess.DATE_BYTE_SIZE * 2) + ", '0')");
            if (rpadChar != null) {
                str.append(',');
                appendValue(str, MetricAccess.DATE_BYTE_SIZE * 2 + MetricAccess.AMOUNT_BYTE_SIZE * 2 + MetricAccess.AMOUNT_BYTE_SIZE * 2);
                str.append(", '");
                str.append(rpadChar);
                str.append("')");
            }
        }

        private void appendHexEncodeIncrementAmountSql(StringBuilder str, List<Object> parameters, String columnIdentifier, int position, double amount) {
            // position is 1 or 2
            // columnIdentifier is "`data`" unless it is aliased - already quoted
            str.append("LPAD(");
                str.append("HEX(");
                    // conv(hex(substr(data, 1+4, 8)), 16, 10)
                    appendMetricSelectAmountSql(str, columnIdentifier, position);
                    str.append('+');
                    appendBindValue(str, (long) (amount * MetricAccess.AMOUNT_DECIMAL_SHIFT), parameters);
                str.append(" )");
            str.append(", " + (MetricAccess.AMOUNT_BYTE_SIZE * 2) + ", '0')");
        }

        /* ******************* METRICS ******************* */

    }

    public static class PostgreSQL extends SqlVendor {

        @Override
        protected String getSetUpResourcePath() {
            return "postgres/schema-12.sql";
        }

        @Override
        protected boolean hasTable(Connection connection, String tableName) throws SQLException {
            return getTables(connection).contains(tableName.toLowerCase(Locale.ENGLISH));
        }

        @Override
        protected void catchSetUpError(SQLException error) throws SQLException {
            if (!Arrays.asList("42P07").contains(error.getSQLState())) {
                throw error;
            }
        }

        @Override
        public void appendIdentifier(StringBuilder builder, String identifier) {
            builder.append(identifier.toLowerCase());
        }

        @Override
        public void appendAlias(StringBuilder builder, String alias) {
            builder.append("\"" + alias + "\"");
        }

        @Override
        protected void appendUuid(StringBuilder builder, UUID value) {
            builder.append("'" + value.toString() + "'");
        }

        @Override
        public void appendValue(StringBuilder builder, Object value) {
            if (value instanceof String) {
                builder.append("'" + StringUtils.escapeSql((String) value) + "'");

            } else if (value instanceof Location) {
                Location valueLocation = (Location) value;
                builder.append("ST_GeomFromText('POINT(");
                builder.append(valueLocation.getX());
                builder.append(' ');
                builder.append(valueLocation.getY());
                builder.append(")', 4326)");

            } else {
                super.appendValue(builder, value);
            }
        }

        @Override
        protected void appendBytes(StringBuilder builder, byte[] value) {
            builder.append('\'');
            builder.append(new String(value, StandardCharsets.UTF_8).replace("'", "''"));
            builder.append('\'');
        }

        @Override
        protected void appendWhereRegion(StringBuilder builder, Region region, String field) {
            builder.append(field);
            builder.append(" <-> ST_SetSRID(ST_MakePoint(");
            builder.append(region.getX());
            builder.append(", ");
            builder.append(region.getY());
            builder.append("), 4326) < ");
            builder.append(region.getRadius());
        }

        @Override
        protected void appendWhereLocation(StringBuilder builder, Location location, String field) {
            builder.append("ST_Contains(");
            builder.append(field);
            builder.append(", ST_GEOMFROMTEXT('POINT(");
            builder.append(SqlDatabase.quoteValue(location.getX()));
            builder.append(' ');
            builder.append(SqlDatabase.quoteValue(location.getY()));
            builder.append(")', 4326))");
        }

        @Override
        protected void appendNearestLocation(
                StringBuilder orderbyBuilder,
                StringBuilder selectBuilder,
                Location location, String field) {

            StringBuilder builder = new StringBuilder();

            builder.append(field);
            builder.append(" <-> ST_SetSRID(ST_MakePoint(");
            builder.append(location.getX());
            builder.append(", ");
            builder.append(location.getY());
            builder.append("), 4326) ");

            orderbyBuilder.append(builder);
            selectBuilder.append(builder);
        }

        @Override
        public void appendBindLocation(StringBuilder builder, Location location, List<Object> parameters) {
            builder.append("ST_GeomFromText(?, 4326)");
            if (location != null && parameters != null) {
                parameters.add("POINT(" + location.getX() + " " + location.getY() + ")");
            }
        }

        @Override
        public void appendBindRegion(StringBuilder builder, Region region, List<Object> parameters) {
            builder.append("ST_GeomFromText(?, 4326)");
            if (parameters != null) {
                StringBuilder b = new StringBuilder();

                b.append("MULTIPOLYGON(");
                for (Region.Polygon polygon : region.getPolygons()) {
                    for (Region.LinearRing ring : polygon) {
                        b.append("((");
                        for (Region.Coordinate coordinate : ring) {
                            b.append(SqlDatabase.quoteValue(coordinate.getLatitude()));
                            b.append(' ');
                            b.append(SqlDatabase.quoteValue(coordinate.getLongitude()));
                            b.append(", ");
                        }
                        b.setLength(b.length() - 2);
                        b.append(")), ");
                    }
                }

                for (Region.Circle circles : region.getCircles()) {
                    for (Region.Polygon polygon : circles.getPolygons()) {
                        for (Region.LinearRing ring : polygon) {
                            b.append("((");
                            for (Region.Coordinate coordinate : ring) {
                                b.append(SqlDatabase.quoteValue(coordinate.getLatitude()));
                                b.append(' ');
                                b.append(SqlDatabase.quoteValue(coordinate.getLongitude()));
                                b.append(", ");
                            }
                            b.setLength(b.length() - 2);
                            b.append(")), ");
                        }
                    }
                }

                b.setLength(b.length() - 2);
                b.append(")");

                parameters.add(b);
            }
        }

        @Override
        public void appendBindUuid(StringBuilder builder, UUID uuid, List<Object> parameters) {
            builder.append('?');
            if (uuid != null && parameters != null) {
                parameters.add(uuid);
            }
        }

        @Override
        public String convertRawToStringSql(String field) {
            return "CONVERT_FROM(" + field + ", 'UTF-8')";
        }

        @Override
        public UUID getUuid(ResultSet result, int col) throws SQLException {
            return UuidUtils.fromString(result.getString(col));
        }

        @Override
        public String getSelectTimestampMillisSql() {
            return "SELECT ROUND(EXTRACT(EPOCH FROM NOW())*1000)";
        }

        /* ******************* METRICS ******************* */
        @Override
        public void appendMetricUpdateDataSql(StringBuilder sql, String columnIdentifier, List<Object> parameters, double amount, long eventDate, boolean increment, boolean updateFuture) {

            sql.append("CONCAT(");
                // timestamp
                appendHexEncodeExistingTimestampSql(sql, columnIdentifier);
                sql.append(',');
                // cumulativeAmount and amount
                if (increment) {
                    // cumulativeAmount should always be incremented
                    appendHexEncodeIncrementAmountSql(sql, parameters, columnIdentifier, MetricAccess.CUMULATIVEAMOUNT_POSITION, amount);
                    sql.append(',');
                    if (updateFuture) {
                        // if we're updating future rows, only update the interval amount if it's the exact eventDate
                        sql.append("CASE WHEN ");
                            appendIdentifier(sql, columnIdentifier);
                            sql.append(" <= ");
                            appendMetricEncodeTimestampSql(sql, parameters, eventDate, 'F');
                            sql.append(" THEN "); // if it's the exact date, then update the amount
                            appendHexEncodeIncrementAmountSql(sql, parameters, columnIdentifier, MetricAccess.AMOUNT_POSITION, amount);
                            sql.append(" ELSE "); // if it's a date in the future, leave the date alone
                            appendHexEncodeIncrementAmountSql(sql, parameters, columnIdentifier, MetricAccess.AMOUNT_POSITION, 0);
                        sql.append(" END ");
                    } else {
                        appendHexEncodeIncrementAmountSql(sql, parameters, columnIdentifier, MetricAccess.AMOUNT_POSITION, amount);
                    }
                } else {
                    appendHexEncodeSetAmountSql(sql, parameters, amount);
                    sql.append(',');
                    appendHexEncodeSetAmountSql(sql, parameters, amount);
                }
            sql.append(" )");
        }

        @Override
        public void appendMetricFixDataSql(StringBuilder sql, String columnIdentifier, List<Object> parameters, long eventDate, double cumulativeAmount, double amount) {
            sql.append("CONCAT(");
                // timestamp
                appendHexEncodeExistingTimestampSql(sql, columnIdentifier);
                sql.append(',');
                // cumulativeAmount
                appendHexEncodeSetAmountSql(sql, parameters, cumulativeAmount);
                sql.append(',');
                // amount
                appendHexEncodeSetAmountSql(sql, parameters, amount);
            sql.append(" )");
        }

        @Override
        public void appendMetricEncodeTimestampSql(StringBuilder str, List<Object> parameters, long timestamp, Character rpadHexChar) {
            if (rpadHexChar != null) {
                str.append("RPAD(");
            }
            str.append("LPAD(");
                str.append("TO_HEX(");
                    if (parameters == null) {
                        appendValue(str, (int) (timestamp / MetricAccess.DATE_DECIMAL_SHIFT));
                    } else {
                        appendBindValue(str, (int) (timestamp / MetricAccess.DATE_DECIMAL_SHIFT), parameters);
                    }
                str.append(')');
            str.append(", " + (MetricAccess.DATE_BYTE_SIZE * 2) + ", '0')");
            if (rpadHexChar != null) {
                str.append(',');
                appendValue(str, MetricAccess.DATE_BYTE_SIZE * 2 + MetricAccess.AMOUNT_BYTE_SIZE * 2 + MetricAccess.AMOUNT_BYTE_SIZE * 2);
                str.append(", '");
                str.append(rpadHexChar);
                str.append("')");
            }
        }

        @Override
        public void appendMetricSelectAmountSql(StringBuilder str, String columnIdentifier, int position) {
            str.append(" ('x'||");
                str.append("SUBSTRING(");
                    appendIdentifier(str, columnIdentifier);
                    str.append(',');
                    appendValue(str, 1 + (MetricAccess.DATE_BYTE_SIZE * 2) + ((position - 1) * MetricAccess.AMOUNT_BYTE_SIZE * 2));
                    str.append(',');
                    appendValue(str, MetricAccess.AMOUNT_BYTE_SIZE * 2);
                str.append(')');
            str.append(")::bit(");
            str.append(MetricAccess.AMOUNT_BYTE_SIZE * 8);
            str.append(")::bigint");
        }

        @Override
        public void appendMetricSelectTimestampSql(StringBuilder str, String columnIdentifier) {
            str.append(" ('x'||");
                str.append("SUBSTRING(");
                    appendIdentifier(str, columnIdentifier);
                    str.append(',');
                    appendValue(str, 1);
                    str.append(',');
                    appendValue(str, MetricAccess.DATE_BYTE_SIZE * 2);
                str.append(')');
            str.append(")::bit(");
            str.append(MetricAccess.DATE_BYTE_SIZE * 8);
            str.append(")::bigint");
        }

        @Override
        public void appendMetricDateFormatTimestampSql(StringBuilder str, String columnIdentifier, MetricInterval metricInterval) {
            str.append("TO_CHAR(TO_TIMESTAMP(");
            appendMetricSelectTimestampSql(str, columnIdentifier);
            str.append('*');
            appendValue(str, (MetricAccess.DATE_DECIMAL_SHIFT / 1000L));
            str.append(")::TIMESTAMP,");
            appendValue(str, metricInterval.getSqlTruncatedDateFormat(this));
            str.append(')');
        }

        @Override
        public void appendBindMetricBytes(StringBuilder str, byte[] bytes, List<Object> parameters) {
            appendValue(str, StringUtils.hex(bytes));
        }

        @Override
        public void appendMetricDataBytes(StringBuilder str, String columnIdentifier) {
            str.append("DECODE(");
            str.append(columnIdentifier);
            str.append(", 'HEX')");
        }

        private void appendHexEncodeExistingTimestampSql(StringBuilder str, String columnIdentifier) {
            // columnIdentifier is "data" or "max(`data`)" - already quoted
            str.append("SUBSTRING(");
                appendIdentifier(str, columnIdentifier);
                str.append(',');
                appendValue(str, 1);
                str.append(',');
                appendValue(str, MetricAccess.DATE_BYTE_SIZE * 2);
            str.append(')');
        }

        private void appendHexEncodeIncrementAmountSql(StringBuilder str, List<Object> parameters, String columnIdentifier, int position, double amount) {
            // position is 1 or 2
            // columnIdentifier is "`data`" unless it is aliased - already quoted
            str.append("LPAD(");
                str.append("TO_HEX(");
                    // conv(hex(substr(data, 1+4, 8)), 16, 10)
                    appendMetricSelectAmountSql(str, columnIdentifier, position);
                    str.append('+');
                    appendBindValue(str, (long) (amount * MetricAccess.AMOUNT_DECIMAL_SHIFT), parameters);
                str.append(" )");
            str.append(", " + (MetricAccess.AMOUNT_BYTE_SIZE * 2) + ", '0')");
        }

        private void appendHexEncodeSetAmountSql(StringBuilder str, List<Object> parameters, double amount) {
            str.append("LPAD(");
                str.append("TO_HEX(");
                    appendBindValue(str, (long) (amount * MetricAccess.AMOUNT_DECIMAL_SHIFT), parameters);
                str.append(" )");
            str.append(", " + (MetricAccess.AMOUNT_BYTE_SIZE * 2) + ", '0')");
        }

        /* ******************* METRICS ******************* */
    }

    public static class Oracle extends SqlVendor {

        @Override
        public void appendIdentifier(StringBuilder builder, String identifier) {
            builder.append(identifier.replace("\"", "\"\""));
        }

        @Override
        protected void appendUuid(StringBuilder builder, UUID value) {
            appendBytes(builder, UuidUtils.toBytes(value));
        }

        @Override
        protected void appendBytes(StringBuilder builder, byte[] value) {
            builder.append("HEXTORAW('");
            builder.append(StringUtils.hex(value));
            builder.append("')");
        }

        @Override
        protected void appendWhereRegion(StringBuilder builder, Region region, String field) {
            throw new UnsupportedIndexException(this, field);
        }

        @Override
        protected void appendNearestLocation(
                StringBuilder orderbyBuilder,
                StringBuilder selectBuilder,
                Location location, String field) {
            throw new UnsupportedIndexException(this, field);
        }

        @Override
        protected String rewriteQueryWithLimitClause(String query, int limit, long offset) {
            return String.format("SELECT * FROM "
                    + "    (SELECT a.*, ROWNUM rnum FROM "
                    + "        (%s) a "
                    + "      WHERE ROWNUM <= %d)"
                    + " WHERE rnum  >= %d", query, offset + limit, offset);
        }

        @Override
        public String getLimitOffsetPlaceholder() {
            return "";
        }

        @Override
        public void appendBindLocation(StringBuilder builder, Location location, List<Object> parameters) {
            builder.append("SDO_GEOMETRY(2001, 8307, SDO_POINT_TYPE(?, ?, NULL), NULL, NULL)");
            if (location != null && parameters != null) {
                parameters.add(location.getX());
                parameters.add(location.getY());
            }
        }

        @Override
        public Set<String> getTables(Connection connection) throws SQLException {
            Set<String> tableNames = new HashSet<String>();
            Statement statement = connection.createStatement();

            try {
                ResultSet result = statement.executeQuery("SELECT TABLE_NAME FROM USER_TABLES");

                try {
                    while (result.next()) {
                        String name = result.getString("TABLE_NAME");

                        if (name != null) {
                            tableNames.add(name);
                        }
                    }

                } finally {
                    result.close();
                }

            } finally {
                statement.close();
            }

            return tableNames;
        }

        @Override
        public boolean hasInRowIndex(Connection connection, String recordTable) throws SQLException {
            PreparedStatement statement = connection.prepareStatement(
                    "SELECT 1 FROM USER_TAB_COLS WHERE TABLE_NAME = ? AND COLUMN_NAME = ?");

            try {
                statement.setString(1, recordTable);
                statement.setString(2, SqlDatabase.IN_ROW_INDEX_COLUMN);

                ResultSet result = statement.executeQuery();

                try {
                    while (result.next()) {
                        return true;
                    }

                } finally {
                    result.close();
                }

            } finally {
                statement.close();
            }

            return false;
        }

        @Override
        public boolean supportsDistinctBlob() {
            return false;
        }

        // This is untested.
        // @Override
        // public String getSelectTimestampMillisSql() {
        //     return "SELECT ROUND((SYSTIMESTAMP - TO_DATE('01-01-1970 00:00:00', 'DD-MM-YYYY HH24:MI:SS')) * 24 * 60 * 60 * 1000) FROM DUAL";
        // }

    }
}
