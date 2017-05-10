package com.psddev.dari.db;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.psddev.dari.util.AsyncConsumer;
import com.psddev.dari.util.AsyncQueue;
import com.psddev.dari.util.CompactMap;
import com.psddev.dari.util.Task;
import com.psddev.dari.util.UuidUtils;

class MetricAccess {

    protected static final Logger LOGGER = LoggerFactory.getLogger(MetricAccess.class);

    public static final String METRIC_TABLE = "Metric";
    public static final String METRIC_ID_FIELD = "id";
    public static final String METRIC_TYPE_FIELD = "typeId";
    public static final String METRIC_SYMBOL_FIELD = "symbolId";
    public static final String METRIC_DIMENSION_FIELD = "dimensionId";
    public static final String METRIC_DIMENSION_TABLE = "MetricDimension";
    public static final String METRIC_DIMENSION_VALUE_FIELD = "value";
    public static final String METRIC_DATA_FIELD = "data";
    public static final String METRIC_CACHE_EXTRA_PREFIX = "dari.metric.cache.";

    public static final int AMOUNT_DECIMAL_PLACES = 6;
    public static final long AMOUNT_DECIMAL_SHIFT = (long) Math.pow(10, AMOUNT_DECIMAL_PLACES);
    public static final long DATE_DECIMAL_SHIFT = 60000L;
    public static final int CUMULATIVEAMOUNT_POSITION = 1;
    public static final int AMOUNT_POSITION = 2;
    public static final int DATE_BYTE_SIZE = 4;
    public static final int AMOUNT_BYTE_SIZE = 8;

    private static final int QUERY_TIMEOUT = 3;
    private static final int DIMENSION_CACHE_SIZE = 1000;

    private static final String CACHE_MIN = "min";
    private static final String CACHE_MAX = "max";

    private static final transient Cache<String, UUID> DIMENSION_CACHE = CacheBuilder.newBuilder().maximumSize(DIMENSION_CACHE_SIZE).build();

    private static final ConcurrentMap<String, MetricAccess> METRIC_ACCESSES = new ConcurrentHashMap<String, MetricAccess>();

    private final String symbol;
    private final String fieldName;
    private final MetricAccessDatabase db;
    private final UUID typeId;

    private MetricInterval eventDateProcessor;

    protected MetricAccess(MetricAccessDatabase database, UUID typeId, ObjectField field, MetricInterval interval) {
        this.db = database;
        this.typeId = typeId;
        this.symbol = field.getUniqueName();
        this.fieldName = field.getInternalName();
        this.eventDateProcessor = interval;
    }

    public UUID getTypeId() {
        return typeId;
    }

    public MetricInterval getEventDateProcessor() {
        if (eventDateProcessor == null) {
            eventDateProcessor = new MetricInterval.Hourly();
        }
        return eventDateProcessor;
    }

    public MetricAccessDatabase getDatabase() {
        return db;
    }

    public int getSymbolId() {
        return db.getSymbolId(symbol);
    }

    // This method should strip the minutes and seconds off of a timestamp, or otherwise process it
    public long getEventDate(DateTime time) {
        long eventDate;
        if (time == null) {
            time = new DateTime(db.now());
        }
        if (time.getMillis() > db.now()) {
            throw new RuntimeException("Metric.eventDate may not be a date in the future.");
        }
        eventDate = getEventDateProcessor().process(time);
        return eventDate;
    }

    public DateTime getLastUpdate(UUID id, String dimensionValue) throws SQLException {
        byte[] data = getMaxData(id, getDimensionId(dimensionValue), null);
        return data != null ? new DateTime(Static.timestampFromBytes(data)) : null;
    }

    public DateTime getFirstUpdate(UUID id, String dimensionValue) throws SQLException {
        List<byte[]> data = getMaxMinData(id, getDimensionId(dimensionValue), null, null);
        return data != null && data.size() == 2 ? new DateTime(Static.timestampFromBytes(data.get(1))) : null;
    }

    public Double getMetric(UUID id, String dimensionValue, Long startTimestamp, Long endTimestamp) throws SQLException {
        if (startTimestamp == null) {
            byte[] data = getMaxData(id, getDimensionId(dimensionValue), endTimestamp);
            if (data == null) {
                return null;
            }
            return Static.amountFromBytes(data, CUMULATIVEAMOUNT_POSITION);
        } else {
            List<byte[]> datas = getMaxMinData(id, getDimensionId(dimensionValue), startTimestamp, endTimestamp);
            if (datas.get(0) == null) {
                return null;
            }
            double maxCumulativeAmount = Static.amountFromBytes(datas.get(0), CUMULATIVEAMOUNT_POSITION);
            double minCumulativeAmount = Static.amountFromBytes(datas.get(1), CUMULATIVEAMOUNT_POSITION);
            double minAmount = Static.amountFromBytes(datas.get(1), AMOUNT_POSITION);
            return maxCumulativeAmount - (minCumulativeAmount - minAmount);
        }
    }

    /**
     * Cached in CachingDatabase (if available) on id, dimensionId, and endTimestamp
     * @param id Can't be {@code null}.
     * @param dimensionId Can't be {@code null}.
     */
    private byte[] getMaxData(UUID id, UUID dimensionId, Long endTimestamp) throws SQLException {
        CachingDatabase cachingDb = Static.getCachingDatabase();
        byte[] data;
        if (hasCachedData(cachingDb, id, dimensionId, endTimestamp, CACHE_MAX)) {
            data = getCachedData(cachingDb, id, dimensionId, endTimestamp, CACHE_MAX);
        } else {
            data = Static.getDataByIdAndDimension(getDatabase(), id, getTypeId(), getSymbolId(), dimensionId, null, endTimestamp, false);
            putCachedData(cachingDb, id, dimensionId, endTimestamp, data, CACHE_MAX);
        }
        return data;
    }

    /**
     * Cached in CachingDatabase (if available) on id, dimensionId, startTimestamp, and endTimestamp
     * @param dimensionId Can't be {@code null}.
     * @return List of two elements, maxData and minData
     */
    private List<byte[]> getMaxMinData(UUID id, UUID dimensionId, Long startTimestamp, Long endTimestamp) throws SQLException {
        CachingDatabase cachingDb = Static.getCachingDatabase();
        List<byte[]> result = null;
        if (hasCachedData(cachingDb, id, dimensionId, startTimestamp, CACHE_MIN) && hasCachedData(cachingDb, id, dimensionId, endTimestamp, CACHE_MAX)) {
            result = new ArrayList<byte[]>();
            result.add(getCachedData(cachingDb, id, dimensionId, endTimestamp, CACHE_MAX));
            result.add(getCachedData(cachingDb, id, dimensionId, startTimestamp, CACHE_MIN));
            if ((result.get(0) == null) != (result.get(1) == null)) {
                result = null;
            }
        }
        if (result == null) {
            result = Static.getMaxMinDataByIdAndDimension(getDatabase(), id, getTypeId(), getSymbolId(), dimensionId, startTimestamp, endTimestamp, false);
            if (result.isEmpty()) {
                result.add(null);
                result.add(null);
            }
            putCachedData(cachingDb, id, dimensionId, endTimestamp, result.get(0), CACHE_MAX);
            putCachedData(cachingDb, id, dimensionId, startTimestamp, result.get(1), CACHE_MIN);
        }
        return result;
    }

    private boolean hasCachedData(CachingDatabase cachingDb, UUID id, UUID dimensionId, Long timestamp, String position) {
        Map<String, Object> extras = getCachedStateExtras(cachingDb, id);
        if (extras == null) {
            return false;
        }
        synchronized (extras) {
            return (extras.containsKey(METRIC_CACHE_EXTRA_PREFIX + getSymbolId() + '.' + dimensionId + '.' + timestamp + '.' + position));
        }
    }

    private byte[] getCachedData(CachingDatabase cachingDb, UUID id, UUID dimensionId, Long timestamp, String position) {
        Map<String, Object> extras = getCachedStateExtras(cachingDb, id);
        if (extras != null) {
            synchronized (extras) {
                return (byte[]) extras.get(METRIC_CACHE_EXTRA_PREFIX + getSymbolId() + '.' + dimensionId + '.' + timestamp + '.' + position);
            }
        }
        return null;
    }

    private void putCachedData(CachingDatabase cachingDb, UUID id, UUID dimensionId, Long timestamp, byte[] data, String position) {
        Map<String, Object> extras = getCachedStateExtras(cachingDb, id);
        if (extras != null) {
            synchronized (extras) {
                extras.put(METRIC_CACHE_EXTRA_PREFIX + getSymbolId() + '.' + dimensionId + '.' + timestamp + '.' + position, data);
            }
        }
    }

    private void clearCachedData(CachingDatabase cachingDb, UUID id) {
        Map<String, Object> extras = getCachedStateExtras(cachingDb, id);
        if (extras != null) {
            synchronized (extras) {
                Set<String> keys = new HashSet<String>(extras.keySet());
                for (String key : keys) {
                    if (key.startsWith(METRIC_CACHE_EXTRA_PREFIX)) {
                        extras.remove(key);
                    }
                }
            }
        }
    }

    private Map<String, Object> getCachedStateExtras(CachingDatabase cachingDb, UUID id) {
        if (cachingDb != null && cachingDb.getObjectCache().containsKey(id)) {
            Object obj = cachingDb.getObjectCache().get(id);
            if (obj != null && obj instanceof Recordable) {
                return ((Recordable) obj).getState().getExtras();
            }
        }
        return null;
    }

    public Double getMetricSum(UUID id, Long startTimestamp, Long endTimestamp) throws SQLException {
        return getMetric(id, null, null, null);
    }

    public Map<String, Double> getMetricValues(UUID id, Long startTimestamp, Long endTimestamp) throws SQLException {
        return Static.getMetricDimensionsById(getDatabase(), id, getTypeId(), getSymbolId(), startTimestamp, endTimestamp, false);
    }

    public Map<DateTime, Double> getMetricTimeline(UUID id, String dimensionValue, Long startTimestamp, Long endTimestamp, MetricInterval metricInterval) throws SQLException {
        if (metricInterval == null) {
            metricInterval = getEventDateProcessor();
        }
        return Static.getMetricTimelineByIdAndDimension(getDatabase(), id, getTypeId(), getSymbolId(), getDimensionId(dimensionValue), startTimestamp, endTimestamp, metricInterval, false);
    }

    public void incrementMetric(UUID id, DateTime time, String dimensionValue, Double amount) throws SQLException {
        incrementMetricByDimensionId(id, time, getDimensionId(dimensionValue), amount);
    }

    public void incrementMetricByDimensionId(UUID id, DateTime time, UUID dimensionId, Double amount) throws SQLException {
        // This actually causes some problems if it's not here
        if (amount == 0) {
            return;
        }
        boolean isImplicitEventDate = (time == null);
        long eventDate = getEventDate(time);
        Static.doIncrementUpdateOrInsert(getDatabase(), id, getTypeId(), getSymbolId(), dimensionId, amount, eventDate, isImplicitEventDate);
        if (!dimensionId.equals(UuidUtils.ZERO_UUID)) {
            // Do an additional increment for the null dimension to maintain the sum
            Static.doIncrementUpdateOrInsert(getDatabase(), id, getTypeId(), getSymbolId(), UuidUtils.ZERO_UUID, amount, eventDate, isImplicitEventDate);
        }
        clearCachedData(Static.getCachingDatabase(), id);
        recalculateImmediateIndexedMethods(id);
    }

    public void setMetric(UUID id, DateTime time, String dimensionValue, Double amount) throws SQLException {
        setMetricByDimensionId(id, time, getDimensionId(dimensionValue), amount);
        clearCachedData(Static.getCachingDatabase(), id);
    }

    public void setMetricByDimensionId(UUID id, DateTime time, UUID dimensionId, Double amount) throws SQLException {
        // This only works if we're not tracking eventDate
        if (getEventDate(time) != 0L) {
            throw new RuntimeException("MetricAccess.setMetric() can only be used if EventDateProcessor is None");
        }
        Static.doSetUpdateOrInsert(getDatabase(), id, getTypeId(), getSymbolId(), dimensionId, amount, 0L);
        if (!dimensionId.equals(UuidUtils.ZERO_UUID)) {
            // Do an additional increment for the null dimension to maintain the sum
            Double allDimensionsAmount = Static.calculateMetricSumById(getDatabase(), id, getTypeId(), getSymbolId(), null, null, true);
            Static.doSetUpdateOrInsert(getDatabase(), id, getTypeId(), getSymbolId(), UuidUtils.ZERO_UUID, allDimensionsAmount, 0L);
        }
        clearCachedData(Static.getCachingDatabase(), id);
        recalculateImmediateIndexedMethods(id);
    }

    public void deleteMetric(UUID id) throws SQLException {
        Static.doMetricDelete(getDatabase(), id, getTypeId(), getSymbolId());
        clearCachedData(Static.getCachingDatabase(), id);
        recalculateImmediateIndexedMethods(id);
    }

    public void reconstructCumulativeAmounts(UUID id) throws SQLException {
        Static.doReconstructCumulativeAmounts(getDatabase(), id, getTypeId(), getSymbolId(), null);
        clearCachedData(Static.getCachingDatabase(), id);
        recalculateImmediateIndexedMethods(id);
    }

    public void resummarize(UUID id, UUID dimensionId, MetricInterval interval, Long startTimestamp, Long endTimestamp) throws SQLException {
        Static.doResummarize(getDatabase(), id, getTypeId(), getSymbolId(), dimensionId, interval, startTimestamp, endTimestamp);
        clearCachedData(Static.getCachingDatabase(), id);
    }

    public Task submitResummarizeAllTask(MetricInterval interval, Long startTimestamp, Long endTimestamp, int numParallel, String executor, String name) {
        ResummarizeTask task = new ResummarizeTask(getDatabase(), getSymbolId(), interval, startTimestamp, endTimestamp, numParallel, executor, name);
        task.submit();
        return task;
    }

    private void recalculateImmediateIndexedMethods(UUID id) {
        Set<ObjectMethod> immediateMethods = new HashSet<ObjectMethod>();
        for (ObjectMethod method : getRecalculableObjectMethods(db, typeId, fieldName)) {
            RecalculationFieldData methodData = method.as(RecalculationFieldData.class);
            if (methodData.isImmediate()) {
                immediateMethods.add(method);
            }
        }
        if (!immediateMethods.isEmpty()) {
            Object obj = Query.fromAll().master().noCache().where("_id = ?", id).first();
            State state = State.getInstance(obj);
            if (state != null) {
                Database stateDb = state.getDatabase();
                stateDb.beginIsolatedWrites();
                try {
                    for (ObjectMethod method : immediateMethods) {
                        method.recalculate(state);
                    }
                    stateDb.commitWrites();
                } finally {
                    stateDb.endWrites();
                }
            }
        }
    }

    private static Set<ObjectMethod> getRecalculableObjectMethods(Database db, UUID typeId, String fieldName) {
        Set<ObjectMethod> methods = new HashSet<ObjectMethod>();
        ObjectField field = db.getEnvironment().getField(fieldName);
        ObjectType type = ObjectType.getInstance(typeId);
        if (field == null) {
            field = type.getField(fieldName);
        }
        if (field != null) {
            FieldData fieldData = field.as(FieldData.class);
            if (fieldData != null) {
                methods.addAll(fieldData.getRecalculableObjectMethods(type));
            }
        }
        return methods;
    }

    public static UUID getDimensionIdByValue(MetricAccessDatabase db, String dimensionValue) {
        if (dimensionValue == null || "".equals(dimensionValue)) {
            return UuidUtils.ZERO_UUID;
        }
        UUID dimensionId = DIMENSION_CACHE.getIfPresent(dimensionValue);
        if (dimensionId == null) {
            try {
                dimensionId = Static.getDimensionIdByValue(db, dimensionValue, false);
                if (dimensionId == null) {
                    dimensionId = UuidUtils.createSequentialUuid();
                    Static.doInsertDimensionValue(db, dimensionId, dimensionValue);
                }
                DIMENSION_CACHE.put(dimensionValue, dimensionId);
            } catch (SQLException e) {
                throw new DatabaseException(db, "Error in MetricAccess.getDimensionIdByValue() : " + e.getLocalizedMessage());
            }
        }
        return dimensionId;
    }

    public UUID getDimensionId(String dimensionValue) throws SQLException {
        if (dimensionValue == null || "".equals(dimensionValue)) {
            return UuidUtils.ZERO_UUID;
        }
        UUID dimensionId = DIMENSION_CACHE.getIfPresent(dimensionValue);
        if (dimensionId == null) {
            dimensionId = Static.getDimensionIdByValue(db, dimensionValue, false);
            if (dimensionId == null) {
                dimensionId = UuidUtils.createSequentialUuid();
                Static.doInsertDimensionValue(db, dimensionId, dimensionValue);
            }
            DIMENSION_CACHE.put(dimensionValue, dimensionId);
        }
        return dimensionId;
    }

    static class UpdateFailedException extends Exception {
        private static final long serialVersionUID = 1L;
    }

    /** {@link MetricAccess} utility methods. */
    static final class Static {

        // Methods that generate SQL statements

        private static String getDataSql(MetricAccessDatabase db, UUID id, UUID typeId, Integer symbolId, UUID dimensionId, Long minEventDate, Long maxEventDate, boolean selectMinData, boolean doDecodeToBytes, String extraSelectSql, String extraGroupBySql, String extraWhereSql) {
            StringBuilder sqlBuilder = new StringBuilder();
            SqlVendor vendor = db.getMetricVendor();

            sqlBuilder.append("SELECT ");

            if (dimensionId == null) {
                vendor.appendIdentifier(sqlBuilder, METRIC_DIMENSION_FIELD);
                sqlBuilder.append(", ");
            }

            StringBuilder maxDataBuilder = new StringBuilder("MAX(");
            vendor.appendIdentifier(maxDataBuilder, METRIC_DATA_FIELD);
            maxDataBuilder.append(')');
            if (doDecodeToBytes) {
                vendor.appendMetricDataBytes(sqlBuilder, maxDataBuilder.toString());
            } else {
                sqlBuilder.append(maxDataBuilder);
            }
            sqlBuilder.append(' ');
            vendor.appendIdentifier(sqlBuilder, "maxData");

            if (selectMinData) {
                sqlBuilder.append(", ");
                StringBuilder minDataBuilder = new StringBuilder("MIN(");
                vendor.appendIdentifier(minDataBuilder, METRIC_DATA_FIELD);
                minDataBuilder.append(')');
                if (doDecodeToBytes) {
                    vendor.appendMetricDataBytes(sqlBuilder, minDataBuilder.toString());
                } else {
                sqlBuilder.append(minDataBuilder);
                }
                sqlBuilder.append(' ');
                vendor.appendIdentifier(sqlBuilder, "minData");
            }

            if (extraSelectSql != null && !"".equals(extraSelectSql)) {
                sqlBuilder.append(", ");
                sqlBuilder.append(extraSelectSql);
            }

            sqlBuilder.append(" FROM ");
            sqlBuilder.append(Static.getMetricTableIdentifier(db));
            sqlBuilder.append(" WHERE ");
            vendor.appendIdentifier(sqlBuilder, METRIC_ID_FIELD);
            sqlBuilder.append(" = ");
            vendor.appendValue(sqlBuilder, id);

            if (symbolId != null) {
                sqlBuilder.append(" AND ");
                vendor.appendIdentifier(sqlBuilder, METRIC_SYMBOL_FIELD);
                sqlBuilder.append(" = ");
                vendor.appendValue(sqlBuilder, symbolId);
            }

            sqlBuilder.append(" AND ");
            vendor.appendIdentifier(sqlBuilder, METRIC_TYPE_FIELD);
            sqlBuilder.append(" = ");
            vendor.appendValue(sqlBuilder, typeId);

            if (dimensionId != null) {
                sqlBuilder.append(" AND ");
                vendor.appendIdentifier(sqlBuilder, METRIC_DIMENSION_FIELD);
                sqlBuilder.append(" = ");
                vendor.appendValue(sqlBuilder, dimensionId);
            } else {
                sqlBuilder.append(" AND ");
                vendor.appendIdentifier(sqlBuilder, METRIC_DIMENSION_FIELD);
                sqlBuilder.append(" != ");
                vendor.appendValue(sqlBuilder, UuidUtils.ZERO_UUID);
            }

            if (maxEventDate != null) {
                sqlBuilder.append(" AND ");
                vendor.appendIdentifier(sqlBuilder, METRIC_DATA_FIELD);
                sqlBuilder.append(" < ");
                vendor.appendMetricEncodeTimestampSql(sqlBuilder, null, maxEventDate, '0');
            }

            if (minEventDate != null) {
                sqlBuilder.append(" AND ");
                vendor.appendIdentifier(sqlBuilder, METRIC_DATA_FIELD);
                sqlBuilder.append(" >= ");
                vendor.appendMetricEncodeTimestampSql(sqlBuilder, null, minEventDate, '0');
            }

            if (extraWhereSql != null) {
                sqlBuilder.append(" AND ");
                sqlBuilder.append(extraWhereSql);
            }

            if (dimensionId == null) {
                sqlBuilder.append(" GROUP BY ");
                vendor.appendIdentifier(sqlBuilder, METRIC_DIMENSION_FIELD);
                if (extraGroupBySql != null && !"".equals(extraGroupBySql)) {
                    sqlBuilder.append(", ");
                    sqlBuilder.append(extraGroupBySql);
                }
            } else if (extraGroupBySql != null && !"".equals(extraGroupBySql)) {
                sqlBuilder.append(" GROUP BY ");
                sqlBuilder.append(extraGroupBySql);
            }

            return sqlBuilder.toString();
        }

        private static String getAllDataSql(MetricAccessDatabase db, UUID id, UUID typeId, int symbolId, UUID dimensionId, Long minEventDate, Long maxEventDate, boolean doDecodeToBytes) {
            StringBuilder sqlBuilder = new StringBuilder();
            SqlVendor vendor = db.getMetricVendor();

            sqlBuilder.append("SELECT ");

            if (dimensionId == null) {
                vendor.appendIdentifier(sqlBuilder, METRIC_DIMENSION_FIELD);
                sqlBuilder.append(", ");
            }

            if (doDecodeToBytes) {
                vendor.appendMetricDataBytes(sqlBuilder, METRIC_DATA_FIELD);
            } else {
                sqlBuilder.append(METRIC_DATA_FIELD);
            }

            sqlBuilder.append(" FROM ");
            sqlBuilder.append(Static.getMetricTableIdentifier(db));
            sqlBuilder.append(" WHERE ");
            vendor.appendIdentifier(sqlBuilder, METRIC_ID_FIELD);
            sqlBuilder.append(" = ");
            vendor.appendValue(sqlBuilder, id);

            sqlBuilder.append(" AND ");
            vendor.appendIdentifier(sqlBuilder, METRIC_SYMBOL_FIELD);
            sqlBuilder.append(" = ");
            vendor.appendValue(sqlBuilder, symbolId);

            if (typeId != null) {
                sqlBuilder.append(" AND ");
                vendor.appendIdentifier(sqlBuilder, METRIC_TYPE_FIELD);
                sqlBuilder.append(" = ");
                vendor.appendValue(sqlBuilder, typeId);
            }

            if (dimensionId != null) {
                sqlBuilder.append(" AND ");
                vendor.appendIdentifier(sqlBuilder, METRIC_DIMENSION_FIELD);
                sqlBuilder.append(" = ");
                vendor.appendValue(sqlBuilder, dimensionId);
            }

            if (maxEventDate != null) {
                sqlBuilder.append(" AND ");
                vendor.appendIdentifier(sqlBuilder, METRIC_DATA_FIELD);
                sqlBuilder.append(" < ");
                vendor.appendMetricEncodeTimestampSql(sqlBuilder, null, maxEventDate, '0');
            }

            if (minEventDate != null) {
                sqlBuilder.append(" AND ");
                vendor.appendIdentifier(sqlBuilder, METRIC_DATA_FIELD);
                sqlBuilder.append(" >= ");
                vendor.appendMetricEncodeTimestampSql(sqlBuilder, null, minEventDate, '0');
            }

            sqlBuilder.append(" ORDER BY ");
            if (dimensionId == null) {
                vendor.appendIdentifier(sqlBuilder, METRIC_DIMENSION_FIELD);
                sqlBuilder.append(", ");
            }
            vendor.appendIdentifier(sqlBuilder, METRIC_DATA_FIELD);

            return sqlBuilder.toString();
        }

        private static String getSumSql(MetricAccessDatabase db, UUID id, UUID typeId, int symbolId, Long minEventDate, Long maxEventDate) {
            StringBuilder sqlBuilder = new StringBuilder();
            SqlVendor vendor = db.getMetricVendor();
            String innerSql = getDataSql(db, id, typeId, symbolId, null, minEventDate, maxEventDate, true, false, null, null, null);

            sqlBuilder.append("SELECT ");
            appendSelectCalculatedAmountSql(sqlBuilder, vendor, "minData", "maxData", true);
            sqlBuilder.append(" FROM (");
            sqlBuilder.append(innerSql);
            sqlBuilder.append(") x");

            return sqlBuilder.toString();
        }

        private static String getDimensionsSql(MetricAccessDatabase db, UUID id, UUID typeId, int symbolId, Long minEventDate, Long maxEventDate) {
            StringBuilder sqlBuilder = new StringBuilder();
            SqlVendor vendor = db.getMetricVendor();
            String innerSql = getDataSql(db, id, typeId, symbolId, null, minEventDate, maxEventDate, true, false, null, null, null);

            sqlBuilder.append("SELECT ");
            StringBuilder dimValField = new StringBuilder();
            vendor.appendIdentifier(dimValField, "d");
            dimValField.append('.');
            vendor.appendIdentifier(dimValField, METRIC_DIMENSION_VALUE_FIELD);
            sqlBuilder.append(vendor.convertRawToStringSql(METRIC_DIMENSION_VALUE_FIELD));
            sqlBuilder.append(", ");
            appendSelectCalculatedAmountSql(sqlBuilder, vendor, "minData", "maxData", true);
            sqlBuilder.append(" FROM (");
            sqlBuilder.append(innerSql);
            sqlBuilder.append(") x ");
            sqlBuilder.append(" JOIN "); // This could be a left join if we want to include NULL dimension values in this query.
            vendor.appendIdentifier(sqlBuilder, METRIC_DIMENSION_TABLE);
            sqlBuilder.append(' ');
            vendor.appendIdentifier(sqlBuilder, "d");
            sqlBuilder.append(" ON (");
            vendor.appendIdentifier(sqlBuilder, "x");
            sqlBuilder.append('.');
            vendor.appendIdentifier(sqlBuilder, METRIC_DIMENSION_FIELD);
            sqlBuilder.append(" = ");
            vendor.appendIdentifier(sqlBuilder, "d");
            sqlBuilder.append('.');
            vendor.appendIdentifier(sqlBuilder, METRIC_DIMENSION_FIELD);
            sqlBuilder.append(')');
            sqlBuilder.append(" GROUP BY ");
            vendor.appendIdentifier(sqlBuilder, "d");
            sqlBuilder.append('.');
            vendor.appendIdentifier(sqlBuilder, METRIC_DIMENSION_VALUE_FIELD);

            return sqlBuilder.toString();
        }

        private static String getTimelineSql(MetricAccessDatabase db, UUID id, UUID typeId, int symbolId, UUID dimensionId, Long minEventDate, Long maxEventDate, MetricInterval metricInterval, boolean doDecodeToBytes) {

            SqlVendor vendor = db.getMetricVendor();

            StringBuilder extraSelectSqlBuilder = new StringBuilder("MIN(");
            vendor.appendMetricSelectTimestampSql(extraSelectSqlBuilder, METRIC_DATA_FIELD);
            extraSelectSqlBuilder.append(") * ");
            vendor.appendValue(extraSelectSqlBuilder, DATE_DECIMAL_SHIFT);
            extraSelectSqlBuilder.append(' ');
            vendor.appendIdentifier(extraSelectSqlBuilder, "eventDate");

            StringBuilder extraGroupBySqlBuilder = new StringBuilder();
            vendor.appendMetricDateFormatTimestampSql(extraGroupBySqlBuilder, METRIC_DATA_FIELD, metricInterval);

            StringBuilder sqlBuilder = new StringBuilder();
            String innerSql = getDataSql(db, id, typeId, symbolId, dimensionId, minEventDate, maxEventDate, true, doDecodeToBytes, extraSelectSqlBuilder.toString(), extraGroupBySqlBuilder.toString(), null);
            sqlBuilder.append(innerSql);
            sqlBuilder.append(" ORDER BY ");
            if (dimensionId == null) {
                vendor.appendIdentifier(sqlBuilder, "dimensionId");
                sqlBuilder.append(", ");
            }
            vendor.appendIdentifier(sqlBuilder, "eventDate");

            return sqlBuilder.toString();
        }

        private static String getUpdateSql(MetricAccessDatabase db, List<Object> parameters, UUID id, UUID typeId, int symbolId, UUID dimensionId, double amount, long eventDate, boolean increment, boolean updateFuture) {
            StringBuilder updateBuilder = new StringBuilder("UPDATE ");
            SqlVendor vendor = db.getMetricVendor();
            updateBuilder.append(Static.getMetricTableIdentifier(db));
            updateBuilder.append(" SET ");

            vendor.appendIdentifier(updateBuilder, METRIC_DATA_FIELD);
            updateBuilder.append(" = ");

            vendor.appendMetricUpdateDataSql(updateBuilder, METRIC_DATA_FIELD, parameters, amount, eventDate, increment, updateFuture);

            updateBuilder.append(" WHERE ");
            vendor.appendIdentifier(updateBuilder, METRIC_ID_FIELD);
            updateBuilder.append(" = ");
            vendor.appendBindValue(updateBuilder, id, parameters);

            updateBuilder.append(" AND ");
            vendor.appendIdentifier(updateBuilder, METRIC_TYPE_FIELD);
            updateBuilder.append(" = ");
            vendor.appendBindValue(updateBuilder, typeId, parameters);

            updateBuilder.append(" AND ");
            vendor.appendIdentifier(updateBuilder, METRIC_SYMBOL_FIELD);
            updateBuilder.append(" = ");
            vendor.appendBindValue(updateBuilder, symbolId, parameters);

            updateBuilder.append(" AND ");
            vendor.appendIdentifier(updateBuilder, METRIC_DIMENSION_FIELD);
            updateBuilder.append(" = ");
            vendor.appendBindValue(updateBuilder, dimensionId, parameters);

            updateBuilder.append(" AND ");
            vendor.appendIdentifier(updateBuilder, METRIC_DATA_FIELD);

            updateBuilder.append(" >= ");
            vendor.appendMetricEncodeTimestampSql(updateBuilder, parameters, eventDate, '0');

            if (!updateFuture) {
                updateBuilder.append(" AND ");
                vendor.appendIdentifier(updateBuilder, METRIC_DATA_FIELD);
                updateBuilder.append(" <= ");
                vendor.appendMetricEncodeTimestampSql(updateBuilder, parameters, eventDate, 'F');
            }

            return updateBuilder.toString();
        }

        private static String getRepairTypeIdSql(MetricAccessDatabase db, List<Object> parameters, UUID id, UUID typeId, UUID dimensionId, int symbolId, long eventDate) {
                // String repairSql = getRepairTypeIdSql(db, repairParameters, id, typeId, symbolId, eventDate);
            StringBuilder updateBuilder = new StringBuilder("UPDATE ");
            SqlVendor vendor = db.getMetricVendor();
            updateBuilder.append(Static.getMetricTableIdentifier(db));
            updateBuilder.append(" SET ");

            vendor.appendIdentifier(updateBuilder, METRIC_TYPE_FIELD);
            updateBuilder.append(" = ");
            vendor.appendBindValue(updateBuilder, typeId, parameters);

            updateBuilder.append(" WHERE ");
            vendor.appendIdentifier(updateBuilder, METRIC_ID_FIELD);
            updateBuilder.append(" = ");
            vendor.appendBindValue(updateBuilder, id, parameters);

            updateBuilder.append(" AND ");
            vendor.appendIdentifier(updateBuilder, METRIC_SYMBOL_FIELD);
            updateBuilder.append(" = ");
            vendor.appendBindValue(updateBuilder, symbolId, parameters);

            updateBuilder.append(" AND ");
            vendor.appendIdentifier(updateBuilder, METRIC_DIMENSION_FIELD);
            updateBuilder.append(" = ");
            vendor.appendBindValue(updateBuilder, dimensionId, parameters);

            updateBuilder.append(" AND ");
            vendor.appendIdentifier(updateBuilder, METRIC_DATA_FIELD);
            updateBuilder.append(" >= ");
            vendor.appendMetricEncodeTimestampSql(updateBuilder, null, eventDate, '0');

            updateBuilder.append(" AND ");
            vendor.appendIdentifier(updateBuilder, METRIC_DATA_FIELD);
            updateBuilder.append(" <= ");
            vendor.appendMetricEncodeTimestampSql(updateBuilder, null, eventDate, 'F');

            return updateBuilder.toString();
        }

        private static String getFixDataRowSql(MetricAccessDatabase db, List<Object> parameters, UUID id, UUID typeId, int symbolId, UUID dimensionId, long eventDate, double cumulativeAmount, double amount) {
            StringBuilder updateBuilder = new StringBuilder("UPDATE ");
            SqlVendor vendor = db.getMetricVendor();
            updateBuilder.append(Static.getMetricTableIdentifier(db));
            updateBuilder.append(" SET ");

            vendor.appendIdentifier(updateBuilder, METRIC_DATA_FIELD);
            updateBuilder.append(" = ");

            vendor.appendMetricFixDataSql(updateBuilder, METRIC_DATA_FIELD, parameters, eventDate, cumulativeAmount, amount);

            updateBuilder.append(" WHERE ");
            vendor.appendIdentifier(updateBuilder, METRIC_ID_FIELD);
            updateBuilder.append(" = ");
            vendor.appendBindValue(updateBuilder, id, parameters);

            updateBuilder.append(" AND ");
            vendor.appendIdentifier(updateBuilder, METRIC_TYPE_FIELD);
            updateBuilder.append(" = ");
            vendor.appendBindValue(updateBuilder, typeId, parameters);

            updateBuilder.append(" AND ");
            vendor.appendIdentifier(updateBuilder, METRIC_SYMBOL_FIELD);
            updateBuilder.append(" = ");
            vendor.appendBindValue(updateBuilder, symbolId, parameters);

            updateBuilder.append(" AND ");
            vendor.appendIdentifier(updateBuilder, METRIC_DIMENSION_FIELD);
            updateBuilder.append(" = ");
            vendor.appendBindValue(updateBuilder, dimensionId, parameters);

            updateBuilder.append(" AND ");
            vendor.appendIdentifier(updateBuilder, METRIC_DATA_FIELD);
            updateBuilder.append(" >= ");
            vendor.appendMetricEncodeTimestampSql(updateBuilder, null, eventDate, '0');

            updateBuilder.append(" AND ");
            vendor.appendIdentifier(updateBuilder, METRIC_DATA_FIELD);
            updateBuilder.append(" <= ");
            vendor.appendMetricEncodeTimestampSql(updateBuilder, null, eventDate, 'F');

            return updateBuilder.toString();
        }

        private static String getMetricInsertSql(MetricAccessDatabase db, List<Object> parameters, UUID id, UUID typeId, int symbolId, UUID dimensionId, double amount, double cumulativeAmount, long eventDate) {
            SqlVendor vendor = db.getMetricVendor();
            StringBuilder insertBuilder = new StringBuilder("INSERT INTO ");
            insertBuilder.append(Static.getMetricTableIdentifier(db));
            insertBuilder.append(" (");
            Map<String, Object> cols = new CompactMap<String, Object>();
            cols.put(METRIC_ID_FIELD, id);
            cols.put(METRIC_TYPE_FIELD, typeId);
            cols.put(METRIC_SYMBOL_FIELD, symbolId);
            cols.put(METRIC_DIMENSION_FIELD, dimensionId);
            for (Map.Entry<String, Object> entry : cols.entrySet()) {
                vendor.appendIdentifier(insertBuilder, entry.getKey());
                insertBuilder.append(", ");
            }
            vendor.appendIdentifier(insertBuilder, METRIC_DATA_FIELD);
            insertBuilder.append(") VALUES (");
            for (Map.Entry<String, Object> entry : cols.entrySet()) {
                vendor.appendBindValue(insertBuilder, entry.getValue(), parameters);
                insertBuilder.append(", ");
            }
            vendor.appendBindMetricBytes(insertBuilder, toBytes(eventDate, cumulativeAmount, amount), parameters);
            insertBuilder.append(')');
            return insertBuilder.toString();
        }

        private static String getDeleteMetricSql(MetricAccessDatabase db, UUID id, UUID typeId, int symbolId) {
            SqlVendor vendor = db.getMetricVendor();
            StringBuilder sqlBuilder = new StringBuilder();
            sqlBuilder.append("DELETE FROM ");
            sqlBuilder.append(Static.getMetricTableIdentifier(db));
            sqlBuilder.append(" WHERE ");
            vendor.appendIdentifier(sqlBuilder, METRIC_SYMBOL_FIELD);
            sqlBuilder.append(" = ");
            vendor.appendValue(sqlBuilder, symbolId);
            sqlBuilder.append(" AND ");
            vendor.appendIdentifier(sqlBuilder, METRIC_ID_FIELD);
            sqlBuilder.append(" = ");
            vendor.appendValue(sqlBuilder, id);
            sqlBuilder.append(" AND ");
            vendor.appendIdentifier(sqlBuilder, METRIC_TYPE_FIELD);
            sqlBuilder.append(" = ");
            vendor.appendValue(sqlBuilder, typeId);
            return sqlBuilder.toString();
        }

        private static String getDimensionIdByValueSql(MetricAccessDatabase db, String dimensionValue) {
            SqlVendor vendor = db.getMetricVendor();
            StringBuilder sqlBuilder = new StringBuilder();
            sqlBuilder.append("SELECT ");
            vendor.appendIdentifier(sqlBuilder, METRIC_DIMENSION_FIELD);
            sqlBuilder.append(" FROM ");
            vendor.appendIdentifier(sqlBuilder, METRIC_DIMENSION_TABLE);
            sqlBuilder.append(" WHERE ");
            vendor.appendIdentifier(sqlBuilder, METRIC_DIMENSION_VALUE_FIELD);
            sqlBuilder.append(" = ");
            vendor.appendValue(sqlBuilder, dimensionValue);
            return sqlBuilder.toString();
        }

        private static String getInsertDimensionValueSql(MetricAccessDatabase db, List<Object> parameters, UUID dimensionId, String dimensionValue) {
            SqlVendor vendor = db.getMetricVendor();
            StringBuilder insertBuilder = new StringBuilder("INSERT INTO ");
            vendor.appendIdentifier(insertBuilder, METRIC_DIMENSION_TABLE);
            insertBuilder.append(" (");
            vendor.appendIdentifier(insertBuilder, METRIC_DIMENSION_FIELD);
            insertBuilder.append(", ");
            vendor.appendIdentifier(insertBuilder, METRIC_DIMENSION_VALUE_FIELD);
            insertBuilder.append(") VALUES (");
            vendor.appendBindValue(insertBuilder, dimensionId, parameters);
            insertBuilder.append(", ");
            vendor.appendBindValue(insertBuilder, dimensionValue, parameters);
            insertBuilder.append(')');
            return insertBuilder.toString();
        }

        // Methods that generate complicated bits of SQL

        public static void appendSelectCalculatedAmountSql(StringBuilder str, SqlVendor vendor, String minDataColumnIdentifier, String maxDataColumnIdentifier, boolean includeSum) {

            str.append("ROUND(");
            if (includeSum) {
                str.append("SUM");
            }
            str.append('(');
            vendor.appendMetricSelectAmountSql(str, maxDataColumnIdentifier, CUMULATIVEAMOUNT_POSITION);
            str.append(" - (");
            vendor.appendMetricSelectAmountSql(str, minDataColumnIdentifier, CUMULATIVEAMOUNT_POSITION);
            str.append(" - ");
            vendor.appendMetricSelectAmountSql(str, minDataColumnIdentifier, AMOUNT_POSITION);
            str.append(") ");

            str.append(')');
            str.append(" / ");
            vendor.appendValue(str, AMOUNT_DECIMAL_SHIFT);
            str.append(',');
            vendor.appendValue(str, AMOUNT_DECIMAL_PLACES);
            str.append(") ");

        }

        // methods that convert bytes into values and back again

        private static byte[] toBytes(long eventDate, double cumulativeAmount, double amount) {

            Long cumulativeAmountLong = (long) (cumulativeAmount * AMOUNT_DECIMAL_SHIFT);
            Long amountLong = (long) (amount * AMOUNT_DECIMAL_SHIFT);
            Integer eventDateInt = (int) (eventDate / DATE_DECIMAL_SHIFT);

            int size, offset = 0;
            byte[] bytes = new byte[DATE_BYTE_SIZE + AMOUNT_BYTE_SIZE + AMOUNT_BYTE_SIZE];

            // first 4 bytes: timestamp
            size = DATE_BYTE_SIZE;
            for (int i = 0; i < size; ++i) {
                bytes[i + offset] = (byte) (eventDateInt >> (size - i - 1 << 3));
            }
            offset += size;

            // second 8 bytes: cumulativeAmount
            size = AMOUNT_BYTE_SIZE;
            for (int i = 0; i < size; ++i) {
                bytes[i + offset] = (byte) (cumulativeAmountLong >> (size - i - 1 << 3));
            }
            offset += size;

            // last 8 bytes: amount
            size = AMOUNT_BYTE_SIZE;
            for (int i = 0; i < 8; ++i) {
                bytes[i + offset] = (byte) (amountLong >> (size - i - 1 << 3));
            }

            return bytes;
        }

        private static double amountFromBytes(byte[] bytes, int position) {
            long amountLong = 0;

            int offset = DATE_BYTE_SIZE + ((position - 1) * AMOUNT_BYTE_SIZE);

            for (int i = 0; i < AMOUNT_BYTE_SIZE; ++i) {
                amountLong = (amountLong << 8) | (bytes[i + offset] & 0xff);
            }

            return (double) amountLong / AMOUNT_DECIMAL_SHIFT;
        }

        private static long timestampFromBytes(byte[] bytes) {
            long timestamp = 0;

            for (int i = 0; i < DATE_BYTE_SIZE; ++i) {
                timestamp = (timestamp << 8) | (bytes[i] & 0xff);
            }

            return timestamp * DATE_DECIMAL_SHIFT;
        }

        // methods that actually touch the database

        private static void doIncrementUpdateOrInsert(MetricAccessDatabase db, UUID id, UUID typeId, int symbolId, UUID dimensionId, double incrementAmount, long eventDate, boolean isImplicitEventDate) throws SQLException {
            SqlVendor vendor = db.getMetricVendor();
            Connection connection = db.openConnection();
            try {

                if (isImplicitEventDate) {
                    // If they have not passed in an eventDate, we can assume a couple of things:
                    // 1) The event date is the CURRENT date
                    // 2) There is NOT any FUTURE data
                    // 3) We CANNOT assume the row exists
                    List<Object> updateParameters = new ArrayList<Object>();

                    // Try to do an update. This is the best case scenario, and does not require any reads.
                    String updateSql = getUpdateSql(db, updateParameters, id, typeId, symbolId, dimensionId, incrementAmount, eventDate, true, false);
                    int rowsAffected = SqlDatabase.Static.executeUpdateWithList(vendor, connection, updateSql, updateParameters);
                    if (0 == rowsAffected) {
                        // There is no data for the current date. Now we have to read
                        // the previous cumulative amount so we can insert a new row.
                        byte[] data = getDataByIdAndDimension(db, id, typeId, symbolId, dimensionId, null, null, true);
                        double previousCumulativeAmount = 0.0d;
                        if (data != null) {
                            previousCumulativeAmount = amountFromBytes(data, CUMULATIVEAMOUNT_POSITION);
                        }
                        // Try to insert, if that fails then try the update again
                        List<Object> insertParameters = new ArrayList<Object>();
                        String insertSql = getMetricInsertSql(db, insertParameters, id, typeId, symbolId, dimensionId, incrementAmount, previousCumulativeAmount + incrementAmount, eventDate);
                        tryInsertThenUpdate(db, connection, insertSql, insertParameters, updateSql, updateParameters);
                    }

                } else {

                    // First, find the max eventDate. Under normal circumstances, this will either be null (INSERT), before our eventDate (INSERT) or equal to our eventDate (UPDATE).
                    byte[] data = getDataByIdAndDimension(db, id, typeId, symbolId, dimensionId, null, null, true);

                    if (data == null || timestampFromBytes(data) < eventDate) {
                        // No data for this eventDate; insert.
                        double previousCumulativeAmount = 0.0d;
                        if (data != null) {
                            previousCumulativeAmount = amountFromBytes(data, CUMULATIVEAMOUNT_POSITION);
                        }

                        List<Object> insertParameters = new ArrayList<Object>();
                        String insertSql = getMetricInsertSql(db, insertParameters, id, typeId, symbolId, dimensionId, incrementAmount, previousCumulativeAmount + incrementAmount, eventDate);

                        List<Object> updateParameters = new ArrayList<Object>();
                        String updateSql = getUpdateSql(db, updateParameters, id, typeId, symbolId, dimensionId, incrementAmount, eventDate, true, false);

                        tryInsertThenUpdate(db, connection, insertSql, insertParameters, updateSql, updateParameters);
                    } else if (timestampFromBytes(data) == eventDate) {
                        // There is data for this eventDate; update it.
                        List<Object> updateParameters = new ArrayList<Object>();
                        String updateSql = getUpdateSql(db, updateParameters, id, typeId, symbolId, dimensionId, incrementAmount, eventDate, true, false);
                        SqlDatabase.Static.executeUpdateWithList(vendor, connection, updateSql, updateParameters);
                    } else { // if (timestampFromBytes(data) > eventDate)
                        // The max(eventDate) in the table is greater than our
                        // event date. If there exists a row in the past, UPDATE it
                        // or if not, INSERT. Either way we will be updating future
                        // data, so just INSERT with a value of 0 if necessary, then
                        // UPDATE all rows.
                        byte[] oldData = getDataByIdAndDimension(db, id, typeId, symbolId, dimensionId, null, eventDate, true);
                        if (oldData == null || timestampFromBytes(oldData) < eventDate) {
                            double previousCumulativeAmount = 0.0d;
                            if (oldData != null) {
                                previousCumulativeAmount = amountFromBytes(oldData, CUMULATIVEAMOUNT_POSITION);
                            }
                            List<Object> insertParameters = new ArrayList<Object>();
                            String insertSql = getMetricInsertSql(db, insertParameters, id, typeId, symbolId, dimensionId, 0, previousCumulativeAmount, eventDate);

                            tryInsertThenUpdate(db, connection, insertSql, insertParameters, null, null); // the UPDATE is going to be executed regardless of whether this fails - it's only inserting 0 anyway.
                        }
                        // Now update all the future rows.
                        List<Object> updateParameters = new ArrayList<Object>();
                        String updateSql = getUpdateSql(db, updateParameters, id, typeId, symbolId, dimensionId, incrementAmount, eventDate, true, true);
                        SqlDatabase.Static.executeUpdateWithList(vendor, connection, updateSql, updateParameters);
                    }
                }

            } catch (UpdateFailedException e) {
                // There is an existing row that has the wrong type ID (bad data). Repair it and try again.
                List<Object> repairParameters = new ArrayList<Object>();
                String repairSql = getRepairTypeIdSql(db, repairParameters, id, typeId, dimensionId, symbolId, eventDate);
                SqlDatabase.Static.executeUpdateWithList(vendor, connection, repairSql, repairParameters);
                doIncrementUpdateOrInsert(db, id, typeId, symbolId, dimensionId, incrementAmount, eventDate, isImplicitEventDate);
            } finally {
                db.closeConnection(connection);
            }
        }

        // This is for the occasional race condition when we check for the existence of a row, it does not exist, then two threads try to insert at (almost) the same time.
        private static void tryInsertThenUpdate(MetricAccessDatabase db, Connection connection, String insertSql, List<Object> insertParameters, String updateSql, List<Object> updateParameters) throws SQLException, UpdateFailedException {
            SqlVendor vendor = db.getMetricVendor();

            try {
                SqlDatabase.Static.executeUpdateWithList(vendor, connection, insertSql, insertParameters);
            } catch (SQLException ex) {
                if (db.getMetricVendor().isDuplicateKeyException(ex)) {
                    // Try the update again, maybe we lost a race condition.
                    if (updateSql != null) {
                        int rowsAffected = SqlDatabase.Static.executeUpdateWithList(vendor, connection, updateSql, updateParameters);
                        if (0 == rowsAffected) {
                            // The only practical way this query updated 0 rows is if there is an existing row with the wrong typeId.
                            throw new UpdateFailedException();
                        }
                    }
                } else {
                    throw ex;
                }
            }
        }

        private static void doSetUpdateOrInsert(MetricAccessDatabase db, UUID id, UUID typeId, int symbolId, UUID dimensionId, double amount, long eventDate) throws SQLException {
            SqlVendor vendor = db.getMetricVendor();
            Connection connection = db.openConnection();
            if (eventDate != 0L) {
                throw new RuntimeException("MetricAccess.Static.doSetUpdateOrInsert() can only be used if EventDatePrecision is NONE; eventDate is " + eventDate + ", should be 0L.");
            }
            try {
                List<Object> updateParameters = new ArrayList<Object>();
                String updateSql = getUpdateSql(db, updateParameters, id, typeId, symbolId, dimensionId, amount, eventDate, false, false);
                int rowsAffected = SqlDatabase.Static.executeUpdateWithList(vendor, connection, updateSql, updateParameters);
                if (rowsAffected == 0) {
                    List<Object> insertParameters = new ArrayList<Object>();
                    String insertSql = getMetricInsertSql(db, insertParameters, id, typeId, symbolId, dimensionId, amount, amount, eventDate);
                    tryInsertThenUpdate(db, connection, insertSql, insertParameters, updateSql, updateParameters);
                }
            } catch (UpdateFailedException e) {
                // There is an existing row that has the wrong type ID (bad data). Repair it and try again.
                List<Object> repairParameters = new ArrayList<Object>();
                String repairSql = getRepairTypeIdSql(db, repairParameters, id, typeId, dimensionId, symbolId, eventDate);
                SqlDatabase.Static.executeUpdateWithList(vendor, connection, repairSql, repairParameters);
                doSetUpdateOrInsert(db, id, typeId, symbolId, dimensionId, amount, eventDate);
            } finally {
                db.closeConnection(connection);
            }
        }

        static void doMetricDelete(MetricAccessDatabase db, UUID id, UUID typeId, int symbolId) throws SQLException {
            Connection connection = db.openConnection();
            List<Object> parameters = new ArrayList<Object>();
            try {
                String sql = getDeleteMetricSql(db, id, typeId, symbolId);
                SqlDatabase.Static.executeUpdateWithList(db.getMetricVendor(), connection, sql, parameters);
            } finally {
                db.closeConnection(connection);
            }
        }

        static void doInsertDimensionValue(MetricAccessDatabase db, UUID dimensionId, String dimensionValue) throws SQLException {
            Connection connection = db.openConnection();
            List<Object> parameters = new ArrayList<Object>();
            try {
                String sql = getInsertDimensionValueSql(db, parameters, dimensionId, dimensionValue);
                SqlDatabase.Static.executeUpdateWithList(db.getMetricVendor(), connection, sql, parameters);
            } finally {
                db.closeConnection(connection);
            }
        }

        static void doFixDataRow(MetricAccessDatabase db, UUID id, UUID typeId, int symbolId, UUID dimensionId, long eventDate, double cumulativeAmount, double amount) throws SQLException {
            Connection connection = db.openConnection();
            List<Object> parameters = new ArrayList<Object>();
            try {
                String updateSql = getFixDataRowSql(db, parameters, id, typeId, symbolId, dimensionId, eventDate, cumulativeAmount, amount);
                SqlDatabase.Static.executeUpdateWithList(db.getMetricVendor(), connection, updateSql, parameters);
            } finally {
                db.closeConnection(connection);
            }
        }

        static void doResummarize(MetricAccessDatabase db, UUID id, UUID typeId, int symbolId, UUID dimensionId, MetricInterval interval, Long minEventDate, Long maxEventDate) throws SQLException {
            String selectSql = getAllDataSql(db, id, typeId, symbolId, dimensionId, minEventDate, maxEventDate, true);

            Connection connection = db.openConnection();
            try {
                Statement statement = connection.createStatement();
                try {
                    ResultSet result = db.executeQueryBeforeTimeout(statement, selectSql, QUERY_TIMEOUT);
                    try {
                        Long lastIntervalTimestamp = null;
                        Long lastTimestamp = null;
                        Long firstTimestamp = null;
                        Double currentIntervalAmount = 0d;
                        Double currentIntervalCumulativeAmount = 0d;
                        while (result.next()) {
                            byte[] data = result.getBytes(1);

                            double amt = amountFromBytes(data, AMOUNT_POSITION);
                            double cumAmt = amountFromBytes(data, CUMULATIVEAMOUNT_POSITION);
                            long timestamp = timestampFromBytes(data);
                            Long intervalTimestamp = interval.process(new DateTime(timestamp));

                            if (!intervalTimestamp.equals(lastIntervalTimestamp)) {
                                if (lastIntervalTimestamp != null) {
                                    doResummarizeDataRows(db, id, typeId, symbolId, dimensionId, lastIntervalTimestamp, firstTimestamp, lastTimestamp, currentIntervalAmount, currentIntervalCumulativeAmount);
                                }
                                firstTimestamp = timestamp;
                                lastIntervalTimestamp = intervalTimestamp;
                                currentIntervalAmount = 0d;
                            }
                            lastTimestamp = timestamp;
                            currentIntervalAmount += amt;
                            currentIntervalCumulativeAmount = cumAmt;
                        }
                        if (lastIntervalTimestamp != null) {
                            doResummarizeDataRows(db, id, typeId, symbolId, dimensionId, lastIntervalTimestamp, firstTimestamp, lastTimestamp, currentIntervalAmount, currentIntervalCumulativeAmount);
                        }
                    } finally {
                        result.close();
                    }
                } finally {
                    statement.close();
                }
            } finally {
                db.closeConnection(connection);
            }
        }

        private static void doResummarizeDataRows(MetricAccessDatabase db, UUID id, UUID typeId, int symbolId, UUID dimensionId, long eventDate, long firstTimestamp, long lastTimestamp, double amount, double cumulativeAmount) throws SQLException {
            if (firstTimestamp == lastTimestamp && lastTimestamp == eventDate) {
                // nothing to do. . .
                return;
            }

            SqlVendor vendor = db.getMetricVendor();
            Connection connection = db.openConnection();

            try {
                // First delete old rows, then insert the new row
                List<Object> deleteParameters = new ArrayList<Object>();
                String deleteSql = getDeleteDataRowsBetweenSql(db, deleteParameters, id, typeId, symbolId, dimensionId, firstTimestamp, lastTimestamp);

                List<Object> insertParameters = new ArrayList<Object>();
                String insertSql = getMetricInsertSql(db, insertParameters, id, typeId, symbolId, dimensionId, amount, cumulativeAmount, eventDate);

                SqlDatabase.Static.executeUpdateWithList(vendor, connection, deleteSql, deleteParameters);
                SqlDatabase.Static.executeUpdateWithList(vendor, connection, insertSql, insertParameters);

            } finally {
                db.closeConnection(connection);
            }

        }

        private static String getDeleteDataRowsBetweenSql(MetricAccessDatabase db, List<Object> parameters, UUID id, UUID typeId, int symbolId, UUID dimensionId, Long minEventDate, Long maxEventDate) {
            SqlVendor vendor = db.getMetricVendor();
            StringBuilder sqlBuilder = new StringBuilder();
            sqlBuilder.append("DELETE FROM ");
            sqlBuilder.append(Static.getMetricTableIdentifier(db));

            sqlBuilder.append(" WHERE ");
            vendor.appendIdentifier(sqlBuilder, METRIC_SYMBOL_FIELD);
            sqlBuilder.append(" = ");
            vendor.appendBindValue(sqlBuilder, symbolId, parameters);

            sqlBuilder.append(" AND ");
            vendor.appendIdentifier(sqlBuilder, METRIC_ID_FIELD);
            sqlBuilder.append(" = ");
            vendor.appendBindValue(sqlBuilder, id, parameters);

            sqlBuilder.append(" AND ");
            vendor.appendIdentifier(sqlBuilder, METRIC_TYPE_FIELD);
            sqlBuilder.append(" = ");
            vendor.appendBindValue(sqlBuilder, typeId, parameters);

            sqlBuilder.append(" AND ");
            vendor.appendIdentifier(sqlBuilder, METRIC_DIMENSION_FIELD);
            sqlBuilder.append(" = ");
            vendor.appendBindValue(sqlBuilder, dimensionId, parameters);

            sqlBuilder.append(" AND ");
            vendor.appendIdentifier(sqlBuilder, METRIC_DATA_FIELD);
            sqlBuilder.append(" >= ");
            vendor.appendMetricEncodeTimestampSql(sqlBuilder, parameters, minEventDate, '0');

            sqlBuilder.append(" AND ");
            vendor.appendIdentifier(sqlBuilder, METRIC_DATA_FIELD);
            sqlBuilder.append(" <= ");
            vendor.appendMetricEncodeTimestampSql(sqlBuilder, parameters, maxEventDate, 'F');

            return sqlBuilder.toString();
        }

        static void doReconstructCumulativeAmounts(MetricAccessDatabase db, UUID id, UUID typeId, int symbolId, Long minEventDate) throws SQLException {

            // for each row, ordered by date, keep a running total of amount and update it into cumulativeAmount
            String selectSql = getAllDataSql(db, id, typeId, symbolId, null, minEventDate, null, true);
            Connection connection = db.openConnection();
            try {
                Statement statement = connection.createStatement();
                try {
                    ResultSet result = db.executeQueryBeforeTimeout(statement, selectSql, QUERY_TIMEOUT);
                    try {
                        UUID lastDimensionId = null;
                        double correctCumAmt = 0, calcAmt = 0, amt = 0, cumAmt = 0, lastCorrectCumAmt = 0;
                        long timestamp = 0;
                        while (result.next()) {
                            UUID dimensionId = UuidUtils.fromBytes(result.getBytes(1));
                            if (lastDimensionId == null || !dimensionId.equals(lastDimensionId)) {
                                // new dimension, reset the correctCumAmt. This depends
                                // on getAllDataSql ordering by dimensionId, data.
                                correctCumAmt = 0;
                                lastCorrectCumAmt = 0;
                            }
                            lastDimensionId = dimensionId;

                            byte[] data = result.getBytes(2);
                            amt = amountFromBytes(data, AMOUNT_POSITION);
                            cumAmt = amountFromBytes(data, CUMULATIVEAMOUNT_POSITION);
                            timestamp = timestampFromBytes(data);

                            // if this amount is not equal to this cumulative amount
                            // minus the previous CORRECT cumulative amount, adjust
                            // this cumulative amount UPWARDS OR DOWNWARDS to match it.
                            calcAmt = cumAmt - lastCorrectCumAmt;
                            if (calcAmt != amt) {
                                correctCumAmt = lastCorrectCumAmt + amt;
                            } else {
                                correctCumAmt = cumAmt;
                            }

                            if (correctCumAmt != cumAmt) {
                                doFixDataRow(db, id, typeId, symbolId, dimensionId, timestamp, correctCumAmt, amt);
                            }

                            lastCorrectCumAmt = correctCumAmt;
                        }
                    } finally {
                        result.close();
                    }
                } finally {
                    statement.close();
                }
            } finally {
                db.closeConnection(connection);
            }

        }

        // METRIC SELECT
        private static Double calculateMetricSumById(MetricAccessDatabase db, UUID id, UUID typeId, int symbolId, Long minEventDate, Long maxEventDate, boolean master) throws SQLException {
            // This method actually calculates the sum rather than just pulling the null dimension
            String sql = getSumSql(db, id, typeId, symbolId, minEventDate, maxEventDate);
            Double amount = null;
            Connection connection = master ? db.openConnection() : db.openReadConnection();
            try {
                Statement statement = connection.createStatement();
                try {
                    ResultSet result = db.executeQueryBeforeTimeout(statement, sql, QUERY_TIMEOUT);
                    try {
                        if (result.next()) {
                            amount = result.getDouble(1);
                        }
                    } finally {
                        result.close();
                    }
                } finally {
                    statement.close();
                }
            } finally {
                db.closeConnection(connection);
            }
            return amount;
        }

        private static Map<String, Double> getMetricDimensionsById(MetricAccessDatabase db, UUID id, UUID typeId, int symbolId, Long minEventDate, Long maxEventDate, boolean master) throws SQLException {
            String sql = getDimensionsSql(db, id, typeId, symbolId, minEventDate, maxEventDate);
            Map<String, Double> values = new HashMap<String, Double>();
            Connection connection = master ? db.openConnection() : db.openReadConnection();
            try {
                Statement statement = connection.createStatement();
                try {
                    ResultSet result = db.executeQueryBeforeTimeout(statement, sql, QUERY_TIMEOUT);
                    try {
                        while (result.next()) {
                            values.put(result.getString(1), result.getDouble(2));
                        }
                    } finally {
                        result.close();
                    }
                } finally {
                    statement.close();
                }
            } finally {
                db.closeConnection(connection);
            }
            return values;
        }

        private static Map<DateTime, Double> getMetricTimelineByIdAndDimension(MetricAccessDatabase db, UUID id, UUID typeId, int symbolId, UUID dimensionId, Long minEventDate, Long maxEventDate, MetricInterval metricInterval, boolean master) throws SQLException {
            String sql = getTimelineSql(db, id, typeId, symbolId, dimensionId, minEventDate, maxEventDate, metricInterval, true);

            Map<DateTime, Double> values = new CompactMap<DateTime, Double>();
            Connection connection = master ? db.openConnection() : db.openReadConnection();

            try {
                Statement statement = connection.createStatement();
                try {
                    ResultSet result = db.executeQueryBeforeTimeout(statement, sql, QUERY_TIMEOUT);
                    try {
                        while (result.next()) {
                            byte[] maxData = result.getBytes(1);
                            byte[] minData = result.getBytes(2);
                            long timestamp = result.getLong(3);
                            timestamp = metricInterval.process(new DateTime(timestamp));
                            double maxCumulativeAmount = amountFromBytes(maxData, CUMULATIVEAMOUNT_POSITION);
                            double minCumulativeAmount = amountFromBytes(minData, CUMULATIVEAMOUNT_POSITION);
                            double minAmount = amountFromBytes(minData, AMOUNT_POSITION);
                            double intervalAmount = maxCumulativeAmount - (minCumulativeAmount - minAmount);
                            values.put(new DateTime(timestamp), intervalAmount);
                        }
                    } finally {
                        result.close();
                    }
                } finally {
                    statement.close();
                }
            } finally {
                db.closeConnection(connection);
            }
            return values;
        }

        private static byte[] getDataByIdAndDimension(MetricAccessDatabase db, UUID id, UUID typeId, int symbolId, UUID dimensionId, Long minEventDate, Long maxEventDate, boolean master) throws SQLException {
            String sql = getDataSql(db, id, typeId, symbolId, dimensionId, minEventDate, maxEventDate, false, true, null, null, null);
            byte[] data = null;
            Connection connection = master ? db.openConnection() : db.openReadConnection();
            try {
                Statement statement = connection.createStatement();
                try {
                    ResultSet result = db.executeQueryBeforeTimeout(statement, sql, QUERY_TIMEOUT);
                    try {
                        if (result.next()) {
                            data = result.getBytes(1);
                        }
                    } finally {
                        result.close();
                    }
                } finally {
                    statement.close();
                }
            } finally {
                db.closeConnection(connection);
            }
            return data;
        }

        private static List<byte[]> getMaxMinDataByIdAndDimension(MetricAccessDatabase db, UUID id, UUID typeId, int symbolId, UUID dimensionId, Long minEventDate, Long maxEventDate, boolean master) throws SQLException {
            List<byte[]> datas = new ArrayList<byte[]>();
            String sql = getDataSql(db, id, typeId, symbolId, dimensionId, minEventDate, maxEventDate, true, true, null, null, null);
            Connection connection = master ? db.openConnection() : db.openReadConnection();
            try {
                Statement statement = connection.createStatement();
                try {
                    ResultSet result = db.executeQueryBeforeTimeout(statement, sql, QUERY_TIMEOUT);
                    try {
                        if (result.next()) {
                            datas.add(result.getBytes(1));
                            datas.add(result.getBytes(2));
                        }
                    } finally {
                        result.close();
                    }
                } finally {
                    statement.close();
                }
            } finally {
                db.closeConnection(connection);
            }
            return datas;
        }

        private static class DistinctIdsIterator implements Iterator<Metric.DistinctIds> {

            private static final int QUERY_TIMEOUT = 0;

            private final MetricAccessDatabase database;
            private final UUID typeId;
            private final int symbolId;
            private final Long minEventDate;
            private final Long maxEventDate;
            private final int fetchSize;
            private List<Metric.DistinctIds> items;
            private boolean done = false;

            private int index = 0;

            private UUID lastTypeId = null;
            private UUID lastId = null;
            private UUID lastDimensionId = null;

            public DistinctIdsIterator(MetricAccessDatabase database, UUID typeId, int symbolId, Long minEventDate, Long maxEventDate, int fetchSize) {
                this.database = database;
                this.typeId = typeId;
                this.symbolId = symbolId;
                this.minEventDate = minEventDate;
                this.maxEventDate = maxEventDate;
                this.fetchSize = fetchSize;
            }

            @Override
            public Metric.DistinctIds next() {
                if (hasNext()) {
                    Metric.DistinctIds result = items.get(index);
                    ++ index;
                    return result;

                } else {
                    throw new NoSuchElementException();
                }
            }

            @Override
            public boolean hasNext() {
                if (items != null && items.isEmpty()) {
                    return false;
                }
                if (items == null || index >= items.size()) {
                    if (done) {
                        return false;
                    }
                    try {
                        fetchNext();
                    } catch (SQLException e) {
                        throw new DatabaseException(this.database, e);
                    }
                    index = 0;
                    if (items.size() < 1) {
                        return false;
                    }
                    if (items.size() < fetchSize) {
                        done = true;
                    }
                }
                return true;
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }

            private void fetchNext() throws SQLException {
                String sql = getSql();
                items = new ArrayList<Metric.DistinctIds>();
                Connection connection = database.openConnection();
                try {
                    Statement statement = connection.createStatement();
                    try {
                        ResultSet result = database.executeQueryBeforeTimeout(statement, sql, QUERY_TIMEOUT);
                        try {
                            SqlVendor vendor = database.getMetricVendor();
                            while (result.next()) {
                                lastId = vendor.getUuid(result, 1);
                                lastDimensionId = vendor.getUuid(result, 2);
                                lastTypeId = vendor.getUuid(result, 3);
                                items.add(new Metric.DistinctIds(lastId, lastTypeId, lastDimensionId));
                            }
                        } finally {
                            result.close();
                        }
                    } finally {
                        statement.close();
                    }
                } finally {
                    database.closeConnection(connection);
                }
            }

            private String getSql() {
                SqlVendor vendor = database.getMetricVendor();
                StringBuilder sql = new StringBuilder();
                sql.append("SELECT ");
                vendor.appendIdentifier(sql, MetricAccess.METRIC_ID_FIELD);
                sql.append(",");
                vendor.appendIdentifier(sql, MetricAccess.METRIC_DIMENSION_FIELD);
                sql.append(",");
                vendor.appendIdentifier(sql, MetricAccess.METRIC_TYPE_FIELD);
                sql.append(" FROM ");
                sql.append(MetricAccess.Static.getMetricTableIdentifier(database));
                sql.append(" WHERE ");
                vendor.appendIdentifier(sql, MetricAccess.METRIC_SYMBOL_FIELD);
                sql.append(" = ");
                vendor.appendValue(sql, symbolId);
                if (typeId != null) {
                    sql.append(" AND ");
                    vendor.appendIdentifier(sql, MetricAccess.METRIC_TYPE_FIELD);
                    sql.append(" = ");
                    vendor.appendValue(sql, typeId);
                }
                if (maxEventDate != null) {
                    sql.append(" AND ");
                    vendor.appendIdentifier(sql, METRIC_DATA_FIELD);
                    sql.append(" < ");
                    vendor.appendMetricEncodeTimestampSql(sql, null, maxEventDate, '0');
                }
                if (minEventDate != null) {
                    sql.append(" AND ");
                    vendor.appendIdentifier(sql, METRIC_DATA_FIELD);
                    sql.append(" >= ");
                    vendor.appendMetricEncodeTimestampSql(sql, null, minEventDate, '0');
                }

                if (lastId != null && lastTypeId != null && lastDimensionId != null) {
                    sql.append(" AND ("); vendor.appendIdentifier(sql, MetricAccess.METRIC_TYPE_FIELD); sql.append(" > "); vendor.appendValue(sql, lastTypeId);  sql.append(" OR ("); // AND (typeId > lastTypeId OR (
                    vendor.appendIdentifier(sql, MetricAccess.METRIC_TYPE_FIELD); sql.append(" = "); vendor.appendValue(sql, lastTypeId); sql.append(" AND (");                       //     typeId = lastTypeId AND (
                    vendor.appendIdentifier(sql, MetricAccess.METRIC_ID_FIELD); sql.append(" > "); vendor.appendValue(sql, lastId); sql.append(" OR (");                              //         id > lastId OR (
                    vendor.appendIdentifier(sql, MetricAccess.METRIC_ID_FIELD); sql.append(" = "); vendor.appendValue(sql, lastId); sql.append(" AND (");                             //             id = lastId AND (
                    vendor.appendIdentifier(sql, MetricAccess.METRIC_DIMENSION_FIELD); sql.append(" > "); vendor.appendValue(sql, lastDimensionId);                                   //                 dimensionId > lastDimensionId
                    sql.append(")))))");                                                                                                                                              //             )))))
                }

                sql.append(" GROUP BY ");
                vendor.appendIdentifier(sql, MetricAccess.METRIC_TYPE_FIELD);
                sql.append(",");
                vendor.appendIdentifier(sql, MetricAccess.METRIC_ID_FIELD);
                sql.append(",");
                vendor.appendIdentifier(sql, MetricAccess.METRIC_DIMENSION_FIELD);

                sql.append(" ORDER BY ");
                vendor.appendIdentifier(sql, MetricAccess.METRIC_TYPE_FIELD);
                sql.append(",");
                vendor.appendIdentifier(sql, MetricAccess.METRIC_ID_FIELD);
                sql.append(",");
                vendor.appendIdentifier(sql, MetricAccess.METRIC_DIMENSION_FIELD);

                return vendor.rewriteQueryWithLimitClause(sql.toString(), fetchSize, 0);
            }
        }

        public static Iterator<Metric.DistinctIds> getDistinctIds(MetricAccessDatabase database, UUID typeId, int symbolId, Long startTimestamp, Long endTimestamp) {
            return new DistinctIdsIterator(database, typeId, symbolId, startTimestamp, endTimestamp, 1000);
        }

        private static UUID getDimensionIdByValue(MetricAccessDatabase db, String dimensionValue, boolean master) throws SQLException {
            String sql = getDimensionIdByValueSql(db, dimensionValue);
            Connection connection = master ? db.openConnection() : db.openReadConnection();
            try {
                Statement statement = connection.createStatement();
                try {
                    ResultSet result = db.executeQueryBeforeTimeout(statement, sql, QUERY_TIMEOUT);
                    try {
                        if (result.next()) {
                            return db.getMetricVendor().getUuid(result, 1);
                        }
                    } finally {
                        result.close();
                    }
                } finally {
                    statement.close();
                }
            } finally {
                db.closeConnection(connection);
            }
            return null;
        }

        public static void preFetchMetricSums(UUID id, UUID dimensionId, Long startTimestamp, Long endTimestamp, Collection<MetricAccess> metricAccesses, boolean master) throws SQLException {
            if (metricAccesses.isEmpty()) {
                return;
            }
            CachingDatabase cachingDb = getCachingDatabase();
            if (cachingDb == null) {
                return;
            }
            Iterator<MetricAccess> iter = metricAccesses.iterator();
            MetricAccess ma = iter.next();
            UUID typeId = ma.getTypeId();
            MetricAccessDatabase db = ma.getDatabase();
            if (db == null) {
                return;
            }
            Map<Integer, MetricAccess> maBySymbolId = new HashMap<Integer, MetricAccess>();
            StringBuilder symbolIdsString = new StringBuilder();
            do {
                symbolIdsString.append(ma.getSymbolId());
                symbolIdsString.append(',');
                maBySymbolId.put(ma.getSymbolId(), ma);
            } while (iter.hasNext() && (ma = iter.next()) != null);
            symbolIdsString.setLength(symbolIdsString.length() - 1);

            boolean selectMinData = true;
            if (startTimestamp == null) {
                selectMinData = false;
            }
            String extraSelectSql = METRIC_SYMBOL_FIELD;
            String extraGroupBySql = METRIC_SYMBOL_FIELD;
            String extraWhereSql = METRIC_SYMBOL_FIELD + " IN (" + symbolIdsString.toString() + ")";

            String sql = getDataSql(db, id, typeId, null, dimensionId, startTimestamp, endTimestamp, selectMinData, true, extraSelectSql, extraGroupBySql, extraWhereSql);

            Connection connection = master ? db.openConnection() : db.openReadConnection();
            try {
                Statement statement = connection.createStatement();
                try {
                    ResultSet result = db.executeQueryBeforeTimeout(statement, sql, QUERY_TIMEOUT);
                    try {
                        while (result.next()) {

                            byte[] maxData = result.getBytes(1);
                            byte[] minData = null;
                            int symbolId;
                            if (selectMinData) {
                                minData = result.getBytes(2);
                                symbolId = result.getInt(3);
                            } else {
                                minData = null;
                                symbolId = result.getInt(2);
                            }
                            MetricAccess metricAccess = maBySymbolId.get(symbolId);
                            if (selectMinData) {
                                metricAccess.putCachedData(cachingDb, id, dimensionId, startTimestamp, minData, CACHE_MIN);
                            }
                            metricAccess.putCachedData(cachingDb, id, dimensionId, endTimestamp, maxData, CACHE_MAX);
                            metricAccesses.remove(metricAccess);
                        }
                    } finally {
                        result.close();
                    }
                } finally {
                    statement.close();
                }
            } finally {
                db.closeConnection(connection);
            }

            // If we did not find data, we still need to cache that fact.
            iter = metricAccesses.iterator();
            while (iter.hasNext()) {
                MetricAccess metricAccess = iter.next();
                if (selectMinData) {
                    metricAccess.putCachedData(cachingDb, id, dimensionId, startTimestamp, null, CACHE_MIN);
                }
                metricAccess.putCachedData(cachingDb, id, dimensionId, endTimestamp, null, CACHE_MAX);
            }

        }

        public static MetricAccess getMetricAccess(Database db, ObjectType type, ObjectField field) {
            if (db == null || field == null) {
                return null;
            }
            StringBuilder keyBuilder = new StringBuilder(db.getName());
            keyBuilder.append(':');
            // For some reason metrics are being created with the wrong typeId; make sure a new typeId busts the cache
            keyBuilder.append((type != null ? type.getId() : UuidUtils.ZERO_UUID));
            keyBuilder.append(':');
            keyBuilder.append(field.getUniqueName());
            keyBuilder.append(':');
            keyBuilder.append(field.as(MetricAccess.FieldData.class).getEventDateProcessorClassName());
            String maKey = keyBuilder.toString();
            MetricAccess metricAccess = METRIC_ACCESSES.get(maKey);
            if (metricAccess == null) {
                MetricAccessDatabase sqlDb = null;
                while (db instanceof ForwardingDatabase) {
                    db = ((ForwardingDatabase) db).getDelegate();
                }
                if (db instanceof MetricAccessDatabase) {
                    sqlDb = (MetricAccessDatabase) db;
                } else if (db instanceof AggregateDatabase) {
                    if (((AggregateDatabase) db).getDefaultDelegate() instanceof MetricAccessDatabase) {
                        sqlDb = (MetricAccessDatabase) ((AggregateDatabase) db).getDefaultDelegate();
                    } else {
                        sqlDb = (MetricAccessDatabase) ((AggregateDatabase) db).getFirstDelegateByClass(MetricAccessDatabase.class);
                    }
                }
                if (sqlDb != null) {
                    metricAccess = new MetricAccess(sqlDb, (type != null ? type.getId() : UuidUtils.ZERO_UUID), field, field.as(MetricAccess.FieldData.class).getEventDateProcessor());
                    METRIC_ACCESSES.put(maKey, metricAccess);
                }
            }
            return metricAccess != null
                    ? metricAccess
                    : new NullMetricAccess(
                            type != null ? type.getId() : UuidUtils.ZERO_UUID,
                            field,
                            field.as(MetricAccess.FieldData.class).getEventDateProcessor());
        }

        public static CachingDatabase getCachingDatabase() {
            Database db = Database.Static.getDefault();
            while (db instanceof ForwardingDatabase) {
                if (db instanceof CachingDatabase) {
                    return (CachingDatabase) db;
                }
                db = ((ForwardingDatabase) db).getDelegate();
            }
            return null;
        }

        public static String getMetricTableIdentifier(MetricAccessDatabase database) {
            String catalog = database.getMetricCatalog();

            if (catalog == null) {
                StringBuilder str = new StringBuilder();
                database.getMetricVendor().appendIdentifier(str, METRIC_TABLE);

                return str.toString();

            } else {
                SqlVendor vendor = database.getMetricVendor();
                StringBuilder str = new StringBuilder();

                vendor.appendIdentifier(str, catalog);
                str.append(".");
                vendor.appendIdentifier(str, METRIC_TABLE);

                return str.toString();

            }
        }

    }

    // MODIFICATIONS

    @Record.FieldInternalNamePrefix("dari.metric.")
    public static class FieldData extends Modification<ObjectField> {

        private transient MetricInterval eventDateProcessor;

        private boolean metricValue;
        private String eventDateProcessorClassName;

        public boolean isMetricValue() {
            return metricValue;
        }

        public void setMetricValue(boolean metricValue) {
            this.metricValue = metricValue;
        }

        public String getEventDateProcessorClassName() {
            return eventDateProcessorClassName;
        }

        @SuppressWarnings("unchecked")
        public MetricInterval getEventDateProcessor() {
            if (eventDateProcessor == null) {
                if (eventDateProcessorClassName == null) {
                    return null;
                } else {
                    try {
                        Class<MetricInterval> cls = (Class<MetricInterval>) Class.forName(eventDateProcessorClassName);
                        eventDateProcessor = cls.newInstance();
                    } catch (ClassNotFoundException e) {
                        throw new RuntimeException(e);
                    } catch (InstantiationException e) {
                        throw new RuntimeException(e);
                    } catch (IllegalAccessException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
            return eventDateProcessor;
        }

        public void setEventDateProcessorClass(Class<? extends MetricInterval> eventDateProcessorClass) {
            this.eventDateProcessorClassName = eventDateProcessorClass.getName();
        }

        public void setEventDateProcessorClassName(String eventDateProcessorClassName) {
            this.eventDateProcessorClassName = eventDateProcessorClassName;
        }

        public String getRecalculableFieldName() {
            return getOriginalObject().as(RecalculationFieldData.class).getMetricFieldName();
        }

        public void setRecalculableFieldName(String fieldName) {
            getOriginalObject().as(RecalculationFieldData.class).setMetricFieldName(fieldName);
        }

        public Set<ObjectMethod> getRecalculableObjectMethods(ObjectType type) {
            Set<ObjectMethod> methods = recalculableObjectMethods.get();
            methods.addAll(type.as(TypeData.class).getRecalculableObjectMethods(getOriginalObject().getInternalName()));
            return methods;
        }

        private final transient Supplier<Set<ObjectMethod>> recalculableObjectMethods = Suppliers.memoizeWithExpiration(new Supplier<Set<ObjectMethod>>() {

            @Override
            public Set<ObjectMethod> get() {
                Set<ObjectMethod> methods = new HashSet<ObjectMethod>();
                String fieldName = getOriginalObject().getInternalName();

                if (fieldName != null) {
                    for (ObjectMethod method : getState().getDatabase().getEnvironment().getMethods()) {
                        if (fieldName.equals(method.as(FieldData.class).getRecalculableFieldName())) {
                            methods.add(method);
                        }
                    }

                    ObjectType parentType = getOriginalObject().getParentType();
                    if (parentType != null) {
                        for (ObjectMethod method : parentType.getMethods()) {
                            if (fieldName.equals(method.as(FieldData.class).getRecalculableFieldName())) {
                                methods.add(method);
                            }
                        }
                    }
                }

                return methods;
            }

        }, 5, TimeUnit.SECONDS);

    }

    @Record.FieldInternalNamePrefix("dari.metric.")
    public static class TypeData extends Modification<ObjectType> {

        public Set<ObjectMethod> getRecalculableObjectMethods(String metricFieldName) {
            Set<ObjectMethod> methods = recalculableObjectMethods.get().get(metricFieldName);
            if (methods == null) {
                methods = new HashSet<ObjectMethod>();
            }
            return methods;
        }

        private final transient Supplier<Map<String, Set<ObjectMethod>>> recalculableObjectMethods = Suppliers.memoizeWithExpiration(new Supplier<Map<String, Set<ObjectMethod>>>() {

            @Override
            public Map<String, Set<ObjectMethod>> get() {
                Map<String, Set<ObjectMethod>> methods = new CompactMap<String, Set<ObjectMethod>>();

                for (ObjectMethod method : getOriginalObject().getMethods()) {
                    String fieldName = method.as(FieldData.class).getRecalculableFieldName();
                    if (!methods.containsKey(fieldName)) {
                        methods.put(fieldName, new HashSet<ObjectMethod>());
                    }
                    methods.get(fieldName).add(method);
                }

                return methods;
            }

        }, 5, TimeUnit.SECONDS);

    }
}

class ResummarizeTask extends Task {
    private static final int QUEUE_SIZE = 200;
    private final MetricAccessDatabase database;
    private final int symbolId;
    private final MetricInterval interval;
    private final Long startTimestamp;
    private final Long endTimestamp;
    private final int numConsumers;
    private final String executor;
    private final String name;
    private final AsyncQueue<Metric.DistinctIds> queue = new AsyncQueue<Metric.DistinctIds>(new ArrayBlockingQueue<Metric.DistinctIds>(QUEUE_SIZE));
    private final List<ResummarizeConsumer> consumers = new ArrayList<ResummarizeConsumer>();

    public ResummarizeTask(MetricAccessDatabase database, int symbolId, MetricInterval interval, Long startTimestamp, Long endTimestamp, int numConsumers, String executor, String name) {
        super(executor, name);
        this.database = database;
        this.symbolId = symbolId;
        this.interval = interval;
        this.startTimestamp = startTimestamp;
        this.endTimestamp = endTimestamp;
        this.numConsumers = numConsumers;
        this.executor = executor;
        this.name = name;
    }

    public void doTask() throws Exception {
        DistributedLock lock = new DistributedLock(database, executor + ":" + name);
        boolean locked = false;
        try {
            if (lock.tryLock()) {
                locked = true;
                for (int i = 0; i < numConsumers; i++) {
                    ResummarizeConsumer consumer = new ResummarizeConsumer(database, symbolId, interval, startTimestamp, endTimestamp, queue, executor);
                    consumers.add(consumer);
                    consumer.submit();
                }
                Iterator<Metric.DistinctIds> iter = MetricAccess.Static.getDistinctIds(database, null, symbolId, startTimestamp, endTimestamp);
                int i = 0;
                while (shouldContinue() && iter.hasNext()) {
                    this.setProgressIndex(++i);
                    Metric.DistinctIds tuple = iter.next();
                    queue.add(tuple);
                }
                this.setProgressTotal(i);
                queue.closeAutomatically();
                boolean done;
                do {
                    Thread.sleep(1000);
                    done = true;
                    for (Task task : consumers) {
                        if (task.isRunning()) {
                            done = false;
                        }
                    }
                } while (shouldContinue() && !done);
            }
        } finally {
            if (locked) {
                lock.unlock();
            }
        }
    }
}

class ResummarizeConsumer extends AsyncConsumer<Metric.DistinctIds> {
    private final MetricAccessDatabase database;
    private final int symbolId;
    private final MetricInterval interval;
    private final Long startTimestamp;
    private final Long endTimestamp;

    public ResummarizeConsumer(MetricAccessDatabase database, int symbolId, MetricInterval interval, Long startTimestamp, Long endTimestamp, AsyncQueue<Metric.DistinctIds> input, String executor) {
        super(executor, input);
        this.database = database;
        this.symbolId = symbolId;
        this.interval = interval;
        this.startTimestamp = startTimestamp;
        this.endTimestamp = endTimestamp;
    }

    @Override
    protected void consume(Metric.DistinctIds tuple) throws Exception {
        MetricAccess.Static.doResummarize(database, tuple.id, tuple.typeId, symbolId, tuple.dimensionId, interval, startTimestamp, endTimestamp);
    }

}
