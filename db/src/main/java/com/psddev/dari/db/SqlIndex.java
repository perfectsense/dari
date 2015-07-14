package com.psddev.dari.db;

import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.psddev.dari.util.LocaleUtils;
import com.psddev.dari.util.ObjectToIterable;
import com.psddev.dari.util.ObjectUtils;
import com.psddev.dari.util.StringUtils;

/** Internal representations of all SQL index tables. */
public enum SqlIndex {

    CUSTOM(
        new AbstractTable(2, "id", "typeId", "symbolId", "value") {

            @Override
            public String getName(SqlDatabase database, ObjectIndex index) {
                String name = SqlDatabase.Static.getIndexTable(index);
                if (ObjectUtils.isBlank(name)) {
                    throw new IllegalStateException(String.format(
                            "[%s] needs @SqlDatabase.FieldIndexTable annotation!",
                            index));
                } else {
                    return name;
                }
            }

            @Override
            public Object convertKey(SqlDatabase database, ObjectIndex index, String key) {
                return database.getSymbolId(key);
            }

            @Override
            public boolean isReadOnly(ObjectIndex index) {
                List<String> indexFieldNames = index.getFields();
                ObjectStruct parent = index.getParent();

                for (String fieldName : indexFieldNames) {
                    ObjectField field = parent.getField(fieldName);

                    if (field != null) {
                        return field.as(SqlDatabase.FieldData.class).isIndexTableReadOnly();
                    }
                }

                return false;
            }

            @Override
            public String getTypeIdField(SqlDatabase database, ObjectIndex index) {
                if (database.hasColumn(getName(database, index), "typeId")) {
                    return "typeId";
                } else {
                    return null;
                }
            }
        }
    ),

    LOCATION(
        new NameSingleValueTable(1, "RecordLocation"),
        new SymbolIdSingleValueTable(2, "RecordLocation2"),
        new TypeIdSymbolIdSingleValueTable(3, "RecordLocation3")
    ),

    REGION(
        new SymbolIdSingleValueTable(1, "RecordRegion"),
        new TypeIdSymbolIdSingleValueTable(2, "RecordRegion2")
    ),

    NUMBER(
        new NameSingleValueTable(1, "RecordNumber"),
        new SymbolIdSingleValueTable(2, "RecordNumber2"),
        new TypeIdSymbolIdSingleValueTable(3, "RecordNumber3")
    ),

    STRING(
        new NameSingleValueTable(1, "RecordString") {
            @Override
            protected Object convertValue(SqlDatabase database, ObjectIndex index, int fieldIndex, Object value) {
                String string = value.toString();
                return string.length() > 400 ? string.substring(0, 400) : string;
            }
        },

        new SymbolIdSingleValueTable(2, "RecordString2") {
            @Override
            protected Object convertValue(SqlDatabase database, ObjectIndex index, int fieldIndex, Object value) {
                return stringToBytes(value.toString(), 500);
            }
        },

        new SymbolIdSingleValueTable(3, "RecordString3") {
            @Override
            protected Object convertValue(SqlDatabase database, ObjectIndex index, int fieldIndex, Object value) {
                String valueString = value.toString().trim();
                if (!index.isCaseSensitive()) {
                    valueString = valueString.toLowerCase(Locale.ENGLISH);
                }
                return stringToBytes(valueString, 500);
            }
        },

        new TypeIdSymbolIdSingleValueTable(4, "RecordString4") {
            @Override
            protected Object convertValue(SqlDatabase database, ObjectIndex index, int fieldIndex, Object value) {
                String valueString = StringUtils.trimAndCollapseWhitespaces(value.toString());
                if (!index.isCaseSensitive()) {
                    valueString = valueString.toLowerCase(Locale.ENGLISH);
                }
                return stringToBytes(valueString, 500);
            }
        }
    ),

    UUID(
        new NameSingleValueTable(1, "RecordUuid"),
        new SymbolIdSingleValueTable(2, "RecordUuid2"),
        new TypeIdSymbolIdSingleValueTable(3, "RecordUuid3")
    );

    private final Table[] tables;

    private SqlIndex(Table... tables) {
        this.tables = tables;
    }

    /**
     * Returns the table that can be used to read the values of the given
     * {@code index} from the given {@code database}.
     */
    public Table getReadTable(SqlDatabase database, ObjectIndex index) {
        for (Table table : tables) {
            if (database.hasTable(table.getName(database, index))) {
                return table;
            }
        }
        return tables[tables.length - 1];
    }

    /**
     * Returns all tables that should be written to when updating the
     * values of the index in the given {@code database}.
     */
    public List<Table> getWriteTables(SqlDatabase database, ObjectIndex index) {
        List<Table> writeTables = new ArrayList<Table>();

        for (Table table : tables) {
            if (database.hasTable(table.getName(database, index)) && !table.isReadOnly(index)) {
                writeTables.add(table);
            }
        }

        if (writeTables.isEmpty()) {
            Table lastTable = tables[tables.length - 1];

            if (!lastTable.isReadOnly(index)) {
                writeTables.add(lastTable);
            }
        }

        return writeTables;
    }

    public interface Table {

        public int getVersion();

        public boolean isReadOnly(ObjectIndex index);

        public String getName(SqlDatabase database, ObjectIndex index);

        public String getIdField(SqlDatabase database, ObjectIndex index);

        public String getKeyField(SqlDatabase database, ObjectIndex index);

        public String getValueField(SqlDatabase database, ObjectIndex index, int fieldIndex);

        public String getTypeIdField(SqlDatabase database, ObjectIndex index);

        public Object convertKey(SqlDatabase database, ObjectIndex index, String key);

        public String prepareInsertStatement(
                SqlDatabase database,
                Connection connection,
                ObjectIndex index) throws SQLException;

        public default String prepareUpdateStatement(
                SqlDatabase database,
                Connection connection,
                ObjectIndex index) throws SQLException {
            throw new UnsupportedOperationException();
        }

        public void bindInsertValues(
                SqlDatabase database,
                ObjectIndex index,
                UUID id,
                UUID typeId,
                IndexValue indexValue,
                Set<String> bindKeys,
                List<List<Object>> parameters) throws SQLException;

        public default void bindUpdateValues(
                SqlDatabase database,
                ObjectIndex index,
                UUID id,
                UUID typeId,
                IndexValue indexValue,
                Set<String> bindKeys,
                List<List<Object>> parameters) throws SQLException {
            throw new UnsupportedOperationException();
        }
    }

    private abstract static class AbstractTable implements Table {

        private final int version;
        private final String idField;
        private final String typeIdField;
        private final String keyField;
        private final String valueField;

        public AbstractTable(int version, String idField, String typeIdField, String keyField, String valueField) {
            this.version = version;
            this.idField = idField;
            this.typeIdField = typeIdField;
            this.keyField = keyField;
            this.valueField = valueField;
        }

        @Override
        public int getVersion() {
            return version;
        }

        @Override
        public boolean isReadOnly(ObjectIndex index) {
            return false;
        }

        @Override
        public String getIdField(SqlDatabase database, ObjectIndex index) {
            return idField;
        }

        @Override
        public String getTypeIdField(SqlDatabase database, ObjectIndex index) {
            return typeIdField;
        }

        @Override
        public String getKeyField(SqlDatabase database, ObjectIndex index) {
            return keyField;
        }

        @Override
        public String getValueField(SqlDatabase database, ObjectIndex index, int fieldIndex) {
            List<String> indexFieldNames = index.getFields();
            ObjectStruct parent = index.getParent();

            for (String fieldName : indexFieldNames) {
                ObjectField field = parent.getField(fieldName);

                if (field != null
                        && field.as(SqlDatabase.FieldData.class).isIndexTableSameColumnNames()) {
                    String valueFieldName = indexFieldNames.get(fieldIndex);
                    int dotAt = valueFieldName.lastIndexOf(".");

                    if (dotAt > -1) {
                        valueFieldName = valueFieldName.substring(dotAt + 1);
                    }

                    return valueFieldName;
                } else if (field != null
                        && field.as(SqlDatabase.FieldData.class).getIndexTableColumnName() != null) {
                    return field.as(SqlDatabase.FieldData.class).getIndexTableColumnName();
                }
            }

            return fieldIndex > 0 ? valueField + (fieldIndex + 1) : valueField;
        }

        @Override
        public Object convertKey(SqlDatabase database, ObjectIndex index, String key) {
            return key;
        }

        protected Object convertValue(SqlDatabase database, ObjectIndex index, int fieldIndex, Object value) {
            ObjectStruct parent = index.getParent();
            ObjectField field = parent.getField(index.getFields().get(fieldIndex));
            String type = field.getInternalItemType();

            if (ObjectField.DATE_TYPE.equals(type)
                    || ObjectField.NUMBER_TYPE.equals(type)
                    || ObjectField.LOCATION_TYPE.equals(type)
                    || ObjectField.REGION_TYPE.equals(type)) {
                return value;

            } else if (value instanceof UUID) {
                return value;

            } else if (value instanceof String) {
                String valueString = StringUtils.trimAndCollapseWhitespaces(value.toString());
                if (!index.isCaseSensitive() && database.comparesIgnoreCase()) {
                    valueString = valueString.toLowerCase(Locale.ENGLISH);
                }
                return stringToBytes(valueString, 500);

            } else {
                return value.toString();
            }
        }

        protected static byte[] stringToBytes(String value, int length) {
            byte[] bytes = value.getBytes(StandardCharsets.UTF_8);

            if (bytes.length <= length) {
                return bytes;

            } else {
                byte[] shortened = new byte[length];
                System.arraycopy(bytes, 0, shortened, 0, length);
                return shortened;
            }
        }

        @Override
        public String prepareInsertStatement(
                SqlDatabase database,
                Connection connection,
                ObjectIndex index) throws SQLException {

            SqlVendor vendor = database.getVendor();
            int fieldsSize = index.getFields().size();
            StringBuilder insertBuilder = new StringBuilder();

            insertBuilder.append("INSERT INTO ");
            vendor.appendIdentifier(insertBuilder, getName(database, index));
            insertBuilder.append(" (");
            vendor.appendIdentifier(insertBuilder, getIdField(database, index));
            insertBuilder.append(",");
            if (getTypeIdField(database, index) != null) {
                vendor.appendIdentifier(insertBuilder, getTypeIdField(database, index));
                insertBuilder.append(",");
            }
            vendor.appendIdentifier(insertBuilder, getKeyField(database, index));

            for (int i = 0; i < fieldsSize; ++ i) {
                insertBuilder.append(",");
                vendor.appendIdentifier(insertBuilder, getValueField(database, index, i));
            }

            insertBuilder.append(") VALUES");
            insertBuilder.append(" (?, ?, ");
            if (getTypeIdField(database, index) != null) {
                insertBuilder.append("?, ");
            }

            // Add placeholders for each value in this index.
            for (int i = 0; i < fieldsSize; ++ i) {
                if (i != 0) {
                    insertBuilder.append(", ");
                }

                if (SqlIndex.Static.getByIndex(index) == SqlIndex.LOCATION) {
                    vendor.appendBindLocation(insertBuilder, null, null);
                } else if (SqlIndex.Static.getByIndex(index) == SqlIndex.REGION) {
                    vendor.appendBindRegion(insertBuilder, null, null);
                } else {
                    insertBuilder.append("?");
                }
            }

            insertBuilder.append(")");

            return insertBuilder.toString();
        }

        public String prepareUpdateStatement(
                SqlDatabase database,
                Connection connection,
                ObjectIndex index) throws SQLException {

            SqlVendor vendor = database.getVendor();
            int fieldsSize = index.getFields().size();
            StringBuilder updateBuilder = new StringBuilder();

            updateBuilder.append("UPDATE ");
            vendor.appendIdentifier(updateBuilder, getName(database, index));
            updateBuilder.append(" SET ");

            for (int i = 0; i < fieldsSize; ++ i) {
                vendor.appendIdentifier(updateBuilder, getValueField(database, index, i));
                updateBuilder.append(" = ");
                if (SqlIndex.Static.getByIndex(index) == SqlIndex.LOCATION) {
                    vendor.appendBindLocation(updateBuilder, null, null);
                } else if (SqlIndex.Static.getByIndex(index) == SqlIndex.REGION) {
                    vendor.appendBindRegion(updateBuilder, null, null);
                } else {
                    updateBuilder.append("?");
                }
                updateBuilder.append(", ");
            }
            if (fieldsSize > 0) {
                updateBuilder.setLength(updateBuilder.length() - 2);
            }

            updateBuilder.append(" WHERE ");
            vendor.appendIdentifier(updateBuilder, getIdField(database, index));
            updateBuilder.append(" = ?");

            if (getTypeIdField(database, index) != null) {
                updateBuilder.append(" AND ");
                vendor.appendIdentifier(updateBuilder, getTypeIdField(database, index));
                updateBuilder.append(" = ?");
            }

            updateBuilder.append(" AND ");
            vendor.appendIdentifier(updateBuilder, getKeyField(database, index));
            updateBuilder.append(" = ?");

            return updateBuilder.toString();
        }

        @Override
        public void bindInsertValues(
                SqlDatabase database,
                ObjectIndex index,
                UUID id,
                UUID typeId,
                IndexValue indexValue,
                Set<String> bindKeys,
                List<List<Object>> parameters) throws SQLException {

            SqlVendor vendor = database.getVendor();
            Object indexKey = convertKey(database, index, indexValue.getUniqueName());
            int fieldsSize = index.getFields().size();
            StringBuilder insertBuilder = new StringBuilder();
            boolean writeIndex = true;

            for (Object[] valuesArray : indexValue.getValuesArray()) {
                StringBuilder bindKeyBuilder = new StringBuilder();
                bindKeyBuilder.append(id.toString());
                bindKeyBuilder.append(indexKey);

                for (int i = 0; i < fieldsSize; i++) {
                    Object parameter = convertValue(database, index, i, valuesArray[i]);
                    vendor.appendValue(bindKeyBuilder, parameter);

                    if (ObjectUtils.isBlank(parameter)) {
                        writeIndex = false;
                        break;
                    }
                }

                String bindKey = bindKeyBuilder.toString();

                if (writeIndex && !bindKeys.contains(bindKey)) {
                    List<Object> rowData = new ArrayList<Object>();

                    vendor.appendBindValue(insertBuilder, id, rowData);
                    if (getTypeIdField(database, index) != null) {
                        vendor.appendBindValue(insertBuilder, typeId, rowData);
                    }
                    vendor.appendBindValue(insertBuilder, indexKey, rowData);

                    for (int i = 0; i < fieldsSize; i++) {
                        Object parameter = convertValue(database, index, i, valuesArray[i]);
                        vendor.appendBindValue(insertBuilder, parameter, rowData);
                    }

                    bindKeys.add(bindKey);
                    parameters.add(rowData);
                }
            }
        }

        public void bindUpdateValues(
                SqlDatabase database,
                ObjectIndex index,
                UUID id,
                UUID typeId,
                IndexValue indexValue,
                Set<String> bindKeys,
                List<List<Object>> parameters) throws SQLException {

            SqlVendor vendor = database.getVendor();
            Object indexKey = convertKey(database, index, indexValue.getUniqueName());
            int fieldsSize = index.getFields().size();
            StringBuilder updateBuilder = new StringBuilder();
            boolean writeIndex = true;

            for (Object[] valuesArray : indexValue.getValuesArray()) {
                StringBuilder bindKeyBuilder = new StringBuilder();
                bindKeyBuilder.append(id.toString());
                bindKeyBuilder.append(indexKey);

                for (int i = 0; i < fieldsSize; i++) {
                    Object parameter = convertValue(database, index, i, valuesArray[i]);
                    vendor.appendValue(bindKeyBuilder, parameter);

                    if (ObjectUtils.isBlank(parameter)) {
                        writeIndex = false;
                        break;
                    }
                }

                String bindKey = bindKeyBuilder.toString();

                if (writeIndex && !bindKeys.contains(bindKey)) {
                    List<Object> rowData = new ArrayList<Object>();

                    for (int i = 0; i < fieldsSize; i++) {
                        Object parameter = convertValue(database, index, i, valuesArray[i]);
                        vendor.appendBindValue(updateBuilder, parameter, rowData);
                    }

                    vendor.appendBindValue(updateBuilder, id, rowData);
                    if (getTypeIdField(database, index) != null) {
                        vendor.appendBindValue(updateBuilder, typeId, rowData);
                    }
                    vendor.appendBindValue(updateBuilder, indexKey, rowData);

                    bindKeys.add(bindKey);
                    parameters.add(rowData);
                }
            }
        }
    }

    private static class SingleValueTable extends AbstractTable {

        private String name;

        public SingleValueTable(int version, String name, String idField, String typeIdField, String keyField, String valueField) {
            super(version, idField, typeIdField, keyField, valueField);
            this.name = name;
        }

        @Override
        public String getName(SqlDatabase database, ObjectIndex index) {
            return name;
        }
    }

    private static class NameSingleValueTable extends SingleValueTable {

        public NameSingleValueTable(int version, String name) {
            super(version, name, "recordId", null, "name", "value");
        }
    }

    private static class SymbolIdSingleValueTable extends SingleValueTable {

        public SymbolIdSingleValueTable(int version, String name) {
            super(version, name, "id", null, "symbolId", "value");
        }

        public SymbolIdSingleValueTable(int version, String name, String typeIdField) {
            super(version, name, "id", typeIdField, "symbolId", "value");
        }

        @Override
        public Object convertKey(SqlDatabase database, ObjectIndex index, String key) {
            return database.getSymbolId(key);
        }
    }

    private static class TypeIdSymbolIdSingleValueTable extends SymbolIdSingleValueTable {

        public TypeIdSymbolIdSingleValueTable(int version, String name) {
            super(version, name, "typeId");
        }
    }

    public static final class IndexValue {

        private final ObjectField[] prefixes;
        private final ObjectIndex index;
        private final Object[][] valuesArray;

        private IndexValue(ObjectField[] prefixes, ObjectIndex index, Object[][] valuesArray) {
            this.prefixes = prefixes;
            this.index = index;
            this.valuesArray = valuesArray;
        }

        public ObjectIndex getIndex() {
            return index;
        }

        public Object[][] getValuesArray() {
            return valuesArray;
        }

        /**
         * Returns a unique name that identifies this index value.
         * This is a helper method for database implementations and
         * isn't meant for general consumption.
         */
        public String getUniqueName() {
            StringBuilder nameBuilder = new StringBuilder();

            if (prefixes == null) {
                if (index.getParent() instanceof ObjectType) {
                    nameBuilder.append(index.getJavaDeclaringClassName());
                    nameBuilder.append('/');
                }

            } else {
                nameBuilder.append(prefixes[0].getUniqueName());
                nameBuilder.append('/');
                for (int i = 1, length = prefixes.length; i < length; ++ i) {
                    nameBuilder.append(prefixes[i].getInternalName());
                    nameBuilder.append('/');
                }
            }

            Iterator<String> indexFieldsIterator = index.getFields().iterator();
            nameBuilder.append(indexFieldsIterator.next());
            while (indexFieldsIterator.hasNext()) {
                nameBuilder.append(',');
                nameBuilder.append(indexFieldsIterator.next());
            }

            return nameBuilder.toString();
        }

        public String getInternalType() {
            List<String> fields = index.getFields();
            return index.getParent().getField(fields.get(fields.size() - 1)).getInternalItemType();
        }
    }

    /** {@linkplain SqlIndex} utility methods. */
    public static final class Static {

        /**
         * Returns the instance that should be used to index values
         * of the given field {@code type}.
         */
        public static SqlIndex getByType(String type) {
            if (ObjectField.DATE_TYPE.equals(type)
                    || ObjectField.NUMBER_TYPE.equals(type)) {
                return SqlIndex.NUMBER;

            } else if (ObjectField.LOCATION_TYPE.equals(type)) {
                return SqlIndex.LOCATION;

            } else if (ObjectField.REGION_TYPE.equals(type)) {
                return SqlIndex.REGION;

            } else if (ObjectField.RECORD_TYPE.equals(type)
                    || ObjectField.UUID_TYPE.equals(type)) {
                return SqlIndex.UUID;

            } else {
                return SqlIndex.STRING;
            }
        }

        /**
         * Returns the instance that should be used to index values
         * of the given {@code index}.
         */
        public static SqlIndex getByIndex(ObjectIndex index) {
            List<String> fieldNames = index.getFields();
            ObjectField field = index.getParent().getField(fieldNames.get(0));

            if (fieldNames.size() > 1
                    || (field != null
                    && field.as(SqlDatabase.FieldData.class).getIndexTable() != null)) {
                return SqlIndex.CUSTOM;

            } else {
                String type = field != null ? field.getInternalItemType() : index.getType();

                return getByType(type);
            }
        }

        /**
         * Deletes all index rows associated with the given {@code states}.
         */
        public static void deleteByStates(
                SqlDatabase database,
                Connection connection,
                List<State> states)
                throws SQLException {
            deleteByStates(database, connection, null, states);
        }

        private static void deleteByStates(
                SqlDatabase database,
                Connection connection,
                ObjectIndex onlyIndex,
                List<State> states)
                throws SQLException {

            if (states == null || states.isEmpty()) {
                return;
            }

            Set<ObjectStruct> structs = new HashSet<ObjectStruct>();
            List<ObjectIndex> customIndexes = new ArrayList<ObjectIndex>();
            SqlVendor vendor = database.getVendor();
            StringBuilder idsBuilder = new StringBuilder(" IN (");

            structs.add(database.getEnvironment());

            for (State state : states) {
                ObjectType type = state.getType();
                if (type != null) {
                    structs.add(type);
                }

                vendor.appendUuid(idsBuilder, state.getId());
                idsBuilder.append(",");
            }

            idsBuilder.setCharAt(idsBuilder.length() - 1, ')');

            for (ObjectStruct struct : structs) {
                for (ObjectIndex index : struct.getIndexes()) {
                    ObjectField field = index.getParent().getField(index.getFields().get(0));
                    if (field != null
                            && (index.getFields().size() > 1
                            || field.as(SqlDatabase.FieldData.class).getIndexTable() != null)) {
                        customIndexes.add(index);
                    }
                }
            }

            for (SqlIndex sqlIndex : SqlIndex.values()) {
                if (sqlIndex != SqlIndex.CUSTOM) {
                    for (Table table : sqlIndex.getWriteTables(database, null)) {
                        StringBuilder deleteBuilder = new StringBuilder();
                        deleteBuilder.append("DELETE FROM ");
                        vendor.appendIdentifier(deleteBuilder, table.getName(database, onlyIndex));
                        deleteBuilder.append(" WHERE ");
                        vendor.appendIdentifier(deleteBuilder, table.getIdField(database, onlyIndex));
                        deleteBuilder.append(idsBuilder);
                        if (onlyIndex != null && table.getKeyField(database, onlyIndex) != null) {
                            deleteBuilder.append(" AND ");
                            vendor.appendIdentifier(deleteBuilder, table.getKeyField(database, onlyIndex));
                            deleteBuilder.append(" = ");
                            deleteBuilder.append(database.getSymbolId(onlyIndex.getUniqueName()));
                        }
                        SqlDatabase.Static.executeUpdateWithArray(connection, deleteBuilder.toString());
                    }
                }
            }

            for (ObjectIndex index : customIndexes) {
                if (onlyIndex != null && !onlyIndex.equals(index)) {
                    continue;
                }
                for (Table table : CUSTOM.getWriteTables(database, index)) {
                    StringBuilder deleteBuilder = new StringBuilder();
                    deleteBuilder.append("DELETE FROM ");
                    vendor.appendIdentifier(deleteBuilder, table.getName(database, index));
                    deleteBuilder.append(" WHERE ");
                    vendor.appendIdentifier(deleteBuilder, table.getIdField(database, index));
                    deleteBuilder.append(idsBuilder);
                    if (onlyIndex != null && table.getKeyField(database, onlyIndex) != null) {
                        deleteBuilder.append(" AND ");
                        vendor.appendIdentifier(deleteBuilder, table.getKeyField(database, onlyIndex));
                        deleteBuilder.append(" = ");
                        deleteBuilder.append(database.getSymbolId(onlyIndex.getUniqueName()));
                    }
                    SqlDatabase.Static.executeUpdateWithArray(connection, deleteBuilder.toString());
                }
            }
        }

        public static void updateByStates(
                SqlDatabase database,
                Connection connection,
                ObjectIndex index,
                List<State> states)
                throws SQLException {

            Map<String, String> updateQueries = new HashMap<String, String>();
            Map<String, List<List<Object>>> updateParameters = new HashMap<String, List<List<Object>>>();
            Map<String, Set<String>> updateBindKeys = new HashMap<String, Set<String>>();
            Map<String, List<State>> updateStates = new HashMap<String, List<State>>();
            Set<State> needDeletes = new HashSet<State>();
            Set<State> needInserts = new HashSet<State>();

            for (State state : states) {
                UUID id = state.getId();
                UUID typeId = state.getVisibilityAwareTypeId();

                List<IndexValue> indexValues = new ArrayList<IndexValue>();
                Map<String, Object> stateValues = state.getValues();
                collectIndexValues(state, indexValues, null, state.getDatabase().getEnvironment(), stateValues, index);
                ObjectType type = state.getType();
                if (type != null) {
                    ObjectField field = type.getField(index.getField());
                    if (field != null && field.isInternalCollectionType()) {
                        needInserts.add(state);
                        continue;
                    }
                    collectIndexValues(state, indexValues, null, type, stateValues, index);
                }

                for (IndexValue indexValue : indexValues) {
                    for (SqlIndex.Table table : getByIndex(index).getWriteTables(database, index)) {

                        String name = table.getName(database, index);
                        String sqlQuery = updateQueries.get(name);
                        List<List<Object>> parameters = updateParameters.get(name);
                        Set<String> bindKeys = updateBindKeys.get(name);
                        List<State> tableStates = updateStates.get(name);
                        if (sqlQuery == null && parameters == null && tableStates == null) {
                            if (table instanceof AbstractTable) {
                                sqlQuery = ((AbstractTable) table).prepareUpdateStatement(database, connection, index);
                            } else {
                                throw new IllegalStateException("Table " + table.getName(database, index) + " does not support updates.");
                            }
                            updateQueries.put(name, sqlQuery);

                            parameters = new ArrayList<List<Object>>();
                            updateParameters.put(name, parameters);

                            bindKeys = new HashSet<String>();
                            updateBindKeys.put(name, bindKeys);

                            tableStates = new ArrayList<State>();
                            updateStates.put(name, tableStates);
                        }

                        if (table instanceof AbstractTable) {
                            ((AbstractTable) table).bindUpdateValues(database, index, id, typeId, indexValue, bindKeys, parameters);
                            tableStates.add(state);
                        } else {
                            throw new IllegalStateException("Table " + table.getName(database, index) + " does not support updates.");
                        }
                    }
                }
                if (indexValues.isEmpty()) {
                    needDeletes.add(state);
                }

            }

            for (Map.Entry<String, String> entry : updateQueries.entrySet()) {
                String name = entry.getKey();
                String sqlQuery = entry.getValue();
                List<List<Object>> parameters = updateParameters.get(name);
                List<State> tableStates = updateStates.get(name);
                try {
                    if (!parameters.isEmpty()) {
                        int[] rows = SqlDatabase.Static.executeBatchUpdate(connection, sqlQuery, parameters);
                        for (int i = 0; i < rows.length; i++) {
                            if (rows[i] == 0) {
                                needInserts.add(tableStates.get(i));
                            }
                        }
                    }
                } catch (BatchUpdateException bue) {
                    SqlDatabase.Static.logBatchUpdateException(bue, sqlQuery, parameters);
                    throw bue;
                }
            }
            if (!needDeletes.isEmpty()) {
                deleteByStates(database, connection, index, new ArrayList<State>(needDeletes));
            }
            if (!needInserts.isEmpty()) {
                List<State> insertStates = new ArrayList<State>(needInserts);
                deleteByStates(database, connection, index, insertStates);
                insertByStates(database, connection, index, insertStates);
            }
        }

        /**
         * Inserts all index rows associated with the given {@code states}.
         */
        public static Map<State, String> insertByStates(
                SqlDatabase database,
                Connection connection,
                List<State> states)
                throws SQLException {
            return insertByStates(database, connection, null, states);
        }

        private static Map<State, String> insertByStates(
                SqlDatabase database,
                Connection connection,
                ObjectIndex onlyIndex,
                List<State> states)
                throws SQLException {

            Map<State, String> inRowIndexes = new HashMap<State, String>();
            if (states == null || states.isEmpty()) {
                return inRowIndexes;
            }

            Map<String, String> insertQueries = new HashMap<String, String>();
            Map<String, List<List<Object>>> insertParameters = new HashMap<String, List<List<Object>>>();
            Map<String, Set<String>> insertBindKeys = new HashMap<String, Set<String>>();

            for (State state : states) {
                UUID id = state.getId();
                UUID typeId = state.getVisibilityAwareTypeId();

                for (IndexValue indexValue : getIndexValues(state)) {
                    ObjectIndex index = indexValue.getIndex();
                    if (onlyIndex != null && !onlyIndex.equals(index)) {
                        continue;
                    }

                    if (database.hasInRowIndex() && index.isShortConstant()) {
                        StringBuilder inRowIndex = new StringBuilder();
                        String current = inRowIndexes.get(state);

                        if (current != null) {
                            inRowIndex.append(current);

                        } else {
                            inRowIndex.append(';');
                        }

                        int nameId = database.getSymbolId(index.getUniqueName());
                        for (Object[] values : indexValue.getValuesArray()) {
                            StringBuilder tokenBuilder = new StringBuilder();
                            tokenBuilder.append(nameId);
                            tokenBuilder.append("=");
                            tokenBuilder.append(database.getSymbolId(values[0].toString()));
                            tokenBuilder.append(";");
                            String token = tokenBuilder.toString();
                            if (inRowIndex.indexOf(";" + token) < 0) {
                                inRowIndex.append(token);
                            }
                        }

                        inRowIndexes.put(state, inRowIndex.toString());
                        continue;
                    }

                    for (SqlIndex.Table table : getByIndex(index).getWriteTables(database, index)) {
                        String name = table.getName(database, index);
                        String sqlQuery = insertQueries.get(name);
                        List<List<Object>> parameters = insertParameters.get(name);
                        Set<String> bindKeys = insertBindKeys.get(name);
                        if (sqlQuery == null && parameters == null) {
                            sqlQuery = table.prepareInsertStatement(database, connection, index);
                            insertQueries.put(name, sqlQuery);

                            parameters = new ArrayList<List<Object>>();
                            insertParameters.put(name, parameters);

                            bindKeys = new HashSet<String>();
                            insertBindKeys.put(name, bindKeys);
                        }

                        table.bindInsertValues(database, index, id, typeId, indexValue, bindKeys, parameters);
                    }
                }
            }

            for (Map.Entry<String, String> entry : insertQueries.entrySet()) {
                String name = entry.getKey();
                String sqlQuery = entry.getValue();
                List<List<Object>> parameters = insertParameters.get(name);
                try {
                    if (!parameters.isEmpty()) {
                        SqlDatabase.Static.executeBatchUpdate(connection, sqlQuery, parameters);
                    }
                } catch (BatchUpdateException bue) {
                    SqlDatabase.Static.logBatchUpdateException(bue, sqlQuery, parameters);
                    throw bue;
                }
            }

            return inRowIndexes;
        }

        /**
         * Returns a list of indexable values in this state. This is a helper
         * method for database implementations and isn't meant for general
         * consumption.
         */
        public static List<IndexValue> getIndexValues(State state) {
            List<IndexValue> indexValues = new ArrayList<IndexValue>();
            Map<String, Object> values = state.getValues();

            collectIndexValues(state, indexValues, null, state.getDatabase().getEnvironment(), values);

            ObjectType type = state.getType();
            if (type != null) {
                collectIndexValues(state, indexValues, null, type, values);
            }

            return indexValues;
        }

        private static void collectIndexValues(
                State state,
                List<IndexValue> indexValues,
                ObjectField[] prefixes,
                ObjectStruct struct,
                Map<String, Object> stateValues,
                ObjectIndex index) {

            List<Set<Object>> valuesList = new ArrayList<Set<Object>>();

            for (String fieldName : index.getFields()) {
                ObjectField field = struct.getField(fieldName);
                if (field == null) {
                    return;
                }

                Set<Object> values = new HashSet<Object>();
                Object fieldValue;
                if (field instanceof ObjectMethod) {
                    StringBuilder path = new StringBuilder();
                    if (prefixes != null) {
                        for (ObjectField fieldPrefix : prefixes) {
                            path.append(fieldPrefix.getInternalName());
                            path.append("/");
                        }
                    }
                    path.append(field.getInternalName());
                    fieldValue = state.getByPath(path.toString());
                } else {
                    fieldValue = stateValues.get(field.getInternalName());
                }

                collectFieldValues(state, indexValues, prefixes, struct, field, values, fieldValue);
                if (values.isEmpty()) {
                    return;
                }

                valuesList.add(values);
            }

            int valuesListSize = valuesList.size();
            int permutationSize = 1;
            for (Set<Object> values : valuesList) {
                permutationSize *= values.size();
            }

            // Calculate all permutations on multi-field indexes.
            Object[][] permutations = new Object[permutationSize][valuesListSize];
            int partitionSize = permutationSize;
            for (int i = 0; i < valuesListSize; ++ i) {
                Set<Object> values = valuesList.get(i);
                int valuesSize = values.size();
                partitionSize /= valuesSize;
                for (int p = 0; p < permutationSize;) {
                    for (Object value : values) {
                        for (int k = 0; k < partitionSize; ++ k, ++ p) {
                            permutations[p][i] = value;
                        }
                    }
                }
            }

            indexValues.add(new IndexValue(prefixes, index, permutations));

        }

        private static void collectIndexValues(
                State state,
                List<IndexValue> indexValues,
                ObjectField[] prefixes,
                ObjectStruct struct,
                Map<String, Object> stateValues) {

            for (ObjectIndex index : struct.getIndexes()) {
                collectIndexValues(state, indexValues, prefixes, struct, stateValues, index);
            }
        }

        private static void collectFieldValues(
                State state,
                List<IndexValue> indexValues,
                ObjectField[] prefixes,
                ObjectStruct struct,
                ObjectField field,
                Set<Object> values,
                Object value) {

            if (value == null) {
                return;
            }

            Iterable<Object> valueIterable = ObjectToIterable.iterable(value);
            if (valueIterable != null) {
                for (Object item : valueIterable) {
                    collectFieldValues(state, indexValues, prefixes, struct, field, values, item);
                }

            } else if (value instanceof Map) {
                for (Object item : ((Map<?, ?>) value).values()) {
                    collectFieldValues(state, indexValues, prefixes, struct, field, values, item);
                }

            } else if (value instanceof Recordable) {
                State valueState = ((Recordable) value).getState();

                if (ObjectField.RECORD_TYPE.equals(field.getInternalItemType())) {
                    ObjectType valueType = valueState.getType();

                    if (field.isEmbedded()
                            || (valueType != null && valueType.isEmbedded())) {
                        int last;
                        ObjectField[] newPrefixes;

                        if (prefixes != null) {
                            last = prefixes.length;
                            newPrefixes = new ObjectField[last + 1];
                            System.arraycopy(prefixes, 0, newPrefixes, 0, last);

                        } else {
                            newPrefixes = new ObjectField[1];
                            last = 0;
                        }

                        newPrefixes[last] = field;
                        collectIndexValues(state, indexValues, newPrefixes, state.getDatabase().getEnvironment(), valueState.getValues());
                        collectIndexValues(state, indexValues, newPrefixes, valueType, valueState.getValues());

                    } else {
                        values.add(valueState.getId());
                    }

                } else {
                    values.add(valueState.getId());
                }

            } else if (value instanceof Character
                    || value instanceof CharSequence
                    || value instanceof URI
                    || value instanceof URL) {
                values.add(value.toString());

            } else if (value instanceof Date) {
                values.add(((Date) value).getTime());

            } else if (value instanceof Enum) {
                values.add(((Enum<?>) value).name());

            } else if (value instanceof Locale) {
                values.add(LocaleUtils.toLanguageTag((Locale) value));

            } else {
                values.add(value);
            }
        }

        // --- Deprecated ---

        /** Use {@link #getByType} instead. */
        @Deprecated
        public static SqlIndex getInstance(String type) {
            return getByType(type);
        }

        /** Use {@link #getByIndex} instead. */
        @Deprecated
        public static SqlIndex getInstance(ObjectIndex index) {
            return getByIndex(index);
        }
    }
}
