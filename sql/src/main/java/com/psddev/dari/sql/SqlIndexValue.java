package com.psddev.dari.sql;

import com.psddev.dari.db.ObjectField;
import com.psddev.dari.db.ObjectIndex;
import com.psddev.dari.db.ObjectMethod;
import com.psddev.dari.db.ObjectStruct;
import com.psddev.dari.db.ObjectType;
import com.psddev.dari.db.Recordable;
import com.psddev.dari.db.State;
import com.psddev.dari.util.ObjectToIterable;

import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

class SqlIndexValue {

    private final ObjectField[] prefixes;
    private final ObjectIndex index;
    private final Object[][] valuesArray;

    /**
     * Returns a list of indexable values in this state. This is a helper
     * method for database implementations and isn't meant for general
     * consumption.
     */
    public static List<SqlIndexValue> find(State state) {
        List<SqlIndexValue> indexValues = new ArrayList<>();
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
            List<SqlIndexValue> indexValues,
            ObjectField[] prefixes,
            ObjectStruct struct,
            Map<String, Object> stateValues,
            ObjectIndex index) {

        List<Set<Object>> valuesList = new ArrayList<>();

        for (String fieldName : index.getFields()) {
            ObjectField field = struct.getField(fieldName);
            if (field == null) {
                return;
            }

            Set<Object> values = new HashSet<>();
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

        indexValues.add(new SqlIndexValue(prefixes, index, permutations));

    }

    private static void collectIndexValues(
            State state,
            List<SqlIndexValue> indexValues,
            ObjectField[] prefixes,
            ObjectStruct struct,
            Map<String, Object> stateValues) {

        for (ObjectIndex index : struct.getIndexes()) {
            collectIndexValues(state, indexValues, prefixes, struct, stateValues, index);
        }
    }

    private static void collectFieldValues(
            State state,
            List<SqlIndexValue> indexValues,
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
            values.add(((Locale) value).toLanguageTag());

        } else {
            values.add(value);
        }
    }

    public SqlIndexValue(ObjectField[] prefixes, ObjectIndex index, Object[][] valuesArray) {
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
            for (int i = 1, length = prefixes.length; i < length; ++i) {
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
