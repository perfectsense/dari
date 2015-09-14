package com.psddev.dari.db;

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

import com.psddev.dari.util.CompactMap;
import com.psddev.dari.util.ObjectToIterable;
import com.psddev.dari.util.ObjectUtils;
import com.psddev.dari.util.TypeReference;

/** Description of how field values in a state can be queried. */
public class ObjectIndex {

    private static final TypeReference<List<String>> LIST_STRING_TYPE_REF = new TypeReference<List<String>>() { };

    private static final String FIELDS_KEY = "fields";
    private static final String TYPE_KEY = "type";
    private static final String IS_UNIQUE_KEY = "isUnique";
    private static final String CASE_SENSITIVE_KEY = "caseSensitive";
    private static final String VISIBILITY_KEY = "visibility";
    private static final String JAVA_DECLARING_CLASS_NAME_KEY = "java.declaringClass";
    private static final String LEGACY_FIELD_KEY = "field";

    private final ObjectStruct parent;
    private String name;

    private List<String> fields;
    private String type;
    private boolean isUnique;
    private boolean caseSensitive;
    private boolean visibility;
    private String javaDeclaringClassName;
    private Map<String, Object> options;

    /**
     * Creates an instance that's contained in the given {@code parent}
     * using the given index {@code definition} map.
     */
    public ObjectIndex(ObjectStruct parent, Map<String, Object> definition) {
        if (parent == null) {
            throw new IllegalArgumentException("Parent is required!");
        }

        this.parent = parent;
        if (definition == null) {
            return;
        }

        definition = new CompactMap<String, Object>(definition);

        List<String> fields = ObjectUtils.to(LIST_STRING_TYPE_REF, definition.remove(FIELDS_KEY));
        if (fields == null) {
            String field = ObjectUtils.to(String.class, definition.remove(LEGACY_FIELD_KEY));
            if (field != null) {
                fields = new ArrayList<String>();
                fields.add(field);
            }
        }

        setFields(fields);
        setType(ObjectUtils.to(String.class, definition.remove(TYPE_KEY)));
        setUnique(ObjectUtils.to(boolean.class, definition.remove(IS_UNIQUE_KEY)));
        setCaseSensitive(ObjectUtils.to(boolean.class, definition.remove(CASE_SENSITIVE_KEY)));
        setVisibility(ObjectUtils.to(boolean.class, definition.remove(VISIBILITY_KEY)));
        setJavaDeclaringClassName(ObjectUtils.to(String.class, definition.remove(JAVA_DECLARING_CLASS_NAME_KEY)));
        setOptions(definition);
    }

    /** Converts this index to a definition map. */
    public Map<String, Object> toDefinition() {
        Map<String, Object> definition = new CompactMap<String, Object>();
        definition.putAll(getOptions());
        definition.put(FIELDS_KEY, getFields());
        definition.put(TYPE_KEY, getType());
        definition.put(IS_UNIQUE_KEY, isUnique());
        definition.put(CASE_SENSITIVE_KEY, isCaseSensitive());
        definition.put(VISIBILITY_KEY, isVisibility());
        definition.put(JAVA_DECLARING_CLASS_NAME_KEY, getJavaDeclaringClassName());
        definition.put(LEGACY_FIELD_KEY, getField());
        return definition;
    }

    public ObjectStruct getParent() {
        return parent;
    }

    public String getName() {
        return name != null ? name : getField();
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<String> getFields() {
        if (fields == null) {
            fields = new ArrayList<String>();
        }
        return fields;
    }

    public void setFields(List<String> fields) {
        this.fields = fields;
    }

    public String getField() {
        for (String field : getFields()) {
            return field;
        }
        return null;
    }

    public void setField(String field) {
        List<String> fields = new ArrayList<String>();
        if (field != null) {
            fields.add(field);
        }
        setFields(fields);
    }

    public boolean isShortConstant() {
        if (!isUnique()) {
            List<String> fields = getFields();

            if (fields.size() == 1) {
                ObjectField field = getParent().getField(fields.get(0));
                return field != null
                        && (ObjectField.BOOLEAN_TYPE.equals(field.getInternalItemType())
                        || !ObjectUtils.isBlank(field.getValues()));
            }
        }

        return false;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public boolean isUnique() {
        return isUnique;
    }

    public void setUnique(boolean isUnique) {
        this.isUnique = isUnique;
    }

    public boolean isCaseSensitive() {
        return caseSensitive;
    }

    public void setCaseSensitive(boolean caseSensitive) {
        this.caseSensitive = caseSensitive;
    }

    public boolean isVisibility() {
        return visibility;
    }

    public void setVisibility(boolean visibility) {
        this.visibility = visibility;
    }

    /** Returns the name of the class that declared this index. */
    public String getJavaDeclaringClassName() {
        return javaDeclaringClassName;
    }

    /** Sets the name of the class that declared this index. */
    public void setJavaDeclaringClassName(String className) {
        this.javaDeclaringClassName = className;
    }

    public Map<String, Object> getOptions() {
        if (options == null) {
            options = new CompactMap<String, Object>();
        }
        return options;
    }

    public void setOptions(Map<String, Object> options) {
        this.options = options;
    }

    public String getPrefix() {
        ObjectStruct parent = getParent();

        if (parent instanceof ObjectType) {
            String prefix = getJavaDeclaringClassName();

            if (ObjectUtils.isBlank(prefix)) {
                ObjectType type = (ObjectType) parent;
                prefix = type.getObjectClassName();

                if (ObjectUtils.isBlank(prefix)) {
                    prefix = type.getId().toString();
                }
            }

            return prefix + "/";

        } else {
            return "";
        }
    }

    public String getUniqueName() {
        return getPrefix() + getName();
    }

    public String getSymbol() {
        StringBuilder nameBuilder = new StringBuilder();
        Iterator<String> indexFieldsIterator = getFields().iterator();

        nameBuilder.append(getPrefix());
        nameBuilder.append(indexFieldsIterator.next());

        while (indexFieldsIterator.hasNext()) {
            nameBuilder.append(',');
            nameBuilder.append(indexFieldsIterator.next());
        }

        return nameBuilder.toString();
    }

    public Object getValue(State state) {
        return getValue(state, getField());
    }

    private Object getValue(State state, String field) {
        int index = field.indexOf('/');
        if (index != -1) {
            Object value = state.getByPath(field.substring(0, index));

            if (value instanceof Iterable || value instanceof Map) {
                Iterable<?> iterable;
                if (value instanceof Iterable) {
                    iterable = (Iterable<?>) value;
                } else {
                    iterable = ((Map<?, ?>) value).values();
                }

                List<Object> values = new ArrayList<Object>();
                for (Object object : iterable) {
                    if (object instanceof Recordable) {
                        value = getValue(((Recordable) object).getState(),
                                field.substring(index + 1));
                        if (value != null) {
                            values.add(value);
                        }
                    }
                }
                return values;

            } else if (value instanceof Recordable) {
                return getValue(((Recordable) value).getState(),
                        field.substring(index + 1));
            } else {
                return null;
            }
        } else {
            return state.getByPath(field);
        }
    }

    public Object[][] getValuePermutations(State state) {
        List<String> fields = getFields();
        int fieldsSize = fields.size();
        Set<?>[] valuesArray = new Set<?>[fieldsSize];
        int permutationSize = 1;

        for (int i = 0; i < fieldsSize; ++ i) {
            String fieldName = fields.get(i);
            ObjectField field = getParent().getField(fieldName);
            Object value = state.getByPath(fieldName);

            if (!ObjectUtils.isBlank(value)) {
                Set<Object> values = new HashSet<Object>();
                collectValues(values, field, value);
                if (!values.isEmpty()) {
                    valuesArray[i] = values;
                    permutationSize *= values.size();
                    continue;
                }
            }

            return null;
        }

        Object[][] permutations = new Object[permutationSize][fieldsSize];
        int partitionSize = permutationSize;

        for (int i = 0; i < fieldsSize; ++ i) {
            Set<?> values = valuesArray[i];
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

        return permutations;
    }

    private static void collectValues(Set<Object> values, ObjectField field, Object value) {
        if (value == null) {
            return;
        }

        Iterable<Object> valueIterable = ObjectToIterable.iterable(value);
        if (valueIterable != null) {
            for (Object item : valueIterable) {
                collectValues(values, field, item);
            }

        } else if (value instanceof Map) {
            for (Object item : ((Map<?, ?>) value).values()) {
                collectValues(values, field, item);
            }

        } else if (value instanceof Recordable) {
            State valueState = ((Recordable) value).getState();
            ObjectType valueType = valueState.getType();
            if (!((valueType != null
                    && valueType.isEmbedded())
                    || (field != null
                    && ObjectField.RECORD_TYPE.equals(field.getInternalItemType())
                    && field.isEmbedded()))) {
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

    /**
     * Validates the given {@code state} against the constraints defined
     * in this index.
     *
     * @return {@code true} if there aren't any errors.
     */
    public boolean validate(State state) {
        if (isUnique()) {
            Object value = getValue(state);
            if (!ObjectUtils.isBlank(value)) {

                Object duplicate = Query
                        .from(Object.class)
                        .where("id != ?", state.getId())
                        .and(getUniqueName() + " = ?", value)
                        .using(state.getDatabase())
                        .referenceOnly()
                        .first();

                if (duplicate != null) {
                    state.addError(state.getField(getField()), "Must be unique!");
                    return false;
                }
            }
        }

        return true;
    }

    /**
     * Creates a query that can return all objects that reference this
     * index.
     */
    public Query<Object> createItemsQuery() {
        Query<Object> query;

        String declaringClass = getJavaDeclaringClassName();
        if (ObjectUtils.isBlank(declaringClass)) {
            query = Query.fromGroup(declaringClass);

        } else {
            ObjectStruct parent = getParent();
            if (parent instanceof ObjectType) {
                query = Query.fromType((ObjectType) parent);

            } else {
                query = Query.fromAll();
            }
        }

        query.resolveToReferenceOnly();
        query.fields(getField());

        return query;
    }

    // --- Object support ---

    @Override
    public boolean equals(Object other) {

        if (this == other) {
            return true;

        } else if (other instanceof ObjectIndex) {
            ObjectIndex otherIndex = (ObjectIndex) other;
            return ObjectUtils.equals(getParent(), otherIndex.getParent())
                    && getFields().equals(otherIndex.getFields());

        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return ObjectUtils.hashCode(getParent(), getFields());
    }

    @Override
    public String toString() {
        return toDefinition().toString();
    }

    /** {@link ObjectIndex} utility methods. */
    public static final class Static {

        /**
         * Converts the given index {@code definitions} into a map of
         * instances.
         */
        public static Map<String, ObjectIndex> convertDefinitionsToInstances(
                ObjectStruct parent,
                List<Map<String, Object>> definitions) {

            Map<String, ObjectIndex> instances = new CompactMap<String, ObjectIndex>();
            if (definitions != null) {
                for (Map<String, Object> definition : definitions) {
                    ObjectIndex instance = new ObjectIndex(parent, definition);
                    instances.put(instance.getField(), instance);
                }
            }
            return instances;
        }

        /**
         * Converts all given index {@code instances} into a list of
         * definitions.
         */
        public static List<Map<String, Object>> convertInstancesToDefinitions(List<ObjectIndex> instances) {
            List<Map<String, Object>> definitions = new ArrayList<Map<String, Object>>();
            if (instances != null) {
                for (ObjectIndex instance : instances) {
                    definitions.add(instance.toDefinition());
                }
            }
            return definitions;
        }
    }
}
