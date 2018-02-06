package com.psddev.dari.db;

import java.lang.annotation.Annotation;
import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Field;
import java.lang.reflect.GenericDeclaration;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.lang.reflect.WildcardType;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.google.common.base.Optional;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.psddev.dari.util.CollectionUtils;
import com.psddev.dari.util.CompactMap;
import com.psddev.dari.util.ObjectUtils;
import com.psddev.dari.util.StorageItem;
import com.psddev.dari.util.StringUtils;
import com.psddev.dari.util.TypeDefinition;
import com.psddev.dari.util.TypeReference;

/** Description of how field values can be stored in a state. */
@ObjectField.Embedded
public class ObjectField extends Record {

    public static final String ANY_TYPE = "any";
    public static final String BOOLEAN_TYPE = "boolean";
    public static final String DATE_TYPE = "date";
    public static final String FILE_TYPE = "file";
    public static final String LIST_TYPE = "list";
    public static final String LOCATION_TYPE = "location";
    public static final String MAP_TYPE = "map";
    public static final String METRIC_TYPE = "metric";
    public static final String NUMBER_TYPE = "number";
    public static final String RECORD_TYPE = "record";
    public static final String REFERENTIAL_TEXT_TYPE = "referentialText";
    public static final String REGION_TYPE = "region";
    public static final String SET_TYPE = "set";
    public static final String TEXT_TYPE = "text";
    public static final String URI_TYPE = "uri";
    public static final String URL_TYPE = "url";
    public static final String UUID_TYPE = "uuid";
    public static final String LOCALE_TYPE = "locale";

    private static final TypeReference<Set<String>> SET_STRING_TYPE_REF = new TypeReference<Set<String>>() { };

    private static final Map<Class<?>, String> COLLECTION_CLASS_TO_TYPE = new HashMap<Class<?>, String>();
    private static final Map<Class<?>, String> CLASS_TO_TYPE = new HashMap<Class<?>, String>();
    private static final Map<String, Set<Class<?>>> TYPE_TO_CLASS = new HashMap<String, Set<Class<?>>>();
    static {

        COLLECTION_CLASS_TO_TYPE.put(List.class, LIST_TYPE);
        COLLECTION_CLASS_TO_TYPE.put(Map.class, MAP_TYPE);
        COLLECTION_CLASS_TO_TYPE.put(Set.class, SET_TYPE);

        CLASS_TO_TYPE.putAll(COLLECTION_CLASS_TO_TYPE);

        CLASS_TO_TYPE.put(Boolean.class, BOOLEAN_TYPE);
        CLASS_TO_TYPE.put(boolean.class, BOOLEAN_TYPE);

        CLASS_TO_TYPE.put(Number.class, NUMBER_TYPE);
        CLASS_TO_TYPE.put(Byte.class, NUMBER_TYPE);
        CLASS_TO_TYPE.put(byte.class, NUMBER_TYPE);
        CLASS_TO_TYPE.put(Double.class, NUMBER_TYPE);
        CLASS_TO_TYPE.put(double.class, NUMBER_TYPE);
        CLASS_TO_TYPE.put(Float.class, NUMBER_TYPE);
        CLASS_TO_TYPE.put(float.class, NUMBER_TYPE);
        CLASS_TO_TYPE.put(Integer.class, NUMBER_TYPE);
        CLASS_TO_TYPE.put(int.class, NUMBER_TYPE);
        CLASS_TO_TYPE.put(Long.class, NUMBER_TYPE);
        CLASS_TO_TYPE.put(long.class, NUMBER_TYPE);
        CLASS_TO_TYPE.put(Short.class, NUMBER_TYPE);
        CLASS_TO_TYPE.put(short.class, NUMBER_TYPE);

        CLASS_TO_TYPE.put(Date.class, DATE_TYPE);
        CLASS_TO_TYPE.put(StorageItem.class, FILE_TYPE);
        CLASS_TO_TYPE.put(Location.class, LOCATION_TYPE);
        CLASS_TO_TYPE.put(Metric.class, METRIC_TYPE);
        CLASS_TO_TYPE.put(Recordable.class, RECORD_TYPE);
        CLASS_TO_TYPE.put(Region.class, REGION_TYPE);
        CLASS_TO_TYPE.put(ReferentialText.class, REFERENTIAL_TEXT_TYPE);
        CLASS_TO_TYPE.put(String.class, TEXT_TYPE);
        CLASS_TO_TYPE.put(URI.class, URI_TYPE);
        CLASS_TO_TYPE.put(URL.class, URL_TYPE);
        CLASS_TO_TYPE.put(UUID.class, UUID_TYPE);
        CLASS_TO_TYPE.put(Locale.class, LOCALE_TYPE);
        CLASS_TO_TYPE.put(Object.class, ANY_TYPE);

        for (Map.Entry<Class<?>, String> e : CLASS_TO_TYPE.entrySet()) {
            Class<?> type = e.getKey();
            String name = e.getValue();
            Set<Class<?>> types = TYPE_TO_CLASS.get(name);
            if (types == null) {
                types = new HashSet<Class<?>>();
                TYPE_TO_CLASS.put(name, types);
            }
            types.add(type);
        }
    }

    private static final String COLLECTION_MAXIMUM_KEY = "collectionMaximum";
    private static final String COLLECTION_MINIMUM_KEY = "collectionMinimum";
    private static final String DEPRECATED_KEY = "deprecated";
    private static final String DISPLAY_NAME_KEY = "label";
    private static final String INTERNAL_NAME_KEY = "name";
    private static final String INTERNAL_TYPE_KEY = "type";
    private static final String IS_DENORMALIZED_KEY = "isDenormalized";
    private static final String DENORMALIZED_FIELDS_KEY = "denormalizedFields";
    private static final String IS_EMBEDDED_KEY = "isEmbedded";
    private static final String IS_REQUIRED_KEY = "isRequired";
    private static final String JUNCTION_FIELD_KEY = "junctionField";
    private static final String JUNCTION_POSITION_FIELD_KEY = "junctionPositionField";
    private static final String MINIMUM_KEY = "minimum";
    private static final String STEP_KEY = "step";
    private static final String MAXIMUM_KEY = "maximum";
    private static final String PATTERN_KEY = "pattern";
    private static final String DEFAULT_VALUE_KEY = "defaultValue";
    private static final String PREDICATE_KEY = "predicate";
    private static final String RAW_KEY = "raw";
    private static final String VALUES_KEY = "values";
    private static final String GROUPS_KEY = "groups";
    private static final String VALUE_TYPES_KEY = "valueTypes";
    private static final String GENERIC_ARGUMENT_INDEX_KEY = "genericArgumentIndex";
    private static final String GENERIC_ARGUMENTS_KEY = "genericArguments";
    private static final String JAVA_FIELD_NAME_KEY = "java.field";
    private static final String JAVA_DECLARING_CLASS_NAME_KEY = "java.declaringClass";
    private static final String JAVA_ENUM_CLASS_NAME_KEY = "java.enumClass";
    private static final String IS_JAVA_FIELD_TYPE_PRIMITIVE_KEY = "isJavaFieldTypePrimitive";

    private final transient ObjectStruct parent;

    private Number collectionMinimum;
    private Number collectionMaximum;
    private boolean deprecated;

    @InternalName("label")
    protected String displayName;

    @InternalName("name")
    private String internalName;

    @InternalName("type")
    private String internalType;

    private boolean isDenormalized;
    private Set<String> denormalizedFields;
    private boolean isEmbedded;
    private boolean isRequired;
    private String junctionField;
    private String junctionPositionField;
    private Number minimum;
    private Number step;
    private Number maximum;
    private String pattern;
    private Object defaultValue;
    private String predicate;
    private boolean raw;
    private Set<Value> values;

    private Set<String> groups;

    @InternalName("valueTypes")
    private Set<ObjectType> types;

    private Number genericArgumentIndex;
    private List<ObjectType> genericArguments;

    @InternalName("java.field")
    private String javaFieldName;

    @InternalName("java.declaringClass")
    private String javaDeclaringClassName;

    @InternalName("java.enumClass")
    private String javaEnumClassName;

    @InternalName("java.isFieldTypePrimitive")
    private boolean isJavaFieldTypePrimitive;

    private transient Map<String, Object> options;

    public ObjectField(ObjectField field) {
        parent = field.parent;
        collectionMaximum = field.collectionMaximum;
        collectionMinimum = field.collectionMinimum;
        deprecated = field.deprecated;
        displayName = field.displayName;
        internalName = field.internalName;
        internalType = field.internalType;
        isDenormalized = field.isDenormalized;
        isEmbedded = field.isEmbedded;
        isJavaFieldTypePrimitive = field.isJavaFieldTypePrimitive;
        isRequired = field.isRequired;
        junctionField = field.junctionField;
        junctionPositionField = field.junctionPositionField;
        minimum = field.minimum;
        step = field.step;
        maximum = field.maximum;
        pattern = field.pattern;
        defaultValue = field.defaultValue;
        predicate = field.predicate;
        raw = field.raw;
        types = field.types != null ? new LinkedHashSet<ObjectType>(field.types) : null;
        genericArgumentIndex = field.genericArgumentIndex;
        genericArguments = field.genericArguments != null ? new ArrayList<ObjectType>(field.genericArguments) : null;
        values = field.values != null ? new LinkedHashSet<Value>(field.values) : null;
        javaFieldName = field.javaFieldName;
        javaDeclaringClassName = field.javaDeclaringClassName;
        javaEnumClassName = field.javaEnumClassName;
        options = field.options != null ? new CompactMap<String, Object>(field.options) : null;
    }

    /**
     * Creates an instance that's contained in the given {@code parent}
     * using the given field {@code definition} map.
     */
    public ObjectField(ObjectStruct parent, Map<String, Object> definition) {
        if (parent == null) {
            throw new IllegalArgumentException("Parent is required!");
        }

        this.parent = parent;
        if (definition == null) {
            return;
        }

        DatabaseEnvironment environment = parent.getEnvironment();
        definition = new CompactMap<String, Object>(definition);
        definition.putAll(getState().getRawValues());
        getState().getRawValues().putAll(definition);

        collectionMaximum = (Number) definition.remove(COLLECTION_MAXIMUM_KEY);
        collectionMinimum = (Number) definition.remove(COLLECTION_MINIMUM_KEY);
        deprecated = Boolean.TRUE.equals(definition.remove(DEPRECATED_KEY));
        displayName = (String) definition.remove(DISPLAY_NAME_KEY);
        internalName = (String) definition.remove(INTERNAL_NAME_KEY);
        internalType = (String) definition.remove(INTERNAL_TYPE_KEY);
        isDenormalized = Boolean.TRUE.equals(definition.remove(IS_DENORMALIZED_KEY));
        denormalizedFields = ObjectUtils.to(SET_STRING_TYPE_REF, definition.remove(DENORMALIZED_FIELDS_KEY));
        isEmbedded = Boolean.TRUE.equals(definition.remove(IS_EMBEDDED_KEY));
        isJavaFieldTypePrimitive = Boolean.TRUE.equals(definition.remove(IS_JAVA_FIELD_TYPE_PRIMITIVE_KEY));
        isRequired = Boolean.TRUE.equals(definition.remove(IS_REQUIRED_KEY));
        junctionField = (String) definition.remove(JUNCTION_FIELD_KEY);
        junctionPositionField = (String) definition.remove(JUNCTION_POSITION_FIELD_KEY);
        minimum = (Number) definition.remove(MINIMUM_KEY);
        step = (Number) definition.remove(STEP_KEY);
        maximum = (Number) definition.remove(MAXIMUM_KEY);
        pattern = (String) definition.remove(PATTERN_KEY);
        defaultValue = definition.remove(DEFAULT_VALUE_KEY);
        predicate = (String) definition.remove(PREDICATE_KEY);
        raw = Boolean.TRUE.equals(definition.remove(RAW_KEY));
        groups = ObjectUtils.to(SET_STRING_TYPE_REF, definition.remove(GROUPS_KEY));

        @SuppressWarnings("unchecked")
        Collection<String> typeIds = (Collection<String>) definition.remove(VALUE_TYPES_KEY);
        if (typeIds == null) {
            types = null;
        } else {
            types = new LinkedHashSet<ObjectType>();
            for (String idString : typeIds) {
                ObjectType type = environment.getTypeById(ObjectUtils.to(UUID.class, idString));
                if (type != null) {
                    types.add(type);
                }
            }
        }

        genericArgumentIndex = (Number) definition.remove(GENERIC_ARGUMENT_INDEX_KEY);

        @SuppressWarnings("unchecked")
        Collection<String> genericArgumentIds = (Collection<String>) definition.remove(GENERIC_ARGUMENTS_KEY);

        if (genericArgumentIds == null) {
            genericArguments = null;

        } else {
            genericArguments = new ArrayList<ObjectType>();

            for (String idString : genericArgumentIds) {
                ObjectType type = environment.getTypeById(ObjectUtils.to(UUID.class, idString));

                if (type != null) {
                    genericArguments.add(type);
                }
            }
        }

        @SuppressWarnings("unchecked")
        List<Map<String, String>> valueMaps = (List<Map<String, String>>) definition.remove(VALUES_KEY);
        if (valueMaps == null) {
            values = null;
        } else {
            values = new LinkedHashSet<Value>();
            for (Map<String, String> valueDefinition : valueMaps) {
                values.add(new Value(valueDefinition));
            }
        }

        javaFieldName = (String) definition.remove(JAVA_FIELD_NAME_KEY);
        javaDeclaringClassName = (String) definition.remove(JAVA_DECLARING_CLASS_NAME_KEY);
        javaEnumClassName = (String) definition.remove(JAVA_ENUM_CLASS_NAME_KEY);
        options = definition;
    }

    /** Converts this field to a definition map. */
    public Map<String, Object> toDefinition() {
        Map<String, Object> definition = new CompactMap<String, Object>(200);

        List<String> typeIds = new ArrayList<String>();
        for (ObjectType type : getTypes()) {
            if (type != null) {
                typeIds.add(type.getId().toString());
            }
        }

        List<String> genericArgumentIds = new ArrayList<String>();

        for (ObjectType type : getGenericArguments()) {
            if (type != null) {
                genericArgumentIds.add(type.getId().toString());
            }
        }

        List<Map<String, String>> valueDefinitions = new ArrayList<Map<String, String>>();
        Set<Value> values = getValues();
        if (values != null) {
            for (Value value : values) {
                valueDefinitions.add(value.toDefinition());
            }
        }

        definition.putAll(getOptions());
        definition.putAll(getState());
        definition.put(COLLECTION_MAXIMUM_KEY, collectionMaximum);
        definition.put(COLLECTION_MINIMUM_KEY, collectionMinimum);
        definition.put(DEPRECATED_KEY, deprecated);
        definition.put(DISPLAY_NAME_KEY, displayName);
        definition.put(INTERNAL_NAME_KEY, internalName);
        definition.put(INTERNAL_TYPE_KEY, internalType);
        definition.put(IS_DENORMALIZED_KEY, isDenormalized);
        definition.put(DENORMALIZED_FIELDS_KEY, denormalizedFields);
        definition.put(IS_EMBEDDED_KEY, isEmbedded);
        definition.put(IS_JAVA_FIELD_TYPE_PRIMITIVE_KEY, isJavaFieldTypePrimitive);
        definition.put(IS_REQUIRED_KEY, isRequired);
        definition.put(JUNCTION_FIELD_KEY, junctionField);
        definition.put(JUNCTION_POSITION_FIELD_KEY, junctionPositionField);
        definition.put(MINIMUM_KEY, minimum);
        definition.put(STEP_KEY, step);
        definition.put(MAXIMUM_KEY, maximum);
        definition.put(PATTERN_KEY, pattern);
        definition.put(DEFAULT_VALUE_KEY, defaultValue);
        definition.put(PREDICATE_KEY, predicate);
        definition.put(RAW_KEY, raw);
        definition.put(VALUES_KEY, valueDefinitions.isEmpty() ? null : valueDefinitions);
        definition.put(GROUPS_KEY, groups);
        definition.put(VALUE_TYPES_KEY, typeIds.isEmpty() ? null : typeIds);
        definition.put(GENERIC_ARGUMENT_INDEX_KEY, genericArgumentIndex);
        definition.put(GENERIC_ARGUMENTS_KEY, genericArgumentIds.isEmpty() ? null : genericArgumentIds);
        definition.put(JAVA_FIELD_NAME_KEY, javaFieldName);
        definition.put(JAVA_DECLARING_CLASS_NAME_KEY, javaDeclaringClassName);
        definition.put(JAVA_ENUM_CLASS_NAME_KEY, javaEnumClassName);

        return definition;
    }

    public ObjectStruct getParent() {
        return parent;
    }

    public ObjectType getParentType() {
        ObjectStruct parent = getParent();
        return parent instanceof ObjectType ? (ObjectType) parent : null;
    }

    /** Returns the maximum number of items in the field value. */
    public Number getCollectionMaximum() {
        return collectionMaximum;
    }

    /** Sets the maximum number of items in the field value. */
    public void setCollectionMaximum(Number maximum) {
        this.collectionMaximum = maximum;
    }

    /** Returns the minimum number of items in the field value. */
    public Number getCollectionMinimum() {
        return collectionMinimum;
    }

    /** Sets the minimum number of items in the field value. */
    public void setCollectionMinimum(Number minimum) {
        this.collectionMinimum = minimum;
    }

    /** Returns {@code true} if this field is deprecated. */
    public boolean isDeprecated() {
        return deprecated;
    }

    /** Sets whether this field is deprecated. */
    public void setDeprecated(boolean deprecated) {
        this.deprecated = deprecated;
    }

    /** Returns the display name. */
    public String getDisplayName() {
        if (!ObjectUtils.isBlank(displayName)) {
            return displayName;
        }

        String name = getJavaFieldName();

        if (ObjectUtils.isBlank(name)) {
            name = getInternalName();
        }

        int dotAt = name.lastIndexOf('.');

        if (dotAt > -1) {
            name = name.substring(dotAt + 1);
        }

        int dollarAt = name.lastIndexOf('$');

        if (dollarAt > -1) {
            name = name.substring(dollarAt + 1);
        }

        name = StringUtils.toLabel(name);

        if (!name.endsWith("?")
                && BOOLEAN_TYPE.equals(getInternalItemType())) {
            name += "?";
        }

        return name;
    }

    /** Sets the display name. */
    public void setDisplayName(String displayName) {
        this.displayName = displayName;
    }

    /** Returns the internal name. */
    public String getInternalName() {
        return internalName;
    }

    /** Sets the internal name. */
    public void setInternalName(String internalName) {
        this.internalName = internalName;
    }

    /** Returns the internal type. */
    public String getInternalType() {
        return internalType;
    }

    /** Sets the internal type. */
    public void setInternalType(String internalType) {
        this.internalType = internalType;
    }

    /** Returns {@code true} if the field value should be denormalized. */
    public boolean isDenormalized() {
        return isDenormalized;
    }

    /** Sets whether the field value should be denormalized. */
    public void setDenormalized(boolean isDenormalized) {
        this.isDenormalized = isDenormalized;
    }

    /**
     * Returns the set of all field names that should be denormalized
     * within this field value.
     */
    public Set<String> getDenormalizedFields() {
        if (denormalizedFields == null) {
            denormalizedFields = new HashSet<String>();
        }
        return denormalizedFields;
    }

    /**
     * Returns the effective set of all field names that should be
     * denormalized within this field value.
     */
    public Set<ObjectField> getEffectiveDenormalizedFields(ObjectType valueType) {
        Set<ObjectField> denormalizedFields = null;

        if (valueType != null) {
            Set<String> denormalizedFieldNames = isDenormalized() ? getDenormalizedFields()
                    : valueType.isDenormalized() ? valueType.getDenormalizedFields()
                    : null;

            if (denormalizedFieldNames != null) {
                denormalizedFields = new HashSet<ObjectField>();

                for (String fieldName : denormalizedFieldNames) {
                    ObjectField field = valueType.getField(fieldName);
                    if (field != null) {
                        denormalizedFields.add(field);
                    }
                }

                if (denormalizedFields.isEmpty()) {
                    denormalizedFields.addAll(valueType.getFields());
                }
            }
        }

        return denormalizedFields;
    }

    /**
     * Sets the set of all field names that should be denormalized
     * within this field value.
     */
    public void setDenormalizedFields(Set<String> denormalizedFields) {
        this.denormalizedFields = denormalizedFields;
    }

    /** Returns {@code true} if the field value should be embedded. */
    public boolean isEmbedded() {
        return isEmbedded;
    }

    /** Sets whether the field value should be embedded. */
    public void setEmbedded(boolean isEmbedded) {
        this.isEmbedded = isEmbedded;
    }

    /** Returns {@code true} if the original java field is a primitive */
    public boolean isJavaFieldTypePrimitive() {
        return isJavaFieldTypePrimitive;
    }

    /** Sets whether the original java field is a javaFieldTypePrimitive. */
    public void setJavaFieldTypePrimitive(boolean javaFieldTypePrimitive) {
        isJavaFieldTypePrimitive = javaFieldTypePrimitive;
    }

    /** Returns {@code true} if the field value is required. */
    public boolean isRequired() {
        return isRequired;
    }

    /** Sets whether the field value is required. */
    public void setRequired(boolean isRequired) {
        this.isRequired = isRequired;
    }

    public String getJunctionField() {
        return junctionField;
    }

    public void setJunctionField(String junctionField) {
        this.junctionField = junctionField;
    }

    public String getJunctionPositionField() {
        return junctionPositionField;
    }

    public void setJunctionPositionField(String junctionPositionField) {
        this.junctionPositionField = junctionPositionField;
    }

    public List<Object> findJunctionItems(State state) {
        Set<ObjectType> types = getTypes();

        if (types.isEmpty()) {
            return new ArrayList<Object>();

        } else {
            ObjectType type = types.iterator().next();
            Predicate resolveInvisiblePredicate = null;
            Query<Object> query = Query
                    .fromType(type)
                    .where(getJunctionField() + " = ?", state.getId());

            if (state.isResolveInvisible()) {
                DatabaseEnvironment environment = Database.Static.getDefault().getEnvironment();
                List<ObjectIndex> indexes = environment.getIndexes();

                indexes.addAll(type.getIndexes());

                for (ObjectIndex index : indexes) {
                    if (index.isVisibility()) {
                        FindVisibilityValuesTrigger trigger = new FindVisibilityValuesTrigger(index);
                        String indexName = index.getUniqueName();

                        state.fireTrigger(trigger, false);

                        for (Object value : trigger.getValues()) {
                            resolveInvisiblePredicate = CompoundPredicate.combine(
                                             PredicateParser.OR_OPERATOR,
                                             resolveInvisiblePredicate,
                                             PredicateParser.Static.parse(indexName + " = ?", value));
                        }

                        resolveInvisiblePredicate = CompoundPredicate.combine(
                                             PredicateParser.OR_OPERATOR,
                                             resolveInvisiblePredicate,
                                             PredicateParser.Static.parse(indexName + " = missing or " + indexName + " = true"));
                    }
                }
            }

            if (resolveInvisiblePredicate != null) {
                query.and(resolveInvisiblePredicate);
            }

            String junctionPositionField = getJunctionPositionField();

            if (!ObjectUtils.isBlank(junctionPositionField)) {
                query.sortAscending(junctionPositionField);
            }
            return query.selectAll();
        }
    }

    public Number getMinimum() {
        return minimum;
    }

    public void setMinimum(Number minimum) {
        this.minimum = minimum;
    }

    public Number getStep() {
        return step;
    }

    public void setStep(Number step) {
        this.step = step;
    }

    public Number getMaximum() {
        return maximum;
    }

    public void setMaximum(Number maximum) {
        this.maximum = maximum;
    }

    public String getPattern() {
        return pattern;
    }

    public void setPattern(String pattern) {
        this.pattern = pattern;
    }

    /** Returns the default field value. */
    public Object getDefaultValue() {
        return defaultValue;
    }

    /** Sets the default field value. */
    public void setDefaultValue(Object defaultValue) {
        this.defaultValue = defaultValue;
    }

    public String getPredicate() {
        return predicate;
    }

    public void setPredicate(String predicate) {
        this.predicate = predicate;
    }

    public boolean isRaw() {
        return raw;
    }

    public void setRaw(boolean raw) {
        this.raw = raw;
    }

    public Set<String> getGroups() {
        if (groups == null) {
            groups = new LinkedHashSet<String>();
        }
        return groups;
    }

    public void setGroups(Set<String> groups) {
        this.groups = groups;
    }

    /** Returns the valid field types. */
    public Set<ObjectType> getTypes() {
        if (types == null) {
            types = new LinkedHashSet<ObjectType>();
        }
        return types;
    }

    /** Sets the valid field types. */
    public void setTypes(Set<ObjectType> types) {
        this.types = types;
    }

    public Number getGenericArgumentIndex() {
        return genericArgumentIndex;
    }

    public void setGenericArgumentIndex(Number genericArgumentIndex) {
        this.genericArgumentIndex = genericArgumentIndex;
    }

    public List<ObjectType> getGenericArguments() {
        if (genericArguments == null) {
            genericArguments = new ArrayList<ObjectType>();
        }
        return genericArguments;
    }

    public void setGenericArguments(List<ObjectType> genericArguments) {
        this.genericArguments = genericArguments;
    }

    /** Returns the valid field values. */
    public Set<Value> getValues() {
        return values;
    }

    /** Sets the valid field values. */
    public void setValues(Set<Value> values) {
        this.values = values;
    }

    /** Returns the Java field name. */
    public String getJavaFieldName() {
        return javaFieldName;
    }

    /** Returns the Java field. */
    public Field getJavaField(Class<?> objectClass) {
        if (getJavaFieldName() == null) {
            return null;
        }

        Class<?> declaringClass = ObjectUtils.getClassByName(getJavaDeclaringClassName());

        return declaringClass != null && declaringClass.isAssignableFrom(objectClass)
                ? javaFieldCache.getUnchecked(objectClass).orNull()
                : null;
    }

    private final transient LoadingCache<Class<?>, Optional<Field>> javaFieldCache = CacheBuilder
            .newBuilder()
            .build(new CacheLoader<Class<?>, Optional<Field>>() {

        @Override
        public Optional<Field> load(Class<?> objectClass) {
            return Optional.fromNullable(TypeDefinition.getInstance(objectClass).getField(getJavaFieldName()));
        }
    });

    /** Sets the Java field name. */
    public void setJavaFieldName(String fieldName) {
        this.javaFieldName = fieldName;
    }

    /** Returns the Java declaring class name. */
    public String getJavaDeclaringClassName() {
        return javaDeclaringClassName;
    }

    /** Sets the Java declaring class name. */
    public void setJavaDeclaringClassName(String className) {
        this.javaDeclaringClassName = className;
    }

    /** Returns the Java enum class name used to convert the field value. */
    public String getJavaEnumClassName() {
        return javaEnumClassName;
    }

    /** Sets the Java enum class name used to convert the field value. */
    public void setJavaEnumClassName(String className) {
        this.javaEnumClassName = className;
    }

    /** Returns the map of custom option values. */
    public Map<String, Object> getOptions() {
        if (options == null) {
            options = new CompactMap<String, Object>();
        }
        return options;
    }

    /** Sets the map of custom option values. */
    public void setOptions(Map<String, Object> options) {
        this.options = options;
    }

    /**
     * Returns the label, which is either the display name or is generated
     * based on the internal name.
     */
    public String getLabel() {
        String displayName = getDisplayName();
        if (ObjectUtils.isBlank(displayName)) {
            String internalName = getInternalName();
            int dotAt = internalName.lastIndexOf('.');
            if (dotAt > -1) {
                internalName = internalName.substring(dotAt + 1, internalName.length());
            }
            return StringUtils.toLabel(internalName);
        } else {
            return displayName;
        }
    }

    /** Validates the field value in the given record. */
    public void validate(State state) {

        boolean shouldValidate = true;
        ObjectType type = state.getType();

        if (type != null) {
            for (String className : type.getModificationClassNames()) {
                Class<?> modificationClass = ObjectUtils.getClassByName(className);
                if (modificationClass != null && ConditionallyValidatable.class.isAssignableFrom(modificationClass)) {
                    if (!((ConditionallyValidatable) state.as(modificationClass)).shouldValidate(this)) {
                        shouldValidate = false;
                        break;
                    }
                }
            }
        }

        if (shouldValidate) {
            Object obj = state.getOriginalObjectOrNull();
            if (obj instanceof ConditionallyValidatable) {
                shouldValidate = ((ConditionallyValidatable) obj).shouldValidate(this);
            }
        }

        if (!shouldValidate) {
            return;
        }

        Object value = state.get(getInternalName());
        if (isRequired() && ObjectUtils.isBlank(value)) {
            state.addError(this, "Required!");
        } else {
            validateValue(state, getInternalType(), value);
        }
    }

    /**
     * Recursively check the given {@code value} against the given
     * {@code internalType}.
     */
    private void validateValue(State state, String internalType, Object value) {

        if (internalType == null || value == null) {
            return;
        }

        if (value instanceof Enum) {
            value = ((Enum<?>) value).name();
        }

        // Separate internal type like list/map/text into list and map/text.
        String subType = "";
        int slashAt = internalType.indexOf('/');
        if (slashAt > -1) {
            subType = internalType.substring(slashAt + 1);
            internalType = internalType.substring(0, slashAt);
        }

        boolean isCompatible = false;
        Set<Class<?>> classes = TYPE_TO_CLASS.get(internalType);
        if (classes == null) {
            classes = CLASS_TO_TYPE.keySet();
        }
        for (Class<?> c : classes) {
            if (c.isAssignableFrom(value.getClass())) {
                isCompatible = true;
                break;
            }
        }
        if (!isCompatible) {
            state.addError(this, String.format(
                    "Must be compatible with one of the following types: %s",
                    classes));
        }

        String predicate = getPredicate();
        if (!ObjectUtils.isBlank(predicate)
                && RECORD_TYPE.equals(internalType)
                && !PredicateParser.Static.evaluate(value, predicate, state)) {
            state.addError(this, String.format("Must match %s!", predicate));
        }

        if (COLLECTION_CLASS_TO_TYPE.values().contains(internalType)) {
            Collection<?> values = MAP_TYPE.equals(internalType) ? ((Map<?, ?>) value).values() : (Collection<?>) value;
            int valuesSize = values.size();
            Number min = getCollectionMinimum();
            if (min != null && valuesSize < min.intValue()) {
                state.addError(this, String.format("Must contain at least %s item(s)!", min));
            }
            Number max = getCollectionMaximum();
            if (max != null && valuesSize > max.intValue()) {
                state.addError(this, String.format("Cannot exceed %s item(s)!", max));
            }
            for (Object e : values) {
                validateValue(state, subType, e);
            }

        } else if (NUMBER_TYPE.equals(internalType)) {
            double number = ((Number) value).doubleValue();
            Number min = getMinimum();
            if (min != null && number < min.doubleValue()) {
                state.addError(this, String.format("Must be greater than or equal to %s!", min));
            }
            Number max = getMaximum();
            if (max != null && number > max.doubleValue()) {
                state.addError(this, String.format("Must be less than or equal to %s!", max));
            }

        } else if (TEXT_TYPE.equals(internalType)) {
            String string = (String) value;
            int stringLength = string.length();
            Number min = getMinimum();
            if (min != null && stringLength < min.intValue()) {
                state.addError(this, String.format("Must be at least %s characters!", min.intValue()));
            }
            Number max = getMaximum();
            if (max != null && stringLength > max.intValue()) {
                state.addError(this, String.format("Cannot exceed %s characters!", max.intValue()));
            }
            String pattern = getPattern();
            if (!(ObjectUtils.isBlank(pattern) || StringUtils.matches(string, pattern))) {
                state.addError(this, String.format("Must match %s pattern!", pattern));
            }
        }
    }

    /**
     * Returns a unique name that can be used to identify this field
     * among all fields.
     */
    public String getUniqueName() {
        ObjectType parentType = getParentType();
        String internalName = getInternalName();
        if (parentType == null) {
            return internalName;
        }

        String prefix = getJavaDeclaringClassName();
        if (ObjectUtils.isBlank(prefix)) {
            prefix = parentType.getObjectClassName();
            if (ObjectUtils.isBlank(prefix)) {
                prefix = parentType.getId().toString();
            }
        }

        return prefix + "/" + internalName;
    }

    /**
     * Returns {@code true} if the internal type is one of the collection
     * types.
     */
    public boolean isInternalCollectionType() {
        return getInternalType().contains("/");
    }

    /**
     * Returns the internal item type. For example, given {@code list/text},
     * this method would return {@code text}.
     */
    public String getInternalItemType() {
        String internalType = getInternalType();
        int slashAt = internalType.lastIndexOf('/');
        return slashAt > -1 ? internalType.substring(slashAt + 1) : internalType;
    }

    /**
     * Sets the internal type based on the given {@code environment},
     * {@code objectClass}, and {@code javaType}. For example, {@link String}
     * class would be equivalent to {@code text}.
     */
    public void setInternalType(DatabaseEnvironment environment, Class<?> objectClass, Type javaType) {
        setInternalType(translateType(environment, objectClass, javaType));
    }

    private String translateType(DatabaseEnvironment environment, Class<?> objectClass, Type javaType) {
        return translateType(environment, objectClass, javaType, -1);
    }

    // Translates Java type to field internal type.
    private String translateType(DatabaseEnvironment environment, Class<?> objectClass, Type javaType, int javaTypeIndex) {

        // Simple translation like String to text.
        if (javaType instanceof Class) {
            Class<?> javaTypeClass = (Class<?>) javaType;
            setJavaFieldTypePrimitive(javaTypeClass.isPrimitive());

            if (javaTypeClass.equals(Object.class)) {
                return ANY_TYPE;

            } else if (javaTypeClass.equals(Metric.class)) {
                return METRIC_TYPE;

            } else if (Recordable.class.isAssignableFrom(javaTypeClass)) {
                ObjectType type = environment.getTypeByClass(javaTypeClass);

                if (type != null) {
                    if (javaTypeIndex < 0) {
                        getTypes().add(type);

                    } else {
                        CollectionUtils.set(getGenericArguments(), javaTypeIndex, type);
                    }
                }

                return RECORD_TYPE;

            } else if (Enum.class.isAssignableFrom(javaTypeClass)) {
                @SuppressWarnings("unchecked")
                Class<Enum<?>> enumClass = (Class<Enum<?>>) javaTypeClass;
                Set<Value> values = new LinkedHashSet<Value>();

                for (Enum<?> enumConstant : enumClass.getEnumConstants()) {
                    Value value = new Value();
                    value.setLabel(enumConstant.toString());
                    value.setValue(enumConstant.name());
                    values.add(value);
                }

                setJavaEnumClassName(javaTypeClass.getName());
                setValues(values);
                return TEXT_TYPE;

            } else if (StorageItem.class.isAssignableFrom(javaTypeClass)) {

                return FILE_TYPE;
            }

            String type = CLASS_TO_TYPE.get(javaTypeClass);

            if (type != null) {
                if (javaTypeClass.equals(long.class)
                        || javaTypeClass.equals(Long.class)) {
                    setMinimum(Long.MIN_VALUE);
                    setStep(1);
                    setMaximum(Long.MAX_VALUE);

                } else if (javaTypeClass.equals(int.class)
                        || javaTypeClass.equals(Integer.class)) {
                    setMinimum(Integer.MIN_VALUE);
                    setStep(1);
                    setMaximum(Integer.MAX_VALUE);

                } else if (javaTypeClass.equals(short.class)
                        || javaTypeClass.equals(Short.class)) {
                    setMinimum(Short.MIN_VALUE);
                    setStep(1);
                    setMaximum(Short.MAX_VALUE);

                } else if (javaTypeClass.equals(byte.class)
                        || javaTypeClass.equals(Byte.class)) {
                    setMinimum(Byte.MIN_VALUE);
                    setStep(1);
                    setMaximum(Byte.MAX_VALUE);
                }

                return type;
            }

        // Nested translation like List<String> to list/text.
        } else if (javaType instanceof ParameterizedType) {
            ParameterizedType javaTypeParamed = (ParameterizedType) javaType;
            Type[] javaTypeArgs = javaTypeParamed.getActualTypeArguments();
            Type firstRawType = javaTypeParamed.getRawType();
            String firstType = translateType(environment, objectClass, firstRawType);

            if (RECORD_TYPE.equals(firstType)) {
                for (int i = 0, length = javaTypeArgs.length; i < length; ++ i) {
                    translateType(environment, objectClass, javaTypeArgs[i], i);
                }

                return firstType;

            // Assume List, Map, or Set.
            } else {
                return firstType + "/" + translateType(environment, objectClass, javaTypeArgs[javaTypeArgs.length - 1]);
            }

        // Complex translation like List<T> to list/record.
        } else if (javaType instanceof TypeVariable) {
            TypeVariable<?> javaTypeVar = (TypeVariable<?>) javaType;
            GenericDeclaration container = javaTypeVar.getGenericDeclaration();

            for (Type current = objectClass; true;) {
                if (current instanceof ParameterizedType) {
                    ParameterizedType currentParamed = (ParameterizedType) current;
                    Type currentRaw = currentParamed.getRawType();

                    if (currentRaw.equals(container)) {
                        TypeVariable<?>[] currentRawParams = ((GenericDeclaration) currentRaw).getTypeParameters();
                        int index = 0;
                        int length = currentRawParams.length;

                        for (; index < length; ++ index) {
                            if (currentRawParams[index].equals(javaTypeVar)) {
                                break;
                            }
                        }

                        if (index < length) {
                            Type[] currentArgs = currentParamed.getActualTypeArguments();

                            if (index < currentArgs.length) {
                                setGenericArgumentIndex(index);
                                return translateType(environment, objectClass, currentArgs[index], javaTypeIndex < 0 ? -1 : index);
                            }
                        }

                        break;

                    } else {
                        current = currentRaw;
                    }

                } else if (current instanceof Class) {
                    current = ((Class<?>) current).getGenericSuperclass();

                } else {
                    break;
                }
            }

            @SuppressWarnings("rawtypes")
            TypeVariable[] typeParams = objectClass.getTypeParameters();

            if (typeParams != null) {
                for (int i = 0, length = typeParams.length; i < length; ++ i) {
                    if (javaTypeVar.equals(typeParams[i])) {
                        setGenericArgumentIndex(i);
                        break;
                    }
                }
            }

            for (Type bound : javaTypeVar.getBounds()) {
                try {
                    return translateType(environment, objectClass, bound);
                } catch (IllegalArgumentException error) {
                    // If the bound type can't be translated correctly,
                    // try the next one.
                }
            }

        // Wildcards such as ?, ? extends, or ? super
        } else if (javaType instanceof WildcardType) {
            for (Type bound : ((WildcardType) javaType).getUpperBounds()) {
                try {
                    return translateType(environment, objectClass, bound);

                } catch (IllegalArgumentException error) {
                    // If the bound type can't be translated correctly,
                    // try the next one.
                }
            }
        }

        ObjectStruct parent = getParent();

        if (parent instanceof ObjectType) {
            throw new IllegalArgumentException(String.format(
                    "Can't use [%s] for [%s] in [%s]!",
                    javaType, getInternalName(), ((ObjectType) parent).getObjectClassName()));

        } else {
            throw new IllegalArgumentException(String.format(
                    "Can't use [%s] for [%s]!",
                    javaType, getInternalName()));
        }
    }

    /**
     * Finds all the concrete types that are compatible with the set of
     * valid field value types.
     */
    public Set<ObjectType> findConcreteTypes() {
        Set<ObjectType> concreteTypes = new LinkedHashSet<ObjectType>();
        Set<String> groups = getGroups();
        Set<ObjectType> types = getTypes();
        DatabaseEnvironment environment = getParent().getEnvironment();

        if (groups.isEmpty() && types.isEmpty()) {
            for (ObjectType type : environment.getTypesByGroup(Object.class.getName())) {
                if (!type.isAbstract()) {
                    concreteTypes.add(type);
                }
            }

        } else if (!groups.isEmpty()) {
            for (String group : groups) {
                for (ObjectType compatibleType : environment.getTypesByGroup(group)) {
                    if (!compatibleType.isAbstract()) {
                        concreteTypes.add(compatibleType);
                    }
                }
            }

        } else {
            for (ObjectType type : types) {
                for (ObjectType compatibleType : environment.getTypesByGroup(type.getObjectClassName())) {
                    if (!compatibleType.isAbstract()) {
                        concreteTypes.add(compatibleType);
                    }
                }
            }
        }

        return concreteTypes;
    }

    /**
     * Returns {@code true} if this field contains a {@link Metric} value.
     */
    public boolean isMetric() {
        Set<ObjectType> types = getTypes();

        return getInternalItemType().equals(METRIC_TYPE)
                || (types != null
                && !types.isEmpty()
                && Metric.class.equals(types.iterator().next().getObjectClass()));
    }

    // --- Object support ---

    @Override
    public boolean equals(Object other) {

        if (this == other) {
            return true;

        } else if (other instanceof ObjectField) {
            ObjectField otherField = (ObjectField) other;
            return ObjectUtils.equals(getParent(), otherField.getParent())
                    && ObjectUtils.equals(getInternalName(), otherField.getInternalName());

        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return ObjectUtils.hashCode(getParent(), getInternalName());
    }

    @Override
    public String toString() {
        return toDefinition().toString();
    }

    // ---

    /** Description of a value that can be stored in a field. */
    @Embedded
    public static class Value extends Record {

        private static final String LABEL_KEY = "label";
        private static final String VALUE_KEY = "value";

        private String label;
        private String value;

        /** Creates a blank instance. */
        public Value() {
        }

        /** Creates an instance from the given value {@code definition}. */
        public Value(Map<String, String> definition) {
            setLabel(ObjectUtils.to(String.class, definition.get(LABEL_KEY)));
            setValue(ObjectUtils.to(String.class, definition.get(VALUE_KEY)));
        }

        /** Converts this value to a definition map. */
        public Map<String, String> toDefinition() {
            Map<String, String> definition = new CompactMap<String, String>();
            definition.put(LABEL_KEY, getLabel());
            definition.put(VALUE_KEY, getValue());
            return definition;
        }

        /** Returns the descriptive label. */
        public String getLabel() {
            return ObjectUtils.isBlank(label) ? getValue() : label;
        }

        /** Sets the descriptive label. */
        public void setLabel(String label) {
            this.label = label;
        }

        /** Returns the value to be stored in the field. */
        public String getValue() {
            return value;
        }

        /** Sets the value to be stored in the field. */
        public void setValue(String value) {
            this.value = value;
        }

        // --- Comparable support --

        @Override
        public int compareTo(Record value) {
            return getLabel().compareTo(value.getLabel());
        }

        // --- Object support ---

        @Override
        public boolean equals(Object other) {

            if (this == other) {
                return true;

            } else if (other instanceof Value) {
                Value otherValue = (Value) other;
                return ObjectUtils.equals(getValue(), otherValue.getValue());

            } else {
                return false;
            }
        }

        @Override
        public int hashCode() {
            return ObjectUtils.hashCode(getValue());
        }

        @Override
        public String toString() {
            return getLabel();
        }
    }

    /**
     * Specifies the processor class that can manipulate a field
     * definition using the target annotation.
     */
    @Documented
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.ANNOTATION_TYPE)
    public @interface AnnotationProcessorClass {
        Class<? extends AnnotationProcessor<? extends Annotation>> value();
    }

    /** Simple processor that can manipulate a field definition. */
    public static interface AnnotationProcessor<A extends Annotation> {
        public void process(ObjectType type, ObjectField field, A annotation);
    }

    /** {@link ObjectField} utility methods. */
    public static final class Static {

        /**
         * Converts all given field {@code definitions} into a map of
         * instances.
         */
        public static Map<String, ObjectField> convertDefinitionsToInstances(
                ObjectStruct parent,
                List<Map<String, Object>> definitions) {

            Map<String, ObjectField> instances = new CompactMap<String, ObjectField>();
            if (definitions != null) {
                for (Map<String, Object> definition : definitions) {
                    ObjectField instance;
                    if (definition.get(JAVA_FIELD_NAME_KEY) == null
                            && definition.get(ObjectMethod.JAVA_METHOD_NAME_KEY) != null) {
                        instance = new ObjectMethod(parent, definition);
                    } else {
                        instance = new ObjectField(parent, definition);
                    }
                    instance.getState().setDatabase(parent.getEnvironment().getDatabase());
                    instances.put(instance.getInternalName(), instance);
                }
            }
            return instances;
        }

        /**
         * Converts all given field {@code instances} into a list of
         * definitions.
         */
        public static List<Map<String, Object>> convertInstancesToDefinitions(List<ObjectField> instances) {
            List<Map<String, Object>> definitions = new ArrayList<Map<String, Object>>();
            for (ObjectField instance : instances) {
                definitions.add(instance.toDefinition());
            }
            return definitions;
        }
    }

    private static class FindVisibilityValuesTrigger implements Trigger {

        private final ObjectIndex index;
        private final Set<Object> values = new HashSet<Object>();

        public FindVisibilityValuesTrigger(ObjectIndex index) {
            this.index = index;
        }

        public Set<Object> getValues() {
            return values;
        }

        @Override
        public void execute(Object object) {
            if (!(object instanceof VisibilityValues)) {
                return;
            }

            for (Object o : ((VisibilityValues) object).findVisibilityValues(index)) {
                if (o != null) {
                    getValues().add(o);
                }
            }
        }
    }
}
