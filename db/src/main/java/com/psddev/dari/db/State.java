package com.psddev.dari.db;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.psddev.dari.util.CompactMap;
import com.psddev.dari.util.ConversionException;
import com.psddev.dari.util.ConversionFunction;
import com.psddev.dari.util.Converter;
import com.psddev.dari.util.LoadingCacheMap;
import com.psddev.dari.util.ObjectToIterable;
import com.psddev.dari.util.ObjectUtils;
import com.psddev.dari.util.Profiler;
import com.psddev.dari.util.StorageItem;
import com.psddev.dari.util.StringUtils;
import com.psddev.dari.util.TypeDefinition;
import com.psddev.dari.util.UuidUtils;

import javax.annotation.Nonnull;
import javax.annotation.ParametersAreNonnullByDefault;

/** Represents the state of an object stored in the database. */
public class State implements Map<String, Object> {

    public static final String ID_KEY = "_id";
    public static final String TYPE_KEY = "_type";
    public static final String LABEL_KEY = "_label";

    private static final Logger LOGGER = LoggerFactory.getLogger(State.class);
    private static final String PROFILER_EVENT_PREFIX = "State: ";
    private static final String RESOLVE_REFERENCE_PROFILER_EVENT = PROFILER_EVENT_PREFIX + "Resolve Reference";

    /**
     * {@linkplain Query#getOptions Query option} that contains the object
     * whose fields are being resolved automatically.
     */
    public static final String REFERENCE_RESOLVING_QUERY_OPTION = "dari.referenceResolving";

    public static final String REFERENCE_FIELD_QUERY_OPTION = "dari.referenceField";

    public static final String UNRESOLVED_TYPE_IDS_QUERY_OPTION = "dari.unresolvedTypeIds";

    public static final String SUB_DATA_STATE_EXTRA_PREFIX = "dari.subDataState.";

    private static final String ATOMIC_OPERATIONS_EXTRA = "dari.atomicOperations";
    private static final String MODIFICATIONS_EXTRA = "dari.modifications";

    private static final int STATUS_FLAG_OFFSET = 16;
    private static final int STATUS_FLAG_MASK = -1 >>> STATUS_FLAG_OFFSET;
    private static final int ALL_RESOLVED_FLAG = 1;
    private static final int RESOLVE_TO_REFERENCE_ONLY_FLAG = 1 << 1;
    private static final int RESOLVE_WITHOUT_CACHE = 1 << 2;
    private static final int RESOLVE_USING_MASTER = 1 << 3;
    private static final int RESOLVE_INVISIBLE = 1 << 4;

    private static final ThreadLocal<List<Listener>> LISTENERS_LOCAL = new ThreadLocal<>();

    private final Map<Class<?>, Object> linkedObjects = new CompactMap<>();
    private Database database;
    private UUID id;
    private UUID typeId;
    private final Map<String, Object> rawValues = new CompactMap<>();
    private Map<String, Object> extras;
    private Map<ObjectField, List<String>> errors;
    private volatile int flags;

    /**
     * Returns the state associated with the given {@code object}.
     *
     * @throws IllegalArgumentException If the given {@code object}
     *         doesn't have any state associated to it.
     */
    public static State getInstance(Object object) {

        if (object == null) {
            return null;

        } else if (object instanceof State) {
            return (State) object;

        } else if (object instanceof Recordable) {
            return ((Recordable) object).getState();

        } else {
            throw new IllegalArgumentException(String.format(
                    "Can't retrieve state from an instance of [%s]!",
                    object.getClass().getName()));
        }
    }

    /**
     * Links the given {@code object} to this state so that changes on
     * either side are copied over.
     */
    public void linkObject(Object object) {
        if (object != null) {
            linkedObjects.put(object.getClass(), object);
        }
    }

    /**
     * Unlinks the given {@code object} from this state so that changes
     * on either side are no longer copied over.
     */
    public void unlinkObject(Object object) {
        if (object != null) {
            linkedObjects.remove(object.getClass());
        }
    }

    /**
     * Returns the effective originating database.
     *
     * <p>This method will check the following:</p>
     *
     * <ul>
     * <li>{@linkplain #getRealDatabase Real originating database}.</li>
     * <li>{@linkplain Database.Static#getDefault Default database}.</li>
     * <li>{@linkplain ObjectType#getSourceDatabase Source database}.</li>
     * <li>
     *
     * @return Never {@code null}.
     */
    public Database getDatabase() {
        if (database == null) {
            Database newDatabase = Database.Static.getDefault();
            this.database = newDatabase;
            ObjectType type = getType();

            if (type != null) {
                newDatabase = type.getSourceDatabase();

                if (newDatabase != null) {
                    this.database = newDatabase;
                }
            }
        }

        return database;
    }

    /**
     * Returns the real originating database.
     *
     * @return May be {@code null}.
     */
    public Database getRealDatabase() {
        return database;
    }

    /**
     * Sets the originating database.
     *
     * @param database May be {@code null}.
     */
    public void setDatabase(Database database) {
        this.database = database;
    }

    /**
     * Returns the unique ID.
     *
     * @return Never {@code null}.
     */
    public UUID getId() {
        if (id == null) {
            this.id = UuidUtils.createSequentialUuid();
        }

        return this.id;
    }

    /**
     * Sets the unique ID.
     *
     * @param id
     *        If {@code null}, next call to {@link #getId} will generate
     *        a new unique ID.
     */
    public void setId(UUID id) {
        this.id = id;
    }

    /** Returns the type ID. */
    public UUID getTypeId() {
        if (typeId == null) {
            UUID newTypeId = UuidUtils.ZERO_UUID;

            if (!linkedObjects.isEmpty()) {
                Database database = getDatabase();

                if (database != null) {
                    ObjectType type = database.getEnvironment().getTypeByClass(linkedObjects.keySet().iterator().next());

                    if (type != null) {
                        newTypeId = type.getId();
                    }
                }
            }

            typeId = newTypeId;
        }

        return typeId;
    }

    /** Returns the type ID modified by the visibility index values. */
    public UUID getVisibilityAwareTypeId() {
        ObjectType type = getType();

        if (type != null) {
            updateVisibilities(getDatabase().getEnvironment().getIndexes());
            updateVisibilities(type.getIndexes());

            @SuppressWarnings("unchecked")
            List<String> visibilities = (List<String>) get("dari.visibilities");

            if (visibilities != null && !visibilities.isEmpty()) {
                String field = visibilities.get(visibilities.size() - 1);
                Object value = toSimpleValue(get(field), false, false);

                if (value != null) {
                    byte[] typeId = UuidUtils.toBytes(getTypeId());
                    byte[] md5 = StringUtils.md5(field + "/" + value.toString().trim().toLowerCase(Locale.ENGLISH));

                    for (int i = 0, length = typeId.length; i < length; ++ i) {
                        typeId[i] ^= md5[i];
                    }

                    return UuidUtils.fromBytes(typeId);
                }
            }
        }

        return getTypeId();
    }

    private void updateVisibilities(List<ObjectIndex> indexes) {
        @SuppressWarnings("unchecked")
        List<String> visibilities = (List<String>) get("dari.visibilities");

        for (ObjectIndex index : indexes) {
            if (index.isVisibility()) {
                String field = index.getField();
                Object value = toSimpleValue(index.getValue(this), false, false);

                if (value == null) {
                    if (visibilities != null) {
                        visibilities.remove(field);

                        if (visibilities.isEmpty()) {
                            remove("dari.visibilities");
                        }
                    }

                } else {
                    if (visibilities == null) {
                        visibilities = new ArrayList<>();
                        put("dari.visibilities", visibilities);
                    }

                    if (!visibilities.contains(field)) {
                        visibilities.add(field);
                    }
                }
            }
        }
    }

    /** Sets the type ID. */
    public void setTypeId(UUID typeId) {
        this.typeId = typeId;
    }

    /** Returns the type. */
    public ObjectType getType() {
        ObjectType type = getDatabase().getEnvironment().getTypeById(getTypeId());
        if (type == null) {

            // During the bootstrapping process, the type for the root
            // type is not available, so fake it here.
            for (Object object : linkedObjects.values()) {
                if (object instanceof ObjectType && getId().equals(getTypeId())) {
                    type = (ObjectType) object;
                    type.setObjectClassName(ObjectType.class.getName());
                    type.initialize();
                }
                break;
            }
        }

        return type;
    }

    /**
     * Sets the type. This method may also change the originating
     * database based on the the given {@code type}.
     */
    public void setType(ObjectType type) {
        if (type == null) {
            setTypeId(null);
        } else {
            setTypeId(type.getId());
            setDatabase(type.getState().getDatabase());
        }
    }

    /**
     * Returns {@code true} if this state is visible (all visibility-indexed
     * fields are {@code null}).
     */
    public boolean isVisible() {
        getVisibilityAwareTypeId();
        return ObjectUtils.isBlank(get("dari.visibilities"));
    }

    public ObjectField getField(String name) {
        ObjectField field = getDatabase().getEnvironment().getField(name);
        if (field == null) {
            ObjectType type = getType();
            if (type != null) {
                field = type.getField(name);
            }
        }
        return field;
    }

    public ObjectMethod getMethod(String name) {
        ObjectField field = getField(name);
        if (field instanceof ObjectMethod) {
            return (ObjectMethod) field;
        } else {
            return null;
        }
    }

    /** Returns the status. */
    public StateStatus getStatus() {
        int statusFlag = flags >>> STATUS_FLAG_OFFSET;
        for (StateStatus status : StateStatus.values()) {
            if (statusFlag == status.getFlag()) {
                return status;
            }
        }
        return null;
    }

    /**
     * Returns {@code true} if this state has never been saved to the
     * database.
     */
    public boolean isNew() {
        return (flags >>> STATUS_FLAG_OFFSET) == 0;
    }

    private boolean checkStatus(StateStatus status) {
        return ((flags >>> STATUS_FLAG_OFFSET) & status.getFlag()) > 0;
    }

    /**
     * Returns {@code true} if this state was recently deleted from the
     * database.
     */
    public boolean isDeleted() {
        return checkStatus(StateStatus.DELETED);
    }

    /**
     * Returns {@code true} if this state only contains the reference
     * to the object (ID and type ID).
     */
    public boolean isReferenceOnly() {
        return checkStatus(StateStatus.REFERENCE_ONLY);
    }

    /**
     * Sets the status. This method will also clear all pending atomic
     * operations and existing validation errors.
     */
    public void setStatus(StateStatus status) {
        flags &= STATUS_FLAG_MASK;

        if (status != null) {
            flags |= status.getFlag() << STATUS_FLAG_OFFSET;
        }

        getExtras().remove(ATOMIC_OPERATIONS_EXTRA);

        if (errors != null) {
            errors.clear();
        }
    }

    /** Returns the map of all the fields values. */
    public Map<String, Object> getValues() {
        resolveReferences();
        return this;
    }

    /** Sets the map of all the values. */
    public void setValues(Map<String, Object> values) {
        clear();
        putAll(values);
    }

    /**
     * Returns a map of all values converted to only simple types:
     * {@code null}, {@link java.lang.Boolean}, {@link java.lang.Number},
     * {@link java.lang.String}, {@link java.util.ArrayList}, or
     * {@link CompactMap}.
     */
    public Map<String, Object> getSimpleValues() {
        return getSimpleValues(false);
    }

    public Map<String, Object> getSimpleValues(boolean withTypeNames) {
        Set<Map.Entry<String, Object>> entries = getValues().entrySet();
        Map<String, Object> values = new CompactMap<>();

        for (Object entryObject : entries.toArray()) {
            if (entryObject == null) {
                continue;
            }

            @SuppressWarnings("unchecked")
            Map.Entry<String, Object> entry = (Map.Entry<String, Object>) entryObject;
            String name = entry.getKey();
            Object value = entry.getValue();
            ObjectField field = getField(name);

            if (field == null) {
                values.put(name, toSimpleValue(value, false, withTypeNames));

            } else if (value != null && !(value instanceof Metric)) {
                values.put(name, toSimpleValue(value, field.isEmbedded(), withTypeNames));
            }
        }

        values.put(StateValueUtils.ID_KEY, getId().toString());
        if (withTypeNames && getType() != null) {
            values.put(StateValueUtils.TYPE_KEY, getType().getInternalName());
        } else {
            values.put(StateValueUtils.TYPE_KEY, getTypeId().toString());
        }
        return values;
    }

    /**
     * Similar to {@link #getSimpleValues()} but only returns those values
     * with fields strictly defined on this State's type.
     * @deprecated No replacement
     * @see #getSimpleValues()
     */
    @Deprecated
    public Map<String, Object> getSimpleFieldedValues() {
        Map<String, Object> values = new CompactMap<>();
        for (Map.Entry<String, Object> e : getValues().entrySet()) {
            String name = e.getKey();
            ObjectField field = getField(name);
            if (field != null) {
                values.put(name, toSimpleValue(e.getValue(), field.isEmbedded(), false));
            }
        }
        values.put(StateValueUtils.ID_KEY, getId().toString());
        values.put(StateValueUtils.TYPE_KEY, getTypeId().toString());
        return values;
    }

    /**
     * Converts the given {@code value} into an instance of one of
     * the simple types listed in {@link #getSimpleValues}.
     */
    protected static Object toSimpleValue(Object value, boolean isEmbedded, boolean withTypeNames) {

        if (value == null) {
            return null;
        }

        Iterable<Object> valueIterable = ObjectToIterable.iterable(value);
        if (valueIterable != null) {
            List<Object> list = new ArrayList<>();
            for (Object item : valueIterable) {
                list.add(toSimpleValue(item, isEmbedded, withTypeNames));
            }
            return list;

        } else if (value instanceof Map) {
            Map<String, Object> map = new CompactMap<>();
            for (Map.Entry<?, ?> entry : ((Map<?, ?>) value).entrySet()) {
                Object key = entry.getKey();
                if (key != null) {
                    map.put(key.toString(), toSimpleValue(entry.getValue(), isEmbedded, withTypeNames));
                }
            }
            return map;

        } else if (value instanceof Query) {
            return ((Query<?>) value).getState().getSimpleValues(withTypeNames);

        } else if (value instanceof Metric) {
            return null;

        } else if (value instanceof Recordable) {
            State valueState = ((Recordable) value).getState();
            if (valueState.isNew()) {
                ObjectType type;
                if (isEmbedded
                        || ((type = valueState.getType()) != null
                        && type.isEmbedded())) {
                    return valueState.getSimpleValues(withTypeNames);
                }
            }

            Map<String, Object> map = new CompactMap<>();
            map.put(StateValueUtils.REFERENCE_KEY, valueState.getId().toString());
            if (withTypeNames && valueState.getType() != null) {
                map.put(StateValueUtils.TYPE_KEY, valueState.getType().getInternalName());
            } else {
                map.put(StateValueUtils.TYPE_KEY, valueState.getTypeId().toString());
            }
            return map;

        } else if (value instanceof Boolean
                || value instanceof Number
                || value instanceof String) {
            return value;

        } else if (value instanceof Character
                || value instanceof CharSequence
                || value instanceof URI
                || value instanceof URL
                || value instanceof UUID) {
            return value.toString();

        } else if (value instanceof Date) {
            return ((Date) value).getTime();

        } else if (value instanceof Enum) {
            return ((Enum<?>) value).name();

        } else if (value instanceof Locale) {
            return ((Locale) value).toLanguageTag();

        } else if (Query.MISSING_VALUE.equals(value)) {
            return Query.SERIALIZED_MISSING_VALUE;

        } else {
            return toSimpleValue(ObjectUtils.to(Map.class, value), isEmbedded, withTypeNames);
        }
    }

    /**
     * Returns the value associated with the given {@code path}.
     *
     * @param path If {@code null}, returns {@code null}.
     * @return May be {@code null}.
     */
    public Object getByPath(String path) {
        if (path == null) {
            return null;
        }

        DatabaseEnvironment environment = getDatabase().getEnvironment();
        Object value = this;

        KEY: for (String key; path != null;) {
            int slashAt = path.indexOf('/');

            if (slashAt > -1) {
                key = path.substring(0, slashAt);
                path = path.substring(slashAt + 1);

            } else {
                key = path;
                path = null;
            }

            if (key.indexOf('.') > -1
                    && environment.getTypeByName(key) != null) {
                continue;
            }

            if (value instanceof Recordable) {
                value = ((Recordable) value).getState();
            }

            if (key.endsWith("()")
                    || ((key.startsWith("get") || key.startsWith("is") || key.startsWith("has")
                    || key.contains(".get") || key.contains(".is") || key.contains(".has"))
                    && (value instanceof State && ((State) value).isMethod(key)))) {
                if (value instanceof State) {

                    Class<?> keyClass = null;

                    if (key.endsWith("()")) {
                        key = key.substring(0, key.length() - 2);
                    }

                    int dotAt = key.lastIndexOf('.');

                    ObjectMethod objMethod = State.getInstance(value).getMethod(key);

                    if (dotAt > -1) {
                        keyClass = ObjectUtils.getClassByName(key.substring(0, dotAt));
                        key = key.substring(dotAt + 1);
                    }

                    if (keyClass == null) {
                        Object valueObj = ((State) value).getOriginalObjectOrNull();
                        if (valueObj != null) {
                            value = valueObj;
                            keyClass = value.getClass();
                        }

                    } else {
                        value = ((State) value).as(keyClass);
                    }

                    Set<String> checkedMods = new HashSet<>();
                    for (Class<?> c = keyClass; c != null; c = c.getSuperclass()) {
                        try {
                            Class<?> modC = null;
                            Method keyMethod;
                            if (objMethod != null) {
                                keyMethod = objMethod.getJavaMethod(c);
                                if (keyMethod == null) {
                                    for (String className : State.getInstance(value).getType().getModificationClassNames()) {
                                        if (checkedMods.contains(className)) {
                                            continue;
                                        }
                                        checkedMods.add(className);
                                        modC = ObjectUtils.getClassByName(className);
                                        if (modC != null) {
                                            keyMethod = objMethod.getJavaMethod(modC);
                                            if (keyMethod != null) {
                                                break;
                                            }
                                        }
                                    }
                                    if (keyMethod == null && Object.class.equals(c)) {
                                        modC = ObjectUtils.getClassByName(objMethod.getJavaDeclaringClassName());
                                        if (modC != null) {
                                            keyMethod = objMethod.getJavaMethod(modC);
                                        }
                                    }
                                }
                            } else {
                                keyMethod = c.getMethod(key);
                            }
                            if (keyMethod == null) {
                                continue;
                            }
                            keyMethod.setAccessible(false);
                            State valueState = State.getInstance(value);
                            Object invokeValue = modC != null ? valueState.as(modC) : valueState.as(c);
                            if (objMethod != null && objMethod.hasSingleObjectMethodParameter()) {
                                value = keyMethod.invoke(invokeValue, objMethod);
                            } else {
                                value = keyMethod.invoke(invokeValue);
                            }
                            continue KEY;

                        } catch (IllegalAccessException error) {
                            throw new IllegalStateException(error);

                        } catch (InvocationTargetException error) {
                            throw Throwables.propagate(error.getCause());

                        } catch (NoSuchMethodException error) {
                            // Try to find the method in the super class.
                        }
                    }
                }

                return null;
            }

            if (value instanceof State) {
                State valueState = (State) value;

                if (ID_KEY.equals(key)) {
                    value = valueState.getId();
                } else if (TYPE_KEY.equals(key)) {
                    value = valueState.getType();
                } else if (LABEL_KEY.equals(key)) {
                    value = valueState.getLabel();
                } else {
                    value = valueState.get(key);
                }

            } else if (value instanceof Map) {
                value = ((Map<?, ?>) value).get(key);

            } else if (value instanceof List) {
                Integer index = ObjectUtils.to(Integer.class, key);

                if (index != null) {
                    List<?> list = (List<?>) value;
                    int listSize = list.size();

                    if (index < 0) {
                        index += listSize;
                    }

                    if (index >= 0 && index < listSize) {
                        value = list.get(index);
                        continue;
                    }
                }

                return null;

            } else {
                return null;
            }
        }

        return value;
    }

    private boolean isMethod(String key) {

        if (getDatabase().getEnvironment().isMethod(key)) {
            return true;
        }

        if (getType() != null) {
            if (getType().isMethod(key)) {
                return true;
            }
        }

        return false;
    }

    public Map<String, Object> getRawValues() {
        return rawValues;
    }

    /**
     * Returns the field associated with the given {@code name} as an
     * instance of the given {@code returnType}. This version of get will
     * not trigger reference resolution which avoids a round-trip to the
     * database.
     */
    public Object getRawValue(String name) {
        Object value = rawValues;

        for (String part : StringUtils.split(name, "/")) {

            if (value == null) {
                break;

            } else if (value instanceof Recordable) {
                value = ((Recordable) value).getState().getByPath(part);

            } else if (value instanceof Map) {
                value = ((Map<?, ?>) value).get(part);

            } else if (value instanceof List) {
                Integer index = ObjectUtils.to(Integer.class, part);
                if (index != null) {
                    List<?> list = (List<?>) value;
                    if (index >= 0 && index < list.size()) {
                        value = list.get(index);
                        continue;
                    }
                }

                value = null;
                break;
            }
        }

        return value;
    }

    /** Puts the given field value at given name. */
    @SuppressWarnings("unchecked")
    public void putByPath(String name, Object value) {
        Map<String, Object> parent = getValues();
        String[] parts = StringUtils.split(name, "/");
        int last = parts.length - 1;
        for (int i = 0; i < last; i ++) {
            String part = parts[i];
            Object child = parent.get(part);
            if (child instanceof Recordable) {
                parent = ((Recordable) child).getState().getValues();
            } else {
                if (!(child instanceof Map)) {
                    child = new CompactMap<String, Object>();
                    parent.put(part, child);
                }
                parent = (Map<String, Object>) child;
            }
        }
        parent.put(parts[last], value);
    }

    /** Returns the indexes for the ObjectType returned by {@link #getType()}
     *  as well as any embedded indexes on this State. */
    public Set<ObjectIndex> getIndexes() {
        Set<ObjectIndex> indexes = new LinkedHashSet<>();
        ObjectType type = getType();
        if (type != null) {
            indexes.addAll(type.getIndexes());

            for (Map.Entry<String, Object> entry : getValues().entrySet()) {
                ObjectField field = getField(entry.getKey());
                if (field != null) {
                    getEmbeddedIndexes(indexes, null, field, entry.getValue());
                }
            }
        }
        return indexes;
    }

    /** Recursively gathers all the embedded Indexes for this State object. */
    private void getEmbeddedIndexes(Set<ObjectIndex> indexes,
            String parentFieldName,
            ObjectField field, Object fieldValue) {

        if (fieldValue instanceof Recordable) {
            State state = State.getInstance(fieldValue);
            ObjectType type = state.getType();
            if (type == null) {
                return;
            }

            if (field.isEmbedded() || type.isEmbedded()) {
                for (ObjectIndex i : type.getIndexes()) {
                    ObjectIndex index = new ObjectIndex(getType(), null);

                    StringBuilder builder = new StringBuilder();
                    StringBuilder builder2 = new StringBuilder();

                    if (parentFieldName != null) {
                        builder.append(parentFieldName).append('/');
                        builder2.append(parentFieldName).append('/');
                    }
                    builder.append(field.getInternalName()).append('/');
                    builder2.append(field.getInternalName()).append('/');
                    builder.append(i.getField());
                    builder2.append(i.getUniqueName());

                    String fieldName = builder.toString();
                    index.setField(fieldName);
                    index.setType(i.getType());
                    index.setUnique(i.isUnique());

                    // get the declaring class of the first level field name
                    int slashAt = fieldName.indexOf('/');
                    String firstLevelFieldName = fieldName.substring(0, slashAt);
                    ObjectField firstLevelField = getField(firstLevelFieldName);
                    if (firstLevelField != null && firstLevelField.getJavaDeclaringClassName() != null) {
                        index.setJavaDeclaringClassName(firstLevelField.getJavaDeclaringClassName());
                    } else {
                        index.setJavaDeclaringClassName(getType().getObjectClassName());
                    }

                    indexes.add(index);

                    ObjectIndex index2 = new ObjectIndex(getType(), null);
                    index2.setName(builder2.toString());
                    index2.setField(index.getField());
                    index2.setType(index.getType());
                    index2.setUnique(index.isUnique());
                    index2.setJavaDeclaringClassName(index.getJavaDeclaringClassName());
                    indexes.add(index2);
                }

                if (parentFieldName == null) {
                    parentFieldName = field.getInternalName();
                } else {
                    parentFieldName += "/" + field.getInternalName();
                }
                Map<String, Object> values = state.getValues();

                for (Map.Entry<String, Object> entry : values.entrySet()) {
                    ObjectField objectField = state.getField(entry.getKey());
                    if (objectField != null) {
                        getEmbeddedIndexes(indexes, parentFieldName,
                                objectField, entry.getValue());
                    }
                }
            }
        } else if (fieldValue instanceof Map) {
            for (Map.Entry<?, ?> entry : ((Map<?, ?>) fieldValue).entrySet()) {
                getEmbeddedIndexes(indexes, parentFieldName, field, entry.getValue());
            }
        } else if (fieldValue instanceof Iterable) {
            for (Object listItem : (Iterable<?>) fieldValue) {
                getEmbeddedIndexes(indexes, parentFieldName, field, listItem);
            }
        }
    }

    /**
     * Returns an unmodifiable list of all the atomic operations pending on
     * this record.
     */
    public List<AtomicOperation> getAtomicOperations() {
        Map<String, Object> extras = getExtras();
        @SuppressWarnings("unchecked")
        List<AtomicOperation> ops = (List<AtomicOperation>) extras.get(ATOMIC_OPERATIONS_EXTRA);

        if (ops == null) {
            ops = new ArrayList<>();
            extras.put(ATOMIC_OPERATIONS_EXTRA, ops);
        }

        return ops;
    }

    // Queues up an atomic operation and updates the internal state.
    private void queueAtomicOperation(AtomicOperation operation) {
        operation.execute(this);
        getAtomicOperations().add(operation);
    }

    /**
     * Increments the field associated with the given {@code name} by the
     * given {@code value}. If the field contains a non-numeric value, it
     * will be set to 0 first.
     */
    public void incrementAtomically(String name, double value) {
        queueAtomicOperation(new AtomicOperation.Increment(name, value));
    }

    /**
     * Decrements the field associated with the given {@code name} by the
     * given {@code value}. If the field contains a non-numeric value, it
     * will be set to 0 first.
     */
    public void decrementAtomically(String name, double value) {
        incrementAtomically(name, -value);
    }

    /**
     * Adds the given {@code value} to the collection field associated with
     * the given {@code name}.
     */
    public void addAtomically(String name, Object value) {
        queueAtomicOperation(new AtomicOperation.Add(name, value));
    }

    /**
     * Removes all instances of the given {@code value} in the collection
     * field associated with the given {@code name}.
     */
    public void removeAtomically(String name, Object value) {
        queueAtomicOperation(new AtomicOperation.Remove(name, value));
    }

    /**
     * Replaces the field value at the given {@code name} with the given
     * {@code value}. If the object changes in the database before
     * {@link #save()} is called, {@link AtomicOperation.ReplacementException}
     * will be thrown.
     */
    public void replaceAtomically(String name, Object value) {
        queueAtomicOperation(new AtomicOperation.Replace(name, getByPath(name), value));
    }

    /**
     * Puts the given {@code value} into the field associated with the
     * given {@code name} atomically.
     */
    public void putAtomically(String name, Object value) {
        queueAtomicOperation(new AtomicOperation.Put(name, value));
    }

    /** Returns all the fields with validation errors from this record. */
    public Set<ObjectField> getErrorFields() {
        return errors != null
                ? Collections.unmodifiableSet(errors.keySet())
                : Collections.<ObjectField>emptySet();
    }

    /**
     * Returns all the validation errors for the given field from this
     * record.
     */
    public List<String> getErrors(ObjectField field) {
        if (errors != null) {
            List<String> messages = errors.get(field);
            if (messages != null && !messages.isEmpty()) {
                return Collections.unmodifiableList(messages);
            }
        }
        return Collections.emptyList();
    }

    /** Returns true if this record has any validation errors. */
    public boolean hasAnyErrors() {
        if (errors != null) {
            for (List<String> messages : errors.values()) {
                if (messages != null && !messages.isEmpty()) {
                    return true;
                }
            }
        }

        ObjectType type = getType();
        if (type != null) {
            for (ObjectField field : type.getFields()) {
                if (hasErrorsForValue(get(field.getInternalName()), field.isEmbedded())) {
                    return true;
                }
            }
        }

        return false;
    }

    private boolean hasErrorsForValue(Object value, boolean embedded) {
        if (value instanceof Map) {
            value = ((Map<?, ?>) value).values();
        }

        if (value instanceof Iterable) {
            for (Object item : (Iterable<?>) value) {
                if (hasErrorsForValue(item, embedded)) {
                    return true;
                }
            }

        } else if (value instanceof Recordable) {
            State valueState = ((Recordable) value).getState();

            if (embedded) {
                if (valueState.hasAnyErrors()) {
                    return true;
                }

            } else {
                ObjectType valueType = valueState.getType();
                if (valueType != null && valueType.isEmbedded() && valueState.hasAnyErrors()) {
                    return true;
                }
            }
        }

        return false;
    }

    /**
     * Returns true if the given field in this record has any validation
     * errors.
     */
    public boolean hasErrors(ObjectField field) {
        return errors != null && errors.get(field).size() > 0;
    }

    public void clearAllErrors() {
        errors = null;
    }

    /** Returns a modifiable map of all the extras values from this state. */
    public Map<String, Object> getExtras() {
        if (extras == null) {
            extras = new CompactMap<>();
        }
        return extras;
    }

    /**
     * Returns the extra value associated with the given {@code name} from
     * this state.
     */
    public Object getExtra(String name) {
        return extras != null ? extras.get(name) : null;
    }

    public void addError(ObjectField field, String message) {

        if (errors == null) {
            errors = new CompactMap<>();
        }

        List<String> messages = errors.get(field);
        if (messages == null) {
            messages = new ArrayList<>();
            errors.put(field, messages);
        }
        messages.add(message);
    }

    public boolean isResolveToReferenceOnly() {
        return (flags & RESOLVE_TO_REFERENCE_ONLY_FLAG) != 0;
    }

    public void setResolveToReferenceOnly(boolean resolveToReferenceOnly) {
        if (resolveToReferenceOnly) {
            flags |= RESOLVE_TO_REFERENCE_ONLY_FLAG;
        } else {
            flags &= ~RESOLVE_TO_REFERENCE_ONLY_FLAG;
        }
    }

    public boolean isResolveUsingCache() {
        return (flags & RESOLVE_WITHOUT_CACHE) == 0;
    }

    public void setResolveUsingCache(boolean resolveUsingCache) {
        if (resolveUsingCache) {
            flags &= ~RESOLVE_WITHOUT_CACHE;
        } else {
            flags |= RESOLVE_WITHOUT_CACHE;
        }
    }

    public boolean isResolveUsingMaster() {
        return (flags & RESOLVE_USING_MASTER) != 0;
    }

    public void setResolveUsingMaster(boolean resolveUsingMaster) {
        if (resolveUsingMaster) {
            flags |= RESOLVE_USING_MASTER;
        } else {
            flags &= ~RESOLVE_USING_MASTER;
        }
    }

    public boolean isResolveInvisible() {
        return (flags & RESOLVE_INVISIBLE) != 0;
    }

    public void setResolveInvisible(boolean resolveInvisible) {
        if (resolveInvisible) {
            flags |= RESOLVE_INVISIBLE;
        } else {
            flags &= ~RESOLVE_INVISIBLE;
        }
    }

    /**
     * Returns a descriptive label for this state.
     */
    public String getLabel() {
        Object object = getOriginalObjectOrNull();
        String label = null;

        if (object instanceof Record) {
            try {
                label = ((Record) object).getLabel();

            } catch (RuntimeException error) {
                // Ignore errors from a bad implementation of #getLabel
                // and fall back to the default label algorithm.
            }
        }

        return ObjectUtils.isBlank(label)
                ? getDefaultLabel()
                : label;
    }

    // To check for circular references in resolving labels.
    private static final ThreadLocal<Map<UUID, String>> LABEL_CACHE = new ThreadLocal<>();

    /**
     * Returns the default, descriptive label for this state.
     * Fields specified in {@link Recordable.LabelFields} are used to
     * construct it. If the referenced field value contains a state,
     * it may call itself, but not more than once per state.
     */
    protected String getDefaultLabel() {
        ObjectType type = getType();
        if (type != null) {

            StringBuilder label = new StringBuilder();
            for (String field : type.getLabelFields()) {
                Object value = getByPath(field);
                if (value != null) {

                    String valueString;
                    if (value instanceof Recordable) {
                        State valueState = ((Recordable) value).getState();
                        UUID valueId = valueState.getId();

                        Map<UUID, String> cache = LABEL_CACHE.get();
                        boolean isFirst = false;
                        if (cache == null) {
                            cache = new HashMap<>();
                            LABEL_CACHE.set(cache);
                            isFirst = true;
                        }

                        try {
                            if (cache.containsKey(valueId)) {
                                valueString = cache.get(valueId);
                                if (valueString == null) {
                                    valueString = valueId.toString();
                                }

                            } else {
                                cache.put(valueId, null);
                                valueString = valueState.getLabel();
                                cache.put(valueId, valueString);
                            }

                        } finally {
                            if (isFirst) {
                                LABEL_CACHE.remove();
                            }
                        }

                    } else if (value instanceof Iterable<?>) {
                        StringBuilder iterableLabel = new StringBuilder();
                        iterableLabel.append('[');

                        for (Object item : (Iterable<?>) value) {
                            if (item instanceof Recordable) {
                                iterableLabel.append(((Recordable) item).getState().getLabel());
                            } else {
                                iterableLabel.append(item.toString());
                            }
                            iterableLabel.append(", ");
                        }

                        if (iterableLabel.length() > 2) {
                            iterableLabel.setLength(iterableLabel.length() - 2);
                        }
                        iterableLabel.append(']');
                        valueString = iterableLabel.toString();

                    } else {
                        valueString = value.toString();
                    }

                    if (valueString.length() > 0) {
                        label.append(valueString);
                        label.append(' ');
                    }
                }
            }

            if (label.length() > 0) {
                label.setLength(label.length() - 1);
                return label.toString();
            }
        }

        return getId().toString();
    }

    /** Returns a storage item that can be used to preview this state. */
    public StorageItem getPreview() {
        ObjectType type = getType();
        if (type != null) {
            String field = type.getPreviewField();
            if (!ObjectUtils.isBlank(field)) {
                Object previewObject = getByPath(field);
                return previewObject instanceof StorageItem ? (StorageItem) previewObject : null;
            }
        }
        return null;
    }

    /**
     * Returns the label that identifies the current visibility in effect.
     *
     * @return May be {@code null}.
     * @see VisibilityLabel
     */
    public String getVisibilityLabel() {
        @SuppressWarnings("unchecked")
        List<String> visibilities = (List<String>) get("dari.visibilities");

        if (visibilities != null && !visibilities.isEmpty()) {
            String fieldName = visibilities.get(visibilities.size() - 1);
            ObjectField field = getField(fieldName);

            if (field != null) {
                Class<?> fieldDeclaring = ObjectUtils.getClassByName(field.getJavaDeclaringClassName());

                if (fieldDeclaring != null
                        && VisibilityLabel.class.isAssignableFrom(fieldDeclaring)) {
                    return ((VisibilityLabel) as(fieldDeclaring)).createVisibilityLabel(field);
                }
            }

            return ObjectUtils.to(String.class, get(fieldName));
        }

        return null;
    }

    public void prefetch() {
        prefetch(getValues());
    }

    private void prefetch(Object object) {
        if (object instanceof Map) {
            for (Object item : ((Map<?, ?>) object).values()) {
                prefetch(item);
            }
        } else if (object instanceof Iterable) {
            for (Object item : (Iterable<?>) object) {
                prefetch(item);
            }
        }
    }

    /**
     * Returns an instance of the given {@code modificationClass} linked
     * to this state.
     */
    public <T> T as(Class<T> objectClass) {
        @SuppressWarnings("unchecked")
        T object = (T) linkedObjects.get(objectClass);
        if (object == null) {
            object = TypeDefinition.getInstance(objectClass).newInstance();
            ((Recordable) object).setState(this);
            copyRawValuesToJavaFields(object);
        }
        return object;
    }

    /**
     * Fires the given {@code trigger} on all objects (the original, the
     * modifications, and the embedded) associated with this state.
     *
     * @param trigger Can't be {@code null}.
     */
    public void fireTrigger(Trigger trigger) {
        fireTrigger(trigger, true);
    }

    /**
     * Fires the given {@code trigger} on all objects (the original and the
     * modifications) associated with this state.
     *
     * @param trigger Can't be {@code null}.
     * @param recursive If {@code true}, fires the given {@code trigger}
     * recursively on all embedded objects.
     */
    @SuppressWarnings("unchecked")
    public void fireTrigger(Trigger trigger, boolean recursive) {

        List<Class<?>> triggerGlobalModifications = getType() == null ? null : (List<Class<?>>) getType().getState().getExtra("trigger.globalModifications." + trigger.getClass().getName());

        if (triggerGlobalModifications == null) {
            triggerGlobalModifications = new ArrayList<>();
            for (ObjectType modType : getDatabase().getEnvironment().getTypesByGroup(Modification.class.getName())) {
                if (modType.isAbstract()) {
                    continue;
                }

                Class<?> modClass = modType.getObjectClass();

                if (modClass != null
                        && Modification.class.isAssignableFrom(modClass)
                        && Modification.Static.getModifiedClasses((Class<? extends Modification<?>>) modClass).contains(Object.class)
                        && !trigger.isMissing(modClass)) {
                    triggerGlobalModifications.add(modClass);
                }
            }
            if (getType() != null) {
                synchronized (getType().getState().getExtras()) {
                    getType().getState().getExtras().put("trigger.globalModifications." + trigger.getClass().getName(), triggerGlobalModifications);
                }
            }
        }

        for (Class<?> modClass : triggerGlobalModifications) {
            if (modClass != null
                    && Modification.class.isAssignableFrom(modClass)
                    && Modification.Static.getModifiedClasses((Class<? extends Modification<?>>) modClass).contains(Object.class)) {
                fireModificationTrigger(trigger, modClass);
            }
        }

        ObjectType type = getType();

        if (type == null
                || type.isAbstract()) {
            return;
        }

        // Type-specific modifications.
        TYPE_CHANGE: while (true) {
            List<Class<?>> triggerTypeModifications = (List<Class<?>>) type.getState().getExtra("trigger.typeModifications." + trigger.getClass().getName());
            if (triggerTypeModifications == null) {
                triggerTypeModifications = new ArrayList<>();
                for (String modClassName : type.getModificationClassNames()) {
                    Class<?> modClass = ObjectUtils.getClassByName(modClassName);

                    if (modClass != null && !trigger.isMissing(modClass)) {
                        triggerTypeModifications.add(modClass);
                    }
                }
                synchronized (type.getState().getExtras()) {
                    type.getState().getExtras().put("trigger.typeModifications." + trigger.getClass().getName(), triggerTypeModifications);
                }
            }
            for (Class<?> modClass : triggerTypeModifications) {
                fireModificationTrigger(trigger, modClass);

                ObjectType newType = getType();

                if (!type.equals(newType)) {
                    type = newType;
                    continue TYPE_CHANGE;
                }
            }

            break;
        }

        // Embedded objects.
        if (recursive) {
            List<ObjectField> recordFields = (List<ObjectField>) type.getState().getExtra("trigger.recordFields");
            if (recordFields == null) {
                recordFields = new ArrayList<>();
                for (ObjectField field : type.getFields()) {
                    if (ObjectField.RECORD_TYPE.equals(field.getInternalItemType())) {
                        recordFields.add(field);
                    }
                }
                synchronized (type.getState().getExtras()) {
                    type.getState().getExtras().put("trigger.recordFields", recordFields);
                }
            }
            for (ObjectField field : recordFields) {
                Object value = get(field.getInternalName());
                if (value != null) {
                    fireValueTrigger(trigger, value, field.isEmbedded());
                }
            }
        }
    }

    private void fireModificationTrigger(Trigger trigger, Class<?> modClass) {
        if (trigger.isMissing(modClass)) {
            return;
        }
        Object modObject;

        try {
            modObject = as(modClass);

        } catch (RuntimeException ex) {
            return;
        }

        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(
                    "Firing trigger [{}] from [{}] on [{}]",
                    trigger,
                    modClass.getName(),
                    State.getInstance(modObject));
        }
        trigger.execute(modObject);
    }

    private void fireValueTrigger(Trigger trigger, Object value, boolean embedded) {
        if (value instanceof Map) {
            value = ((Map<?, ?>) value).values();
        }

        if (value instanceof Iterable) {
            for (Object item : (Iterable<?>) value) {
                fireValueTrigger(trigger, item, embedded);
            }

        } else if (value instanceof Recordable) {
            State valueState = ((Recordable) value).getState();

            Class<?> valueClass = valueState.getType() != null ? valueState.getType().getObjectClass() : null;
            if (valueClass == null || trigger.isMissing(valueClass)) {
                return;
            }

            if (embedded) {
                valueState.fireTrigger(trigger);

            } else {
                ObjectType valueType = valueState.getType();

                if (valueType != null && valueType.isEmbedded()) {
                    valueState.fireTrigger(trigger);
                }
            }
        }
    }

    /**
     * Returns the original object, which is an instance of the Java class
     * associated with the type.
     *
     * @return May be {@code null}.
     */
    public Object getOriginalObjectOrNull() {
        ObjectType type = getType();

        if (type != null) {
            Class<?> objectClass = type.getObjectClass();

            if (objectClass == null) {
                objectClass = Record.class;
            }

            for (Object object : linkedObjects.values()) {
                if (objectClass.equals(object.getClass())) {
                    return object;
                }
            }

            return as(objectClass);
        }

        return null;
    }

    /**
     * Returns the original object, which is an instance of the Java class
     * associated with the type.
     *
     * @return Never {@code null}.
     * @throws IllegalStateException If the state is type-less or the type
     * isn't associated with a Java class.
     */
    public Object getOriginalObject() {
        Object object = getOriginalObjectOrNull();

        if (object != null) {
            return object;

        } else {
            throw new IllegalStateException(String.format(
                    "No original object associated with [%s]!",
                    getId()));
        }
    }

    /** Returns a set of all objects that can be used with this state. */
    @SuppressWarnings("all")
    public Collection<Record> getObjects() {
        List<Record> objects = new ArrayList<Record>();
        objects.addAll((Collection) linkedObjects.values());
        return objects;
    }

    public void beforeFieldGet(String name) {
        List<Listener> listeners = LISTENERS_LOCAL.get();

        if (listeners != null && !listeners.isEmpty()) {
            for (Listener listener : listeners) {
                listener.beforeFieldGet(this, name);
            }
        }
    }

    /**
     * Resolves the reference possibly stored in the given {@code field}.
     * This method doesn't need to be used directly in typical cases, because
     * it will be called automatically by {@link LazyLoadEnhancer}.
     *
     * @param field If {@code null}, resolves all references.
     */
    public void resolveReference(String field) {
        if ((flags & ALL_RESOLVED_FLAG) != 0) {
            return;
        }

        /*
        if (field != null) {
            Object value = rawValues.get(field);

            if (value == null) {
                ObjectField f = getField(field);

                if (f != null && f.isMetric()) {
                    Profiler.Static.startThreadEvent(RESOLVE_REFERENCE_PROFILER_EVENT, this, field);

                    try {
                        put(f.getInternalName(), new Metric(this, f));

                    } finally {
                        Profiler.Static.stopThreadEvent();
                    }
                }

            } else if (!linkedObjects.isEmpty()) {
                UUID id = StateValueUtils.toIdIfReference(value);

                if (id != null) {
                    Profiler.Static.startThreadEvent(RESOLVE_REFERENCE_PROFILER_EVENT, this, field);

                    try {
                        Object object = linkedObjects.values().iterator().next();
                        Map<UUID, Object> references = StateValueUtils.resolveReferences(getDatabase(), object, Collections.singleton(value), field);

                        put(field, references.get(id));

                    } finally {
                        Profiler.Static.stopThreadEvent();
                    }
                }
            }

            return;
        }
        */

        synchronized (this) {
            if ((flags & ALL_RESOLVED_FLAG) != 0) {
                return;
            }

            flags |= ALL_RESOLVED_FLAG;

            if (linkedObjects.isEmpty()) {
                return;
            }

            boolean hasPotentialRefs = false;
            Collection<Object> rawValuesValues = rawValues.values();

            for (Object rawValue : rawValuesValues) {
                if (rawValue instanceof Map
                        && ((Map<?, ?>) rawValue).containsKey(StateValueUtils.REFERENCE_KEY)) {
                    hasPotentialRefs = true;
                    break;
                }
            }

            if (!hasPotentialRefs) {
                return;
            }

            Profiler.Static.startThreadEvent(RESOLVE_REFERENCE_PROFILER_EVENT, this);

            try {
                Object object = linkedObjects.values().iterator().next();
                Map<UUID, Object> references = StateValueUtils.resolveReferences(getDatabase(), object, rawValuesValues, field);
                Map<String, Object> resolved = new HashMap<>();
                resolveMetricReferences(resolved);

                for (Map.Entry<? extends String, ?> e : rawValues.entrySet()) {
                    UUID id = StateValueUtils.toIdIfReference(e.getValue());
                    if (id != null) {
                        resolved.put(e.getKey(), references.get(id));
                    }
                }

                for (Map.Entry<String, Object> e : resolved.entrySet()) {
                    put(e.getKey(), e.getValue());
                }

            } finally {
                Profiler.Static.stopThreadEvent();
            }
        }
    }

    /**
     * Instantiate all Metric objects.
     */
    private void resolveMetricReferences(Map<String, Object> map) {
        for (Object obj : linkedObjects.values()) {
            ObjectType type = getDatabase().getEnvironment().getTypeByClass(obj.getClass());
            if (type != null) {
                for (ObjectField metricField : type.getMetricFields()) {
                    map.put(metricField.getInternalName(), new Metric(this, metricField));
                }
            }
        }

        for (ObjectField metricField : getDatabase().getEnvironment().getMetricFields()) {
            map.put(metricField.getInternalName(), new Metric(this, metricField));
        }
    }

    /**
     * Resolves all references to other objects. This method doesn't need to
     * be used directly in typical cases, because it will be called
     * automatically by {@link LazyLoadEnhancer}.
     */
    public void resolveReferences() {
        resolveReference(null);
    }

    private static class OnValidateTrigger extends TriggerOnce {

        @Override
        public void executeOnce(Object object) {
            if (object instanceof Record) {
                ((Record) object).onValidate();
            }
        }
    }

    /**
     * Returns {@code true} if the field values in this state is valid.
     * The validation rules are typically read from annotations such as
     * {@link Recordable.Required}.
     */
    public boolean validate() {
        ObjectType type = getType();
        if (type != null) {
            for (ObjectField field : type.getFields()) {
                field.validate(this);
                validateValue(get(field.getInternalName()), field.isEmbedded());
            }
        }

        DatabaseEnvironment environment = getDatabase().getEnvironment();
        for (ObjectField field : environment.getFields()) {
            field.validate(this);
        }

        fireTrigger(new OnValidateTrigger());

        return !hasAnyErrors();
    }

    private void validateValue(Object value, boolean embedded) {
        if (value instanceof Map) {
            value = ((Map<?, ?>) value).values();
        }

        if (value instanceof Iterable) {
            for (Object item : (Iterable<?>) value) {
                validateValue(item, embedded);
            }

        } else if (value instanceof Recordable) {
            State valueState = ((Recordable) value).getState();

            if (embedded) {
                valueState.validate();

            } else {
                ObjectType valueType = valueState.getType();
                if (valueType != null && valueType.isEmbedded()) {
                    valueState.validate();
                }
            }
        }
    }

    private void copyJavaFieldsToRawValues() {
        DatabaseEnvironment environment = getDatabase().getEnvironment();

        for (Object object : linkedObjects.values()) {
            Class<?> objectClass = object.getClass();
            ObjectType type = environment.getTypeByClass(objectClass);
            if (type == null) {
                continue;
            }

            for (ObjectField field : type.getFields()) {
                Field javaField = field.getJavaField(objectClass);
                if (javaField == null
                        || !javaField.getDeclaringClass().getName().equals(field.getJavaDeclaringClassName())) {
                    continue;
                }

                try {
                    rawValues.put(field.getInternalName(), javaField.get(object));
                } catch (IllegalAccessException ex) {
                    throw new IllegalStateException(ex);
                }
            }
        }
    }

    void copyRawValuesToJavaFields(Object object) {
        Class<?> objectClass = object.getClass();
        ObjectType type = getDatabase().getEnvironment().getTypeByClass(objectClass);
        if (type == null) {
            return;
        }

        for (ObjectField field : type.getFields()) {
            String key = field.getInternalName();
            Object value = StateValueUtils.toJavaValue(getDatabase(), object, field, field.getInternalType(), rawValues.get(key));
            rawValues.put(key, value);

            Field javaField = field.getJavaField(objectClass);
            if (javaField != null) {
                setJavaField(field, javaField, object, key, value);
            }
        }
    }

    private static final Converter CONVERTER; static {
        CONVERTER = new Converter();

        CONVERTER.setThrowError(true);
        CONVERTER.putAllStandardFunctions();
        CONVERTER.putInheritableFunction(Query.class, Query.class, new QueryToQuery());
    }

    @SuppressWarnings("rawtypes")
    private static class QueryToQuery implements ConversionFunction<Query, Query> {

        @Override
        public Query convert(Converter converter, Type returnType, Query query) {
            return query;
        }
    }

    private void setJavaField(
            ObjectField field,
            Field javaField,
            Object object,
            String key,
            Object value) {

        if (!javaField.getDeclaringClass().getName().equals(field.getJavaDeclaringClassName())) {
            return;
        }

        try {
            Type javaFieldType = javaField.getGenericType();

            if ((!javaField.getType().isPrimitive()
                    && !Number.class.isAssignableFrom(javaField.getType()))
                    && (javaFieldType instanceof Class
                    || ((value instanceof StateValueList
                    || value instanceof StateValueMap
                    || value instanceof StateValueSet)
                    && ObjectField.RECORD_TYPE.equals(field.getInternalItemType())))) {
                try {
                    javaField.set(object, value);
                    return;

                } catch (IllegalArgumentException error) {
                    // Ignore since it will be retried below.
                }
            }

            try {
                if (javaFieldType instanceof TypeVariable) {
                    javaField.set(object, value);

                } else if (javaFieldType instanceof Class
                        && ((Class<?>) javaFieldType).isPrimitive()) {
                    javaField.set(object, ObjectUtils.to(javaFieldType, value));

                } else {
                    javaField.set(object, CONVERTER.convert(javaFieldType, value));
                }

            } catch (RuntimeException error) {
                Throwable cause;

                if (error instanceof ConversionException) {
                    cause = error.getCause();

                    if (cause == null) {
                        cause = error;
                    }

                } else {
                    cause = error;
                }

                rawValues.put("dari.trash." + key, value);
                rawValues.put("dari.trashError." + key, cause.getClass().getName());
                rawValues.put("dari.trashErrorMessage." + key, cause.getMessage());
            }

        } catch (IllegalAccessException error) {
            throw new IllegalStateException(error);
        }
    }

    @Override
    public void clear() {
        rawValues.clear();

        DatabaseEnvironment environment = getDatabase().getEnvironment();

        for (Object object : linkedObjects.values()) {
            Class<?> objectClass = object.getClass();
            ObjectType type = environment.getTypeByClass(objectClass);
            if (type == null) {
                continue;
            }

            for (ObjectField field : type.getFields()) {
                Field javaField = field.getJavaField(objectClass);
                if (javaField == null) {
                    continue;
                }

                try {
                    javaField.set(object, ObjectUtils.to(javaField.getGenericType(), null));
                } catch (IllegalAccessException ex) {
                    throw new IllegalStateException(ex);
                }
            }
        }
    }

    @Override
    public boolean containsKey(Object key) {
        return rawValues.containsKey(key);
    }

    @Override
    public boolean containsValue(Object value) {
        copyJavaFieldsToRawValues();
        return rawValues.containsValue(value);
    }

    @Nonnull
    @Override
    public Set<Map.Entry<String, Object>> entrySet() {
        copyJavaFieldsToRawValues();
        return rawValues.entrySet();
    }

    @Override
    public Object get(Object key) {
        if (!(key instanceof String)) {
            return null;
        }

        resolveReferences();

        Object originalObject = getOriginalObjectOrNull();

        for (Object object : linkedObjects.values()) {
            Class<?> objectClass = object.getClass();

            ObjectType type = getDatabase().getEnvironment().getTypeByClass(objectClass);
            if (type == null) {
                continue;
            }

            if (!Modification.class.isAssignableFrom(objectClass)
                    && (originalObject == null
                    || !originalObject.getClass().equals(objectClass))) {
                continue;
            }

            ObjectField field = type.getField((String) key);
            if (field == null) {
                continue;
            }

            Field javaField = field.getJavaField(objectClass);
            if (javaField == null) {
                continue;
            }

            Object value;
            try {
                value = javaField.get(object);
            } catch (IllegalAccessException ex) {
                throw new IllegalStateException(ex);
            }

            rawValues.put(field.getInternalName(), value);
            return value;
        }

        return rawValues.get(key);
    }

    @Override
    public boolean isEmpty() {
        return false;
    }

    @Nonnull
    @Override
    public Set<String> keySet() {
        return rawValues.keySet();
    }

    @Override
    public Object put(String key, Object value) {
        if (key == null) {
            return null;
        }

        if (key.startsWith("_")) {
            if (key.equals(StateValueUtils.ID_KEY)) {
                setId(ObjectUtils.to(UUID.class, value));
            } else if (key.equals(StateValueUtils.TYPE_KEY)) {
                UUID valueTypeId = ObjectUtils.to(UUID.class, value);

                if (valueTypeId != null) {
                    setTypeId(valueTypeId);
                } else {
                    if (value != null) {
                        ObjectType valueType = getDatabase().getEnvironment().getTypeByName(ObjectUtils.to(String.class, value));

                        if (valueType != null) {
                            setTypeId(valueType.getId());
                        }
                    }
                }
            }
            return null;
        }

        boolean first =  true;
        for (Object object : linkedObjects.values()) {
            ObjectField field = State.getInstance(object).getField(key);
            if (first) {
                value = StateValueUtils.toJavaValue(getDatabase(), object, field, field != null ? field.getInternalType() : null, value);
                first = false;
            }

            if (field != null) {
                Field javaField = field.getJavaField(object.getClass());
                if (javaField != null) {
                    setJavaField(field, javaField, object, key, value);
                }
            }
        }

        return rawValues.put(key, value);
    }

    @Override
    public void putAll(@Nonnull Map<? extends String, ?> map) {
        Preconditions.checkNotNull(map);

        if (!linkedObjects.isEmpty()) {
            Object object = linkedObjects.values().iterator().next();

            if (object != null && getType() != null && getType().isLazyLoaded()) {
                for (Map.Entry<? extends String, ?> e : map.entrySet()) {
                    String key = e.getKey();
                    Object value = e.getValue();
                    if (StateValueUtils.toIdIfReference(value) != null) {
                        rawValues.put(key, value);
                    } else {
                        put(key, value);
                    }
                }
                Map<String, Object> metricObjects = new HashMap<>();
                resolveMetricReferences(metricObjects);
                for (Map.Entry<? extends String, ?> e : metricObjects.entrySet()) {
                    put(e.getKey(), e.getValue());
                }
                flags &= ~ALL_RESOLVED_FLAG;
                return;

            } else {
                rawValues.putAll(map);
                resolveReferences();
            }
        }

        for (Map.Entry<? extends String, ?> e : map.entrySet()) {
            put(e.getKey(), e.getValue());
        }
    }

    @Override
    public Object remove(Object key) {
        if (key instanceof String) {
            Object oldValue = put((String) key, null);
            rawValues.remove(key);
            return oldValue;

        } else {
            return null;
        }
    }

    @Override
    public int size() {
        return rawValues.size() + 2;
    }

    @Nonnull
    @Override
    public Collection<Object> values() {
        copyJavaFieldsToRawValues();
        return rawValues.values();
    }

    // --- Object support ---

    @Override
    public boolean equals(Object other) {

        if (this == other) {
            return true;

        } else if (other instanceof State) {
            State otherState = (State) other;
            return getId().equals(otherState.getId());

        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return getId().hashCode();
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("database", getDatabase())
                .add("status", getStatus())
                .add("id", getId())
                .add("typeId", getTypeId())
                .add("simpleValues", getSimpleValues())
                .add("extras", extras)
                .add("errors", errors)
                .toString();
    }

    // --- JSTL support ---

    public Map<String, Object> getAs() {
        @SuppressWarnings("unchecked")
        Map<String, Object> as = (Map<String, Object>) getExtras().get(MODIFICATIONS_EXTRA);

        if (as == null) {
            as = new LoadingCacheMap<>(String.class, CacheBuilder
                    .newBuilder()
                    .<String, Object>build(new CacheLoader<String, Object>() {

                @Override
                @ParametersAreNonnullByDefault
                public Object load(String modificationClassName) {
                    Class<?> modificationClass = ObjectUtils.getClassByName(modificationClassName);

                    if (modificationClass != null) {
                        return as(modificationClass);

                    } else {
                        throw new IllegalArgumentException(String.format(
                                "[%s] isn't a valid class name!", modificationClassName));
                    }
                }
            }));

            extras.put(MODIFICATIONS_EXTRA, as);
        }

        return as;
    }

    // --- Database bridge ---

    /** @see Database#beginWrites() */
    public boolean beginWrites() {
        return getDatabase().beginWrites();
    }

    /** @see Database#commitWrites() */
    public boolean commitWrites() {
        return getDatabase().commitWrites();
    }

    /** @see Database#endWrites() */
    public boolean endWrites() {
        return getDatabase().endWrites();
    }

    /**
     * {@linkplain Database#save Saves} this state to the
     * {@linkplain #getDatabase originating database}.
     */
    public void save() {
        getDatabase().save(this);
    }

    /**
     * Saves this state {@linkplain Database#beginIsolatedWrites immediately}
     * to the {@linkplain #getDatabase originating database}.
     */
    public void saveImmediately() {
        Database database = getDatabase();
        database.beginIsolatedWrites();
        try {
            database.save(this);
            database.commitWrites();
        } finally {
            database.endWrites();
        }
    }

    /**
     * Saves this state {@linkplain Database#commitWritesEventually eventually}
     * to the {@linkplain #getDatabase originating database}.
     */
    public void saveEventually() {
        Database database = getDatabase();
        database.beginWrites();
        try {
            database.save(this);
            database.commitWritesEventually();
        } finally {
            database.endWrites();
        }
    }

    /**
     * {@linkplain Database#saveUnsafely Saves} this state to the
     * {@linkplain #getDatabase originating database} without validating
     * the data.
     */
    public void saveUnsafely() {
        getDatabase().saveUnsafely(this);
    }

    public void saveUniquely() {
        ObjectType type = getType();

        if (type == null) {
            throw new IllegalStateException("No type!");
        }

        Query<?> query = Query.fromType(type).noCache().master();

        for (ObjectIndex index : type.getIndexes()) {
            if (index.isUnique()) {
                for (String field : index.getFields()) {
                    Object value = get(field);
                    query.and(field + " = ?", value != null ? value : Query.MISSING_VALUE);
                }
            }
        }

        if (query.getPredicate() == null) {
            throw new IllegalStateException("No unique indexes!");
        }

        Exception lastError = null;

        for (int i = 0; i < 10; ++ i) {
            Object object = query.first();

            if (object != null) {
                setValues(State.getInstance(object).getValues());
                return;

            } else {
                try {
                    saveImmediately();
                    return;

                } catch (Exception error) {
                    lastError = error;
                }
            }
        }

        throw Throwables.propagate(lastError);
    }

    /**
     * {@linkplain Database#index Indexes} this state data in the
     * {@linkplain #getDatabase originating database}.
     */
    public void index() {
        getDatabase().index(this);
    }

    /**
     * {@linkplain Database#delete Deletes} this state from the
     * {@linkplain #getDatabase originating database}.
     */
    public void delete() {
        getDatabase().delete(this);
    }

    /**
     * Deletes this state {@linkplain Database#beginIsolatedWrites immediately}
     * from the {@linkplain #getDatabase originating database}.
     */
    public void deleteImmediately() {
        Database database = getDatabase();
        database.beginIsolatedWrites();
        try {
            database.delete(this);
            database.commitWrites();
        } finally {
            database.endWrites();
        }
    }

    public abstract static class Listener {
        public void beforeFieldGet(State state, String name) {
        }
    }

    /** {@link State} utility methods. */
    public static final class Static {

        public static void addListener(Listener listener) {
            List<Listener> listeners = LISTENERS_LOCAL.get();

            if (listeners == null) {
                listeners = new ArrayList<>();
                LISTENERS_LOCAL.set(listeners);
            }

            listeners.add(listener);
        }

        public static void removeListener(Listener listener) {
            List<Listener> listeners = LISTENERS_LOCAL.get();

            if (listeners != null) {
                listeners.remove(listener);

                if (listeners.isEmpty()) {
                    LISTENERS_LOCAL.remove();
                }
            }
        }
    }

    // --- Deprecated ---

    /** @deprecated Use {@link #getSimpleValues} instead. */
    @Deprecated
    public Map<String, Object> getJsonObject() {
        return getSimpleValues();
    }

    /** @deprecated Use {@link #getSimpleValues} and {@link ObjectUtils#toJson} instead. */
    @Deprecated
    public String getJsonString() {
        return ObjectUtils.toJson(getSimpleValues());
    }

    /** @deprecated Use {@link #setValues} and {@link ObjectUtils#fromJson} instead. */
    @Deprecated
    @SuppressWarnings("unchecked")
    public void setJsonString(String json) {
        setValues((Map<String, Object>) ObjectUtils.fromJson(json));
    }

    /** @deprecated Use {@link #setStatus} instead. */
    @Deprecated
    public void markSaved() {
        setStatus(StateStatus.SAVED);
    }

    /** @deprecated Use {@link #setStatus} instead. */
    @Deprecated
    public void markDeleted() {
        setStatus(StateStatus.DELETED);
    }

    /** @deprecated Use {@link #setStatus} instead. */
    @Deprecated
    public void markReadonly() {
        setStatus(StateStatus.REFERENCE_ONLY);
    }

    /** @deprecated Use {@link #isResolveToReferenceOnly} instead. */
    @Deprecated
    public boolean isResolveReferenceOnly() {
        return isResolveToReferenceOnly();
    }

    /** @deprecated Use {@link #isResolveReferenceOnly} instead. */
    @Deprecated
    public void setResolveReferenceOnly(boolean isResolveReferenceOnly) {
        setResolveToReferenceOnly(isResolveReferenceOnly);
    }

    /** @deprecated Use {@link #incrementAtomically} instead. */
    @Deprecated
    public void incrementValue(String name, double value) {
        incrementAtomically(name, value);
    }

    /** @deprecated Use {@link #decrementAtomically} instead. */
    @Deprecated
    public void decrementValue(String name, double value) {
        decrementAtomically(name, value);
    }

    /** @deprecated Use {@link #addAtomically} instead. */
    @Deprecated
    public void addValue(String name, Object value) {
        addAtomically(name, value);
    }

    /** @deprecated Use {@link #removeAtomically} instead. */
    @Deprecated
    public void removeValue(String name, Object value) {
        removeAtomically(name, value);
    }

    /** @deprecated Use {@link #replaceAtomically} instead. */
    @Deprecated
    public void replaceValue(String name, Object value) {
        replaceAtomically(name, value);
    }

    /** @deprecated Use {@link #getByPath} instead. */
    @Deprecated
    public Object getValue(String path) {
        return getByPath(path);
    }

    /** @deprecated Use {@link #putByPath} instead. */
    @Deprecated
    public void putValue(String path, Object value) {
        putByPath(path, value);
    }
}
