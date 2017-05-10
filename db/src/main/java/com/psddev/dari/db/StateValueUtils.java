package com.psddev.dari.db;

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

import com.psddev.dari.util.ObjectMap;
import com.psddev.dari.util.ObjectUtils;
import com.psddev.dari.util.StorageItem;

/** State value utility methods. */
abstract class StateValueUtils {

    /**
     * Thread local map used for detecting circular references in
     * {@link #resolveReferences}.
     */
    private static final ThreadLocal<Map<UUID, Object>> CIRCULAR_REFERENCES = new ThreadLocal<Map<UUID, Object>>();

    /** Converts the given {@code object} into an ID if it's a reference. */
    public static UUID toIdIfReference(Object object) {
        return object instanceof Map
                ? ObjectUtils.to(UUID.class, ((Map<?, ?>) object).get(StateSerializer.REFERENCE_KEY))
                : null;
    }

    public static Object toObjectIfReference(Database database, Object object) {
        if (object instanceof Map) {
            Map<?, ?> objectMap = (Map<?, ?>) object;
            UUID id = ObjectUtils.to(UUID.class, objectMap.get(StateSerializer.REFERENCE_KEY));

            if (id != null) {
                UUID typeId = ObjectUtils.to(UUID.class, objectMap.get(StateSerializer.TYPE_KEY));
                ObjectType type;
                if (typeId != null) {
                    type = database.getEnvironment().getTypeById(typeId);
                } else {
                    type = database.getEnvironment().getTypeByName(ObjectUtils.to(String.class, objectMap.get(StateSerializer.TYPE_KEY)));
                }
                if (type == null || type.isAbstract()) {
                    return database.readFirst(Query.from(Object.class).where("_id = ?", id));
                }

                Object reference = type.createObject(id);
                State referenceState = State.getInstance(reference);
                referenceState.setStatus(StateStatus.REFERENCE_ONLY);
                referenceState.setResolveToReferenceOnly(true);
                return reference;
            }
        }

        return null;
    }

    /** Resolves all object references within the given {@code items}. */
    public static Map<UUID, Object> resolveReferences(Database database, Object parent, Iterable<?> items, String field) {
        State parentState = State.getInstance(parent);

        if (parentState != null && parentState.isResolveToReferenceOnly()) {
            Map<UUID, Object> references = new HashMap<UUID, Object>();
            for (Object item : items) {
                Object itemReference = toObjectIfReference(database, item);
                if (itemReference != null) {
                    references.put(State.getInstance(itemReference).getId(), itemReference);
                }
            }
            return references;
        }

        if (parent instanceof Modification) {
            for (Object item : parentState.getObjects()) {
                if (!(item instanceof Modification)) {
                    parent = item;
                    break;
                }
            }
            if (parent instanceof Modification) {
                parent = null;
            }
        }

        boolean isFirst = false;
        try {
            Map<UUID, Object> circularReferences = CIRCULAR_REFERENCES.get();
            if (circularReferences == null) {
                isFirst = true;
                circularReferences = new HashMap<UUID, Object>();
                CIRCULAR_REFERENCES.set(circularReferences);
            }

            if (parentState != null) {
                circularReferences.put(parentState.getId(), parent);
            }

            // Find IDs that have not been resolved yet.
            Map<UUID, Object> references = new HashMap<UUID, Object>();
            Set<UUID> unresolvedIds = new HashSet<UUID>();
            Set<UUID> unresolvedTypeIds = new HashSet<UUID>();
            for (Object item : items) {
                UUID id = toIdIfReference(item);
                if (id != null) {
                    if (circularReferences.containsKey(id)) {
                        references.put(id, circularReferences.get(id));
                    } else if (parentState != null && parentState.getExtras().containsKey(State.SUB_DATA_STATE_EXTRA_PREFIX + id)) {
                       references.put(id, parentState.getExtras().get(State.SUB_DATA_STATE_EXTRA_PREFIX + id));
                    } else {
                        unresolvedIds.add(id);
                        unresolvedTypeIds.add(ObjectUtils.to(UUID.class, ((Map<?, ?>) item).get(StateSerializer.TYPE_KEY)));
                    }
                }
            }

            // Fetch unresolved objects and cache them.
            if (!unresolvedIds.isEmpty()) {
                Query<?> query = Query
                        .from(Object.class)
                        .where("_id = ?", unresolvedIds)
                        .using(database)
                        .option(State.REFERENCE_RESOLVING_QUERY_OPTION, parent)
                        .option(State.REFERENCE_FIELD_QUERY_OPTION, field)
                        .option(State.UNRESOLVED_TYPE_IDS_QUERY_OPTION, unresolvedTypeIds);

                if (parentState != null) {
                    if (!parentState.isResolveUsingCache()) {
                        query.setCache(false);
                    }

                    if (parentState.isResolveUsingMaster()) {
                        query.setMaster(true);
                    }
                }

                for (Object object : query.selectAll()) {
                    UUID id = State.getInstance(object).getId();

                    unresolvedIds.remove(id);
                    circularReferences.put(id, object);
                    references.put(id, object);
                }

                for (UUID id : unresolvedIds) {
                    circularReferences.put(id, null);
                }
            }

            for (Iterator<Map.Entry<UUID, Object>> i = references.entrySet().iterator(); i.hasNext();) {
                Map.Entry<UUID, Object> entry = i.next();
                Object object = entry.getValue();

                if ((parentState == null
                        || !parentState.isResolveInvisible())
                        && object != null
                        && !ObjectUtils.isBlank(State.getInstance(object).getRawValue("dari.visibilities"))) {
                    entry.setValue(null);
                }
            }

            return references;

        } finally {
            if (isFirst) {
                CIRCULAR_REFERENCES.remove();
            }
        }
    }

    public static Map<UUID, Object> resolveReferences(Database database, Object parent, Iterable<?> items) {
        return resolveReferences(database, parent, items, null);
    }

    /**
     * Converts the given {@code value} to an instance of the type that
     * matches the given {@code field} and {@code type} and is most
     * commonly used in Java.
     */
    public static Object toJavaValue(
            Database database,
            Object object,
            ObjectField field,
            String type,
            Object value) {

        if (value == null && (field == null || !field.isMetric())) {
            return null;
        }

        UUID valueId = toIdIfReference(value);
        if (valueId != null) {
            Map<UUID, Object> references = resolveReferences(database, object, Collections.singleton(value));
            value = references.get(valueId);
            if (value == null) {
                return null;
            }
        }

        if (field == null || type == null) {
            return value;
        }

        int slashAt = type.indexOf('/');
        String firstType;
        String subType;

        if (slashAt > -1) {
            firstType = type.substring(0, slashAt);
            subType = type.substring(slashAt + 1);

        } else {
            firstType = type;
            subType = null;
        }

        Converter converter = CONVERTERS.get(firstType);
        if (converter == null) {
            return value;
        }

        try {
            return converter.toJavaValue(database, object, field, subType, value);

        } catch (Exception error) {
            if (object != null) {
                State state = State.getInstance(object);
                String name = field.getInternalName();

                state.put("dari.trash." + name, value);
                state.put("dari.trashError." + name, error.getClass().getName());
                state.put("dari.trashErrorMessage." + name, error.getMessage());
            }

            return null;
        }
    }

    /**
     * Interface that defines how to convert between various
     * representations of a state value.
     */
    private interface Converter {
        Object toJavaValue(
                Database database,
                Object object,
                ObjectField field,
                String subType,
                Object value)
                throws Exception;
    }

    private static final Map<String, Converter> CONVERTERS; static {
        Map<String, Converter> m = new HashMap<String, Converter>();

        m.put(ObjectField.DATE_TYPE, new Converter() {
            @Override
            public Object toJavaValue(
                    Database database,
                    Object object,
                    ObjectField field,
                    String subType,
                    Object value) {

                if (value instanceof Date) {
                    return value;

                } else if (value instanceof Number) {
                    return new Date(((Number) value).longValue());

                } else {
                    return ObjectUtils.to(Date.class, value);
                }
            }
        });

        m.put(ObjectField.FILE_TYPE, new Converter() {
            @Override
            public Object toJavaValue(
                    Database database,
                    Object object,
                    ObjectField field,
                    String subType,
                    Object value) {

                if (value instanceof StorageItem) {
                    return value;

                } else if (value instanceof String) {
                    return StorageItem.Static.createUrl((String) value);

                } else if (value instanceof Map) {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> map = (Map<String, Object>) value;
                    StorageItem item = StorageItem.Static.createIn(ObjectUtils.to(String.class, map.get("storage")));
                    new ObjectMap(item).putAll(map);
                    return item;

                } else {
                    throw new IllegalArgumentException();
                }
            }
        });

        m.put(ObjectField.LIST_TYPE, new Converter() {
            @Override
            public Object toJavaValue(
                    Database database,
                    Object object,
                    ObjectField field,
                    String subType,
                    Object value) {

                if (value instanceof StateValueList) {
                    return value;

                } else {
                    Iterable<?> iterable = ObjectUtils.to(Iterable.class, value);
                    return new StateValueList(database, object, field, subType, iterable);
                }
            }
        });

        m.put(ObjectField.LOCATION_TYPE, new Converter() {
            @Override
            public Object toJavaValue(
                    Database database,
                    Object object,
                    ObjectField field,
                    String subType,
                    Object value) {

                if (value instanceof Location) {
                    return value;

                } else if (value instanceof Map) {
                    Map<?, ?> map = (Map<?, ?>) value;
                    Double x = ObjectUtils.to(Double.class, map.get("x"));
                    Double y = ObjectUtils.to(Double.class, map.get("y"));
                    if (x != null && y != null) {
                        return new Location(x, y);
                    }
                }

                throw new IllegalArgumentException();
            }
        });

        m.put(ObjectField.REGION_TYPE, new Converter() {
            @Override
            public Object toJavaValue(
                    Database database,
                    Object object,
                    ObjectField field,
                    String subType,
                    Object value) {

                if (value instanceof Region) {
                    return value;

                } else if (value instanceof Map) {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> map = (Map<String, Object>) value;

                    Region region = Region.parseGeoJson(map);
                    Region.parseCircles(region, (List<List<Double>>) map.get("circles"));

                    return region;
                }

                throw new IllegalArgumentException();
            }
        });

        m.put(ObjectField.MAP_TYPE, new Converter() {
            @Override
            public Object toJavaValue(
                    Database database,
                    Object object,
                    ObjectField field,
                    String subType,
                    Object value) {

                if (value instanceof StateValueMap) {
                    return value;

                } else if (value instanceof Map) {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> map = (Map<String, Object>) value;
                    return new StateValueMap(database, object, field, subType, map);

                } else {
                    throw new IllegalArgumentException();
                }
            }
        });

        m.put(ObjectField.METRIC_TYPE, new Converter() {
            @Override
            public Object toJavaValue(
                    Database database,
                    Object object,
                    ObjectField field,
                    String subType,
                    Object value) {

                if (value instanceof Metric) {
                    return value;

                } else {
                    Metric metric = new Metric(State.getInstance(object), field);
                    return metric;
                }
            }
        });

        m.put(ObjectField.RECORD_TYPE, new Converter() {
            @Override
            public Object toJavaValue(
                    Database database,
                    Object object,
                    ObjectField field,
                    String subType,
                    Object value) {

                if (value instanceof Map) {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> valueMap = (Map<String, Object>) value;
                    Object typeId = valueMap.get(StateSerializer.TYPE_KEY);

                    if (typeId != null) {
                        State objectState = State.getInstance(object);
                        DatabaseEnvironment environment = objectState.getDatabase().getEnvironment();
                        ObjectType valueType = environment.getTypeById(ObjectUtils.to(UUID.class, typeId));

                        if (valueType == null) {
                            valueType = environment.getTypeByName(ObjectUtils.to(String.class, typeId));
                        }

                        if (valueType != null) {
                            value = valueType.createObject(ObjectUtils.to(UUID.class, valueMap.get(StateSerializer.ID_KEY)));
                            State valueState = State.getInstance(value);

                            valueState.setDatabase(database);
                            valueState.setResolveToReferenceOnly(objectState.isResolveToReferenceOnly());
                            valueState.setResolveInvisible(objectState.isResolveInvisible());
                            valueState.putAll(valueMap);
                        }
                    }

                } else if (!(value instanceof Recordable)) {
                    UUID id = ObjectUtils.to(UUID.class, value);

                    if (id != null) {
                        value = Query.fromAll().where("_id = ?", id).first();
                    }
                }

                if (value == null) {
                    throw new IllegalArgumentException();
                }

                ObjectType valueType = State.getInstance(value).getType();
                UUID valueTypeId = valueType.getId();
                Set<ObjectType> fieldTypes = field.findConcreteTypes();

                if (!fieldTypes.isEmpty()
                        && !fieldTypes.stream().anyMatch(type -> type.getId().equals(valueTypeId))) {

                    Object source = value;
                    Object target = fieldTypes.stream()
                            .map(type -> database.getEnvironment().getStateValueAdapter(valueTypeId, type.getId()))
                            .filter(Objects::nonNull)
                            .map(adapter -> adapter.adapt(source))
                            .filter(Objects::nonNull)
                            .findFirst()
                            .orElse(null);

                    if (target != null) {
                        value = target;
                    }
                }

                return value;
            }
        });

        m.put(ObjectField.REFERENTIAL_TEXT_TYPE, new Converter() {
            @Override
            public Object toJavaValue(
                    Database database,
                    Object object,
                    ObjectField field,
                    String subType,
                    Object value) {

                if (value instanceof ReferentialText) {
                    return value;

                } else {
                    ReferentialText text = new ReferentialText();

                    text.setResolveInvisible(State.getInstance(object).isResolveInvisible());

                    if (value instanceof Iterable) {
                        boolean isFirst = false;

                        try {
                            Map<UUID, Object> circularReferences = StateValueUtils.CIRCULAR_REFERENCES.get();

                            if (circularReferences == null) {
                                isFirst = true;
                                circularReferences = new HashMap<UUID, Object>();
                                StateValueUtils.CIRCULAR_REFERENCES.set(circularReferences);
                            }

                            if (object != null) {
                                State objectState = State.getInstance(object);

                                circularReferences.put(objectState.getId(), object);
                            }

                            for (Object item : (Iterable<?>) value) {
                                text.add(item);
                            }

                        } finally {
                            if (isFirst) {
                                StateValueUtils.CIRCULAR_REFERENCES.remove();
                            }
                        }

                    } else {
                        text.add(value.toString());
                    }

                    return text;
                }
            }
        });

        m.put(ObjectField.SET_TYPE, new Converter() {
            @Override
            public Object toJavaValue(
                    Database database,
                    Object object,
                    ObjectField field,
                    String subType,
                    Object value) {

                if (value instanceof StateValueSet) {
                    return value;

                } else {
                    Iterable<?> iterable = ObjectUtils.to(Iterable.class, value);
                    return new StateValueSet(database, object, field, subType, iterable);
                }
            }
        });

        m.put(ObjectField.TEXT_TYPE, new Converter() {
            @Override
            public Object toJavaValue(
                    Database database,
                    Object object,
                    ObjectField field,
                    String subType,
                    Object value) {

                if (value instanceof byte[]) {
                    value = new String((byte[]) value, StandardCharsets.UTF_8);
                }

                String enumClassName = field.getJavaEnumClassName();
                Class<?> enumClass = ObjectUtils.getClassByName(enumClassName);
                if (enumClass != null && Enum.class.isAssignableFrom(enumClass)) {
                    return ObjectUtils.to(enumClass, value);
                }

                return value.toString();
            }
        });

        m.put(ObjectField.URI_TYPE, new Converter() {
            @Override
            public Object toJavaValue(
                    Database database,
                    Object object,
                    ObjectField field,
                    String subType,
                    Object value)
                    throws URISyntaxException {

                if (value instanceof URI) {
                    return value;

                } else {
                    return new URI(value.toString());
                }
            }
        });

        m.put(ObjectField.URL_TYPE, new Converter() {
            @Override
            public Object toJavaValue(
                    Database database,
                    Object object,
                    ObjectField field,
                    String subType,
                    Object value)
                    throws MalformedURLException {

                if (value instanceof URL) {
                    return value;

                } else {
                    return new URL(value.toString());
                }
            }
        });

        m.put(ObjectField.UUID_TYPE, new Converter() {
            @Override
            public Object toJavaValue(
                    Database database,
                    Object object,
                    ObjectField field,
                    String subType,
                    Object value) {

                if (value instanceof UUID) {
                    return value;

                } else {
                    UUID uuid = ObjectUtils.to(UUID.class, value);
                    if (uuid != null) {
                        return uuid;
                    }
                }

                throw new IllegalArgumentException();
            }
        });

        m.put(ObjectField.LOCALE_TYPE, new Converter() {
            @Override
            public Object toJavaValue(
                    Database database,
                    Object object,
                    ObjectField field,
                    String subType,
                    Object value) {

                if (value instanceof Locale) {
                    return value;

                } else {
                    Locale locale = ObjectUtils.to(Locale.class, value);
                    if (locale != null) {
                        return locale;
                    }
                }

                throw new IllegalArgumentException();
            }
        });

        m.put(ObjectField.ANY_TYPE, new Converter() {
            @Override
            public Object toJavaValue(
                    Database database,
                    Object object,
                    ObjectField field,
                    String subType,
                    Object value) {

                if (Query.SERIALIZED_MISSING_VALUE.equals(value)) {
                    return Query.MISSING_VALUE;
                }
                return value;
            }
        });

        CONVERTERS = m;
    }
}
