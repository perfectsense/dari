package com.psddev.dari.db;

import java.beans.BeanInfo;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.beans.SimpleBeanInfo;
import java.lang.reflect.Method;
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.psddev.dari.db.Recordable.TypePostProcessorClasses;
import com.psddev.dari.util.ClassFinder;
import com.psddev.dari.util.CodeUtils;
import com.psddev.dari.util.Lazy;
import com.psddev.dari.util.ObjectUtils;
import com.psddev.dari.util.Once;
import com.psddev.dari.util.PeriodicCache;
import com.psddev.dari.util.Task;
import com.psddev.dari.util.TypeDefinition;

public class DatabaseEnvironment implements ObjectStruct {

    public static final String GLOBAL_FIELDS_FIELD = "globalFields";
    public static final String GLOBAL_INDEXES_FIELD = "globalIndexes";

    private static final Logger LOGGER = LoggerFactory.getLogger(DatabaseEnvironment.class);

    private final Database database;
    private final boolean initializeClasses;

    {
        CodeUtils.addRedefineClassesListener(new CodeUtils.RedefineClassesListener() {
            @Override
            public void redefined(Set<Class<?>> classes) {
                for (Class<?> c : classes) {
                    if (Recordable.class.isAssignableFrom(c)) {
                        TypeDefinition.Static.invalidateAll();
                        refreshTypes();
                        Introspector.flushCaches();
                        dynamicProperties.reset();
                        break;
                    }
                }
            }
        });
    }

    /** Creates a new instance backed by the given {@code database}. */
    public DatabaseEnvironment(Database database, boolean initializeClasses) {
        this.database = database;
        this.initializeClasses = initializeClasses;
    }

    /** Creates a new instance backed by the given {@code database}. */
    public DatabaseEnvironment(Database database) {
        this(database, true);
    }

    /** Returns the backing database. */
    public Database getDatabase() {
        return database;
    }

    // --- Globals and types cache ---

    /** Globals are stored at FFFFFFFF-FFFF-FFFF-FFFF-FFFFFFFFFFFF. */
    private static final UUID GLOBALS_ID = new UUID(-1L, -1L);

    /** Field where the root type is stored within the globals. */
    private static final String ROOT_TYPE_FIELD = "rootType";

    private volatile State globals;
    private volatile Date lastGlobalsUpdate;
    private volatile Date lastTypesUpdate;
    private volatile TypesCache permanentTypes = new TypesCache();
    private final ThreadLocal<TypesCache> temporaryTypesLocal = new ThreadLocal<TypesCache>();

    /** Aggregate of all maps used to cache type information. */
    private static class TypesCache {

        public final Map<String, ObjectType> byClassName = new HashMap<String, ObjectType>();
        public final Map<UUID, ObjectType> byId = new HashMap<UUID, ObjectType>();
        public final Map<String, ObjectType> byName = new HashMap<String, ObjectType>();
        public final Map<String, Set<ObjectType>> byGroup = new HashMap<String, Set<ObjectType>>();
        public final Set<UUID> changed = new HashSet<UUID>();

        /** Adds the given {@code type} to all type cache maps. */
        public void add(ObjectType type) {
            String className = type.getObjectClassName();
            if (!ObjectUtils.isBlank(className)) {
                byClassName.put(className.toLowerCase(Locale.ENGLISH), type);
            }

            byId.put(type.getId(), type);

            String internalName = type.getInternalName();
            if (!ObjectUtils.isBlank(internalName)) {
                byName.put(internalName.toLowerCase(Locale.ENGLISH), type);
            }

            for (String group : type.getGroups()) {
                if (group != null) {
                    group = group.toLowerCase(Locale.ENGLISH);
                }

                Set<ObjectType> groupTypes = byGroup.get(group);
                if (groupTypes == null) {
                    groupTypes = new HashSet<ObjectType>();
                    byGroup.put(group, groupTypes);
                }
                groupTypes.remove(type);
                groupTypes.add(type);
            }
        }
    }

    private final Once bootstrapOnce = new Once() {

        @Override
        protected void run() {

            // Fetch the globals, which includes a reference to the root
            // type. References to other objects can't be resolved,
            // because the type definitions haven't been loaded yet.
            refreshGlobals();

            ObjectType rootType = getRootType();
            if (rootType != null) {

                // This isn't cleared later, because that's done within
                // {@link refreshTypes} anyway.
                TypesCache temporaryTypes = temporaryTypesLocal.get();
                if (temporaryTypes == null) {
                    temporaryTypes = new TypesCache();
                    temporaryTypesLocal.set(temporaryTypes);
                }
                temporaryTypes.add(rootType);
                LOGGER.info(
                        "Root type ID for [{}] is [{}]",
                        getDatabase().getName(),
                        rootType.getId());
            }

            // Load all other types based on the root type. Then globals
            // again in case they reference other typed objects. Then
            // types again using the information from the fully resolved
            // globals.
            refreshTypes();
            refreshGlobals();
            refreshTypes();

            refresher.scheduleWithFixedDelay(5.0, 5.0);
        }
    };

    /** Task for updating the globals and the types periodically. */
    private final Task refresher = new Task(PeriodicCache.TASK_EXECUTOR_NAME, null) {
        @Override
        public void doTask() {
            Database database = getDatabase();

            Date newGlobalsUpdate = Query
                    .from(Object.class)
                    .where("_id = ?", GLOBALS_ID)
                    .using(database)
                    .lastUpdate();
            if (newGlobalsUpdate != null
                    && (lastGlobalsUpdate == null
                    || newGlobalsUpdate.after(lastGlobalsUpdate))) {
                refreshGlobals();
            }

            Date newTypesUpdate = Query
                    .from(ObjectType.class)
                    .using(database)
                    .lastUpdate();
            if (newTypesUpdate != null
                    && (lastTypesUpdate == null
                    || newTypesUpdate.after(lastTypesUpdate))) {
                refreshTypes();
            }
        }
    };

    // Cache that contains ObjectType.PostProcessors.
    private static final LoadingCache<Class<? extends ObjectType.PostProcessor>, ObjectType.PostProcessor> TYPE_POST_PROCESSORS = CacheBuilder.newBuilder()
            .weakKeys()
            .build(new CacheLoader<Class<? extends ObjectType.PostProcessor>, ObjectType.PostProcessor>() {

                @Override
                public ObjectType.PostProcessor load(Class<? extends ObjectType.PostProcessor> processorClass) {
                    return (ObjectType.PostProcessor) TypeDefinition.getInstance(processorClass).newInstance();
                }
            });

    /** Immediately refreshes all globals using the backing database. */
    public synchronized void refreshGlobals() {
        bootstrapOnce.ensure();

        Database database = getDatabase();
        LOGGER.info("Loading globals from [{}]", database.getName());

        Query<Object> globalsQuery = Query
                .from(Object.class)
                .where("_id = ?", GLOBALS_ID)
                .using(database)
                .noCache();

        State newGlobals = State.getInstance(globalsQuery.first());

        if (newGlobals == null) {
            newGlobals = State.getInstance(globalsQuery.master().first());
        }

        if (newGlobals == null) {
            newGlobals = new State();
            newGlobals.setDatabase(database);
            newGlobals.setId(GLOBALS_ID);
            newGlobals.save();
        }

        globals = newGlobals;
        lastGlobalsUpdate = new Date();
        fieldsCache.reset();
        metricFieldsCache.reset();
        indexesCache.reset();
    }

    /** Immediately refreshes all types using the backing database. */
    public synchronized void refreshTypes() {
        bootstrapOnce.ensure();

        Database database = getDatabase();
        try {

            TypesCache temporaryTypes = temporaryTypesLocal.get();
            if (temporaryTypes == null) {
                temporaryTypes = new TypesCache();
                temporaryTypesLocal.set(temporaryTypes);
            }

            List<ObjectType> types = Query
                    .from(ObjectType.class)
                    .using(database)
                    .selectAll();
            int typesSize = types.size();
            LOGGER.info("Loading [{}] types from [{}]", typesSize, database.getName());

            // Load all types from the database first.
            for (ObjectType type : types) {
                type.getFields().size(); // Pre-fetch.
                temporaryTypes.add(type);
            }

            if (initializeClasses) {

                // Make sure that the root type exists.
                ObjectType rootType = getRootType();
                State rootTypeState;

                if (rootType != null) {
                    rootTypeState = rootType.getState();

                } else {
                    rootType = new ObjectType();
                    rootTypeState = rootType.getState();
                    rootTypeState.setDatabase(database);
                }

                Map<String, Object> rootTypeOriginals = rootTypeState.getSimpleValues();
                UUID rootTypeId = rootTypeState.getId();
                rootTypeState.setTypeId(rootTypeId);
                rootTypeState.clear();
                rootType.setObjectClassName(ObjectType.class.getName());
                rootType.initialize();
                temporaryTypes.add(rootType);

                try {
                    database.beginWrites();

                    // Make the new root type available to other types.
                    temporaryTypes.add(rootType);
                    if (rootTypeState.isNew()) {
                        State globals = getGlobals();
                        globals.put(ROOT_TYPE_FIELD, rootType);
                        globals.save();

                    } else if (!rootTypeState.getSimpleValues().equals(rootTypeOriginals)) {
                        temporaryTypes.changed.add(rootTypeId);
                    }

                    Set<Class<? extends Recordable>> objectClasses = ClassFinder.Static.findClasses(Recordable.class);

                    for (Iterator<Class<? extends Recordable>> i = objectClasses.iterator(); i.hasNext();) {
                        Class<? extends Recordable> objectClass = i.next();

                        try {
                            if (objectClass.isAnonymousClass()) {
                                i.remove();
                            }

                        } catch (IncompatibleClassChangeError error) {
                            i.remove();
                        }
                    }

                    Set<Class<?>> globalModifications = new HashSet<Class<?>>();
                    Map<ObjectType, List<Class<?>>> typeModifications = new HashMap<ObjectType, List<Class<?>>>();

                    // Make sure all types are accessible to the rest of the
                    // system as soon as possible, so that references can be
                    // resolved properly later.
                    for (Class<?> objectClass : objectClasses) {
                        ObjectType type = getTypeByClass(objectClass);

                        if (type == null) {
                            type = new ObjectType();
                            type.getState().setDatabase(database);

                        } else {
                            type.getState().clear();
                        }

                        type.setObjectClassName(objectClass.getName());
                        typeModifications.put(type, new ArrayList<Class<?>>());
                        temporaryTypes.add(type);
                    }

                    // Separate out all modifications from regular types.
                    for (Class<?> objectClass : objectClasses) {
                        if (!Modification.class.isAssignableFrom(objectClass)) {
                            continue;
                        }

                        @SuppressWarnings("unchecked")
                        Set<Class<?>> modifiedClasses = Modification.Static.getModifiedClasses((Class<? extends Modification<?>>) objectClass);
                        if (modifiedClasses.contains(Object.class)) {
                            globalModifications.add(objectClass);
                            continue;
                        }

                        for (Class<?> modifiedClass : modifiedClasses) {
                            List<Class<?>> assignableClasses = new ArrayList<Class<?>>();

                            for (Class<?> c : objectClasses) {
                                if (modifiedClass.isAssignableFrom(c)) {
                                    assignableClasses.add(c);
                                }
                            }

                            for (Class<?> assignableClass : assignableClasses) {
                                ObjectType type = getTypeByClass(assignableClass);

                                if (type != null) {
                                    List<Class<?>> modifications = typeModifications.get(type);
                                    if (modifications == null) {
                                        modifications = new ArrayList<Class<?>>();
                                        typeModifications.put(type, modifications);
                                    }
                                    modifications.add(objectClass);
                                }
                            }
                        }
                    }

                    // Apply global modifications.
                    for (Class<?> modification : globalModifications) {
                        ObjectType.modifyAll(database, modification);
                    }

                    // Initialize all types.
                    List<Class<?>> rootTypeModifications = typeModifications.remove(rootType);
                    initializeAndModify(temporaryTypes, rootType, rootTypeModifications);

                    if (rootTypeModifications != null) {
                        for (Class<?> modification : rootTypeModifications) {
                            ObjectType t = getTypeByClass(modification);
                            initializeAndModify(temporaryTypes, t, typeModifications.remove(t));
                        }
                    }

                    ObjectType fieldType = getTypeByClass(ObjectField.class);
                    List<Class<?>> fieldModifications = typeModifications.remove(fieldType);
                    initializeAndModify(temporaryTypes, fieldType, fieldModifications);

                    if (fieldModifications != null) {
                        for (Class<?> modification : fieldModifications) {
                            ObjectType t = getTypeByClass(modification);
                            initializeAndModify(temporaryTypes, t, typeModifications.remove(t));
                        }
                    }

                    for (Map.Entry<ObjectType, List<Class<?>>> entry : typeModifications.entrySet()) {
                        initializeAndModify(temporaryTypes, entry.getKey(), entry.getValue());
                    }

                    database.commitWrites();

                } finally {
                    database.endWrites();
                }
            }

            // Merge temporary types into new permanent types.
            TypesCache newPermanentTypes = new TypesCache();

            for (ObjectType type : permanentTypes.byId.values()) {
                newPermanentTypes.add(type);
            }

            for (ObjectType type : temporaryTypes.byId.values()) {
                newPermanentTypes.add(type);
            }

            newPermanentTypes.changed.addAll(temporaryTypes.changed);
            newPermanentTypes.changed.addAll(permanentTypes.changed);

            // If any types changed, clear all types' extras.
            if (!temporaryTypes.changed.isEmpty()) {
                for (ObjectType type : newPermanentTypes.byId.values()) {
                    type.getState().getExtras().clear();
                }
            }

            permanentTypes = newPermanentTypes;
            lastTypesUpdate = new Date();

        } finally {
            temporaryTypesLocal.remove();
        }

        ObjectType singletonType = getTypeByClass(Singleton.class);

        if (singletonType != null) {
            for (ObjectType type : singletonType.findConcreteTypes()) {
                if (!Query
                        .fromType(type)
                        .where("_type = ?", type)
                        .master()
                        .noCache()
                        .hasMoreThan(0)) {
                    try {
                        State.getInstance(type.createObject(null)).saveImmediately();
                    } catch (Exception error) {
                        LOGGER.warn(String.format("Can't save [%s] singleton!", type.getLabel()), error);
                    }
                }
            }
        }

        for (ObjectType type : getTypes()) {
            Class<?> objectClass = type.getObjectClass();
            if (objectClass != null) {
                TypePostProcessorClasses tppcAnnotation = objectClass.getAnnotation(TypePostProcessorClasses.class);
                if (tppcAnnotation != null) {
                    for (Class<? extends ObjectType.PostProcessor> processorClass : tppcAnnotation.value()) {
                        ObjectType.PostProcessor processor = (ObjectType.PostProcessor) TYPE_POST_PROCESSORS.getUnchecked(processorClass);
                        processor.process(type);
                    }
                }
            }
        }
    }

    private static void initializeAndModify(TypesCache temporaryTypes, ObjectType type, List<Class<?>> modifications) {
        State typeState = type.getState();
        Map<String, Object> typeOriginals = typeState.getSimpleValues();

        try {
            type.initialize();
            temporaryTypes.add(type);

            // Apply type-specific modifications.
            if (modifications != null) {
                for (Class<?> modification : modifications) {
                    type.modify(modification);
                }
            }

        } catch (IncompatibleClassChangeError ex) {
            LOGGER.info(
                    "Skipped initializing [{}] because its class is in an inconsistent state! ([{}])",
                    type.getInternalName(),
                    ex.getMessage());
        }

        if (typeState.isNew()) {
            type.save();

        } else if (!typeState.getSimpleValues().equals(typeOriginals)) {
            temporaryTypes.changed.add(type.getId());
        }
    }

    /**
     * Returns all global values.
     *
     * @return May be {@code null}.
     */
    public State getGlobals() {
        bootstrapOnce.ensure();
        return globals;
    }

    // Returns the root type from the globals.
    private ObjectType getRootType() {
        State globals = getGlobals();

        if (globals != null) {
            Object rootType = globals.get(ROOT_TYPE_FIELD);

            if (rootType == null) {
                rootType = globals.get("rootRecordType");
            }

            if (rootType instanceof ObjectType) {
                return (ObjectType) rootType;
            }
        }

        return null;
    }

    // --- ObjectStruct support ---

    @Override
    public DatabaseEnvironment getEnvironment() {
        return this;
    }

    @Override
    public List<ObjectField> getFields() {
        return new ArrayList<ObjectField>(fieldsCache.get().values());
    }

    private final transient Lazy<Map<String, ObjectField>> fieldsCache = new Lazy<Map<String, ObjectField>>() {

        @Override
        @SuppressWarnings("unchecked")
        protected Map<String, ObjectField> create() {
            State globals = getGlobals();
            Object definitions = globals != null ? globals.get(GLOBAL_FIELDS_FIELD) : null;

            return ObjectField.Static.convertDefinitionsToInstances(
                    DatabaseEnvironment.this,
                    definitions instanceof List
                            ? (List<Map<String, Object>>) definitions
                            : null);
        }
    };

    @Override
    public ObjectField getField(String name) {
        return fieldsCache.get().get(name);
    }

    @Override
    public void setFields(List<ObjectField> fields) {
        getGlobals().put(GLOBAL_FIELDS_FIELD, ObjectField.Static.convertInstancesToDefinitions(fields));
        fieldsCache.reset();
        metricFieldsCache.reset();
    }

    /**
     * Returns the method associated with the given {@code name} in this type.
     *
     * @param name If {@code null}, returns {@code null}.
     * @return May be {@code null}.
     */
    public ObjectMethod getMethod(String name) {
        if (name == null) {
            return null;
        }

        ObjectField field = getField(name);

        if (field instanceof ObjectMethod) {
            return (ObjectMethod) field;
        } else {
            return null;
        }

    }

    /**
     * Returns all of the methods that Dari is aware of.
     */
    public List<ObjectMethod> getMethods() {
        List<ObjectMethod> methods = new ArrayList<ObjectMethod>();
        for (ObjectField field : getFields()) {
            if (field instanceof ObjectMethod) {
                methods.add((ObjectMethod) field);
            }
        }
        return methods;
    }

    /**
     * Returns whether the given {@code name} is an ObjectMethod in this type.
     *
     * @param name If {@code null}, returns {@code false}.
     */
    public boolean isMethod(String name) {
        return getMethod(name) != null;
    }

    public List<ObjectField> getMetricFields() {
        return metricFieldsCache.get();
    }

    private final transient Lazy<List<ObjectField>> metricFieldsCache = new Lazy<List<ObjectField>>() {

        @Override
        protected List<ObjectField> create() {
            List<ObjectField> metricFields = new ArrayList<ObjectField>();

            for (ObjectField field : getFields()) {
                if (field.isMetric()) {
                    metricFields.add(field);
                }
            }

            return metricFields;
        }
    };

    @Override
    public List<ObjectIndex> getIndexes() {
        return new ArrayList<ObjectIndex>(indexesCache.get().values());
    }

    private final transient Lazy<Map<String, ObjectIndex>> indexesCache = new Lazy<Map<String, ObjectIndex>>() {

        @Override
        @SuppressWarnings("unchecked")
        protected Map<String, ObjectIndex> create() {
            State globals = getGlobals();
            Object definitions = globals != null ? globals.get(GLOBAL_INDEXES_FIELD) : null;

            return ObjectIndex.Static.convertDefinitionsToInstances(
                    DatabaseEnvironment.this,
                    definitions instanceof List
                            ? (List<Map<String, Object>>) definitions
                            : null);
        }
    };

    @Override
    public ObjectIndex getIndex(String name) {
        return indexesCache.get().get(name);
    }

    @Override
    public void setIndexes(List<ObjectIndex> indexes) {
        getGlobals().put(GLOBAL_INDEXES_FIELD, ObjectIndex.Static.convertInstancesToDefinitions(indexes));
        indexesCache.reset();
    }

    /**
     * Initializes the given {@code objectClasses} so that they are
     * usable as {@linkplain ObjectType types}.
     */
    public void initializeTypes(Iterable<Class<?>> objectClasses) {
        bootstrapOnce.ensure();

        Set<String> classNames = new HashSet<String>();
        for (Class<?> objectClass : objectClasses) {
            classNames.add(objectClass.getName());
        }

        for (ObjectType type : getTypes()) {
            UUID id = type.getId();
            if (classNames.contains(type.getObjectClassName())) {
                TypesCache temporaryTypes = temporaryTypesLocal.get();
                if ((temporaryTypes != null
                        && temporaryTypesLocal.get().changed.contains(id))
                        || permanentTypes.changed.contains(id)) {
                    type.save();
                }
            }
        }
    }

    /**
     * Returns all types.
     * @return Never {@code null}. May be modified without any side effects.
     */
    public Set<ObjectType> getTypes() {
        bootstrapOnce.ensure();

        Set<ObjectType> types = new HashSet<ObjectType>();

        TypesCache temporaryTypes = temporaryTypesLocal.get();
        if (temporaryTypes != null) {
            types.addAll(temporaryTypes.byId.values());
        }

        types.addAll(permanentTypes.byId.values());

        return types;
    }

    /**
     * Returns the type associated with the given {@code id}.
     * @return May be {@code null}.
     */
    public ObjectType getTypeById(UUID id) {
        bootstrapOnce.ensure();

        TypesCache temporaryTypes = temporaryTypesLocal.get();
        if (temporaryTypes != null) {
            ObjectType type = temporaryTypes.byId.get(id);
            if (type != null) {
                return type;
            }
        }

        return permanentTypes.byId.get(id);
    }

    /**
     * Returns the type associated with the given {@code objectClass}.
     * @return May be {@code null}.
     */
    public ObjectType getTypeByName(String name) {
        bootstrapOnce.ensure();

        if (name != null) {
            name = name.toLowerCase(Locale.ENGLISH);
        }

        TypesCache temporaryTypes = temporaryTypesLocal.get();
        if (temporaryTypes != null) {
            ObjectType type = temporaryTypes.byName.get(name);
            if (type != null) {
                return type;
            }
        }

        return permanentTypes.byName.get(name);
    }

    /**
     * Returns a set of types associated with the given {@code group}.
     * @return Never {@code null}. May be modified without any side effects.
     */
    public Set<ObjectType> getTypesByGroup(String group) {
        bootstrapOnce.ensure();

        if (group != null) {
            group = group.toLowerCase(Locale.ENGLISH);
        }

        TypesCache temporaryTypes = temporaryTypesLocal.get();
        Set<ObjectType> tTypes;
        if (temporaryTypes != null) {
            tTypes = temporaryTypes.byGroup.get(group);
        } else {
            tTypes = null;
        }

        Set<ObjectType> pTypes = permanentTypes.byGroup.get(group);

        if (tTypes == null) {
            return pTypes == null
                    ? new HashSet<ObjectType>()
                    : new HashSet<ObjectType>(pTypes);

        } else {
            tTypes = new HashSet<ObjectType>(tTypes);
            if (pTypes != null) {
                tTypes.addAll(pTypes);
            }
            return tTypes;
        }
    }

    /**
     * Returns the type associated with the given {@code objectClass}.
     * @return May be {@code null}.
     */
    public ObjectType getTypeByClass(Class<?> objectClass) {
        bootstrapOnce.ensure();

        String className = objectClass.getName().toLowerCase(Locale.ENGLISH);
        TypesCache temporaryTypes = temporaryTypesLocal.get();

        if (temporaryTypes != null) {
            ObjectType type = temporaryTypes.byClassName.get(className);
            if (type != null) {
                return type;
            }
        }

        return permanentTypes.byClassName.get(className);
    }

    /**
     * Creates an object represented by the given {@code typeId} and
     * {@code id}.
     */
    public Object createObject(UUID typeId, UUID id) {
        bootstrapOnce.ensure();

        Class<?> objectClass = null;
        ObjectType type = null;
        if (typeId != null && !GLOBALS_ID.equals(id)) {

            if (typeId.equals(id)) {
                objectClass = ObjectType.class;

            } else {
                type = getTypeById(typeId);
                if (type != null) {
                    objectClass = type.isAbstract()
                            ? Record.class
                            : type.getObjectClass();
                }
            }
        }

        boolean hasClass = true;
        if (objectClass == null) {
            objectClass = Record.class;
            hasClass = false;
        }

        Object object = TypeDefinition.getInstance(objectClass).newInstance();
        State state;

        try {
            state = State.getInstance(object);

        } catch (IllegalArgumentException error) {
            object = TypeDefinition.getInstance(Record.class).newInstance();
            state = State.getInstance(object);
            hasClass = false;
        }

        state.setDatabase(getDatabase());
        state.setId(id);
        state.setTypeId(typeId);

        if (type != null) {
            if (!hasClass) {
                for (ObjectField field : type.getFields()) {
                    Object defaultValue = field.getDefaultValue();
                    if (defaultValue != null) {
                        state.put(field.getInternalName(), defaultValue);
                    }
                }
            }
        }

        return object;
    }

    private final transient LoadingCache<String, Integer> beanPropertyIndexes = CacheBuilder.newBuilder()
            .build(new CacheLoader<String, Integer>() {

                private final Map<String, Integer> indexes = new HashMap<String, Integer>();
                private int nextIndex = 0;

                @Override
                public Integer load(String beanProperty) {
                    Integer index = indexes.get(beanProperty);

                    if (index == null) {
                        index = nextIndex;

                        if (index >= 49) {
                            throw new IllegalStateException("Can't use more than 50 @BeanProperty!");
                        }

                        ++ nextIndex;
                        indexes.put(beanProperty, index);
                    }

                    return index;
                }
            });

    private final transient Lazy<List<DynamicProperty>> dynamicProperties = new Lazy<List<DynamicProperty>>() {

        @Override
        protected List<DynamicProperty> create() {
            List<DynamicProperty> properties = new ArrayList<DynamicProperty>();

            for (ObjectType type : getTypes()) {
                if (type.getObjectClass() == null) {
                    continue;
                }

                String beanProperty = type.getJavaBeanProperty();

                if (ObjectUtils.isBlank(beanProperty)) {
                    continue;
                }

                try {
                    properties.add(new DynamicProperty(
                            type,
                            beanProperty,
                            beanPropertyIndexes.get(beanProperty)));

                } catch (Exception error) {
                    // Failed introspection so probably not a valid bean
                    // property.
                }
            }

            return ImmutableList.copyOf(properties);
        }
    };

    private static class DynamicProperty extends SimpleBeanInfo {

        public final ObjectType type;
        public final int index;
        private final PropertyDescriptor descriptor;

        public DynamicProperty(ObjectType type, String name, int index) throws Exception {
            Method readMethod = Record.class.getDeclaredMethod("getDynamicProperty" + index);

            readMethod.setAccessible(true);

            this.type = type;
            this.index = index;
            this.descriptor = new DynamicPropertyDescriptor(name, readMethod);
        }

        @Override
        public PropertyDescriptor[] getPropertyDescriptors() {
            return new PropertyDescriptor[] { descriptor };
        }
    }

    private static class DynamicPropertyDescriptor extends PropertyDescriptor {

        private final Method readMethod;

        public DynamicPropertyDescriptor(String propertyName, Method readMethod) throws IntrospectionException {
            super(propertyName, readMethod, null);

            this.readMethod = readMethod;
        }

        @Override
        public Method getReadMethod() {
            return readMethod;
        }
    }

    /**
     * Returns all additional {@link BeanInfo} instances appropriate for the
     * class represented by the given {@code type}.
     *
     * @param type If {@code null}, returns global {@link BeanInfo} instances.
     * @return May be {@code null}.
     */
    @SuppressWarnings("unchecked")
    public BeanInfo[] getAdditionalBeanInfoByType(ObjectType type) {
        List<BeanInfo> beanInfos = null;

        for (DynamicProperty property : dynamicProperties.get()) {
            boolean add = false;

            if (type != null
                    && type.getModificationClassNames().contains(property.type.getObjectClassName())) {
                add = true;

            } else {
                Class<?> modClass = property.type.getObjectClass();

                if (modClass != null
                        && Modification.class.isAssignableFrom(modClass)
                        && Modification.Static.getModifiedClasses((Class<? extends Modification<?>>) modClass).contains(Object.class)) {
                    add = true;
                }
            }

            if (add) {
                if (beanInfos == null) {
                    beanInfos = new ArrayList<BeanInfo>();
                }

                beanInfos.add(property);
            }
        }

        return beanInfos != null
                ? beanInfos.toArray(new BeanInfo[beanInfos.size()])
                : null;
    }

    /**
     * Returns the type that's bound to the given dynamic property
     * {@code index}.
     *
     * @return May be {@code null}.
     */
    public ObjectType getTypeByDynamicPropertyIndex(int index) {
        for (DynamicProperty property : dynamicProperties.get()) {
            if (property.index == index) {
                return property.type;
            }
        }

        return null;
    }

    // --- Deprecated ---

    /** @deprecated Use {@link #getGlobals} instead. */
    @Deprecated
    public Object getGlobal(String key) {
        State globals = getGlobals();
        return globals != null ? globals.getValue(key) : null;
    }

    /** @deprecated Use {@link #getGlobals} instead. */
    @Deprecated
    public void putGlobal(String key, Object value) {
        State globals = getGlobals();
        globals.putValue(key, value);
        globals.save();
    }
}
