package com.psddev.dari.util;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

/** Converts an object into a map. */
public class ObjectToAnyMap implements ConversionFunction<Object, Map<Object, Object>> {

    private final Map<Type, Class<? extends Map>> implementationClasses = new HashMap<Type, Class<? extends Map>>();
    private final Map<Type, Plan> plans = new HashMap<Type, Plan>();

    /** Creates a new instance. */
    public ObjectToAnyMap() {

        implementationClasses.put(ConcurrentMap.class, ConcurrentHashMap.class);
        implementationClasses.put(ConcurrentNavigableMap.class, ConcurrentSkipListMap.class);
        implementationClasses.put(Map.class, CompactMap.class);
        implementationClasses.put(NavigableMap.class, TreeMap.class);
        implementationClasses.put(SortedMap.class, TreeMap.class);

        includeAllFields(Object.class);
    }

    /**
     * Returns the implementation class used to create an instance of
     * the given {@code returnType}.
     */
    public Class<? extends Map> getImplementationClass(Type returnType) {
        Class<? extends Map> implementationClass = implementationClasses.get(returnType);
        if (implementationClass == null) {
            Class<? extends Map> returnClass = (Class<? extends Map>) TypeDefinition.getInstance(returnType).getObjectClass();
            implementationClass = implementationClasses.get(returnClass);
            if (implementationClass == null) {
                implementationClass = returnClass;
            }
        }
        return implementationClass;
    }

    /**
     * Puts the given {@code implementationClass} to be used for creating
     * an instance of the given {@code returnType}.
     */
    public void putImplementationClass(Type returnType, Class<? extends Map> implementationClass) {
        implementationClasses.put(returnType, implementationClass);
    }

    /**
     * Puts the given {@code implementationClass} to be used for creating
     * an instance of the given {@code returnClass}.
     */
    public <T extends Map> void putImplementationClass(Class<T> returnClass, Class<? extends T> implementationClass) {
        putImplementationClass((Type) returnClass, implementationClass);
    }

    /**
     * Puts the given {@code implementationClass} to be used for creating
     * an instance of the type referenced by the given
     * {@code returnTypeReference}.
     */
    public <T extends Map> void putImplementationClass(TypeReference<T> returnTypeReference, Class<? extends T> implementationClass) {
        putImplementationClass(returnTypeReference.getType(), implementationClass);
    }

    /**
     * Includes the return value from the given {@code function} into the
     * given {@code name} when converting instances of the given {@code type}.
     *
     * @deprecated No replacement.
     */
    @Deprecated
    public void includeFunction(Type type, String name, TransformationFunction<?> function) {
        getPlan(type).includeFunction(name, function);
    }

    /**
     * Includes the given {@code constant} into the given {@code name}
     * when converting instances of the given {@code type}.
     */
    public void includeConstant(Type type, String name, Object constant) {
        getPlan(type).includeConstant(name, constant);
    }

    /**
     * Includes all the non-transient field values when converting instances
     * of the given {@code type}.
     */
    public void includeAllFields(Type type) {
        getPlan(type).includeAllFields();
    }

    /**
     * Includes the field values with the given {@code names} when
     * converting instances of the given {@code type}.
     */
    public void includeFields(Type type, String... names) {
        getPlan(type).includeFields(names);
    }

    /**
     * Includes all the public getter values when converting instances
     * of the given {@code type}.
     */
    public void includeAllGetters(Type type) {
        getPlan(type).includeAllGetters();
    }

    /**
     * Includes the getter values with the given {@code names} when
     * converting instances of the given {@code type}.
     */
    public void includeGetters(Type type, String... names) {
        getPlan(type).includeGetters(names);
    }

    /**
     * Excludes all the values associated with the given {@code names}
     * when converting instances of the given {@code type}.
     */
    public void exclude(Type type, String... names) {
        getPlan(type).exclude(names);
    }

    /**
     * Excludes all the non-transient field values when converting instances
     * of the given {@code type}.
     */
    public void excludeAllFields(Type type) {
        getPlan(type).excludeAllFields();
    }

    /**
     * Excludes all the public getter values when converting instances
     * of the given {@code type}.
     */
    public void excludeAllGetters(Type type) {
        getPlan(type).excludeAllGetters();
    }

    /**
     * Renames the value associated with the given {@code oldName} to the
     * given {@code newName} when converting instances of the given
     * {@code type}.
     */
    public void rename(Type type, String oldName, String newName) {
        getPlan(type).rename(oldName, newName);
    }

    private Plan getPlan(Type type) {
        Plan plan = plans.get(type);
        if (plan == null) {
            plan = new Plan();
            plans.put(type, plan);
        }
        return plan;
    }

    // --- ConversionFunction support ---

    @Override
    public Map<Object, Object> convert(Converter converter, Type returnType, Object object) throws Exception {

        Type keyType;
        Type valueType;
        if (returnType instanceof Class) {
            keyType = null;
            valueType = null;

        } else if (returnType instanceof ParameterizedType) {
            Type[] typeArguments = ((ParameterizedType) returnType).getActualTypeArguments();
            keyType = typeArguments[0];
            valueType = typeArguments[1];

        } else {
            throw new ConversionException(String.format(
                    "Cannot convert to [%s] type!", returnType), null);
        }

        Map<Object, Object> map = TypeDefinition.getInstance(getImplementationClass(returnType)).newInstance();
        Iterator<?> objectIterator = converter.convert(Iterable.class, object).iterator();
        if (objectIterator.hasNext()) {

            Object key = objectIterator.next();
            if (!objectIterator.hasNext() && object == key) {
                boolean hasPlan = false;
                for (Class<? extends Object> c : TypeDefinition.getInstance(object.getClass()).getAssignableClassesAndInterfaces()) {
                    if (c == Object.class && hasPlan) {
                        break;
                    } else {
                        Plan plan = plans.get(c);
                        if (plan != null) {
                            hasPlan = true;
                            plan.putValues(converter, object, keyType, valueType, map);
                        }
                    }
                }

            } else {
                while (true) {

                    Object value;
                    if (key instanceof Map.Entry) {
                        Map.Entry<?, ?> entry = (Map.Entry<?, ?>) key;
                        value = entry.getValue();
                        key = entry.getKey();

                    } else {
                        value = objectIterator.hasNext() ? objectIterator.next() : null;
                    }

                    map.put(
                            keyType != null ? converter.convert(keyType, key) : key,
                            valueType != null ? converter.convert(valueType, value) : value);

                    if (objectIterator.hasNext()) {
                        key = objectIterator.next();
                    } else {
                        break;
                    }
                }
            }
        }

        return map;
    }

    // --- Nested ---

    private static class Plan {

        public enum AllOrder {
            FIELDS,
            FIELDS_GETTERS,
            GETTERS,
            GETTERS_FIELDS;
        }

        @Deprecated
        private final Map<String, TransformationFunction<?>> functions = new HashMap<String, TransformationFunction<?>>();

        private final Map<String, Object> constants = new HashMap<String, Object>();
        private final Map<String, String> renames = new HashMap<String, String>();

        private AllOrder allOrder;

        private final Set<String> includeFields = new HashSet<String>();
        private final Set<String> excludeFields = new HashSet<String>();

        private final Set<String> includeGetters = new HashSet<String>();
        private final Set<String> excludeGetters = new HashSet<String>();

        @Deprecated
        public void includeFunction(String name, TransformationFunction<?> function) {
            exclude(name);
            functions.put(name, function);
        }

        public void includeConstant(String name, Object constant) {
            exclude(name);
            constants.put(name, constant);
        }

        public void includeAllFields() {
            if (allOrder == AllOrder.GETTERS
                    || allOrder == AllOrder.GETTERS_FIELDS) {
                allOrder = AllOrder.FIELDS_GETTERS;
            } else {
                allOrder = AllOrder.FIELDS;
            }
        }

        public void includeFields(String... names) {
            for (String name : names) {
                includeFields.add(name);
                excludeFields.remove(name);
                includeGetters.remove(name);
                excludeGetters.add(name);
            }
        }

        public void includeAllGetters() {
            if (allOrder == AllOrder.FIELDS
                    || allOrder == AllOrder.FIELDS_GETTERS) {
                allOrder = AllOrder.GETTERS_FIELDS;
            } else {
                allOrder = AllOrder.GETTERS;
            }
        }

        public void includeGetters(String... names) {
            for (String name : names) {
                includeFields.remove(name);
                excludeFields.add(name);
                includeGetters.add(name);
                excludeGetters.remove(name);
            }
        }

        public void exclude(String... names) {
            for (String name : names) {
                functions.remove(name);
                constants.remove(name);
                includeFields.remove(name);
                excludeFields.add(name);
                includeGetters.remove(name);
                excludeGetters.add(name);
            }
        }

        public void excludeAllFields() {
            if (allOrder == AllOrder.FIELDS) {
                allOrder = null;
            } else if (allOrder == AllOrder.FIELDS_GETTERS
                    || allOrder == AllOrder.GETTERS_FIELDS) {
                allOrder = AllOrder.GETTERS;
            }
        }

        public void excludeAllGetters() {
            if (allOrder == AllOrder.GETTERS) {
                allOrder = null;
            } else if (allOrder == AllOrder.FIELDS_GETTERS
                    || allOrder == AllOrder.GETTERS_FIELDS) {
                allOrder = AllOrder.FIELDS;
            }
        }

        public void rename(String oldName, String newName) {
            renames.put(oldName, newName);
        }

        @SuppressWarnings("deprecation")
        public void putValues(Converter converter, Object object, Type keyType, Type valueType, Map<Object, Object> map) throws Exception {

            TypeDefinition<?> definition = TypeDefinition.getInstance(object.getClass());
            Map<String, Field> fields = new CompactMap<String, Field>();
            Set<String> fieldNames = new LinkedHashSet<String>();
            Map<String, Method> getters = definition.getAllGetters();
            Set<String> getterNames = new LinkedHashSet<String>(getters.keySet());

            for (Map.Entry<String, List<Field>> e : definition.getAllSerializableFields().entrySet()) {
                String name = e.getKey();
                List<Field> serializableFields = e.getValue();
                fields.put(name, serializableFields.get(serializableFields.size() - 1));
                fieldNames.add(name);
            }

            if (allOrder == AllOrder.FIELDS) {
                getterNames.clear();
            } else if (allOrder == AllOrder.FIELDS_GETTERS) {
                getterNames.removeAll(fieldNames);
            } else if (allOrder == AllOrder.GETTERS) {
                fieldNames.clear();
            } else if (allOrder == AllOrder.GETTERS_FIELDS) {
                fieldNames.removeAll(getterNames);
            } else {
                fieldNames.clear();
                getterNames.clear();
            }

            fieldNames.addAll(includeFields);
            fieldNames.removeAll(excludeFields);
            getterNames.addAll(includeGetters);
            getterNames.removeAll(excludeGetters);

            for (String name : fieldNames) {
                if (!map.containsKey(name)) {
                    Object value = fields.get(name).get(object);
                    map.put(
                            keyType != null ? converter.convert(keyType, name) : name,
                            valueType != null ? converter.convert(valueType, value) : value);
                }
            }

            for (String name : getterNames) {
                if (!map.containsKey(name)) {
                    Object value = getters.get(name).invoke(object);
                    map.put(
                            keyType != null ? converter.convert(keyType, name) : name,
                            valueType != null ? converter.convert(valueType, value) : value);
                }
            }

            for (Map.Entry<String, TransformationFunction<?>> e : functions.entrySet()) {
                String key = e.getKey();
                if (!map.containsKey(key)) {
                    Object value = ((TransformationFunction<Object>) e.getValue()).transform(object);
                    map.put(
                            keyType != null ? converter.convert(keyType, key) : key,
                            valueType != null ? converter.convert(valueType, value) : value);
                }
            }

            for (Map.Entry<String, String> e : renames.entrySet()) {
                String oldName = e.getKey();
                String newName = e.getValue();
                if (map.containsKey(oldName)) {
                    map.put(newName, map.remove(oldName));
                }
            }
        }
    }
}
