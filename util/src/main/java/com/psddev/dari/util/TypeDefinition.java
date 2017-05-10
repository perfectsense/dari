package com.psddev.dari.util;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.stream.Collectors;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import javax.annotation.ParametersAreNonnullByDefault;

public class TypeDefinition<T> {

    private final Type type;

    static {
        CodeUtils.addRedefineClassesListener(classes -> Static.invalidateAll());
    }

    /** Returns an instance based on the given {@code type}. */
    public static TypeDefinition<?> getInstance(Type type) {
        return INSTANCES.getUnchecked(type);
    }

    private static final LoadingCache<Type, TypeDefinition<?>> INSTANCES = CacheBuilder
            .newBuilder()
            .build(new CacheLoader<Type, TypeDefinition<?>>() {

        @Override
        @ParametersAreNonnullByDefault
        public TypeDefinition<?> load(Type type) {
            return new TypeDefinition<>(type);
        }
    });

    /** Returns an instance based on the given {@code objectClass}. */
    @SuppressWarnings("unchecked")
    public static <T> TypeDefinition<T> getInstance(Class<T> objectClass) {
        return (TypeDefinition<T>) getInstance((Type) objectClass);
    }

    /** Returns an instance based on the given {@code typeReference}. */
    @SuppressWarnings("unchecked")
    public static <T> TypeDefinition<T> getInstance(TypeReference<T> typeReference) {
        return (TypeDefinition<T>) getInstance(typeReference.getType());
    }

    /** Creates an instance based on the given {@code type}. */
    protected TypeDefinition(Type type) {
        this.type = type;
    }

    /**
     * Returns the defintion's Type, including any Generic Parameters it knows about
     * ex. TypeDefinition.getInstance(new TypeReference<List<String>>() { }).getType(); // List<String>
     * A TypeReference needs to be provided to getInstance in order for the Parameterized
     * type information to be retained (type erasure)
     */
    public Type getType() {
        return type;
    }

    /**
     * Returns the defintion's Class, not including any Generic Parameters it knows about
     * ex. TypeDefinition.getInstance(new TypeReference<List<String>>() { }).getObjectClass(); // List
     */
    @SuppressWarnings("unchecked")
    public Class<T> getObjectClass() {

        Type type = getType();
        if (type instanceof Class) {
            return (Class<T>) type;

        } else if (type instanceof ParameterizedType) {
            return (Class<T>) ((ParameterizedType) type).getRawType();

        } else {
            throw new IllegalStateException(String.format(
                    "Cannot find an object class based on [%s] type!",
                    type));
        }
    }

    /**
     * Returns an unmodifiable list of all the classes that can be assigned
     * from this one with the most specific match (itself) first.
     */
    public List<Class<? super T>> getAssignableClasses() {
        return assignableClasses.get();
    }

    private final transient Lazy<List<Class<? super T>>> assignableClasses = new Lazy<List<Class<? super T>>>() {

        @Override
        protected List<Class<? super T>> create() {

            Class<T> objectClass = getObjectClass();
            List<Class<? super T>> classes = new ArrayList<>();
            classes.add(objectClass);

            Class<? super T> superClass = objectClass.getSuperclass();
            if (superClass != null) {
                classes.addAll(getInstance(superClass).getAssignableClasses());
            }

            return Collections.unmodifiableList(classes);
        }
    };

    /**
     * Returns an unmodifiable set of all classes and interfaces that are
     * either the same as, or is a super class or super interface of,
     * this type.
     */
    public Set<Class<?>> getAssignableClassesAndInterfaces() {
        return assignableClassesAndInterfaces.get();
    }

    private final transient Lazy<Set<Class<?>>> assignableClassesAndInterfaces = new Lazy<Set<Class<?>>>() {

        @Override
        protected Set<Class<?>> create() {
            Class<T> objectClass = getObjectClass();
            Set<Class<?>> classes = new LinkedHashSet<>();

            classes.add(objectClass);

            for (Class<?> interfaceClass : objectClass.getInterfaces()) {
                classes.addAll(getInstance(interfaceClass).getAssignableClassesAndInterfaces());
            }

            Class<?> superClass = objectClass.getSuperclass();
            if (superClass != null) {
                classes.addAll(getInstance(superClass).getAssignableClassesAndInterfaces());
            }

            return Collections.unmodifiableSet(classes);
        }
    };

    /** Returns an unmodifiable list of all the fields. */
    public List<Field> getAllFields() {
        return allFields.get();
    }

    private final transient Lazy<List<Field>> allFields = new Lazy<List<Field>>() {

        @Override
        protected List<Field> create() {

            Class<T> objectClass = getObjectClass();
            List<Field> fields = new ArrayList<>();
            for (Field field : objectClass.getDeclaredFields()) {
                field.setAccessible(true);
                fields.add(field);
            }

            Class<? super T> superClass = objectClass.getSuperclass();
            if (superClass != null) {
                fields.addAll(getInstance(superClass).getAllFields());
            }

            return Collections.unmodifiableList(fields);
        }
    };

    /**
     * Returns an unmodifiable map of all the serializable (non-static
     * and non-transient) fields with the normalized names as keys.
     */
    public Map<String, List<Field>> getAllSerializableFields() {
        return allSerializableFields.get();
    }

    private final transient Lazy<Map<String, List<Field>>> allSerializableFields = new Lazy<Map<String, List<Field>>>() {

        @Override
        protected Map<String, List<Field>> create() {

            Class<T> objectClass = getObjectClass();
            Map<String, List<Field>> fieldsMap = new CompactMap<>();

            Class<? super T> superClass = objectClass.getSuperclass();
            if (superClass != null) {
                for (Map.Entry<String, List<Field>> entry : getInstance(superClass).getAllSerializableFields().entrySet()) {
                    fieldsMap.put(entry.getKey(), new ArrayList<>(entry.getValue()));
                }
            }

            for (Field field : objectClass.getDeclaredFields()) {
                int mod = field.getModifiers();
                if (!Modifier.isStatic(mod)
                        && !Modifier.isTransient(mod)) {

                    // Normalize common field name formats:
                    // _name, name_, fName or mName.
                    String name = field.getName();
                    if (name.startsWith("_")) {
                        name = name.substring(1);
                    } else if (name.endsWith("_")) {
                        name = name.substring(0, name.length() - 1);
                    } else if (name.length() > 2
                            && (name.charAt(0) == 'f' || name.charAt(0) == 'm')
                            && Character.isUpperCase(name.charAt(1))) {
                        name = Character.toLowerCase(name.charAt(1)) + name.substring(2);
                    }

                    List<Field> fields = fieldsMap.get(name);
                    if (fields == null) {
                        fields = new ArrayList<>();
                        fieldsMap.put(name, fields);
                    }

                    field.setAccessible(true);
                    fields.add(field);
                }
            }

            return fieldsMap;
        }
    };

    /** Returns the first field with the given {@code name}. */
    public Field getField(String name) {
        for (Field field : getAllFields()) {
            if (field.getName().equals(name)) {
                return field;
            }
        }
        List<Field> fields = getAllSerializableFields().get(name);
        return ObjectUtils.isBlank(fields) ? null : fields.get(fields.size() - 1);
    }

    /** Returns the first method with the given {@code name}. */
    public Method getMethod(String name) {
        Method method = getAllGetters().get(name);
        if (method != null) {
            return method;
        }
        for (Method m : getAllMethods()) {
            if (m.getName().equals(name)) {
                return m;
            }
        }
        return null;
    }

    /** Returns an unmodifiable list of all the constructors. */
    public List<Constructor<T>> getConstructors() {
        return constructors.get();
    }

    private final transient Lazy<List<Constructor<T>>> constructors = new Lazy<List<Constructor<T>>>() {

        @Override
        @SuppressWarnings("unchecked")
        protected List<Constructor<T>> create() {
            List<Constructor<T>> constructors = new ArrayList<>();
            for (Constructor<?> constructor : getObjectClass().getDeclaredConstructors()) {
                constructor.setAccessible(true);
                constructors.add((Constructor<T>) constructor);
            }
            return Collections.unmodifiableList(constructors);
        }
    };

    /**
     * Returns the constructor that can be called with the given array of
     * {@code parameterClasses}.
     */
    public Constructor<T> getConstructor(Class<?>... parameterClasses) {
        int length = parameterClasses.length;
        NEXT_CONSTRUCTOR:
            for (Constructor<T> constructor : getConstructors()) {
                Class<?>[] declared = constructor.getParameterTypes();
                if (length == declared.length) {

                    for (int i = 0; i < length; ++ i) {
                        if (!declared[i].isAssignableFrom(parameterClasses[i])) {
                            continue NEXT_CONSTRUCTOR;
                        }
                    }

                    return constructor;
                }
            }

        return null;
    }

    /** Creates a new instance. */
    public T newInstance() {
        Constructor<T> constructor = getConstructor();
        if (constructor == null) {
            throw new IllegalStateException(String.format(
                    "Can't create an instance of [%s] without a nullary constructor!",
                    getObjectClass()));
        }

        try {
            return constructor.newInstance();

        } catch (IllegalAccessException ex) {
            throw new IllegalStateException(ex);

        } catch (InstantiationException ex) {
            throw new IllegalStateException(String.format(
                    "Can't instantiate an instance of [%s]!",
                    getObjectClass()), ex);

        } catch (InvocationTargetException ex) {
            Throwable cause = ex.getCause();
            if (cause == null) {
                cause = ex;
            }
            throw cause instanceof RuntimeException
                    ? (RuntimeException) cause
                    : new RuntimeException(cause.getMessage(), cause);
        }
    }

    /** Returns an unmodifiable list of all the methods. */
    public List<Method> getAllMethods() {
        return allMethods.get();
    }

    private final transient Lazy<List<Method>> allMethods = new Lazy<List<Method>>() {

        @Override
        protected List<Method> create() {

            Class<T> objectClass = getObjectClass();
            List<Method> methods = new ArrayList<>();
            try {
                for (Method method : objectClass.getDeclaredMethods()) {
                    method.setAccessible(true);
                    methods.add(method);
                }
            } catch (NoClassDefFoundError error) {
                // Class isn't available, so can't run methods anyway
            }

            Class<? super T> superClass = objectClass.getSuperclass();
            if (superClass != null) {
                methods.addAll(getInstance(superClass).getAllMethods());
            }

            return Collections.unmodifiableList(methods);
        }
    };

    /**
     * Returns an unmodifiable map of all the getters (public non-static
     * methods that have no parameters and return a value) with the
     * normalized names as keys.
     */
    public Map<String, Method> getAllGetters() {
        return allGetters.get();
    }

    private final transient Lazy<Map<String, Method>> allGetters = new Lazy<Map<String, Method>>() {

        @Override
        protected Map<String, Method> create() {

            Map<String, Method> getters = new CompactMap<>();
            getAllMethods().forEach(method -> {
                if (method.getDeclaringClass() != Object.class) {

                    int mod = method.getModifiers();
                    if (Modifier.isPublic(mod)
                            && !Modifier.isStatic(mod)
                            && method.getReturnType() != void.class
                            && method.getReturnType() != Void.class
                            && method.getParameterTypes().length == 0) {

                        String methodName = method.getName();
                        Matcher nameMatcher = StringUtils.getMatcher(methodName, "^(get|(is|has))([^a-z])(.*)$");
                        if (nameMatcher.matches()) {

                            String name = ObjectUtils.isBlank(nameMatcher.group(2))
                                    ? nameMatcher.group(3).toLowerCase(Locale.ENGLISH) + nameMatcher.group(4)
                                    : methodName;
                            getters.put(name, method);
                        }
                    }
                }
            });

            return Collections.unmodifiableMap(getters);
        }
    };

    /**
     * Returns an unmodifiable map of all the setters (public non-static
     * methods that take one parameter and doesn't return anything) with the
     * normalized names as keys.
     */
    public Map<String, Method> getAllSetters() {
        return allSetters.get();
    }

    private final transient Lazy<Map<String, Method>> allSetters = new Lazy<Map<String, Method>>() {

        @Override
        protected Map<String, Method> create() {

            Map<String, Method> setters = new CompactMap<>();
            getAllMethods().forEach(method -> {
                if (method.getDeclaringClass() != Object.class) {

                    int mod = method.getModifiers();
                    if (Modifier.isPublic(mod)
                            && !Modifier.isStatic(mod)
                            && (method.getReturnType() == void.class || method.getReturnType() == Void.class)
                            && method.getParameterTypes().length == 1) {

                        String methodName = method.getName();
                        Matcher nameMatcher = StringUtils.getMatcher(methodName, "^set([^a-z])(.*)$");
                        if (nameMatcher.matches()) {
                            setters.put(nameMatcher.group(1).toLowerCase(Locale.ENGLISH) + nameMatcher.group(2), method);
                        }
                    }
                }
            });

            return Collections.unmodifiableMap(setters);
        }
    };

    /**
     * Gets the inferred generic type argument class of this type for a given
     * {@code superClass} and {@code genericTypeArgumentIndex}.
     *
     * @param superClass a super class (or interface) of this type.
     * @param genericTypeArgumentIndex the generic type argument index of the
     *      {@code superClass}.
     * @return the inferred generic type argument class for given
     *      {@code superClass} and {@code genericTypeArgumentIndex}.
     */
    public Class<?> getInferredGenericTypeArgumentClass(Class<?> superClass, int genericTypeArgumentIndex) {
        Map<Integer, Class<?>> indexMap = allInferredGenericTypeArguments.get().get(superClass);
        return indexMap != null ? indexMap.get(genericTypeArgumentIndex) : null;
    }

    // Map of super types, to map of the super type's generic variable index to inferred generic type at that index for this TypeDefinition's type.
    private final transient Lazy<Map<Class<?>, Map<Integer, Class<?>>>> allInferredGenericTypeArguments = new Lazy<Map<Class<?>, Map<Integer, Class<?>>>>() {

        @Override
        protected Map<Class<?>, Map<Integer, Class<?>>> create() throws Exception {

            Map<Class<?>, Map<Integer, Class<?>>> map = new HashMap<>();

            if (TypeDefinition.this.type instanceof Class) {
                for (Class<?> superType : getAllSuperTypesWithTypeVariables((Class<?>) TypeDefinition.this.type)) {

                    Map<Integer, Class<?>> indexMap = new HashMap<>();
                    map.put(superType, indexMap);

                    int typeParamCount = superType.getTypeParameters().length;
                    for (int i = 0; i < typeParamCount; i++) {
                        indexMap.put(i, getInferredGenericTypeArgumentClass((Class<?>) TypeDefinition.this.type, superType, i));
                    }
                }
            }

            return map;
        }

        // Gets all super classes with type variables defined starting from the {@code sourceClass}
        private Set<Class<?>> getAllSuperTypesWithTypeVariables(Class<?> sourceClass) {

            Set<Class<?>> superTypes = new HashSet<>();

            // check super class
            Class<?> superClass = sourceClass.getSuperclass();

            if (superClass != null) {
                if (superClass.getTypeParameters().length > 0) {
                    superTypes.add(superClass);
                }
                superTypes.addAll(getAllSuperTypesWithTypeVariables(superClass));
            }

            // check interfaces
            List<Class<?>> interfaces = Arrays.asList(sourceClass.getInterfaces());

            superTypes.addAll(interfaces.stream().filter((i) -> i.getTypeParameters().length > 0).collect(Collectors.toList()));
            interfaces.forEach((i) -> superTypes.addAll(getAllSuperTypesWithTypeVariables(i)));

            return superTypes;
        }

        // gets the inferred generic type argument class for the given {@code sourceClass}
        // based on the give {@code superClass} generic type argument at the specified index.
        private Class<?> getInferredGenericTypeArgumentClass(Class<?> sourceClass, Class<?> superClass, int genericTypeArgumentIndex) {

            List<Map.Entry<Class<?>, Type>> hierarchy = getClassAndGenericSuperTypeHierarchy(sourceClass, superClass);

            int argIndex = genericTypeArgumentIndex;

            for (Iterator<Map.Entry<Class<?>, Type>> it = hierarchy.iterator(); it.hasNext();) {

                Map.Entry<Class<?>, Type> entry = it.next();

                // get list of super class type parameters
                List<Type> superClassTypeVars;

                Type hierarchyGenericSuperClass = entry.getValue();

                if (hierarchyGenericSuperClass instanceof ParameterizedType) {
                    ParameterizedType pt = (ParameterizedType) hierarchyGenericSuperClass;
                    superClassTypeVars = Arrays.asList(pt.getActualTypeArguments());

                } else {
                    break;
                }

                // make sure the var list size is greater than the arg index.
                if (superClassTypeVars.size() > argIndex) {

                    Type superClassTypeVar = superClassTypeVars.get(argIndex);

                    if (superClassTypeVar instanceof Class) {
                        return (Class<?>) superClassTypeVar;

                    } else if (superClassTypeVar instanceof TypeVariable) {

                        Class<?> hierarchyClass = entry.getKey();

                        String superClassTypeVarName = ((TypeVariable) superClassTypeVar).getName();

                        int index = 0;
                        for (TypeVariable classTypeVar : hierarchyClass.getTypeParameters()) {

                            if (superClassTypeVarName.equals(classTypeVar.getName())) {

                                if (it.hasNext()) {
                                    argIndex = index;

                                } else {
                                    superClass = hierarchyClass;
                                    genericTypeArgumentIndex = index;
                                }
                                break;
                            }
                            index++;
                        }
                    } else {
                        break;
                    }
                } else {
                    break;
                }
            }

            TypeVariable[] typeParameters = superClass.getTypeParameters();
            if (typeParameters.length > genericTypeArgumentIndex) {

                Type[] bounds = typeParameters[genericTypeArgumentIndex].getBounds();
                if (bounds.length > 0) {
                    Type bound = bounds[0];
                    if (bound instanceof Class) {
                        return (Class<?>) bound;
                    }
                }
            }

            return null;
        }

        // Returns the list of ClassInfo objects in the class hierarchy from {@code sourceClass}
        // class to {@code superClass} class, where the first element in the list is {@code sourceClass}.
        // If the two classes are not in the same hierarchy, an empty list returned.
        private List<Map.Entry<Class<?>, Type>> getClassAndGenericSuperTypeHierarchy(Class<?> sourceClass, Class<?> superClass) {

            List<Class<?>> classes = getClassHierarchy(sourceClass, superClass);

            List<Class<?>> classesReversed = new ArrayList<>();
            if (classes != null) {
                classesReversed.addAll(classes);
            }
            Collections.reverse(classesReversed);
            classesReversed.add(superClass);

            List<Map.Entry<Class<?>, Type>> hierarchy = new ArrayList<>();

            Class<?> prevClass = null;
            for (Class<?> nextClass : classesReversed) {

                if (prevClass != null) {
                    Type superType = getGenericSuperType(prevClass, nextClass);

                    if (superType != null) {
                        hierarchy.add(new AbstractMap.SimpleImmutableEntry<>(prevClass, superType));
                    }
                }

                prevClass = nextClass;
            }

            Collections.reverse(hierarchy);

            return hierarchy;
        }

        // Returns the list of classes in the class hierarchy from {@code sourceClass} class
        // to {@code superClass} class, where the first element in the list is {@code sourceClass}.
        // If the two classes are not in the same hierarchy, {@code null} is returned.
        private List<Class<?>> getClassHierarchy(Class<?> sourceClass, Class<?> superClass) {

            if (Object.class.equals(sourceClass)) {
                return null;

            } else if (superClass.equals(sourceClass)) {
                return new ArrayList<>();
            }

            List<Class<?>> superTypes = new ArrayList<>();

            Class<?> sourceSuperClass = sourceClass.getSuperclass();
            if (sourceSuperClass != null) {
                superTypes.add(sourceSuperClass);
            }

            List<Class<?>> interfaceClasses = Arrays.asList(sourceClass.getInterfaces());
            superTypes.addAll(interfaceClasses);

            for (Class<?> superType : superTypes) {

                List<Class<?>> hierarchy = getClassHierarchy(superType, superClass);

                if (hierarchy != null) {

                    List<Class<?>> classes = new ArrayList<>();

                    classes.addAll(hierarchy);
                    classes.add(sourceClass);

                    return classes;
                }
            }

            return null;
        }

        // Returns the generic super type of the for the given {@code sourceClass} matching the specified {@code superClass}.
        private Type getGenericSuperType(Class<?> sourceClass, Class<?> superClass) {

            if (superClass.isInterface()) {
                for (Type genericInterface : sourceClass.getGenericInterfaces()) {

                    if (genericInterface instanceof Class) {
                        if (superClass.equals(genericInterface)) {
                            return genericInterface;
                        }

                    } else if (genericInterface instanceof ParameterizedType) {

                        Type rawType = ((ParameterizedType) genericInterface).getRawType();
                        if (superClass.equals(rawType)) {
                            return genericInterface;
                        }
                    }
                }

            } else {
                return sourceClass.getGenericSuperclass();
            }

            return null;
        }
    };

    /** {@link TypeDefinition} utility methods. */
    public static final class Static {

        /** Invalidates all caches. */
        public static void invalidateAll() {
            INSTANCES.invalidateAll();
        }
    }
}
