package com.psddev.dari.util;

import java.lang.reflect.*;
import java.util.*;

import org.junit.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.equalTo;;

public class TypeDefinitionTest {

	
	/**
	 * public static TypeDefinition<?> getInstance(Type type)
	 */
	
    @Test
    public void test_getInstance() {
    	TypeDefinition<Bar> typedef = TypeDefinition.getInstance(Bar.class);
    	assertFalse(null == typedef);
    }
	
    /*
     *  Currently fails... should throw a NullPointerException (or some other "nuh uh" type)
     *  since other calls on this object wind up throwing NullPointerExceptions
     */
    @Ignore // TODO: This should be fixed
    @Test (expected=NullPointerException.class)
    public void test_getInstance_null() {
    	TypeDefinition<Bar> typedef = TypeDefinition.getInstance((Class<Bar>)null);
    }

    
    /**
     * public Type getType()
     */
    @Test
    public void test_getType() {
    	TypeDefinition<String> typedef = TypeDefinition.getInstance(String.class);
    	assertEquals("class java.lang.String", typedef.getType().toString());
    }
    
    /*
     * The current behavior is that getObjectClass() winds up throwing the exception, rather than getInstance()
     */
    @Test (expected=NullPointerException.class)
    public void test_getType_null() {
    	TypeDefinition<Bar> typedef = TypeDefinition.getInstance((Class<Bar>)null);
    	typedef.getType();
    }
	
    @Test // When coming from a class instance, it doesn't get to know the parameterized type
    public void test_getType_collection_from_object() {
    	List<Bar> tmp = new ArrayList<Bar>();
    	TypeDefinition typedef = TypeDefinition.getInstance(tmp.getClass());
    	assertEquals("class java.util.ArrayList", typedef.getType().toString());
    }

    @Test // Coming from a TypeReference, it gets to know the parameterized type
    public void test_getType_collection_from_typeref() {
    	TypeDefinition<List<String>> typedef = TypeDefinition.getInstance(new TypeReference<List<String>>() { });
    	assertEquals("java.util.List<java.lang.String>", typedef.getType().toString());
    }

    @Test
    public void test_getType_array() {
    	assertEquals(String[].class, TypeDefinition.getInstance(String[].class).getType());
    }

    @Test
    public void test_getType_primitive() {
    	assertEquals(int.class, TypeDefinition.getInstance(int.class).getType());
    }

    /**
     * getType vs getObjectClass 
     * TODO: Remove these tests once we find a way in which the two calls are different
     */
    @Test // For parameterized types, they're not the same
    public void test_getTypeVsGetObjectClass_generics() {
    	TypeDefinition<List<String>> typedef = TypeDefinition.getInstance(new TypeReference<List<String>>() { });
    	assertEquals("interface java.util.List", typedef.getObjectClass().toString());
    	assertEquals("java.util.List<java.lang.String>", typedef.getType().toString());
    	assertThat(typedef.getObjectClass().toString(), not(equalTo(typedef.getType().toString())));
    }
    
    @Test // For arrays, they're the same
    public void test_getTypeVsGetObjectClass_array() {
    	assertEquals(TypeDefinition.getInstance(String[].class).getObjectClass(), TypeDefinition.getInstance(String[].class).getType());

    }

    @Test // for primitives, they're the same
    public void test_getTypeVsGetObjectClass_primitive() {
    	assertEquals(TypeDefinition.getInstance(int.class).getObjectClass(), TypeDefinition.getInstance(int.class).getType());
    }
    
    @Test // for primitives, they're the same
    public void test_getTypeVsGetObjectClass_object() {
    	assertEquals(TypeDefinition.getInstance(String.class).getObjectClass(), TypeDefinition.getInstance(String.class).getType());
    }
    /**
     * public Class<T> getObjectClass()
     */
    
    @Test
    public void test_getObjectClass() {
    	TypeDefinition<Bar> typedef = TypeDefinition.getInstance(Bar.class);
    	assertEquals(Bar.class, typedef.getObjectClass());
    }
    
    /*
     * The current behavior is that getObjectClass() winds up throwing the exception, rather than getInstance()
     */
    @Test (expected=NullPointerException.class)
    public void test_getObjectClass_null() {
    	TypeDefinition<Bar> typedef = TypeDefinition.getInstance((Class<Bar>)null);
    	typedef.getObjectClass();
    }
	
    @Test
    public void test_getObjectClass_collection() {
    	List<Bar> tmp = new ArrayList<Bar>();
    	TypeDefinition typedef = TypeDefinition.getInstance(tmp.getClass());
    	assertEquals(ArrayList.class, typedef.getObjectClass());
    }

    @Test
    public void test_getObjectClass_array() {
    	assertEquals(String[].class, TypeDefinition.getInstance(String[].class).getObjectClass());
    }

    @Test
    public void test_getObjectClass_primitive() {
    	assertEquals(int.class, TypeDefinition.getInstance(int.class).getObjectClass());
    }

	/**
	 * public List<Class<? super T>> getAssignableClasses()
	 */
	
    @Test
    public void test_getAssignableClasses_subclass() {
        assertEquals(
                Arrays.asList(Qux.class, Bar.class, Foo.class, Object.class),
                TypeDefinition.getInstance(Qux.class).getAssignableClasses());
    }
    
    @Test
    public void test_getAssignableClasses_Object() {
        assertEquals(
                Arrays.asList(Object.class),
                TypeDefinition.getInstance(Object.class).getAssignableClasses());
    }

    /**
     * public List<Field> getAllFields()
     */
    
    @Test
    public void test_getAllFields() throws NoSuchFieldException {
        assertEquals(
                Arrays.asList(
                Qux.class.getDeclaredField("quxField"),
                Bar.class.getDeclaredField("barField"),
                Bar.class.getDeclaredField("sharedField"),
                Foo.class.getDeclaredField("staticField"),
                Foo.class.getDeclaredField("_privateField"),
                Foo.class.getDeclaredField("fProtectedField"),
                Foo.class.getDeclaredField("mPublicField"),
                Foo.class.getDeclaredField("transientField_"),
                Foo.class.getDeclaredField("sharedField")),
                TypeDefinition.getInstance(Qux.class).getAllFields());
    }

    @Test
    public void test_getAllSerializableFields() throws NoSuchFieldException {

        Map<String, List<Field>> expected = new LinkedHashMap<String, List<Field>>();
        expected.put("privateField", Arrays.asList(Foo.class.getDeclaredField("_privateField")));
        expected.put("protectedField", Arrays.asList(Foo.class.getDeclaredField("fProtectedField")));
        expected.put("publicField", Arrays.asList(Foo.class.getDeclaredField("mPublicField")));
        expected.put("sharedField", Arrays.asList(Foo.class.getDeclaredField("sharedField"), Bar.class.getDeclaredField("sharedField")));
        expected.put("barField", Arrays.asList(Bar.class.getDeclaredField("barField")));
        expected.put("quxField", Arrays.asList(Qux.class.getDeclaredField("quxField")));

        assertEquals(
                expected,
                TypeDefinition.getInstance(Qux.class).getAllSerializableFields());
    }

    
    /**
     * public Field getField(String name)
     */
    
    @Test
    public void test_getField() throws NoSuchFieldException {

        assertEquals(
                Qux.class.getDeclaredField("quxField"),
                TypeDefinition.getInstance(Qux.class).getField("quxField"));

        assertEquals(
                Foo.class.getDeclaredField("staticField"),
                TypeDefinition.getInstance(Qux.class).getField("staticField"));

        assertEquals(
                Bar.class.getDeclaredField("sharedField"),
                TypeDefinition.getInstance(Qux.class).getField("sharedField"));
    }

    
    /**
     * public List<Constructor<T>> getConstructors()
     */
    
    @Test
    public void test_getConstructors() {
        assertEquals(
                Arrays.asList(Qux.class.getDeclaredConstructors()),
                TypeDefinition.getInstance(Qux.class).getConstructors());
    }

    
    /**
     * public Constructor<T> getConstructor(Class<?>... parameterClasses)
     */
    
    @Test
    public void test_getConstructor() throws NoSuchMethodException {
        assertEquals(
                Qux.class.getDeclaredConstructor(Map.class),
                TypeDefinition.getInstance(Qux.class).getConstructor(HashMap.class));
    }
    
    @Test (expected=NoSuchMethodException.class)
    public void test_getConstructor_invalid() throws NoSuchMethodException {
        assertEquals(
                Qux.class.getDeclaredConstructor(List.class),
                TypeDefinition.getInstance(Qux.class).getConstructor(HashMap.class));
    }

    
    /**
     * public List<Method> getAllMethods()
     */
    
    @Test
    public void test_getAllMethods() throws NoSuchMethodException {

        List<Method> expected = new ArrayList<Method>();
        expected.add(Qux.class.getDeclaredMethod("getQux"));
        expected.add(Qux.class.getDeclaredMethod("hasQux"));
        expected.add(Qux.class.getDeclaredMethod("setQux", Object.class));
        expected.add(Bar.class.getDeclaredMethod("getBar"));
        expected.add(Foo.class.getDeclaredMethod("setFoo", Object.class));
        Collections.addAll(expected, Object.class.getDeclaredMethods());

        assertEquals(
                expected,
                TypeDefinition.getInstance(Qux.class).getAllMethods());
    }

    // TODO: Add a test that actually throws a NoSuchMethodException, if it can actually be thrown
    
    /**
     * public Map<String, Method> getAllGetters() 
     */
    
    @Test
    public void test_getAllGetters() throws NoSuchMethodException {

        Map<String, Method> expected = new LinkedHashMap<String, Method>();
        expected.put("qux", Qux.class.getDeclaredMethod("getQux"));
        expected.put("hasQux", Qux.class.getDeclaredMethod("hasQux"));
        expected.put("bar", Bar.class.getDeclaredMethod("getBar"));

        assertEquals(
                expected,
                TypeDefinition.getInstance(Qux.class).getAllGetters());
    }

    @Test
    public void test_getAllGetters_none() throws NoSuchMethodException {
        assertEquals(Collections.<String,Method>emptyMap(), TypeDefinition.getInstance(Object.class).getAllGetters());
    }

    
    /**
     * public Map<String, Method> getAllSetters()
     */
    
    @Test
    public void test_getAllSetters() throws NoSuchMethodException {
        Map<String, Method> expected = new LinkedHashMap<String, Method>() {{
        	put("qux", Qux.class.getDeclaredMethod("setQux", Object.class));
        	put("foo", Foo.class.getDeclaredMethod("setFoo", Object.class));
        }};

        assertEquals(expected, TypeDefinition.getInstance(Qux.class).getAllSetters());
    }

    @Test
    public void test_getAllSetters_none() throws NoSuchMethodException {
        assertEquals(Collections.<String,Method>emptyMap(), TypeDefinition.getInstance(Object.class).getAllSetters());
    }
    
    /**
     * utility code for testing
     */
    private static class Foo {

        private static Object staticField;
        private Object _privateField;
        protected Object fProtectedField;
        public Object mPublicField;
        private transient Object transientField_;
        private Object sharedField;

        public void setFoo(Object foo) {
        }
    }

    private static class Bar extends Foo {

        private Object barField;
        private Object sharedField;

        public Object getBar() {
            return null;
        }
    }

    private static class Qux extends Bar {

        private Object quxField;

        public Qux() {
        }

        public Qux(Map parameter) {
        }

        public Object getQux() {
            return null;
        }

        public boolean hasQux() {
            return false;
        }

        public void setQux(Object qux) {
        }
    }

    @Test
    public void test_getInferredGenericTypeArgumentClass_noSuperClass() {
        assertNull(gigtac(Zero.class, Red.class, 0));
    }

    @Test
    public void test_getInferredGenericTypeArgumentClass_noArgIndex() {
        assertNull(gigtac(Zero.class, Alpha.class, 13));
    }

    @Test
    public void test_getInferredGenericTypeArgumentClass_superClasses() {
        TypeDefinition<Zero> zero = TypeDefinition.getInstance(Zero.class);

        // test super classes
        Class<?>[] superClasses = { One.class, Two.class, Three.class, Four.class, Five.class, Six.class };
        Class<?>[][] allExpected = {
                // class One
                // A         B             C             D            E           F
                { Red.class, Orange.class, Yellow.class, Green.class, Blue.class, Purple.class },
                // class Two
                // A            B           C            D             E             F          G
                { Purple.class, Blue.class, Green.class, Yellow.class, Orange.class, Red.class, Red.class },
                // class Three
                // A         B          C             D             E            F           G             H
                { Red.class, Red.class, Orange.class, Yellow.class, Green.class, Blue.class, Purple.class, Orange.class },
                // class Four
                // A            B             C           D            E             F             G          H          I
                { Orange.class, Purple.class, Blue.class, Green.class, Yellow.class, Orange.class, Red.class, Red.class, Yellow.class },
                // class Five
                // A            B          C          D             E             F            G           H             I             J
                { Yellow.class, Red.class, Red.class, Orange.class, Yellow.class, Green.class, Blue.class, Purple.class, Orange.class, Green.class },
                // class Six
                // A           B             C             D           E            F             G             H          I          J             K
                { Green.class, Orange.class, Purple.class, Blue.class, Green.class, Yellow.class, Orange.class, Red.class, Red.class, Yellow.class, Blue.class }
        };

        int superClassIndex = 0;
        for (Class<?> superClass : superClasses) {

            Class<?>[] superClassExpected = allExpected[superClassIndex];

            int argIndex = 0;
            for (Class<?> expected : superClassExpected) {
                assertEquals(expected, zero.getInferredGenericTypeArgumentClass(superClass, argIndex));
                argIndex++;
            }

            superClassIndex++;
        }
    }

    @Test
    public void test_getInferredGenericTypeArgumentClass_interfaces() {
        assertEquals(Red.class, gigtac(Zero.class, Alpha.class, 0));
        assertEquals(Blue.class, gigtac(Zero.class, Beta.class, 0));
        assertEquals(Red.class, gigtac(Zero.class, Gamma.class, 0));
        assertEquals(Yellow.class, gigtac(Zero.class, Delta.class, 0));
        assertEquals(Yellow.class, gigtac(Zero.class, Epsilon.class, 0));
        assertEquals(Yellow.class, gigtac(Zero.class, Zeta.class, 0));
        assertEquals(Orange.class, gigtac(Zero.class, Eta.class, 0));
        assertEquals(Green.class, gigtac(Zero.class, Theta.class, 0));
        assertEquals(Yellow.class, gigtac(Zero.class, Kappa.class, 0));
    }

    @Test
    public void test_getInferredGenericTypeArgumentClass_noGenerics() {
        assertEquals(Charlie.class, gigtac(Bravo.class, Foxtrot.class, 0));
        assertEquals(Charlie.class, gigtac(Charlie.class, Foxtrot.class, 0));
        assertEquals(Bravo.class, gigtac(Echo.class, Foxtrot.class, 0));
    }

    // Helper method for test_getInferredGenericTypeArgumentClass* tests
    private Class<?> gigtac(Class<?> sourceClass, Class<?> superClass, int argIndex) {
        return TypeDefinition.getInstance(sourceClass).getInferredGenericTypeArgumentClass(superClass, argIndex);
    }

    /**
     * utility code for testing method getInferredGenericTypeArgumentClass()
     */
    private static interface Alpha<A> {
    }
    private static interface Beta<B> {
    }
    private static interface Gamma<G> {
    }
    private static interface Delta<D> extends Kappa<D> {
    }
    private static interface Epsilon<E> extends Zeta<E> {
    }
    private static interface Zeta<Z> {
    }
    private static interface Eta<I> {
    }
    private static interface Theta<T> {
    }
    private static interface Kappa<K> {
    }

    private static class Zero extends One<Red, Orange, Yellow, Green, Blue, Purple> {
    }
    private static class One<A, B, C, D, E, F> extends Two<F, E, D, C, B, A, Red> implements Alpha<A> {
    }
    private static class Two<A, B, C, D, E, F, G> extends Three<G, F, E, D, C, B, A, Orange> implements Beta<B>, Gamma<G> {
    }
    private static class Three<A, B, C, D, E, F, G, H> extends Four<H, G, F, E, D, C, B, A, Yellow> implements Delta<D> {
    }
    private static class Four<A, B, C, D, E, F, G, H, I> extends Five<I, H, G, F, E, D, C, B, A, Green> {
    }
    private static class Five<A, B, C, D, E, F, G, H, I, J> extends Six<J, I, H, G, F, E, D, C, B, A, Blue> implements Epsilon<E>, Eta<I> {
    }
    private static class Six<A, B, C, D, E, F, G, H, I, J, K> implements Theta<E> {
    }

    private static class Red<R> extends Orange {
    }
    private static class Orange extends Yellow {
    }
    private static class Yellow extends Green {
    }
    private static class Green extends Blue {
    }
    private static class Blue extends Purple {
    }
    private static class Purple {
    }

    private static class Bravo extends Charlie {
    }
    private static class Charlie extends Echo {
    }
    private static class Echo<T extends Bravo> implements Foxtrot<T> {
    }
    private static interface Foxtrot<T extends Charlie> {
    }
}
