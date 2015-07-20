package com.psddev.dari.util;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

public class TypeReferenceTest {

    private TypeReference<String> ref;

    @Before
    public void before() {
        ref = new TypeReference<String>() {
        };
    }

    @After
    public void after() {
        ref = null;
    }

    @Test
    public void test_getType() {
        assertThat(ref.getType(), equalTo(String.class));
    }

    @Test
    public void test_compareTo() {
        assertThat(ref.compareTo(null), equalTo(0));
    }

    @Test
    public void test_hashCode() {
        assertThat(ref.hashCode(), equalTo(String.class.hashCode()));
    }

    @Test
    public void test_equals() {
        assertThat(ref.equals(ref), equalTo(true));
        assertThat(ref.equals(new TypeReference<String>() { }), equalTo(true));
        assertThat(ref.equals(null), equalTo(false));
        assertThat(ref.equals(""), equalTo(false));
        assertThat(ref.equals(new TypeReference<Boolean>() { }), equalTo(false));
    }

    @Test
    public void test_toString() {
        assertThat(ref.toString(), equalTo(String.class.toString()));
    }
}
