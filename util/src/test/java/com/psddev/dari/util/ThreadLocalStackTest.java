package com.psddev.dari.util;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.NoSuchElementException;
import java.util.UUID;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

public class ThreadLocalStackTest {

    private static final String INITIAL_VALUE = UUID.randomUUID().toString();

    private ThreadLocalStack<String> stack;

    @Before
    public void before() {
        stack = new ThreadLocalStack<>();
        stack.push(INITIAL_VALUE);
    }

    @After
    public void after() {
        stack = null;
    }

    @Test
    public void push() {
        assertThat(stack.push("push"), equalTo(INITIAL_VALUE));
    }

    @Test(expected = NullPointerException.class)
    public void pushNull() {
        stack.push(null);
    }

    @Test
    public void peek() {
        assertThat(stack.get(), equalTo(INITIAL_VALUE));
    }

    @Test
    public void pop() {
        assertThat(stack.pop(), equalTo(INITIAL_VALUE));
    }

    @Test
    public void popNull() {
        stack.pop();
        assertThat(stack.pop(), nullValue());
    }

    @Test(expected = NoSuchElementException.class)
    public void popOrError() {
        assertThat(stack.popOrError(), equalTo(INITIAL_VALUE));
        stack.popOrError();
    }

    @Test
    public void get() {
        assertThat(stack.get(), equalTo(INITIAL_VALUE));
    }

    @Test(expected = NoSuchElementException.class)
    public void getOrError() {
        stack.pop();
        stack.getOrError();
    }

    @Test
    public void set() {
        String item = UUID.randomUUID().toString();

        stack.set(item);
        assertThat(stack.pop(), equalTo(item));
        assertThat(stack.pop(), nullValue());
    }

    @Test(expected = NullPointerException.class)
    public void setNull() {
        stack.set(null);
    }

    @Test
    public void remove() {
        stack.remove();
        assertThat(stack.pop(), nullValue());
    }

    @Test
    public void with() {
        String item = UUID.randomUUID().toString();

        stack.with(item, () -> assertThat(stack.get(), equalTo(item)));
        assertThat(stack.pop(), equalTo(INITIAL_VALUE));
    }
}
