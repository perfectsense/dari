package com.psddev.dari.util;

import com.google.common.collect.ImmutableSet;
import org.junit.Test;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

public class ClassFinderTest {

    @Test
    public void findClasses() {
        assertThat(
                ClassFinder.findClasses(Interface.class),
                is(ImmutableSet.of(
                        AbstractClass.class,
                        ConcreteInterface.class,
                        ConcreteClass.class
                )));
    }

    @Test
    public void findConcreteClasses() {
        assertThat(
                ClassFinder.findConcreteClasses(Interface.class),
                is(ImmutableSet.of(
                        ConcreteInterface.class,
                        ConcreteClass.class
                )));
    }

    private interface Interface {
    }

    private static abstract class AbstractClass implements Interface {
    }

    private static class ConcreteInterface implements Interface {
    }

    private static class ConcreteClass extends AbstractClass {
    }
}
