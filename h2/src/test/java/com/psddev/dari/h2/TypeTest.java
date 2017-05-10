package com.psddev.dari.h2;

import com.psddev.dari.db.Query;
import com.psddev.dari.db.Record;
import com.psddev.dari.db.Recordable;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.stream.IntStream;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

public class TypeTest extends AbstractTest {

    @BeforeClass
    public static void createModels() {
        IntStream.range(0, 5).forEach(i -> new Foo().save());
        IntStream.range(0, 10).forEach(i -> new Bar().save());
        IntStream.range(0, 20).forEach(i -> new Qux().save());
    }

    @Test
    public void concreteClass() {
        Query<?> query = Query.from(Foo.class);

        assertThat(query.count(), is(5L));
        assertThat(query.selectAll(), everyItem(instanceOf(Foo.class)));
    }

    @Test
    public void abstractInterface() {
        Query<?> query = Query.from(Interface1.class);

        assertThat(query.count(), is(25L));
        assertThat(query.selectAll(), everyItem(instanceOf(Interface1.class)));
    }

    @Test
    public void abstractClass() {
        Query<?> query = Query.from(AbstractModel.class);

        assertThat(query.count(), is(35L));
        assertThat(query.selectAll(), everyItem(instanceOf(AbstractModel.class)));
    }

    @Test
    public void type() {
        Query<?> query = Query.fromAll().where("_type = ?", Bar.class);

        assertThat(query.count(), is(10L));
        assertThat(query.selectAll(), everyItem(instanceOf(Bar.class)));
    }

    public static abstract class AbstractModel extends Record {
    }

    public interface Interface1 extends Recordable {
    }

    public interface Interface2 extends Recordable {
    }

    public static class Foo extends AbstractModel implements Interface1 {
    }

    public static class Bar extends AbstractModel implements Interface2 {
    }

    public static class Qux extends AbstractModel implements Interface1, Interface2 {
    }
}
