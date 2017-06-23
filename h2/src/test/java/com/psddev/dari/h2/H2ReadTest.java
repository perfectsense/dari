package com.psddev.dari.h2;

import org.junit.Test;
import java.util.NoSuchElementException;

public class H2ReadTest extends com.psddev.dari.test.ReadTest {

    @Override
    @Test
    public void iterableById0() {
        super.iterable(false, 0, true);
    }

    @Override
    @Test
    public void iterableById1() {
        super.iterable(false, 1, true);
    }

    @Override
    @Test
    public void iterableNotById0() {
        super.iterable(true, 0, true);
    }

    @Override
    @Test
    public void iterableNotById1() {
        super.iterable(true, 1, true);
    }

    @Override
    @Test(expected = NoSuchElementException.class)
    public void iterableNextById() {
        super.iterableNext(false, true);
    }

    @Override
    @Test(expected = NoSuchElementException.class)
    public void iterableNextNotById() {
        super.iterableNext(true, true);
    }

    @Override
    @Test(expected = UnsupportedOperationException.class)
    public void iterableRemoveById() {
        super.iterableRemove(false, true);
    }

    @Override
    @Test(expected = UnsupportedOperationException.class)
    public void iterableRemoveNotById() {
        super.iterableRemove(true, true);
    }

}
