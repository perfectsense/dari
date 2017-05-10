package com.psddev.dari.h2;

import org.junit.Test;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

public class StringIndexTest extends AbstractIndexTest<StringIndexModel, String> {

    @Override
    protected Class<StringIndexModel> modelClass() {
        return StringIndexModel.class;
    }

    @Override
    protected String value(int index) {
        return String.valueOf((char) ('a' + index));
    }

    @Override
    @Test
    public void invalidValue() {
    }

    @Test
    public void blankValue() {
        StringIndexModel model = model().one(" ").create();
        assertThat(
                query().where("one = missing").first(),
                is(model));
    }

    @Override
    @Test
    public void contains() {
        StringIndexModel model = model().one("abcde").create();
        assertThat(
                query().where("one contains ?", "bcd").first(),
                is(model));
    }

    @Override
    @Test
    public void startsWith() {
        StringIndexModel model = model().one("abcde").create();
        assertThat(
                query().where("one startsWith ?", "abc").first(),
                is(model));
    }
}
