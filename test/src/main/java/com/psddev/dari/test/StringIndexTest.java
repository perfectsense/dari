package com.psddev.dari.test;

import org.junit.Test;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

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
    @Test(expected = IllegalArgumentException.class)
    public void sortClosestEmbeddedOneOne() {
        super.sortClosestEmbeddedOneOne();
    }

    @Override
    @Test(expected = IllegalArgumentException.class)
    public void sortFarthestOneAnd() {
        super.sortFarthestOneAnd();
    }

    @Override
    @Test(expected = IllegalArgumentException.class)
    public void sortClosestReferenceOneOne() {
        super.sortClosestReferenceOneOne();
    }

    @Override
    @Test
    public void sortDescendingOne() {
        super.sortDescendingOne();
    }

    @Override
    @Test
    public void sortAscendingOne() {
        super.sortAscendingOne();
    }

    @Override
    @Test
    public void sortAscendingEmbeddedOneOne() {
        super.sortAscendingEmbeddedOneOne();
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
