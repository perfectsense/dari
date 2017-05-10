package com.psddev.dari.h2;

import org.junit.Test;

public class NumberIndexTest extends AbstractIndexTest<NumberIndexModel, Double> {

    @Override
    protected Class<NumberIndexModel> modelClass() {
        return NumberIndexModel.class;
    }

    @Override
    protected Double value(int index) {
        return (double) index;
    }

    @Override
    @Test(expected = IllegalArgumentException.class)
    public void containsNull() {
        createCompareTestModels();
        query().and("one contains ?", (Object) null).count();
    }

    @Override
    @Test(expected = IllegalArgumentException.class)
    public void startsWithNull() {
        createCompareTestModels();
        query().and("one startsWith ?", (Object) null).count();
    }

    @Override
    @Test
    public void invalidValue() {
    }
}
