package com.psddev.dari.test;

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
    @Test
    public void sortDescendingOne() {
        super.sortDescendingOne();
    }

    @Override
    @Test(expected = IllegalArgumentException.class)
    public void sortClosestEmbeddedOneOne() {
        super.sortClosestEmbeddedOneOne();
    }

    @Override
    @Test
    public void sortAscendingOne() {
        super.sortAscendingOne();
    }

    @Override
    @Test(expected = IllegalArgumentException.class)
    public void contains() {
        super.contains();
    }

    @Override
    @Test
    public void sortAscendingEmbeddedOneOne() {
        super.sortAscendingEmbeddedOneOne();
    }

    @Override
    @Test(expected = IllegalArgumentException.class)
    public void sortClosestReferenceOneOne() {
        super.sortClosestReferenceOneOne();
    }

    @Override
    @Test(expected = IllegalArgumentException.class)
    public void sortFarthestOneAnd() {
        super.sortFarthestOneAnd();
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
