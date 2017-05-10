package com.psddev.dari.h2;

import com.psddev.dari.db.Location;
import com.psddev.dari.db.Region;
import org.junit.Test;

public class RegionIndexTest extends AbstractIndexTest<RegionIndexModel, Region> {

    @Override
    protected Class<RegionIndexModel> modelClass() {
        return RegionIndexModel.class;
    }

    @Override
    protected Region value(int index) {
        return Region.sphericalCircle(0.0d, 0.0d, index + 1);
    }

    @Override
    @Test
    public void contains() {
        createCompareTestModels();
        assertCount(total, "one contains ?", new Location(0.0d, 0.0d));
        assertCount(total - 1, "one contains ?", new Location(1.5d, 0.0d));
        assertCount(total, "one contains ?", Region.sphericalCircle(0.0d, 0.0d, 0.5d));
        assertCount(total - 1, "one contains ?", Region.sphericalCircle(0.0d, 0.0d, 1.5d));
    }

    @Override
    @Test(expected = IllegalArgumentException.class)
    public void startsWithNull() {
        createCompareTestModels();
        query().and("one startsWith ?", (Object) null).count();
    }

    @Test(expected = IllegalArgumentException.class)
    public void containsIllegal() {
        createCompareTestModels();
        query().where("one contains true").count();
    }

    @Test(expected = IllegalArgumentException.class)
    public void eqIllegal() {
        createCompareTestModels();
        query().and("one = true").count();
    }

    @Test
    public void gtNumber() {
        createCompareTestModels();
        assertCount(total, "one > 0");
    }

    @Test(expected = IllegalArgumentException.class)
    public void gtIllegal() {
        createCompareTestModels();
        query().where("one > true").count();
    }

    @Test
    public void geNumber() {
        createCompareTestModels();
        assertCount(total, "one >= 0");
    }

    @Test(expected = IllegalArgumentException.class)
    public void geIllegal() {
        createCompareTestModels();
        query().where("one > true").count();
    }

    @Test
    public void ltNumber() {
        createCompareTestModels();
        assertCount(1, "one < 10");
    }

    @Test(expected = IllegalArgumentException.class)
    public void ltIllegal() {
        createCompareTestModels();
        query().where("one < true").count();
    }

    @Test
    public void leNumber() {
        createCompareTestModels();
        assertCount(1, "one <= 10");
    }

    @Test(expected = IllegalArgumentException.class)
    public void leIllegal() {
        createCompareTestModels();
        query().where("one <= true").count();
    }

    @Override
    @Test
    public void sortAscendingOne() {
    }

    @Override
    @Test
    public void sortAscendingReferenceOneOne() {
    }

    @Override
    @Test
    public void sortAscendingEmbeddedOneOne() {
    }

    @Override
    @Test
    public void sortDescendingOne() {
    }
}
