package com.psddev.dari.test;

import com.psddev.dari.db.Location;
import com.psddev.dari.db.Region;
import org.junit.Test;

public class RegionCircleIndexTest extends AbstractIndexTest<RegionIndexModel, Region> {

    @Override
    protected Class<RegionIndexModel> modelClass() {
        return RegionIndexModel.class;
    }

    @Override
    protected Region value(int index) {
        Region r = Region.empty();
        r.addCircle(new Region.Circle(0.0, 0.0, 1d + index));
        r.addCircle(new Region.Circle(2.0, 2.0, 1d + index + 1));
        return r;
    }

    @Override
    @Test
    public void contains() {
        createCompareTestModels();
        // need an intersecs
        assertCount(total, "one contains ?", new Location(0.0d, 0.0d));
        assertCount(total - 1, "one contains ?", new Location(1.5d, 0.0d));
        assertCount(total, "one contains ?", Region.sphericalCircle(0.0d, 0.0d, 0.5d));
        assertCount(3, "one contains ?", Region.sphericalCircle(0.0d, 0.0d, 1.5d));
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

    // greater thsan 0 does not work for region
    @Test(expected = IllegalArgumentException.class)
    public void gtNumber() {
        createCompareTestModels();
        assertCount(total, "one > 0");
    }

    @Test(expected = IllegalArgumentException.class)
    public void gtIllegal() {
        createCompareTestModels();
        query().where("one > true").count();
    }

    @Test(expected = IllegalArgumentException.class)
    public void geNumber() {
        createCompareTestModels();
        assertCount(total, "one >= 0");
    }

    @Test(expected = IllegalArgumentException.class)
    public void geIllegal() {
        createCompareTestModels();
        query().where("one > true").count();
    }

    @Test(expected = IllegalArgumentException.class)
    public void ltNumber() {
        createCompareTestModels();
        assertCount(1, "one < 10");
    }

    @Test(expected = IllegalArgumentException.class)
    public void ltIllegal() {
        createCompareTestModels();
        query().where("one < true").count();
    }

    @Test(expected = IllegalArgumentException.class)
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
    public void eq() {
        createCompareTestModels();
        assertCount(3L, "one = ?", value(2));
        assertCount(3L, "set = ?", value(2));
        assertCount(3L, "list = ?", value(2));
    }

    @Override
    @Test
    public void ne() {
        createCompareTestModels();
        assertCount(2L, "one != ?", value(2));
        assertCount(2L, "set != ?", value(2));
        assertCount(2L, "list != ?", value(2));
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

    @Override
    @Test
    public void sortClosestEmbeddedOneOne() {
    }

    @Override
    @Test
    public void ge() {
    }

    @Override
    @Test
    public void gt() {
    }

    @Override
    @Test
    public void le() {
    }

    @Override
    @Test
    public void lt() {
    }

    @Override
    @Test
    public void sortClosestReferenceOneOne() {
    }

    @Override
    @Test
    public void sortFarthestOneAnd() {
    }

    @Override
    @Test
    public void sortClosestReferenceOneOneJunkExistsWhere() {
    }
}

