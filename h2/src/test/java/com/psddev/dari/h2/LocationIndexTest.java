package com.psddev.dari.h2;

import com.psddev.dari.db.Location;
import com.psddev.dari.db.Region;
import com.psddev.dari.db.Sorter;
import org.junit.Test;

public class LocationIndexTest extends AbstractIndexTest<LocationIndexModel, Location> {

    @Override
    protected Class<LocationIndexModel> modelClass() {
        return LocationIndexModel.class;
    }

    @Override
    protected Location value(int index) {
        return new Location(index, index);
    }

    @Test
    public void eqRegion() {
        createCompareTestModels();
        assertCount(5, "one = ?", Region.sphericalCircle(0.0d, 0.0d, 5.5d));
    }

    @Test(expected = IllegalArgumentException.class)
    public void eqIllegal() {
        createCompareTestModels();
        query().and("one = true").count();
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
    @Test(expected = IllegalArgumentException.class)
    public void gt() {
        createCompareTestModels();
        query().where("one > ?", value(0)).count();
    }

    @Override
    @Test(expected = IllegalArgumentException.class)
    public void gtNull() {
        createCompareTestModels();
        query().and("one > ?", (Object) null).count();
    }

    @Override
    @Test(expected = IllegalArgumentException.class)
    public void ge() {
        createCompareTestModels();
        query().where("one >= ?", value(0)).count();
    }

    @Override
    @Test(expected = IllegalArgumentException.class)
    public void geNull() {
        createCompareTestModels();
        query().and("one >= ?", (Object) null).count();
    }

    @Override
    @Test(expected = IllegalArgumentException.class)
    public void lt() {
        createCompareTestModels();
        query().where("one < ?", value(0)).count();
    }

    @Override
    @Test(expected = IllegalArgumentException.class)
    public void ltNull() {
        createCompareTestModels();
        query().and("one < ?", (Object) null).count();
    }

    @Override
    @Test(expected = IllegalArgumentException.class)
    public void le() {
        createCompareTestModels();
        query().where("one <= ?", value(0)).count();
    }

    @Override
    @Test(expected = IllegalArgumentException.class)
    public void leNull() {
        createCompareTestModels();
        query().and("one <= ?", (Object) null).count();
    }

    @Override
    @Test(expected = IllegalArgumentException.class)
    public void sortAscendingOne() {
        createSortTestModels();
        query().sortAscending("one").count();
    }

    @Override
    @Test(expected = IllegalArgumentException.class)
    public void sortAscendingReferenceOneOne() {
        super.sortAscendingReferenceOneOne();
    }

    @Override
    @Test(expected = IllegalArgumentException.class)
    public void sortAscendingEmbeddedOneOne() {
        super.sortAscendingEmbeddedOneOne();
    }

    @Override
    @Test(expected = IllegalArgumentException.class)
    public void sortDescendingOne() {
        createSortTestModels();
        query().sortDescending("one").count();
    }

    @Override
    @Test
    public void sortClosestOne() {
        createSortTestModels();
        assertOrder(false, query().sortClosest("one", new Location(0, 0)));
    }

    @Test(expected = IllegalArgumentException.class)
    public void closestIllegalSize() {
        createSortTestModels();
        query().sort(Sorter.CLOSEST_OPERATOR, "one").first();
    }

    @Test(expected = IllegalArgumentException.class)
    public void closestIllegalType() {
        createSortTestModels();
        query().sort(Sorter.CLOSEST_OPERATOR, "one", new Object()).first();
    }

    @Override
    @Test
    public void sortFarthestOne() {
        createSortTestModels();
        assertOrder(true, query().sortFarthest("one", new Location(0, 0)));
    }

    @Test(expected = IllegalArgumentException.class)
    public void farthestIllegalSize() {
        createSortTestModels();
        query().sort(Sorter.FARTHEST_OPERATOR, "one").first();
    }

    @Test(expected = IllegalArgumentException.class)
    public void farthestIllegalType() {
        createSortTestModels();
        query().sort(Sorter.FARTHEST_OPERATOR, "one", new Object()).first();
    }
}
