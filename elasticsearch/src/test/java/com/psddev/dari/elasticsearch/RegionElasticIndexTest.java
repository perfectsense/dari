package com.psddev.dari.elasticsearch;

import com.psddev.dari.db.Location;
import com.psddev.dari.db.Query;
import com.psddev.dari.db.Region;
import org.junit.Test;

import java.util.List;

import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;


public class RegionElasticIndexTest extends AbstractElasticIndexTest<RegionElasticIndexModel, Region> {

    @Override
    protected Class<RegionElasticIndexModel> modelClass() {
        return RegionElasticIndexModel.class;
    }

    @Override
    protected Region value(int index) {
        return Region.sphericalCircle(0.0d, 0.0d, index + 1);
    }

    @Override
    @Test
    public void contains() {
        createCompareTestModels();
        // need an intersecs
        assertCount(total, "one contains ?", new Location(0.0d, 0.0d));
        assertCount(total - 1, "one contains ?", new Location(1.5d, 0.0d));
        assertCount(total, "one contains ?", Region.sphericalCircle(0.0d, 0.0d, 0.5d));
        assertCount(total - 1, "one contains ?", Region.sphericalCircle(0.0d, 0.0d, 1.5d));
    }

    @Override
    @Test
    public void eq() {
        createCompareTestModels();
        assertCount(2L, "one = ?", Region.sphericalCircle(0.0d, 0.0d, 2));
        assertCount(2L, "set = ?", Region.sphericalCircle(0.0d, 0.0d, 2));
        assertCount(2L, "list = ?", Region.sphericalCircle(0.0d, 0.0d, 2));
    }

    @Override
    @Test(expected = IllegalArgumentException.class)
    public void sortClosestEmbeddedOneOne() {
        for (int i = 0, size = 26; i < size; ++ i) {
            model().embeddedAll(model().all(value(i % 2 == 0 ? i : size - i))).create();
        }

        List models = query().where("embeddedOne.one != missing").sortClosest("embeddedOne.one", new Location(0, 0)).selectAll();
    }

    @Override
    @Test(expected = IllegalArgumentException.class)
    public void startsWithNull() {
        createCompareTestModels();
        query().and("one startsWith ?", (Object) null).count();
    }

    @Override
    @Test(expected = Query.NoFieldException.class)
    public void sortClosestReferenceOneOneJunkExistsWhere() {
        for (int i = 0, size = 26; i < size; ++ i) {
            RegionElasticIndexModel reference = model().all(value(i % 2 == 0 ? i : size - i)).create();
            model().referenceAll(reference).create();
        }

        List models = query().where("referenceOne/junk != missing").sortClosest("referenceOne/one", new Location(0, 0)).selectAll();
    }

    @Override
    @Test(expected = IllegalArgumentException.class)
    public void ge() {
        createCompareTestModels();
        assertCount(3L, "one >= ?", value(2));
    }

    @Override
    @Test(expected = IllegalArgumentException.class)
    public void lt() {
        createCompareTestModels();
        assertCount(2L, "one < ?", value(2));
    }

    @Override
    @Test(expected = IllegalArgumentException.class)
    public void le() {
        createCompareTestModels();
        assertCount(3L, "one <= ?", value(2));
    }

    @Override
    @Test(expected = IllegalArgumentException.class)
    public void gt() {
        createCompareTestModels();
        assertCount(2L, "one > ?", value(2));
    }

    @Override
    @Test(expected = IllegalArgumentException.class)
    public void sortClosestReferenceOneOne() {
        for (int i = 0, size = 26; i < size; ++ i) {
            RegionElasticIndexModel reference = model().all(value(i % 2 == 0 ? i : size - i)).create();
            model().referenceAll(reference).create();
        }

        List<RegionElasticIndexModel> models = query().where("referenceOne/one != missing").sortClosest("referenceOne/one", new Location(0, 0)).selectAll();

    }

    // = is within, and != is not within
    @Override
    @Test
    public void ne() {
        createCompareTestModels();
        assertCount(2L, "one != ?", value(2));
        assertCount(2L, "set != ?", value(2));
        assertCount(2L, "list != ?", value(2));
    }

    @Override
    @Test(expected = IllegalArgumentException.class)
    public void sortFarthestOneAnd() {
        createSortTestModels();
        List<RegionElasticIndexModel> models = query().where("one != missing").and("set != missing").and("list != missing").sortFarthest("one", new Location(0, 0)).selectAll();
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

