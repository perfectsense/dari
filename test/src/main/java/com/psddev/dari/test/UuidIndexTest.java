package com.psddev.dari.test;

import com.psddev.dari.db.Location;
import com.psddev.dari.db.Query;
import org.junit.Test;

import java.util.List;
import java.util.UUID;

import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class UuidIndexTest extends AbstractIndexTest<UuidIndexModel, UUID> {

    @Override
    protected Class<UuidIndexModel> modelClass() {
        return UuidIndexModel.class;
    }

    @Override
    protected UUID value(int index) {
        return new UUID(0L, 41L * index);
    }

    @Override
    @Test(expected = Query.NoFieldException.class)
    public void sortClosestReferenceOneOneJunkSort() {
        super.sortClosestReferenceOneOneJunkSort();
    }

    @Override
    @Test(expected = IllegalArgumentException.class)
    public void containsNull() {
        createCompareTestModels();
        query().and("one contains ?", (Object) null).count();
    }

    @Override
    @Test(expected = IllegalArgumentException.class)
    public void sortClosestReferenceOneOne() {
        super.sortClosestReferenceOneOne();
    }

    @Override
    @Test(expected = IllegalArgumentException.class)
    public void startsWithNull() {
        createCompareTestModels();
        query().and("one startsWith ?", (Object) null).count();
    }

    @Override
    @Test
    public void sortDescendingOne() {
        createSortTestModels();
        assertOrder(true, query().sortDescending("one"));
    }

    @Override
    @Test
    public void sortAscendingOne() {
        createSortTestModels();
        assertOrder(false, query().sortAscending("one"));
    }

    @Override
    @Test(expected = IllegalArgumentException.class)
    public void sortClosestEmbeddedOneOne() {
        for (int i = 0, size = 26; i < size; ++ i) {
            model().embeddedAll(model().all(value(i % 2 == 0 ? i : size - i))).create();
        }

        query().where("embeddedOne/one != missing").sortClosest("embeddedOne/one", new Location(0, 0)).selectAll();
    }

    @Override
    @Test
    public void sortAscendingEmbeddedOneOne() {
        for (int i = 0, size = 26; i < size; ++ i) {
            model().embeddedAll(model().all(value(i % 2 == 0 ? i : size - i))).create();
        }

        List<UuidIndexModel> models = query().where("embeddedOne/one != missing").sortAscending("embeddedOne/one").selectAll();

        assertThat(models, hasSize(26));

        for (int i = 0; i < total; ++ i) {
            assertThat(models.get(i).getEmbeddedOne().getOne(), is(value(i)));
        }
    }

    @Override
    @Test(expected = IllegalArgumentException.class)
    public void sortFarthestOneAnd() {
        createSortTestModels();
        query().where("one != missing").and("set != missing").and("list != missing").sortFarthest("one", new Location(0, 0)).selectAll();
    }
}
