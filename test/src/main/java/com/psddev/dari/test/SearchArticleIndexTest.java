package com.psddev.dari.test;

import com.psddev.dari.db.Location;
import com.psddev.dari.db.Query;
import com.psddev.dari.db.Sorter;
import com.psddev.dari.util.TypeDefinition;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.util.List;

import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class SearchArticleIndexTest extends AbstractIndexTest<SearchArticleIndexModel, String> {

    @Override
    protected Class<SearchArticleIndexModel> modelClass() {
        return SearchArticleIndexModel.class;
    }

    @Override
    protected String value(int index) {
        return String.valueOf(index);
    }

    @Test
    public void embeddedTest() {
        createEmbeddedTestModels();
        List<SearchArticleIndexModel> s = query().and("embeddedOne/one = 0").selectAll();
        assertThat("check size", s, hasSize(1));
    }

    @Test
    public void referenceTest() {
        createReferenceTestModels();
        List<SearchArticleIndexModel> s = query().and("referenceOne/one = 0").selectAll();
        assertThat("check size", s, hasSize(1));
    }

    @Test
    public void eqIllegal() {
        createCompareTestModels();
        query().and("one = ?", true).count();
    }

    @Override
    @Test(expected = IllegalArgumentException.class)
    public void sortClosestEmbeddedOneOne() {
        createEmbeddedTestModels();

        List<SearchArticleIndexModel> models = query().where("embeddedOne/one != missing").sortClosest("embeddedOne/one", new Location(0, 0)).selectAll();
    }

    @Test(expected = com.psddev.dari.db.Query.NoIndexException.class)
    public void whereNotIndexed() {
        createReferenceTestModels();

        List<SearchArticleIndexModel> models = query().where("referenceOne/notIndexed != missing").sortClosest("referenceOne/one", new Location(0, 0)).selectAll();

    }

    @Override
    @Test
    public void contains() {
        createCompareTestModels();
        query().where("one contains ?", value(0)).count();
    }

    @Override
    @Test
    public void invalidValue() {
        // Putting Double into String is ok.
        SearchArticleIndexModel model = TypeDefinition.getInstance(modelClass()).newInstance();
        Double x = 20.0;
        model.getState().put("one", x);
        model.save();

        assertThat(
                Query.from(modelClass()).first().getOne(),
                is("20.0"));
    }

    @Override
    @Test
    public void startsWith() {
        super.startsWith();
    }

    @Override
    @Test(expected = IllegalArgumentException.class)
    public void sortFarthestOneAnd() {
        createSortTestModels();
        List<SearchArticleIndexModel> models = query().where("one != missing").and("set != missing").and("list != missing").sortFarthest("one", new Location(0, 0)).selectAll();
        assertThat(models, Matchers.hasSize(total));

    }

    @Override
    @Test(expected = IllegalArgumentException.class)
    public void sortClosestReferenceOneOne() {
        super.sortClosestReferenceOneOne();
    }

    @Override
    @Test
    public void gt() {
        createCompareTestModels();
        query().where("one > ?", value(0)).count();
    }

    @Override
    @Test
    public void ge() {
        createCompareTestModels();
        query().where("one >= ?", value(0)).count();
    }

    @Override
    @Test
    public void lt() {
        createCompareTestModels();
        query().where("one < ?", value(0)).count();
    }

    @Override
    @Test
    public void le() {
        createCompareTestModels();
        query().where("one <= ?", value(0)).count();
    }

    @Override
    @Test
    public void sortAscendingOne() {
        createSortTestModels();
        query().sortAscending("one").count();
    }

    @Override
    @Test
    public void sortAscendingEmbeddedOneOne() {
        super.sortAscendingEmbeddedOneOne();
    }

    @Override
    @Test
    public void sortDescendingOne() {
        createSortTestModels();
        query().sortDescending("one").count();
    }

    @Override
    @Test(expected = IllegalArgumentException.class)
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
