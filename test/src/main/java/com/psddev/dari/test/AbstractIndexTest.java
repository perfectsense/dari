package com.psddev.dari.test;

import com.psddev.dari.db.Location;
import com.psddev.dari.db.Query;
import com.psddev.dari.util.TypeDefinition;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.notNullValue;

import static org.junit.Assert.assertThat;

public abstract class AbstractIndexTest<M extends AbstractIndexModel<M, T>, T> extends AbstractTest {

    protected int total;

    protected abstract Class<M> modelClass();

    protected abstract T value(int index);

    protected ModelBuilder model() {
        return new ModelBuilder();
    }

    @Before
    public void resetTotal() {
        total = 0;
    }

    protected Query<M> query() {
        return Query.from(modelClass());
    }

    @After
    public void deleteModels() {
        query().deleteAll();
    }

    @Test
    public void invalidValue() {
        M model = TypeDefinition.getInstance(modelClass()).newInstance();
        model.getState().put("one", new Object());
        model.save();

        assertThat(
                Query.from(modelClass()).first().getOne(),
                nullValue());
    }

    protected void assertCount(long count, String predicate, Object... parameters) {
        Query<M> query = query().where(predicate, parameters);
        long queryCount = query.count();

        assertThat(predicate + " count", queryCount, is(count));
        assertThat(predicate + " selectAll", query.selectAll().stream().distinct().count(), is(count));
    }

    protected void assertMissing(String field, long count) {
        assertCount(count, field + " = missing");
        assertCount(total - count, field + " != missing");
    }

    @Test
    public void missingOne() {
        model().create();
        model().create();
        model().one(value(0)).create();
        assertMissing("one", 2L);
    }

    @Test
    public void missingSet() {
        model().create();
        model().create();
        model().set(value(0)).create();
        assertMissing("set", 2L);
    }

    /**
     * All null return nothing
     */
    @Test
    public void equalsNull() {
        createCompareTestModels();
        assertCount(0, "one equalsany ?", (Object) null);
    }

    @Test
    public void notEqualsNull() {
        createCompareTestModels();
        assertCount(0, "one notequalsall ?", (Object) null);
    }

    @Test
    public void notEqualsCompoundNull() {
        createCompareTestModels();
        assertCount(5, "_any matchesany * or one notequalsall ?", (Object) null);
    }

    @Test
    public void missingList() {
        model().create();
        model().create();
        model().list(value(0)).create();
        assertMissing("list", 2L);
    }

    @Test
    public void missingReferenceOne() {
        model().referenceOne(model().create()).create();
        assertMissing("referenceOne", 1L);
    }

    @Test
    public void notMissingReferenceOneReferenceOneOne() {
        model().referenceOne(model().create()).create();
        model().referenceOne(model().referenceOne(model().one(value(0)).create()).create()).create();
        model().referenceOne(model().create()).create();
        model().create();
        assertCount(1L, "referenceOne/referenceOne/one != missing");
        assertCount(1L, "referenceOne/referenceOne != missing");
        assertCount(1L, "referenceOne/one != missing");
        assertCount(8L, "_any matchesany *");
        assertCount(4L, "referenceOne = missing");
        assertCount(4L, "referenceOne != missing");
        assertCount(7L, "one = missing");
        assertCount(1L, "one != missing");
    }

    @Test
    public void notMissingReferenceOneOne() {
        model().referenceOne(model().create()).create();
        model().referenceOne(model().one(value(0)).create()).create();
        assertCount(4L, "_any matchesany *");
        assertCount(2L, "referenceOne = missing");
        assertCount(2L, "referenceOne != missing");
        assertCount(3L, "one = missing");
        assertCount(1L, "one != missing");
        assertCount(1L, "referenceOne/one != missing");

        // example referenceOne/one != missing another app way
        String key = "referenceOne/one";
        String[] keyArr = key.split("/");
        List<String> allids = new ArrayList<>();
        List<M> list = new ArrayList<>();
        for (int i = 0; i < keyArr.length; i++) {
            if (allids.size() == 0) {
                list = query().where(keyArr[i] + " != missing").selectAll();
            } else {
                list = query().where(keyArr[i] + " != missing").and("_id = ?", allids).selectAll();
            }
            allids = new ArrayList<>();
            for (M item : list) {
                if (item != null && item.getState() != null && item.getState().getSimpleValues() != null
                        && item.getState().getSimpleValues() instanceof Map) {
                    Map<String, Object> m = item.getState().getSimpleValues();
                    if (m.get(keyArr[i]) != null) {
                        if (m.get(keyArr[i]) instanceof Map) {
                            @SuppressWarnings("unchecked")
                            Map<String, Object> o = (Map<String, Object>) m.get(keyArr[i]);
                            if (o.get("_ref") != null) {
                                allids.add((String) o.get("_ref"));
                            }
                        }
                    }
                }
            }
        }
        assertThat("remaining count", 1, is(list.size()));
    }

    @Test
    public void missingReferenceOneOne() {
        model().referenceOne(model().create()).create();
        model().referenceOne(model().one(value(0)).create()).create();
        assertCount(4L, "_any matchesany *");
        assertCount(1L, "referenceOne/one = missing");

        // example referenceOne/one = missing
        List<M> list = query().where("referenceOne != missing").selectAll();

        List<String> allids = new ArrayList<>();
        for (M item : list) {
            Map<String, Object> m = item.getState().getSimpleValues();
            if (m.get("referenceOne") != null) {
                if (m.get("referenceOne") instanceof Map) {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> o = (Map<String, Object>) m.get("referenceOne");
                    if (o.get("_ref") != null) {
                        allids.add((String) o.get("_ref"));
                    }
                }
            }
        }
        List<M> remaining = query().where("one = missing").and("_id = ?", allids).selectAll();
        assertThat("remaining count", remaining.size(), is(1));

        assertCount(1L, "referenceOne/one != missing");
    }

    @Test
    public void missingReferenceSetSet() {
        model().referenceSet(model().create()).create();
        model().referenceSet(model().create()).create();
        model().referenceSet(model().set(value(0)).create()).create();
        assertCount(2L, "referenceSet/set = missing");
        assertCount(1L, "referenceSet/set != missing");
    }

    @Test
    public void missingReferenceListList() {
        model().referenceList(model().create()).create();
        model().referenceList(model().create()).create();
        model().referenceList(model().list(value(0)).create()).create();
        assertCount(2L, "referenceList/list = missing");
        assertCount(1L, "referenceList/list != missing");
    }

    @Test
    public void checkReferenceListSimple() {
        model().referenceList(model().create()).create();
        model().referenceList(model().create()).create();
        model().referenceList(model().list(value(0)).create()).create();
        assertCount(3L, "referenceList = missing");
        assertCount(3L, "referenceList != missing");
        assertCount(6L, "referenceList = missing or referenceList != missing");
    }

    @Test
    public void missingEmbeddedOne() {
        model().embeddedOne(model()).create();
        assertMissing("embeddedOne", 1L);
    }

    @Test
    public void missingEmbeddedOneOne() {
        model().embeddedOne(model()).create();
        model().embeddedOne(model()).create();
        model().embeddedOne(model().one(value(0))).create();
        assertCount(2L, "embeddedOne/one = missing");
        assertCount(1L, "embeddedOne/one != missing");
    }

    @Test
    public void missingEmbeddedSetSet() {
        model().embeddedSet(model()).create();
        model().embeddedSet(model()).create();
        model().embeddedSet(model().set(value(0))).create();
        assertCount(2L, "embeddedSet/set = missing");
        assertCount(1L, "embeddedSet/set != missing");
    }

    @Test
    public void missingEmbeddedListList() {
        model().embeddedList(model()).create();
        model().embeddedList(model()).create();
        model().embeddedList(model().list(value(0))).create();
        assertCount(2L, "embeddedList/list = missing");
        assertCount(1L, "embeddedList/list != missing");
    }

    protected void createMissingCompoundTestModels() {
        T value0 = value(0);
        model().create();
        model().one(value0).create();
        model().set(value0).create();
        model().list(value0).create();
        model().one(value0).set(value0).create();
        model().one(value0).list(value0).create();
        model().set(value0).list(value0).create();
        model().all(value0).create();
    }

    protected void assertMissingBoth(String field1, String field2, long count) {
        assertCount(count, field1 + " = missing and " + field2 + " = missing");
        assertCount(total - count, field1 + " != missing or " + field2 + " != missing");
    }

    @Test
    public void missingOneAndSet() {
        createMissingCompoundTestModels();
        assertMissingBoth("one", "set", 2L);
    }

    @Test
    public void missingOneAndList() {
        createMissingCompoundTestModels();
        assertMissingBoth("one", "list", 2L);
    }

    @Test
    public void missingSetAndList() {
        createMissingCompoundTestModels();
        assertMissingBoth("set", "list", 2L);
    }

    protected void assertMissingEither(String field1, String field2, long count) {
        assertCount(count, field1 + " = missing or " + field2 + " = missing");
        assertCount(total - count, field1 + " != missing and " + field2 + " != missing");
    }

    @Test
    public void missingOneOrSet() {
        createMissingCompoundTestModels();
        assertMissingEither("one", "set", 6L);
    }

    @Test
    public void missingOneOrList() {
        createMissingCompoundTestModels();
        assertMissingEither("one", "list", 6L);
    }

    @Test
    public void missingSetOrList() {
        createMissingCompoundTestModels();
        assertMissingEither("set", "list", 6L);
    }

    /**
     * referenceOne = missing => this is model().create, and model().one(VALUE(0)).create()
     * referenceOne/one = missing => this is model().referenceOne(model().create()).create(), model().one(VALUE(0)), which is model().create()
     * thus = 3
     */
    @Test
    public void missingReferenceOneOrReferenceOneOne() {
        model().referenceOne(model().create()).create();
        model().referenceOne(model().one(value(0)).create()).create();
        assertCount(3L, "referenceOne = missing or referenceOne/one = missing");
    }

    protected void createCompareTestModels() {
        IntStream.range(0, 5).forEach(i -> model().all(value(i)).create());
    }

    @Test
    public void eq() {
        createCompareTestModels();
        assertCount(1L, "one = ?", value(1));
        assertCount(1L, "set = ?", value(2));
        assertCount(1L, "list = ?", value(2));
    }

    @Test
    public void eqListUUID() {
        createCompareTestModels();
        List<M> model = query().selectAll();

        List<UUID> uuids = new ArrayList<>();
        for (M m : model) {
            uuids.add(m.getId());
        }

        assertCount(uuids.size(), "_id = ?", uuids);
    }

    @Test
    public void eqListOne() {
        createCompareTestModels();
        List<M> model = query().selectAll();

        List<T> listValues = new ArrayList<>();
        for (M m : model) {
            listValues.add(m.getOne());
        }

        assertCount(listValues.size(), "one = ?", listValues);
        assertCount(listValues.size(), "set = ?", listValues);
        assertCount(listValues.size(), "list = ?", listValues);
    }

    @Test
    public void eqOneEmpty() {
        createCompareTestModels();
        assertCount(0L, "one = ?", Collections.emptyList());
    }

    @Test
    public void eqSetEmpty() {
        createCompareTestModels();
        assertCount(0L, "set = ?", Collections.emptyList());
    }

    @Test
    public void eqListEmpty() {
        createCompareTestModels();
        assertCount(0L, "list = ?", Collections.emptyList());
    }

    @Test
    public void ne() {
        createCompareTestModels();
        assertCount(4L, "one != ?", value(2));
        assertCount(4L, "set != ?", value(2));
        assertCount(4L, "list != ?", value(2));
    }

    @Test(expected = IllegalArgumentException.class)
    public void contains() {
        createCompareTestModels();
        query().where("one contains ?", value(0)).count();
    }

    @Test
    public void containsNull() {
        createCompareTestModels();
        assertCount(0, "one contains ?", (Object) null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void containsMissing() {
        createCompareTestModels();
        query().where("one contains missing").count();
    }

    @Test(expected = IllegalArgumentException.class)
    public void startsWith() {
        createCompareTestModels();
        query().where("one startsWith ?", value(0)).count();
    }

    @Test
    public void startsWithNull() {
        createCompareTestModels();
        assertCount(0, "one startsWith ?", (Object) null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void startsWithMissing() {
        createCompareTestModels();
        query().where("one startsWith missing").count();
    }

    @Test
    public void gt() {
        createCompareTestModels();
        assertCount(2L, "one > ?", value(2));
    }

    @Test
    public void gtNull() {
        createCompareTestModels();
        assertCount(0, "one > ?", (Object) null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void gtMissing() {
        createCompareTestModels();
        query().where("one > missing").count();
    }

    @Test
    public void ge() {
        createCompareTestModels();
        assertCount(3L, "one >= ?", value(2));
    }

    @Test
    public void geNull() {
        createCompareTestModels();
        assertCount(0, "one >= ?", (Object) null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void geMissing() {
        createCompareTestModels();
        query().where("one >= missing").count();
    }

    @Test
    public void lt() {
        createCompareTestModels();
        assertCount(2L, "one < ?", value(2));
    }

    @Test
    public void ltNull() {
        createCompareTestModels();
        assertCount(0, "one < ?", (Object) null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void ltMissing() {
        createCompareTestModels();
        query().where("one < missing").count();
    }

    @Test
    public void le() {
        createCompareTestModels();
        assertCount(3L, "one <= ?", value(2));
    }

    @Test
    public void leNull() {
        createCompareTestModels();
        assertCount(0, "one <= ?", (Object) null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void leMissing() {
        createCompareTestModels();
        query().where("one <= missing").count();
    }

    protected void createSortTestModels() {
        for (int i = 0, size = 26; i < size; ++ i) {
            model().all(value(i % 2 == 0 ? i : size - i)).create();
        }
    }

    protected void createEmbeddedTestModels() {
        for (int i = 0, size = 26; i < size; ++ i) {
            model().embeddedAll(model().all(value(i % 2 == 0 ? i : size - i))).create();
        }
    }

    protected void createReferenceTestModels() {
        for (int i = 0, size = 26; i < size; ++i) {
            M reference = model().all(value(i % 2 == 0 ? i : size - i)).create();
            model().referenceAll(reference).create();
        }
    }

    protected void assertOrder(boolean reverse, Query<M> query) {
        List<M> models = query.selectAll();

        assertThat(models, hasSize(total));

        for (int i = 0; i < total; ++ i) {
            assertThat(models.get(i).getOne(), is(value(reverse ? total - 1 - i : i)));
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void sortAscendingOne() {
        createSortTestModels();
        assertOrder(false, query().sortAscending("one"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void sortAscendingReferenceOneOne() {
        for (int i = 0, size = 26; i < size; ++i) {
            M reference = model().all(value(i % 2 == 0 ? i : size - i)).create();
            model().referenceAll(reference).create();
        }

        query().where("referenceOne/one != missing").sortAscending("referenceOne/one").selectAll();
    }

    @Test
    public void sortClosestReferenceOneOne() {
        for (int i = 0, size = 2; i < size; ++ i) {
            M reference = model().all(value(i % 2 == 0 ? i : size - i)).create();
            model().referenceAll(reference).create();
        }

        // Since the field is not denormalized, sort will need to be on "one" since that is what is coming back.
        // the subquery is on referenceOne, and looking for one that is not missing
        List<M> models = query().where("referenceOne/one != missing").selectAll();
        List<UUID> uList = new ArrayList<>();

        assertThat(models, hasSize(total / 2));
        for (int i = 0, size = models.size(); i < size; ++ i) {
            M ref = query().where("_id = ?", models.get(i).getReferenceOne().getId()).first();
            uList.add(ref.getId());
            assertThat(ref.getOne(), is(notNullValue()));
        }

        List<M> withOneSorted = query().where("one != missing").and("_id = ?", uList).sortClosest("one", new Location(0, 0)).selectAll();

        for (int i = 0, size = withOneSorted.size(); i < size; ++ i) {
            assertThat(withOneSorted.get(i).getOne(), is(value(i)));
        }
    }

    @Test
    public void referenceMissingAndNotMissing() {
        for (int i = 0, size = 26; i < size; ++ i) {
            M reference = model().all(value(i + 1)).create();
            model().referenceAll(reference).create();
        }

        List<M> modelAll = query().selectAll();
        List<UUID> listReferenceOne = new ArrayList<>();
        List<UUID> listOne = new ArrayList<>();
        for (M mAll : modelAll) {
            if (mAll.getReferenceOne() != null) {
                listReferenceOne.add(mAll.getId());
                M ref = query().where("_id = ?", mAll.getReferenceOne().getId()).first();
                if (ref.getOne() != null) {
                    listOne.add(mAll.getId());
                }
            }
        }
        assertThat(listReferenceOne, hasSize(total / 2));
        assertThat(listOne, hasSize(total / 2));
        assertThat(listOne, containsInAnyOrder(listReferenceOne.toArray()));

        // 26 from this
        List<M> model1 = query().where("referenceOne != missing").selectAll();
        List<UUID> list1 = new ArrayList<>();
        for (M m1 : model1) {
            if (m1.getReferenceOne() != null) {
                list1.add(m1.getId());
            }
        }
        assertThat(list1, hasSize(total / 2));
        assertThat(list1, containsInAnyOrder(listReferenceOne.toArray()));

        // 26 from this - same ones
        List<M> model2 = query().where("referenceOne/one != missing").selectAll();
        List<UUID> list2 = new ArrayList<>();
        for (M m2 : model2) {
            list2.add(m2.getId());
        }
        assertThat(list2, hasSize(total / 2));
        assertThat(list2, containsInAnyOrder(list1.toArray()));
        assertThat(list2, containsInAnyOrder(listOne.toArray()));

        Set<UUID> list3 = new HashSet<>();
        list3.addAll(list1);
        list3.addAll(list2);
        assertThat(list3, hasSize(total / 2));

        List<M> models = query().where("((referenceOne/one != missing) or (referenceOne != missing))").selectAll();
        List<UUID> modelsList = new ArrayList<>();
        for (M mList : models) {
            modelsList.add(mList.getId());
        }
        assertThat(models, hasSize(total / 2));
        assertThat(modelsList, containsInAnyOrder(list3.toArray()));

        for (int i = 0, size = models.size(); i < size; ++ i) {
            if (models.get(i).getReferenceOne() == null) {
                Assert.fail("getReferenceOne cannot be missing");
            } else {
                M ref = query().where("_id = ?", models.get(i).getReferenceOne().getId()).first();
                if (ref.getOne() == null) {
                    Assert.fail("getReferenceOne/One cannot be missing");
                }
            }
        }
    }

    @Test(expected = Query.NoFieldException.class)
    public void sortClosestReferenceOneOneJunkSort() {
        for (int i = 0, size = 26; i < size; ++ i) {
            M reference = model().all(value(i % 2 == 0 ? i : size - i)).create();
            model().referenceAll(reference).create();
        }

        List<M> models = query().where("referenceOne/one != missing").sortClosest("referenceOne/junk", new Location(0, 0)).selectAll();
    }

    @Test(expected = com.psddev.dari.db.Query.NoFieldException.class)
    public void sortClosestReferenceOneOneJunkExistsWhere() {
        for (int i = 0, size = 26; i < size; ++ i) {
            M reference = model().all(value(i % 2 == 0 ? i : size - i)).create();
            model().referenceAll(reference).create();
        }

        List<M> models = query().where("referenceOne/junk != missing").sortClosest("referenceOne/one", new Location(0, 0)).selectAll();

        assertThat(models, hasSize(0));

    }

    @Test
    public void sortClosestEmbeddedOneOne() {
        for (int i = 0, size = 26; i < size; ++ i) {
            model().embeddedAll(model().all(value(i % 2 == 0 ? i : size - i))).create();
        }

        List<M> models = query().where("embeddedOne/one != missing").sortClosest("embeddedOne/one", new Location(0, 0)).selectAll();

        assertThat(models, hasSize(total));

        for (int i = 0, size = models.size(); i < size; ++ i) {
            assertThat(models.get(i).getEmbeddedOne().getOne(), is(value(i)));
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void sortAscendingEmbeddedOneOne() {
        for (int i = 0, size = 26; i < size; ++ i) {
            model().embeddedAll(model().all(value(i % 2 == 0 ? i : size - i))).create();
        }

        List<M> models = query().where("embeddedOne/one != missing").sortAscending("embeddedOne/one").selectAll();
    }

    @Test(expected = IllegalArgumentException.class)
    public void sortDescendingOne() {
        createSortTestModels();
        assertOrder(true, query().sortDescending("one"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void sortClosestOne() {
        createSortTestModels();
        query().sortClosest("one", new Location(0, 0)).first();
    }

    @Test(expected = IllegalArgumentException.class)
    public void sortFarthestOne() {
        createSortTestModels();
        query().sortFarthest("one", new Location(0, 0)).first();
    }

    @Test(expected = Query.NoFieldException.class)
    public void sortFarthestOneAndField() {
        createSortTestModels();
        query().where("one = missing").and("two = missing").sortFarthest("one", new Location(0, 0)).selectAll();
    }

    @Test
    public void sortFarthestOneAnd() {
        createSortTestModels();
        List<M> models = query().where("one != missing").and("set != missing").and("list != missing").sortFarthest("one", new Location(0, 0)).selectAll();
        assertThat(models, hasSize(total));

        for (int i = 0, size = models.size(); i < size; ++ i) {
            assertThat(models.get(i).getOne(), is(value(total - i - 1)));
        }

    }

    @Test(expected = UnsupportedOperationException.class)
    public void sortUnknown() {
        createSortTestModels();
        query().sort("unknown", "one").first();
    }

    protected class ModelBuilder {

        private final M model;

        public ModelBuilder() {
            model = TypeDefinition.getInstance(modelClass()).newInstance();
        }

        public ModelBuilder all(T value) {
            one(value);
            set(value);
            list(value);
            return this;
        }

        public ModelBuilder one(T value) {
            model.setOne(value);
            return this;
        }

        public ModelBuilder set(T value) {
            model.getSet().add(value);
            return this;
        }

        public ModelBuilder list(T value) {
            model.getList().add(value);
            model.getList().add(value);
            return this;
        }

        public ModelBuilder referenceAll(M reference) {
            referenceOne(reference);
            referenceSet(reference);
            referenceList(reference);
            return this;
        }

        public ModelBuilder referenceOne(M reference) {
            model.setReferenceOne(reference);
            return this;
        }

        public ModelBuilder referenceSet(M reference) {
            model.getReferenceSet().add(reference);
            return this;
        }

        public ModelBuilder referenceList(M reference) {
            model.getReferenceList().add(reference);
            model.getReferenceList().add(reference);
            return this;
        }

        public ModelBuilder embeddedAll(ModelBuilder embeddedBuilder) {
            embeddedOne(embeddedBuilder);
            embeddedSet(embeddedBuilder);
            embeddedList(embeddedBuilder);
            return this;
        }

        public ModelBuilder embeddedOne(ModelBuilder embeddedBuilder) {
            model.setEmbeddedOne(embeddedBuilder.model);
            return this;
        }

        public ModelBuilder embeddedSet(ModelBuilder embeddedBuilder) {
            model.getEmbeddedSet().add(embeddedBuilder.model);
            return this;
        }

        public ModelBuilder embeddedList(ModelBuilder embeddedBuilder) {
            model.getEmbeddedList().add(embeddedBuilder.model);
            model.getEmbeddedList().add(embeddedBuilder.model);
            return this;
        }

        public M create() {
            model.save();
            ++ total;
            return model;
        }
    }
}
