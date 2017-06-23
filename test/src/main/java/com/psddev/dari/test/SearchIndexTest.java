package com.psddev.dari.test;

import com.psddev.dari.db.Database;
import com.psddev.dari.db.DatabaseEnvironment;
import com.psddev.dari.db.Grouping;
import com.psddev.dari.db.ObjectField;
import com.psddev.dari.db.ObjectMethod;
import com.psddev.dari.db.ObjectType;
import com.psddev.dari.db.Query;
import com.psddev.dari.db.State;
import com.psddev.dari.util.PaginatedResult;
import org.apache.commons.lang3.time.DateUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.hasItems;

import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertEquals;

import java.util.UUID;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SearchIndexTest extends AbstractTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(SearchIndexTest.class);

    private static final String FOO = "foo";

    private static boolean searchOverlapModelIndex = false;

    @After
    public void deleteModels() {
        Query.from(SearchIndexModel.class).deleteAllImmediately();
        if (searchOverlapModelIndex) {
            Query.from(SearchOverlapModel.class).deleteAllImmediately();
        }
        Query.from(PersonIndexModel.class).deleteAllImmediately();
        Query.from(MethodComplexModel.class).deleteAllImmediately();
    }

    @Test
    public void testTypes() throws Exception {
        Date now = new Date();
        SearchIndexModel search = new SearchIndexModel();
        search.eid = "939393";
        search.name = "Bill";
        search.message = "tough";
        search.postDate = now;
        search.l = 5L;
        search.isOn = true;
        search.idBox = UUID.fromString("0000014f-74eb-d39d-a9ff-74eb74240000");
        search.save();

        List<SearchIndexModel> fooResult = Query
                .from(SearchIndexModel.class)
                .where("postDate = ?", now)
                .selectAll();
        assertThat(fooResult, hasSize(1));

        fooResult = Query
                .from(SearchIndexModel.class)
                .where("isOn = ?", true)
                .selectAll();
        assertThat(fooResult, hasSize(1));

        fooResult = Query
                .from(SearchIndexModel.class)
                .where("idBox = ?", UUID.fromString("0000014f-74eb-d39d-a9ff-74eb74240000"))
                .selectAll();
        assertThat(fooResult, hasSize(1));

        fooResult = Query
                .from(SearchIndexModel.class)
                .where("l = ?", 5L)
                .selectAll();
        assertThat(fooResult, hasSize(1));

        fooResult = Query
                .from(SearchIndexModel.class)
                .where("l >= ?", 5L)
                .selectAll();
        assertThat(fooResult, hasSize(1));

        fooResult = Query
                .from(SearchIndexModel.class)
                .where("l > ?", 4L)
                .selectAll();
        assertThat(fooResult, hasSize(1));

        fooResult = Query
                .from(SearchIndexModel.class)
                .where("l < ?", 6L)
                .selectAll();
        assertThat(fooResult, hasSize(1));

        fooResult = Query
                .from(SearchIndexModel.class)
                .where("l <= ?", 5L)
                .selectAll();
        assertThat(fooResult, hasSize(1));
    }

    @Test
    public void testOne() throws Exception {
        SearchIndexModel search = new SearchIndexModel();
        search.eid = "939393";
        search.name = "Bill";
        search.message = "tough";
        search.save();

        List<SearchIndexModel> fooResult = Query
                .from(SearchIndexModel.class)
                .where("eid matches ?", "939393")
                .selectAll();

        assertThat(fooResult, hasSize(1));
        assertEquals("939393", fooResult.get(0).eid);
        assertEquals("Bill", fooResult.get(0).name);
        assertEquals("tough", fooResult.get(0).message);
    }

    @Test
    public void oneMatches() throws Exception {
        Stream.of(FOO, "bar", "qux").forEach(string -> {
            SearchIndexModel model = new SearchIndexModel();
            model.one = string;
            model.set.add(FOO);
            model.list.add(FOO);
            model.map.put(FOO, FOO);
            model.save();
        });

        List<SearchIndexModel> fooResult = Query
                .from(SearchIndexModel.class)
                .where("one matches ?", FOO)
                .selectAll();

        assertThat(fooResult, hasSize(1));
        assertThat(fooResult.get(0).one, equalTo(FOO));
    }

    @Test
    public void setMatches() throws Exception {
        Stream.of(FOO, "bar", "qux").forEach(string -> {
            SearchIndexModel model = new SearchIndexModel();
            model.one = FOO;
            model.set.add(string);
            model.list.add(FOO);
            model.map.put(FOO, FOO);
            model.save();
        });

        List<SearchIndexModel> fooResult = Query
                .from(SearchIndexModel.class)
                .where("set matches ?", FOO)
                .selectAll();

        assertThat(fooResult, hasSize(1));
        assertThat(fooResult.get(0).set, hasSize(1));
        assertThat(fooResult.get(0).set.iterator().next(), equalTo(FOO));
    }

    @Test
    public void listMatches() throws Exception {
        Stream.of(FOO, "bar", "qux").forEach(string -> {
            SearchIndexModel model = new SearchIndexModel();
            model.one = FOO;
            model.set.add(FOO);
            model.list.add(string);
            model.map.put(FOO, FOO);
            model.save();
        });

        List<SearchIndexModel> fooResult = Query
                .from(SearchIndexModel.class)
                .where("list matches ?", FOO)
                .selectAll();

        assertThat(fooResult, hasSize(1));
        assertThat(fooResult.get(0).list, hasSize(1));
        assertThat(fooResult.get(0).list.get(0), equalTo(FOO));
    }

    @Test
    public void mapMatches() throws Exception {
        Stream.of(FOO, "bar", "qux").forEach(string -> {
            SearchIndexModel model = new SearchIndexModel();
            model.one = FOO;
            model.set.add(FOO);
            model.list.add(FOO);
            model.map.put(string, string);
            model.save();
        });

        // note this is different from h2, but seems better since it is specific.
        List<SearchIndexModel> fooResult = Query
                .from(SearchIndexModel.class)
                .where("map matches ?", FOO)
                .selectAll();

        assertThat("Size of result", fooResult, hasSize(1));
        assertThat("checking size of map", fooResult.get(0).map.size(), equalTo(1));
        assertThat("checking iterator", fooResult.get(0).map.values().iterator().next(), equalTo(FOO));
    }

    @Test
    public void anyMatches() throws Exception {
        Stream.of(FOO, "bar", "qux").forEach(string -> {
            SearchIndexModel model = new SearchIndexModel();
            model.one = string;
            model.set.add(FOO);
            model.save();
        });

        List<SearchIndexModel> fooResult = Query
                .from(SearchIndexModel.class)
                .where("_any matches ?", FOO)
                .selectAll();

        assertThat(fooResult, hasSize(3));
    }

    @Test
    public void wildcard() throws Exception {
        Stream.of("f", "fo", "foo").forEach(string -> {
            SearchIndexModel model = new SearchIndexModel();
            model.one = string;
            model.save();
        });

        assertThat(Query.from(SearchIndexModel.class).where("one matches ?", "f*").count(), equalTo(3L));
        assertThat(Query.from(SearchIndexModel.class).where("one matches ?", "fo*").count(), equalTo(2L));
        assertThat(Query.from(SearchIndexModel.class).where("one matches ?", "foo*").count(), equalTo(1L));
    }

    // eid of 1 is more relevant since one = foo does not work on H2
    @Test
    public void sortRelevant() throws Exception {
        SearchIndexModel model = new SearchIndexModel();
        model.one = "foo";
        model.name = "qux";
        model.set.add("qux");
        model.list.add("qux");
        model.map.put("qux", "qux");
        model.eid = "1";
        model.save();

        model = new SearchIndexModel();
        model.one = "west";
        model.name = "west";
        model.set.add("west");
        model.list.add(FOO);
        model.map.put("west", "west");
        model.eid = "2";
        model.save();

        model = new SearchIndexModel();
        model.one = "qux";
        model.name = "west";
        model.set.add("west");
        model.list.add("qux");
        model.map.put("qux", "qux");
        model.eid = "3";
        model.save();

        List<SearchIndexModel> fooResult = Query
                .from(SearchIndexModel.class)
                .where("_any matches ?", FOO)
                .sortRelevant(10.0, "one matches ?", FOO)
                .selectAll();

        assertThat(fooResult, hasSize(2));

        assertThat("check 0 and 1", fooResult.get(0).eid, is(equalTo("1")));
        assertThat("check 1 and 2", fooResult.get(1).eid, is(equalTo("2")));
    }

    @Test
    public void testSortString() throws Exception {

        Stream.of("FOO", "BAR", "QUX", "foo", "bar", "qux").forEach(string -> {
            SearchIndexModel model = new SearchIndexModel();
            model.one = string;
            model.set.add("foo");
            model.save();
        });

        List<SearchIndexModel> fooResult = Query
                .from(SearchIndexModel.class)
                .sortAscending("one")
                .selectAll();

        assertThat("check size", fooResult, hasSize(6));
        assertThat("Order 1", fooResult.get(0).one, is("BAR"));
        assertThat("Order 2", fooResult.get(1).one, is("bar"));
        assertThat("Order 3", fooResult.get(2).one, is("FOO"));
        assertThat("Order 3", fooResult.get(3).one, is("foo"));
        assertThat("Order 3", fooResult.get(4).one, is("QUX"));
        assertThat("Order 3", fooResult.get(5).one, is("qux"));
    }

    @Test
    public void testSortStringNeverIndexed() throws Exception {
        Stream.of(1.0f, 2.0f, 3.0f).forEach(f -> {
            SearchIndexModel model = new SearchIndexModel();
            model.f = f;
            model.save();
        });

        // this should not throw exception since the field is annotated @Indexed
        List<SearchIndexModel> fooResult = Query
                .from(SearchIndexModel.class)
                .sortAscending("neverIndexed")
                .selectAll();

         assertThat("check size", fooResult, hasSize(3));
    }

    @Test(expected = Query.NoFieldException.class)
    public void testSortStringNoSuchField() throws Exception {

        Stream.of(1.0f, 2.0f, 3.0f).forEach(f -> {
            SearchIndexModel model = new SearchIndexModel();
            model.f = f;
            model.save();
        });

        List<SearchIndexModel> fooResult = Query
                .from(SearchIndexModel.class)
                .sortAscending("nine")
                .selectAll();
    }

    @Test
    public void testSortFloat() throws Exception {
        Stream.of(1.0f, 2.0f, 3.0f).forEach(f -> {
            SearchIndexModel model = new SearchIndexModel();
            model.f = f;
            model.save();
        });

        List<SearchIndexModel> fooResult = Query
                .from(SearchIndexModel.class)
                .sortAscending("f")
                .selectAll();

        assertThat("check size", fooResult, hasSize(3));
        assertThat("check 0 and 1 order", fooResult.get(0).f, lessThan(fooResult.get(1).f));
        assertThat("check 1 and 2 order", fooResult.get(1).f, lessThan(fooResult.get(2).f));
    }

    @Test
    public void testQueryExtension() throws Exception {
        SearchIndexModel search = new SearchIndexModel();
        search.eid = "111111";
        search.name = "Bill";
        search.message = "Welcome";
        search.save();

        List<SearchIndexModel> r = Query
                .from(SearchIndexModel.class)
                .where("eid matches ?", "111111")
                .selectAll();

        assertThat(r, notNullValue());
        assertThat(r, hasSize(1));
        assertEquals("Bill", r.get(0).getName());
        assertEquals("Welcome", r.get(0).getMessage());
    }

    // sortAscending on floats not working in H2
    @Test
    public void testReferenceAscending() throws Exception {
        Stream.of(1.0f, 2.0f, 3.0f).forEach(f -> {
            SearchIndexModel ref = new SearchIndexModel();
            ref.f = f;
            ref.save();
            SearchIndexModel model = new SearchIndexModel();
            model.setReference(ref);
            model.save();
        });

        List<SearchIndexModel> fooResult = Query
                .from(SearchIndexModel.class)
                .sortAscending("f")
                .selectAll();

        assertThat("check size", fooResult, hasSize(6));
        assertThat("check 0 and 1 order", fooResult.get(0).f, lessThan(fooResult.get(1).f));
        assertThat("check 1 and 2 order", fooResult.get(1).f, lessThan(fooResult.get(2).f));
    }

    @Test
    public void testReferenceGetTypeRef() throws Exception {
        SearchIndexModel ref = new SearchIndexModel();
        ref.f = 1.0f;
        ref.save();
        SearchIndexModel model = new SearchIndexModel();
        model.setReference(ref);
        model.save();

        // [user equalsany 0000015a-ab7e-d2d3-a97f-bffedfc30000]

        List<SearchIndexModel> fooResult = Query
                .from(SearchIndexModel.class)
                .where("reference equalsany ?", ref.getId())
                .selectAll();

        assertThat("check size", fooResult, hasSize(1));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testFloatGroupBySortAscException() throws Exception {
        Stream.of("A", "B", "C", "B", "C", "C").forEach((String f) -> {
            SearchIndexModel model = new SearchIndexModel();
            model.one = f;
            model.save();
        });

        Query.from(SearchIndexModel.class).sortAscending("one").groupBy("f");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testFloatGroupBySortAscException2() throws Exception {
        Stream.of("A", "B", "C", "B", "C", "C").forEach((String f) -> {
            SearchIndexModel model = new SearchIndexModel();
            model.one = f;
            model.save();
        });

        Query.from(SearchIndexModel.class).sortAscending("f").groupBy("one");
    }

    @Test
    public void testFloatGroupBySortGroup2() throws Exception {
        SearchIndexModel model = new SearchIndexModel();
        model.one = "A";
        model.f = 1.0f;
        model.save();
        SearchIndexModel model1 = new SearchIndexModel();
        model1.one = "A";
        model1.f = 1.0f;
        model1.save();
        SearchIndexModel model2 = new SearchIndexModel();
        model2.one = "B";
        model2.f = 5.8f;
        model2.save();
        SearchIndexModel model3 = new SearchIndexModel();
        model3.one = "B";
        model3.f = 1.0f;
        model3.save();
        SearchIndexModel model4 = new SearchIndexModel();
        model4.one = "C";
        model4.f = 2.0f;
        model4.save();
        SearchIndexModel model5 = new SearchIndexModel();
        model5.one = "C";
        model5.f = 2.0f;
        model5.save();
        SearchIndexModel model6 = new SearchIndexModel();
        model6.one = "C";
        model6.f = 2.0f;
        model6.save();

        List<Grouping<SearchIndexModel>> groupings = Query.from(SearchIndexModel.class).groupBy("one", "f");

        assertThat("check size", groupings, hasSize(4));

        List<Grouping<SearchIndexModel>> groupings2 = Query.from(SearchIndexModel.class).sortAscending("one").groupBy("one", "f");
        assertThat(groupings2.get(0).getKeys().get(0), is("A"));

        List<Grouping<SearchIndexModel>> groupings3 = Query.from(SearchIndexModel.class).sortDescending("one").groupBy("one", "f");
        assertThat(groupings3.get(0).getKeys().get(0), is("C"));

        List<Grouping<SearchIndexModel>> groupings4 = Query.from(SearchIndexModel.class).sortDescending("f").groupBy("one", "f");
        assertThat(groupings4.get(0).getKeys().get(0), is("B"));

        PaginatedResult<Grouping<SearchIndexModel>> groupingsPage = Query.from(SearchIndexModel.class).sortDescending("f").groupByPartial(0, 1, "one", "f");
        List<Grouping<SearchIndexModel>> groupings5 = groupingsPage.getItems();
        assertThat("check size 1", groupings5, hasSize(1));

        PaginatedResult<Grouping<SearchIndexModel>> groupingsPage2 = Query.from(SearchIndexModel.class).sortDescending("f").groupByPartial(0, 1, "f");
        List<Grouping<SearchIndexModel>> groupings6 = groupingsPage2.getItems();
        assertThat("check size 2", groupings6, hasSize(1));

    }

    @Test
    public void testFloatGroupBySortAsc() throws Exception {
        Stream.of("A", "B", "C", "B", "C", "C").forEach((String f) -> {
            SearchIndexModel model = new SearchIndexModel();
            model.one = f;
            model.save();
        });

        List<Grouping<SearchIndexModel>> groupings = Query.from(SearchIndexModel.class).sortAscending("one").groupBy("one");

        assertThat("check size", groupings, hasSize(3));

        assertThat("1st check " + groupings.get(0).getKeys().get(0),
                groupings.get(0).getCount(),
                is((long) 1));
        assertThat("2nd check " + groupings.get(1).getKeys().get(0),
                groupings.get(1).getCount(),
                is((long) 2));
        assertThat("3rd check " + groupings.get(2).getKeys().get(0),
                groupings.get(2).getCount(),
                is((long) 3));
        assertThat(groupings.get(0).getKeys().get(0), is("A"));
        assertThat(groupings.get(1).getKeys().get(0), is("B"));
        assertThat(groupings.get(2).getKeys().get(0), is("C"));
    }

    @Test
    public void testFloatGroupBySortDesc() throws Exception {
        Stream.of("A", "B", "C", "B", "C", "C").forEach((String f) -> {
            SearchIndexModel model = new SearchIndexModel();
            model.one = f;
            model.save();
        });

        List<Grouping<SearchIndexModel>> groupings = Query.from(SearchIndexModel.class).sortDescending("one").groupBy("one");

        assertThat("check size", groupings, hasSize(3));

        assertThat("1st check " + groupings.get(0).getKeys().get(0),
                groupings.get(0).getCount(),
                is((long) 3));
        assertThat("2nd check " + groupings.get(1).getKeys().get(0),
                groupings.get(1).getCount(),
                is((long) 2));
        assertThat("3rd check " + groupings.get(2).getKeys().get(0),
                groupings.get(2).getCount(),
                is((long) 1));

        assertThat(groupings.get(0).getKeys().get(0), is("C"));
        assertThat(groupings.get(1).getKeys().get(0), is("B"));
        assertThat(groupings.get(2).getKeys().get(0), is("A"));

    }

    // SqlDatabase does not support group by numeric range
    @Test
    public void testFloatGroupBy() throws Exception {
        Stream.of(1.0f, 2.0f, 3.0f, 2.0f, 3.0f, 3.0f).forEach((Float f) -> {
            SearchIndexModel model = new SearchIndexModel();
            model.f = f;
            model.num = f.intValue();
            model.save();
        });

        List<Grouping<SearchIndexModel>> groupings = Query.from(SearchIndexModel.class).groupBy("f");

        assertThat("check size", groupings, hasSize(3));

        groupings.forEach(g -> {
            String keyLetter = String.valueOf(g.getKeys().get(0));

            assertThat(
                    keyLetter + " check",
                    g.getCount(),
                    is((long) Math.round(Float.parseFloat(keyLetter))));
        });

        List<Grouping<SearchIndexModel>> ranges = Query.from(SearchIndexModel.class).groupBy("num(1,4,1)");
        assertThat("check size", ranges, hasSize(3));
        assertThat("1st check " + ranges.get(0).getKeys().get(0),
                ranges.get(0).getCount(),
                is((long) 1));
        assertThat("2nd check " + ranges.get(1).getKeys().get(0),
                ranges.get(1).getCount(),
                is((long) 2));
        assertThat("3rd check " + ranges.get(2).getKeys().get(0),
                ranges.get(2).getCount(),
                is((long) 3));

    }

    @Test
    public void testSubQuery() {
        SearchIndexModel model = new SearchIndexModel();
        model.one = "first";
        model.save();

        SearchIndexModel model1 = new SearchIndexModel();
        model1.one = "second";
        model1.save();

        List<SearchIndexModel> fooResult = Query
                .from(SearchIndexModel.class).and("id != ?", Query.from(Object.class).where("id = ?", model.getId().toString()))
                .selectAll();
        assertThat("check size", fooResult, hasSize(1));
        assertThat("id check", model1.getId(), is(fooResult.get(0).getId()));

        List<SearchIndexModel> fooResult1 = Query
                .from(SearchIndexModel.class).and("id != ?", Query.from(SearchIndexModel.class).where("one startswith ?", "firs"))
                .selectAll();
        assertThat("check size 2", fooResult1, hasSize(1));
        assertThat("id check 2", model1.getId(), is(fooResult1.get(0).getId()));

    }

    // sortNewest not supported H2
    @Test
    public void testDateNewestBoost() throws Exception {
        Stream.of(new java.util.Date(), DateUtils.addHours(new java.util.Date(), -5), DateUtils.addDays(new java.util.Date(), -5), DateUtils.addDays(new java.util.Date(), -10)).forEach(d -> {
            SearchIndexModel model = new SearchIndexModel();
            model.postDate = d;
            model.save();
        });

        List<SearchIndexModel> fooResult = Query
                .from(SearchIndexModel.class)
                .sortNewest(2.0, "postDate")
                .selectAll();

        assertThat("check size", fooResult, hasSize(4));
        assertThat("check 0 and 1 order", fooResult.get(0).postDate.getTime(), greaterThan(fooResult.get(1).postDate.getTime()));
        assertThat("check 1 and 2 order", fooResult.get(1).postDate.getTime(), greaterThan(fooResult.get(2).postDate.getTime()));
        assertThat("check 2 and 3 order", fooResult.get(2).postDate.getTime(), greaterThan(fooResult.get(3).postDate.getTime()));
    }

    @Test
    public void testDateLessthan() throws Exception {
        Date begin = new java.util.Date();
        Stream.of(begin,
                DateUtils.addHours(begin, -5),
                DateUtils.addDays(begin, -5),
                DateUtils.addDays(begin, -10)).forEach(d -> {
            SearchIndexModel model = new SearchIndexModel();
            model.postDate = d;
            model.save();
        });

        List<SearchIndexModel> fooResult = Query
                .from(SearchIndexModel.class)
                .where("postDate lessthan ?", begin)
                .selectAll();

        // should not include the to:
        assertThat("check size", fooResult, hasSize(3));

        List<SearchIndexModel> fooResult1 = Query
                .from(SearchIndexModel.class)
                .where("postDate lessthan ?", DateUtils.addSeconds(begin, 1))
                .selectAll();
        assertThat("check size", fooResult1, hasSize(4));
    }

    @Test
    public void testDateGreaterthan() throws Exception {
        Date begin = new java.util.Date();
        Stream.of(begin,
                DateUtils.addHours(begin, 1),
                DateUtils.addDays(begin, 1),
                DateUtils.addDays(begin, 2)).forEach(d -> {
            SearchIndexModel model = new SearchIndexModel();
            model.postDate = d;
            model.save();
        });

        List<SearchIndexModel> fooResult = Query
                .from(SearchIndexModel.class)
                .where("postDate greaterthan ?", begin)
                .selectAll();

        // should not include the from:
        assertThat("check size", fooResult, hasSize(3));

        List<SearchIndexModel> fooResult1 = Query
                .from(SearchIndexModel.class)
                .where("postDate greaterthan ?", DateUtils.addSeconds(begin, -1))
                .selectAll();
        assertThat("check size", fooResult1, hasSize(4));
    }

    @Test
    public void testSortAscendingDirectory() throws Exception {

        SearchIndexModel model = new SearchIndexModel();
        model.setOne("/");
        model.save();

        List<SearchIndexModel> fooResult = Query
                .from(SearchIndexModel.class)
                .where("one equals ?", "/")
                .selectAll();

        assertThat("check size", fooResult, hasSize(1));
    }

    @Test
    public void testSortAscending() throws Exception {

        List<SearchIndexModel> fooResult = Query
                .from(SearchIndexModel.class)
                .sortAscending("_id")
                .selectAll();

        assertThat("check size", fooResult, hasSize(0));
    }

    // sortOldest not supported H2
    @Test
    public void testDateOldestBoost() throws Exception {
        Stream.of(new java.util.Date(), DateUtils.addHours(new java.util.Date(), -5), DateUtils.addDays(new java.util.Date(), -5), DateUtils.addDays(new java.util.Date(), -10)).forEach(d -> {
            SearchIndexModel model = new SearchIndexModel();
            model.postDate = d;
            model.save();
        });

        List<SearchIndexModel> fooResult = Query
                .from(SearchIndexModel.class)
                .sortOldest(2.0, "postDate")
                .selectAll();

        assertThat("check size", fooResult, hasSize(4));
        assertThat("check 0 and 1 order", fooResult.get(0).postDate.getTime(), lessThan(fooResult.get(1).postDate.getTime()));
        assertThat("check 1 and 2 order", fooResult.get(1).postDate.getTime(), lessThan(fooResult.get(2).postDate.getTime()));
        assertThat("check 2 and 3 order", fooResult.get(2).postDate.getTime(), lessThan(fooResult.get(3).postDate.getTime()));
    }

    // sortOldest not supported H2
    @Test
    public void testDateOldestBoostRelevant() throws Exception {
        Stream.of(new java.util.Date(), DateUtils.addHours(new java.util.Date(), -5), DateUtils.addDays(new java.util.Date(), -5), DateUtils.addDays(new java.util.Date(), -10)).forEach(d -> {
            SearchIndexModel model = new SearchIndexModel();
            model.postDate = d;
            model.save();
        });

        List<SearchIndexModel> fooResult = Query
                .from(SearchIndexModel.class)
                .sortOldest(2.0, "postDate").sortRelevant(10.0, "postDate matches ?", new java.util.Date())
                .selectAll();

        assertThat("check size", fooResult, hasSize(4));
        assertThat("check 0 and 1 order", fooResult.get(0).postDate.getTime(), lessThan(fooResult.get(1).postDate.getTime()));
        assertThat("check 1 and 2 order", fooResult.get(1).postDate.getTime(), lessThan(fooResult.get(2).postDate.getTime()));
        assertThat("check 2 and 3 order", fooResult.get(2).postDate.getTime(), lessThan(fooResult.get(3).postDate.getTime()));
    }

    @Test
    public void testNumberSort() throws Exception {
        SearchIndexModel model = new SearchIndexModel();
        model.num = 1;
        model.b = 0x30;
        model.d = 1.0;
        model.f = 1.0f;
        model.l = 1L;
        model.shortType = 1;
        model.save();

        SearchOverlapModel model2 = new SearchOverlapModel();
        model2.num = 2;
        model2.b = 0x31;
        model2.d = 2.0;
        model2.f = "b";
        model2.l = 2L;
        model2.shortType = 2;
        model2.save();
        searchOverlapModelIndex = true;

        SearchIndexModel model3 = new SearchIndexModel();
        model3.num = 3;
        model3.b = 0x32;
        model3.d = 3.0;
        model3.f = 3.0f;
        model3.l = 3L;
        model3.shortType = 3;
        model3.save();

        List<SearchIndexModel> fooResult = Query
                .from(SearchIndexModel.class)
                .sortAscending("f")
                .selectAll();

        List<SearchOverlapModel> fooResult2 = Query
                .from(SearchOverlapModel.class)
                .sortAscending("f")
                .selectAll();

        assertThat("check size SearchIndexModel", fooResult, hasSize(2));

        assertThat("check size SearchOverlapModel", fooResult2, hasSize(1));
    }

    @Test
    public void testOverlapElasticTypes() throws Exception {
        Stream.of(1.0f, 2.0f, 3.0f).forEach(f -> {
            SearchIndexModel model = new SearchIndexModel();
            model.f = f;
            model.save();
        });

        Stream.of("1.0", "2.0", "3.0").forEach(f -> {
            SearchOverlapModel model2 = new SearchOverlapModel();
            model2.f = f;
            model2.save();
        });
        searchOverlapModelIndex = true;

        List<SearchIndexModel> fooResult = Query
                .from(SearchIndexModel.class)
                .selectAll();

        List<SearchOverlapModel> fooResult2 = Query
                .from(SearchOverlapModel.class)
                .selectAll();

        assertThat("check size SearchIndexModel", fooResult, hasSize(3));

        assertThat("check size SearchOverlapModel", fooResult2, hasSize(3));
    }

    @Test
    public void testSortOverlapElasticTypes() throws Exception {
        Stream.of(1.0f, 3.0f, 2.0f).forEach(f -> {
            SearchIndexModel model = new SearchIndexModel();
            model.f = f;
            model.save();
        });

        Stream.of("a", "c", "b").forEach(f -> {
            SearchOverlapModel model2 = new SearchOverlapModel();
            model2.f = f;
            model2.save();
        });
        searchOverlapModelIndex = true;

        List<SearchIndexModel> fooResult = Query
                .from(SearchIndexModel.class)
                .sortAscending("f")
                .selectAll();

        assertThat("check size all", fooResult, hasSize(3));
        assertThat("check 0 and 1 order",  fooResult.get(0).f, lessThan(fooResult.get(1).f));
        assertThat("check 1 and 2 order", fooResult.get(1).f, lessThan(fooResult.get(2).f));

        List<SearchOverlapModel> fooResult2 = Query
                .from(SearchOverlapModel.class)
                .sortAscending("f")
                .selectAll();

        assertThat("check size all", fooResult2, hasSize(3));
        assertThat("check 0 and 1 order",  fooResult2.get(0).f, lessThan(fooResult2.get(1).f));
        assertThat("check 1 and 2 order", fooResult2.get(1).f, lessThan(fooResult2.get(2).f));

    }

    // sortOldest not supported in SQL
    @Test
    public void testTimeout() throws Exception {
        Stream.of(new java.util.Date()).forEach(d -> {
            SearchIndexModel model = new SearchIndexModel();
            model.postDate = d;
            model.save();
        });

        List<SearchIndexModel> fooResult = Query
                .from(SearchIndexModel.class)
                .sortOldest(2.0, "postDate")
                .timeout(500.0)
                .selectAll();

        assertThat("check size", fooResult, hasSize(1));
    }

    @Test
    public void testLogin() throws Exception {

        SearchIndexModel model = new SearchIndexModel();
        model.loginTokens.token = UUID.fromString("68a66f18-b668-418b-af69-8dafa6325298");
        model.save();

        List<SearchIndexModel> fooResult = Query
                .from(SearchIndexModel.class)
                .where("loginTokens/token equalsany ?", UUID.fromString("68a66f18-b668-418b-af69-8dafa6325298"))
                .selectAll();

        assertThat("check size", fooResult, hasSize(1));
    }

    @Test
    public void testUUIDlt() throws Exception {

        SearchIndexModel model = new SearchIndexModel();
        model.loginTokens.token = UUID.fromString("68a66f18-b668-418b-af69-8dafa6325298");
        model.save();

        List<SearchIndexModel> fooResult = Query.from(SearchIndexModel.class)
                .where("loginTokens/token < ?", new UUID(0, 0))
                .selectAll();

        assertThat("check size", fooResult, hasSize(0));
    }

    @Test
    public void testUUIDlte() throws Exception {

        SearchIndexModel model = new SearchIndexModel();
        model.loginTokens.token = UUID.fromString("68a66f18-b668-418b-af69-8dafa6325298");
        model.save();

        List<SearchIndexModel> fooResult = Query.from(SearchIndexModel.class)
                .where("loginTokens/token <= ?", UUID.fromString("68a66f18-b668-418b-af69-8dafa6325298"))
                .selectAll();

        assertThat("check size", fooResult, hasSize(1));
    }

    @Test
    public void testUUIDgt() throws Exception {

        SearchIndexModel model = new SearchIndexModel();
        model.loginTokens.token = UUID.fromString("68a66f18-b668-418b-af69-8dafa6325298");
        model.save();

        List<SearchIndexModel> fooResult = Query.from(SearchIndexModel.class)
                .where("loginTokens/token > ?", new UUID(0, 0))
                .selectAll();

        assertThat("check size", fooResult, hasSize(1));
    }

    @Test
    public void testUUIDgte() throws Exception {

        SearchIndexModel model = new SearchIndexModel();
        model.loginTokens.token = UUID.fromString("68a66f18-b668-418b-af69-8dafa6325298");
        model.save();

        List<SearchIndexModel> fooResult = Query.from(SearchIndexModel.class)
                .where("loginTokens/token >= ?", UUID.fromString("68a66f18-b668-418b-af69-8dafa6325298"))
                .selectAll();

        assertThat("check size", fooResult, hasSize(1));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testUUIDmatchesany() throws Exception {

        SearchIndexModel model = new SearchIndexModel();
        model.loginTokens.token = UUID.fromString("68a66f18-b668-418b-af69-8dafa6325298");
        model.save();

        Query.from(SearchIndexModel.class)
                .where("loginTokens/token matchesany ?", new UUID(0, 0))
                .selectAll();

    }

    // H2 is not case insensitive
    @Test
    public void testSearchStemming() {

        SearchIndexModel model = new SearchIndexModel();
        model.one = "Managing The Learning Function";
        model.save();

        List<SearchIndexModel> s = Query.from(SearchIndexModel.class)
                .where("one matchesany ?", "manage")
                .selectAll();
        assertThat(s, hasSize(1));

        List<SearchIndexModel> s1 = Query.from(SearchIndexModel.class)
                .where("one matchesany ?", "lEarn")
                .selectAll();
        assertThat(s1, hasSize(1));

        List<SearchIndexModel> s2 = Query.from(SearchIndexModel.class)
                .where("one matchesany ?", "functIons")
                .selectAll();
        assertThat(s2, hasSize(1));

    }

    @Test
    public void testSearchMatchAllStemming() {

        SearchIndexModel model = new SearchIndexModel();
        model.one = "Managing The Learning Function";
        model.save();

        SearchIndexModel model1 = new SearchIndexModel();
        model1.one = "The restaurant diners are at the function learning";
        model1.save();

        List<String> all = new ArrayList<>();
        all.add("function");
        all.add("diner");

        List<SearchIndexModel> s = Query.from(SearchIndexModel.class)
                .where("one matchesall ?", all)
                .selectAll();
        assertThat(s, hasSize(1));

        List<String> all1 = new ArrayList<>();
        all1.add("the");
        all1.add("learning");

        List<SearchIndexModel> s1 = Query.from(SearchIndexModel.class)
                .where("one matchesall ?", all1)
                .selectAll();
        assertThat(s1, hasSize(2));

        List<String> all2 = new ArrayList<>();
        all2.add("function");
        all2.add("learning");

        List<SearchIndexModel> s2 = Query.from(SearchIndexModel.class)
                .where("one matchesall ?", all2)
                .selectAll();
        assertThat(s2, hasSize(2));

    }

    // H2 does not throw exceptions on UUID and matches
    @Test(expected = IllegalArgumentException.class)
    public void testUUIDmatchesall() throws Exception {

        SearchIndexModel model = new SearchIndexModel();
        model.loginTokens.token = UUID.fromString("68a66f18-b668-418b-af69-8dafa6325298");
        model.save();

        Query.from(SearchIndexModel.class)
                .where("loginTokens/token matchesall ?", new UUID(0, 0))
                .selectAll();
    }

    // H2 does not tokenize so matches works. This is not right.
    @Test(expected = IllegalArgumentException.class)
    public void testUUIDmatchesany2() throws Exception {

        SearchIndexModel model = new SearchIndexModel();
        model.loginTokens.token = UUID.fromString("68a66f18-b668-418b-af69-8dafa6325298");
        model.save();

        Query.from(SearchIndexModel.class)
                .where("loginTokens/token matchesany ?", UUID.fromString("68a66f18-b668-418b-af69-8dafa6325298"))
                .selectAll();
    }

    // H2 does not tokenize so matches works. This is not right.
    @Test(expected = IllegalArgumentException.class)
    public void testUUIDmatchesall2() throws Exception {

        SearchIndexModel model = new SearchIndexModel();
        model.loginTokens.token = UUID.fromString("68a66f18-b668-418b-af69-8dafa6325298");
        model.save();

        Query.from(SearchIndexModel.class)
                .where("loginTokens/token matchesall ?", UUID.fromString("68a66f18-b668-418b-af69-8dafa6325298"))
                .selectAll();
    }

    @Test
    public void testComplexQuery() throws Exception {

        SearchIndexModel model = new SearchIndexModel();
        model.loginTokens.token = UUID.fromString("68a66f18-b668-418b-af69-8dafa6325298");
        model.save();

        List<SearchIndexModel> fooResult = Query
                .from(SearchIndexModel.class)
                .where("(loginTokens/token notequalsall missing and (f equalsany missing and num equalsany missing "
                        + "and set equalsany missing and list equalsany missing and _type notequalsall ?"
                        + ") and _any matchesany '*')", UUID.fromString("68a66f18-b668-418b-af69-8dafa632529"))
                .selectAll();

        // _type notequalsall ?

        assertThat("check size", fooResult, hasSize(1));
    }

    // H2 cannot limit group by
    @Test
    public void testGroupPartial() {
        for (int i = 0; i < 4; i++) {
            for (int j = 0; j < 20; j++) {
                SearchIndexModel model = new SearchIndexModel();
                model.one = "test " + j;
                model.save();
            }
        }

        PaginatedResult<Grouping<SearchIndexModel>> groupBy =
                Query.from(SearchIndexModel.class).groupByPartial(0L, 2, "one");
        for (Grouping<SearchIndexModel> grouping : groupBy.getItems()) {
            List<Object> cycleKeys = grouping.getKeys();
            String name = String.valueOf(cycleKeys.get(0));
            long count = grouping.getCount();
            assertThat(name + " " + count, count, is(4L));
        }

        assertEquals(2, groupBy.getCount());
    }

    // Order is indeterminate without Sort
    @Test
    public void testGroupOrder() {
        for (int i = 1; i <= 4; i++) {
            for (int j = 0; j < i * 2; j++) {
                SearchIndexModel model = new SearchIndexModel();
                model.one = "test " + i;
                model.save();
            }
        }

        // from highest to smallest count
        long count = 4;
        List<Grouping<SearchIndexModel>> groupBy =
                Query.from(SearchIndexModel.class).sortDescending("one").groupBy("one");
        for (Grouping<SearchIndexModel> grouping : groupBy) {
            List<Object> cycleKeys = grouping.getKeys();
            String name = String.valueOf(cycleKeys.get(0));
            assertThat(name + ":testGroupOrder highest to lowest", grouping.getCount(), is(count * 2));
            count = count - 1;
        }

        assertThat(groupBy, hasSize(4));
    }

    @Test
    public void testGroupDateandOne() {
        Date begin = new Date();
        for (int i = 1; i <= 4; i++) {
            int count = i * 2;
            for (int j = 0; j < i * 2; j++) {
                SearchIndexModel model = new SearchIndexModel();
                model.one = String.valueOf(count);
                model.postDate = begin;
                model.save();
            }
        }

        List<Grouping<SearchIndexModel>> groupBy =
                Query.from(SearchIndexModel.class).groupBy("postDate", "one");

        groupBy.forEach(g -> {
            Date postDate = (Date) g.getKeys().get(0);
            String one = String.valueOf(g.getKeys().get(1));
            long count = Integer.parseInt(one);

            assertThat(postDate + ": count", g.getCount(), is(count));
            assertThat(postDate + ": date", one, is(String.valueOf(count)));
        });
        assertThat(groupBy, hasSize(4));

        List<Grouping<SearchIndexModel>> groupBySort =
                Query.from(SearchIndexModel.class).sortDescending("postDate").sortDescending("one").groupBy("postDate", "one");

        long count = 4;
        for (Grouping<SearchIndexModel> grouping : groupBySort) {
            List<Object> cycleKeys = grouping.getKeys();
            Date postDate = (Date) cycleKeys.get(0);
            String one = String.valueOf(grouping.getKeys().get(1));
            assertThat(postDate + ": count", grouping.getCount(), is(count * 2));
            assertThat(postDate + ": one", one, is(String.valueOf(count * 2)));
            count = count - 1;
        }

        assertThat(groupBySort, hasSize(4));
    }

    @Test
    public void testSortGroup() {
        Date begin = new Date();
        Date postDate = begin;
        for (int i = 1; i <= 4; i++) {
            postDate = DateUtils.addMinutes(postDate, 1);
            for (int j = 0; j < i * 2; j++) {
                SearchIndexModel model = new SearchIndexModel();
                if ((j & 1) == 0) {
                    model.one = "even";
                } else {
                    model.one = "odd";
                }
                model.postDate = postDate;
                model.save();
            }
        }

        List<Grouping<SearchIndexModel>> groupPostDate =
                Query.from(SearchIndexModel.class).groupBy("postDate");
        assertThat(groupPostDate, hasSize(4));

        List<Grouping<SearchIndexModel>> groupOne =
                Query.from(SearchIndexModel.class).groupBy("one");
        assertThat(groupOne, hasSize(2));

        List<Grouping<SearchIndexModel>> groupBySort =
                Query.from(SearchIndexModel.class).sortDescending("postDate").sortDescending("one").groupBy("postDate", "one");

        long count = 4;
        String oddEven = "odd";
        for (Grouping<SearchIndexModel> grouping : groupBySort) {
            List<Object> cycleKeys = grouping.getKeys();
            Date postDate2 = (Date) cycleKeys.get(0);
            String one = String.valueOf(grouping.getKeys().get(1));
            assertThat(postDate2 + ": count", grouping.getCount(), is(count));
            assertThat(postDate2 + ": one", one, is(oddEven));
            if (oddEven.equals("odd")) {
                oddEven = "even";
            } else {
                oddEven = "odd";
                count = count - 1;
            }
        }

        assertThat(groupBySort, hasSize(8));
    }

    @Test
    public void testQuery() throws Exception {

        SearchIndexModel model = new SearchIndexModel();
        model.one = "test";
        model.save();
        SearchIndexModel model1 = new SearchIndexModel();
        model1.one = "test";
        model1.save();

        Query<SearchIndexModel> query = Query.from(SearchIndexModel.class);

        List<SearchIndexModel> selectAll = query.selectAll();
        assertThat("check selectAll", selectAll, hasSize(2));

        PaginatedResult<SearchIndexModel> select = query.select(0, 10);
        assertEquals(2, select.getCount());

        SearchIndexModel first = query.first();
        assertThat("check size", first, notNullValue());

        Iterable<SearchIndexModel> iter = query.iterable(10);
        int i = 0;
        for (SearchIndexModel s : iter) {
            i++;
        }
        assertThat("check iter", i, is(2));

        List<Grouping<SearchIndexModel>> groupBy = Query.from(SearchIndexModel.class).groupBy("one");
        Iterator<Grouping<SearchIndexModel>> iGroup = groupBy.iterator();

        while (iGroup.hasNext()) {
            Grouping element = iGroup.next();
            assertEquals(2, element.getCount());
        }

        Query<SearchIndexModel> searchQuery = Query.from(SearchIndexModel.class).where("one = ?", "test");

        List<SearchIndexModel> search = searchQuery.selectAll();
        assertThat("check where", search, hasSize(2));

        ObjectType type = ObjectType.getInstance(SearchIndexModel.class);

        List<Object> search2 = Query.fromType(type).selectAll();
        assertThat("check fromType", search2, hasSize(2));
    }

    @Test
    public void testGlobals() {
        SearchIndexModel model1 = new SearchIndexModel();
        model1.one = "test";
        model1.save();

        List<SearchIndexModel> query = Query.from(SearchIndexModel.class).selectAll();

        Database defaultDatabase = Database.Static.getDefault();
        DatabaseEnvironment environment = defaultDatabase.getEnvironment();
        List<ObjectField> globalFields = environment.getFields();
        assertThat(globalFields.isEmpty(), is(false));
    }

    @Test
    public void testModification() {
        Date begin = new java.util.Date();
        SearchIndexModel model1 = new SearchIndexModel();
        model1.one = "test";
        model1.as(TestModification.class).setUpdateDate(begin);
        model1.save();

        List<SearchIndexModel> sem = Query.from(SearchIndexModel.class).selectAll();
        assertThat("check testModification", sem, hasSize(1));

        List<SearchIndexModel> sem2 = Query.from(SearchIndexModel.class)
                .where("cms.content.updateDate > ?", DateUtils.addHours(new java.util.Date(), -1))
                .sortAscending("cms.content.updateDate")
                .selectAll();
        assertThat("check testModification", sem2, hasSize(1));

        List<SearchIndexModel> sem3 = Query.from(SearchIndexModel.class)
                .where("cms.content.updateDate = ?", begin)
                .sortAscending("cms.content.updateDate")
                .selectAll();
        assertThat("check testModification", sem2, hasSize(1));
    }

    // Need tokenizing properly for H2
    @Test
    public void testMatchesAll() {
        SearchIndexModel model1 = new SearchIndexModel();
        model1.one = "test headline story";
        model1.save();

        SearchIndexModel model2 = new SearchIndexModel();
        model2.one = "another story";
        model2.save();

        List<SearchIndexModel> sem = Query.from(SearchIndexModel.class).where("one matchesany ?", "headline").selectAll();
        assertThat("check matchesany", sem, hasSize(1));

        List<SearchIndexModel> sem1 = Query.from(SearchIndexModel.class).where("one matchesall ?", "headline").selectAll();
        assertThat("check matchesall", sem1, hasSize(1));

        List<String> many = new ArrayList<>();
        many.add("test");
        many.add("headline");
        List<SearchIndexModel> sem2 = Query.from(SearchIndexModel.class).where("one matchesany ?", many).selectAll();
        assertThat("check matchesany 2", sem2, hasSize(1));
        List<SearchIndexModel> sem3 = Query.from(SearchIndexModel.class).where("one matchesall ?", many).selectAll();
        assertThat("check matchesall 2", sem3, hasSize(1));

        List<String> many2 = new ArrayList<>();
        many2.add("test");
        many2.add("story");
        List<SearchIndexModel> sem4 = Query.from(SearchIndexModel.class).where("one matchesany ?", many2).selectAll();
        assertThat("check matchesany 3", sem4, hasSize(2));
        List<SearchIndexModel> sem5 = Query.from(SearchIndexModel.class).where("one matchesall ?", many2).selectAll();
        assertThat("check matchesall 3", sem5, hasSize(1));
    }

    // H2 does not work all case
    @Test
    public void testMatchesAllCase() {
        SearchIndexModel model1 = new SearchIndexModel();
        model1.one = "TeSt HeAdLiNe StOrY";
        model1.save();

        SearchIndexModel model2 = new SearchIndexModel();
        model2.one = "AnOtHeR StOrY thisisalongverylongwordthatgoesonandon specialchar*";
        model2.save();

        List<SearchIndexModel> zeroAny = Query.from(SearchIndexModel.class).where("one matchesany ?", (Object) null).selectAll();
        assertThat("check matchesany null", zeroAny, hasSize(0));

        List<SearchIndexModel> zeroAll = Query.from(SearchIndexModel.class).where("one matchesall ?", (Object) null).selectAll();
        assertThat("check matchesall null", zeroAll, hasSize(0));

        List<SearchIndexModel> sem = Query.from(SearchIndexModel.class).where("one matchesany ?", "headline").selectAll();
        assertThat("check matchesany", sem, hasSize(1));

        List<SearchIndexModel> sem10 = Query.from(SearchIndexModel.class).where("one matchesany ?", "headline*").selectAll();
        assertThat("check matchesany", sem10, hasSize(1));

        List<SearchIndexModel> sem11 = Query.from(SearchIndexModel.class).where("one matchesany ?", "specialchar*").selectAll();
        assertThat("check matchesany", sem11, hasSize(1));

        List<SearchIndexModel> sem1 = Query.from(SearchIndexModel.class).where("one matchesall ?", "headline").selectAll();
        assertThat("check matchesall", sem1, hasSize(1));

        List<SearchIndexModel> contains = Query.from(SearchIndexModel.class).where("one contains ?", "headline").selectAll();
        assertThat("check contains", contains, hasSize(1));

        List<SearchIndexModel> containsPartial = Query.from(SearchIndexModel.class).where("one contains ?", "sto").selectAll();
        assertThat("check containsPartial", containsPartial, hasSize(2));

        List<SearchIndexModel> containsPartial2 = Query.from(SearchIndexModel.class).where("one contains ?", "oth").selectAll();
        assertThat("check containsPartial2", containsPartial2, hasSize(1));

        try {
            Query.from(SearchIndexModel.class).where("* contains ?", "thisisalongverylongwordthatgoesonandon").selectAll();
            Assert.fail("Exception not thrown");
        } catch (IllegalArgumentException error) {
            // squash
        }

        List<String> many = new ArrayList<>();
        many.add("test");
        many.add("headline");
        List<SearchIndexModel> sem2 = Query.from(SearchIndexModel.class).where("one matchesany ?", many).selectAll();
        assertThat("check matchesany sem2", sem2, hasSize(1));
        List<SearchIndexModel> sem3 = Query.from(SearchIndexModel.class).where("one matchesall ?", many).selectAll();
        assertThat("check matchesall sem3", sem3, hasSize(1));

        List<String> many2 = new ArrayList<>();
        many2.add("test");
        many2.add("story");
        List<SearchIndexModel> sem4 = Query.from(SearchIndexModel.class).where("one matchesany ?", many2).selectAll();
        assertThat("check matchesany sem4", sem4, hasSize(2));
        List<SearchIndexModel> sem5 = Query.from(SearchIndexModel.class).where("one matchesall ?", many2).selectAll();
        assertThat("check matchesall sem5", sem5, hasSize(1));

        List<String> many3 = new ArrayList<>();
        many3.add("head");
        many3.add("story");
        List<SearchIndexModel> sem6 = Query.from(SearchIndexModel.class).where("one matchesany ?", many3).selectAll();
        assertThat("check matchesany sem6", sem6, hasSize(2));
        List<SearchIndexModel> sem7 = Query.from(SearchIndexModel.class).where("one matchesall ?", many3).selectAll();
        assertThat("check matchesall sem7", sem7, hasSize(1));
    }

    // Same in SOLR and Elastic since they are to be escaped in Lucene
    private static final Pattern ESCAPE_PATTERN = Pattern.compile("([-+&|!(){}\\[\\]^\"~*?:\\\\\\s/])");

    /**
     * Escapes the given {@code value} so that it's safe to use
     * in a Elastic query.
     *
     * @param value If {@code null}, returns {@code null}.
     */
    public static String escapeValue(Object value) {
        return value != null ? ESCAPE_PATTERN.matcher(value.toString()).replaceAll("\\\\$1") : null;
    }

    // Note - allow user to send anything. Escape is not done, so the client needs to do it
    // for example, escapeValue(special) unless client wants to send * to expand search.
    @Test
    public void testMatchesSpecialChars() {
        List<String> specialChars = new ArrayList<>();
        specialChars.add("+");
        specialChars.add("-");
        specialChars.add("&&");
        specialChars.add("||");
        specialChars.add("!");
        specialChars.add("(");
        specialChars.add(")");
        specialChars.add("{");
        specialChars.add("}");
        specialChars.add("[");
        specialChars.add("]");
        specialChars.add("^");
        specialChars.add("\\");
        specialChars.add("\"");
        specialChars.add("~");
        specialChars.add("*");
        specialChars.add("?");
        specialChars.add(":");
        specialChars.add("/");

        for (String special : specialChars) {
            SearchIndexModel model1 = new SearchIndexModel();
            model1.one = "Te" + special + "St " + special + "Beginning" + " StOrY" + special;
            model1.save();

            SearchIndexModel model2 = new SearchIndexModel();
            model2.one = "Beginning AnOtHeR StOrY" + special;
            model2.save();
        }

        int count = specialChars.size() * 2;
        List<SearchIndexModel> total = Query.from(SearchIndexModel.class).where("one matchesany ?", "beginning").selectAll();
        assertThat("total", total, hasSize(count));

        List<SearchIndexModel> total1 = Query.from(SearchIndexModel.class).where("one matchesany ?", "story").selectAll();
        assertThat("total1", total1, hasSize(count));

        // default is case insensitive.
        List<SearchIndexModel> total3 = Query.from(SearchIndexModel.class).where("one startswith ?", "te").selectAll();
        assertThat("total3", total3, hasSize(count / 2));

        List<SearchIndexModel> total4 = Query.from(SearchIndexModel.class).where("one startswith ?", "Beginning another").selectAll();
        assertThat("total4", total4, hasSize(count / 2));

        // default is case insensitive.
        List<SearchIndexModel> total5 = Query.from(SearchIndexModel.class).where("_label startswith ?", "te").selectAll();
        assertThat("total5", total5, hasSize(count / 2));

        for (String special : specialChars) {
            String specialEscaped = escapeValue(special);
            List<SearchIndexModel> total2a = Query.from(SearchIndexModel.class).where("one matchesany ?", specialEscaped + "beginning").selectAll();
            assertThat("total2a", total2a, hasSize(count));
            List<SearchIndexModel> total2b = Query.from(SearchIndexModel.class).where("one matchesany ?", "te" + specialEscaped + "st").selectAll();
            // This one has to do with St from Story
            assertThat("total2b", total2b, hasSize(count / 2));
            List<SearchIndexModel> total2c = Query.from(SearchIndexModel.class).where("one matchesany ?", "story" + specialEscaped).selectAll();
            assertThat("total2c", total2c, hasSize(count));

            List<SearchIndexModel> total2d = Query.from(SearchIndexModel.class).where("one startswith ?", "Te" + specialEscaped).selectAll();
            assertThat("total2d", total2d, hasSize(0));

            List<SearchIndexModel> total2f = Query.from(SearchIndexModel.class).where("one contains ?", "story" + special).selectAll();
            assertThat("total2f", total2f, hasSize(2));
        }

        for (String special : specialChars) {
            List<SearchIndexModel> first = Query.from(SearchIndexModel.class).where("one = ?", "te" + special + "st").selectAll();
            assertThat("first", first, hasSize(0));
            List<SearchIndexModel> second = Query.from(SearchIndexModel.class).where("one = ?", special + "beginning").selectAll();
            assertThat("first", second, hasSize(0));
            List<SearchIndexModel> third = Query.from(SearchIndexModel.class).where("one = ?", "story" + special).selectAll();
            assertThat("first", third, hasSize(0));
        }

        // raw does not need to be escaped
        for (String special : specialChars) {
            List<SearchIndexModel> first = Query.from(SearchIndexModel.class).where("one = ?", "Te" + special + "St " + special + "Beginning" + " StOrY" + special).selectAll();
            assertThat("first", first, hasSize(1));
        }
    }

    // sortAscending on floats not working in H2
    @Test
    public void testSortNumber() {

        SearchIndexModel model1 = new SearchIndexModel();
        model1.f = 1.0f;
        model1.d = 1.0d;
        model1.num = 100;
        model1.l = 100L;
        model1.shortType = (short) 5;
        model1.save();

        SearchIndexModel model2 = new SearchIndexModel();
        model2.f = 1.1f;
        model2.d = 1.1d;
        model2.num = 200;
        model2.l = 200L;
        model2.shortType = (short) 6;
        model2.save();

        SearchIndexModel model3 = new SearchIndexModel();
        model3.save();

        List<SearchIndexModel> floatNumber = Query.from(SearchIndexModel.class).sortAscending("f").selectAll();
        assertThat("check sort float", floatNumber, hasSize(3));
        assertThat("check 0 and 1 order",  floatNumber.get(0).f, lessThan(floatNumber.get(1).f));
        assertThat(floatNumber.get(2).f, nullValue());

        List<SearchIndexModel> doubleNumber = Query.from(SearchIndexModel.class).sortAscending("d").selectAll();
        assertThat("check sort double", doubleNumber, hasSize(3));
        assertThat("check 0 and 1 order",  doubleNumber.get(0).d, lessThan(doubleNumber.get(1).d));
        assertThat(doubleNumber.get(2).d, nullValue());

        List<SearchIndexModel> intNumber = Query.from(SearchIndexModel.class).sortAscending("num").selectAll();
        assertThat("check sort int", intNumber, hasSize(3));
        assertThat("check 0 and 1 order",  intNumber.get(0).num, lessThan(intNumber.get(1).num));
        assertThat(intNumber.get(2).num, nullValue());

        List<SearchIndexModel> longNumber = Query.from(SearchIndexModel.class).sortAscending("l").selectAll();
        assertThat("check sort long", longNumber, hasSize(3));
        assertThat("check 0 and 1 order",  longNumber.get(0).l, lessThan(longNumber.get(1).l));
        assertThat(longNumber.get(2).l, nullValue());

        List<SearchIndexModel> shortNumber = Query.from(SearchIndexModel.class).sortAscending("shortType").selectAll();
        assertThat("check sort short", shortNumber, hasSize(3));
        assertThat("check 0 and 1 order",  shortNumber.get(0).shortType, lessThan(shortNumber.get(1).shortType));
        assertThat(shortNumber.get(2).shortType, nullValue());
    }

    @Test
    public void testSortTypeFieldAll() {
        SearchIndexModel model1 = new SearchIndexModel();
        model1.one = "test headline story";
        model1.save();

        SearchIndexModel model2 = new SearchIndexModel();
        model2.one = "another story";
        model2.save();

        String sort = ObjectType.getInstance(SearchIndexModel.class).getInternalName() + "/one";

        List<SearchIndexModel> sem = Query.from(SearchIndexModel.class).sortAscending(sort).selectAll();
        assertThat("check matchesany", sem, hasSize(2));
        assertThat("check 0 and 1 order",  sem.get(0).one, lessThan(sem.get(1).one));
        assertThat("equals check", sem.get(0).one, is("another story"));
        assertThat("equals check", sem.get(1).one, is("test headline story"));
    }

    // H2 does not work for this
    @Test
    public void testIndexMethod() {
        MethodIndexModel model = new MethodIndexModel();
        model.setName("story");
        model.save();

        List<MethodIndexModel> tagg = Query.from(MethodIndexModel.class).where("taggable.getFoo2 = ?", "Foo2").selectAll();
        assertThat("check IndexMethod", tagg, hasSize(1));

        List<MethodIndexModel> sem = Query.from(MethodIndexModel.class).where("getFoo = ?", "Foo").selectAll();
        assertThat("check IndexMethod 2", sem, hasSize(1));

        List<MethodIndexModel> sem1 = Query.from(MethodIndexModel.class).where("getInfo matches ?", "larger").selectAll();
        assertThat("check IndexMethod 3", sem1, hasSize(1));

        List<MethodIndexModel> sem2 = Query.from(MethodIndexModel.class).where("getPrefixName = ?", "defaultstory").selectAll();
        assertThat("check IndexMethod 4", sem2, hasSize(1));

        List<Grouping<MethodIndexModel>> groupL = Query.from(MethodIndexModel.class).groupBy("getNameFirstLetter");
        assertThat("check Grouping / agg", groupL, hasSize(1));
        for (Grouping g : groupL) {
            if (g.getKeys().contains("s")) {
                assertThat("group", g.getCount(), is((long) 1));
            }
        }
    }

    // add multi level

    @Test
    public void testIndexes() {

        SearchIndexModel model = new SearchIndexModel();
        model.setOne("story");
        model.save();

        MethodIndexModel model1 = new MethodIndexModel();
        model1.setName("story");
        model1.save();

        State s = model1.getState();
        List<ObjectMethod> methods  = s.getType().getMethods();
        methods.addAll(Database.Static.getDefault().getEnvironment().getMethods());

        List<String> lMethods = new ArrayList<>();
        for (ObjectMethod method : methods) {
            lMethods.add(method.getUniqueName());
        }

        assertThat("check methods Indexed", lMethods, hasSize(6));
        assertThat("check methods Indexed", lMethods,
                hasItems("com.psddev.dari.test.MethodIndexModel/getName",
                         "com.psddev.dari.test.MethodIndexModel/getFoo",
                         "com.psddev.dari.test.MethodIndexModel/taggable.getFoo2",
                         "com.psddev.dari.test.MethodIndexModel/getInfo",
                         "com.psddev.dari.test.MethodIndexModel/getNameFirstLetter",
                         "com.psddev.dari.test.MethodIndexModel/getPrefixName"));
    }

    // H2 does not match all case
    @Test
    public void testComplexTaggedIndexMethod() {
        MethodComplexModel model1 = new MethodComplexModel();
        model1.setGivenName("Mickey");
        model1.setSurname("Mouse");
        model1.save();

        List<MethodComplexModel> tagList = Query.from(MethodComplexModel.class).where("taggable.getNames = ?", "Mickey").selectAll();
        assertThat("check tagged Mickey", tagList, hasSize(1));

        List<MethodComplexModel> tagList2 = Query.from(MethodComplexModel.class).where("taggable.getNames matches ?", "MiCkEy").selectAll();
        assertThat("check tagged Mickey", tagList2, hasSize(1));

        List<MethodComplexModel> tagSet = Query.from(MethodComplexModel.class).where("taggable.getSetNames = ?", "Mickey").selectAll();
        assertThat("check taggedSet Mickey", tagSet, hasSize(1));

        List<MethodComplexModel> tagSet2 = Query.from(MethodComplexModel.class).where("taggable.getSetNames matches ?", "mickey").selectAll();
        assertThat("check tagged matches", tagSet2, hasSize(1));
    }

    // H2 does not match all case
    @Test
    public void testComplexTaggedIndexMethodWords() {
        MethodComplexModel model1 = new MethodComplexModel();
        model1.setGivenName("Mickey Jr");
        model1.setSurname("Mouse");
        model1.save();

        List<MethodComplexModel> tagList = Query.from(MethodComplexModel.class).where("taggable.getNames = ?", "Mickey Jr").selectAll();
        assertThat("check tagged Mickey", tagList, hasSize(1));

        List<MethodComplexModel> tagList2 = Query.from(MethodComplexModel.class).where("taggable.getNames matches ?", "MiCkEy Jr").selectAll();
        assertThat("check tagged Mickey", tagList2, hasSize(1));

        List<MethodComplexModel> tagSet = Query.from(MethodComplexModel.class).where("taggable.getSetNames = ?", "Mickey Jr").selectAll();
        assertThat("check taggedSet Mickey", tagSet, hasSize(1));

        List<MethodComplexModel> tagSet2 = Query.from(MethodComplexModel.class).where("taggable.getSetNames matches ?", "mickey jr").selectAll();
        assertThat("check tagged matches", tagSet2, hasSize(1));
    }

    // some issue with _any on H2
    @Test
    public void testDenormalizedTags() {
        IndexTag t = new IndexTag();
        t.setName("pizza");
        t.save();

        IndexTag nt = new IndexTag();
        t.setName("noindex");
        nt.save();

        DenormalizedReferenceModel model1 = new DenormalizedReferenceModel();
        model1.setName("Mickey");
        model1.setIndexedTag(t);
        model1.setUnindexedTag(nt);
        model1.save();

        List<DenormalizedReferenceModel> tagList = Query.from(DenormalizedReferenceModel.class).where("_any matches ?", "pizza").selectAll();
        assertThat("check size", tagList, hasSize(1));

        List<DenormalizedReferenceModel> tagList2 = Query.from(DenormalizedReferenceModel.class).where("_any matches ?", "noindex").selectAll();
        assertThat("check size 2", tagList2, hasSize(0));

        List<DenormalizedReferenceModel> tagList3 = Query.from(DenormalizedReferenceModel.class).where("taggable.indexedTag/name = ?", "pizza").selectAll();
        assertThat("check size 3", tagList3, hasSize(1));

        // should not match
        List<DenormalizedReferenceModel> tagList4 = Query.from(DenormalizedReferenceModel.class).where("taggable.indexedTag/name matches ?", "pizza").selectAll();
        assertThat("check size 4", tagList4, hasSize(1));
    }

    @Test
    public void testHtml() {
        SearchIndexModel search = new SearchIndexModel();
        search.eid = "939393";
        search.name = "Bill";
        search.message = "<p>tough</p> car";
        search.save();

        List<SearchIndexModel> fooHtml = Query
                .from(SearchIndexModel.class)
                .where("message = ?", "tough car")
                .selectAll();
        assertThat(fooHtml, hasSize(1));

        List<SearchIndexModel> fooHtml2 = Query
                .from(SearchIndexModel.class)
                .where("message matches ?", "tough")
                .selectAll();
        assertThat(fooHtml2, hasSize(1));

        SearchIndexModel search1 = new SearchIndexModel();
        search1.eid = "939344";
        search1.name = "Joe";
        search1.message = "0000015b-3d08-dbe0-a5df-3fdc8d650000";
        search1.save();

        List<SearchIndexModel> fooResult = Query
                .from(SearchIndexModel.class)
                .where("message = ?", "0000015b-3d08-dbe0-a5df-3fdc8d650000")
                .selectAll();
        assertThat(fooResult, hasSize(1));
    }

    @Test
    public void testAnyField() {
        SearchIndexModel search = new SearchIndexModel();
        search.eid = "939393";
        search.name = "Bill";
        search.message = "<p>tough</p> car";
        search.save();

        List<SearchIndexModel> fooAny = Query
                .from(SearchIndexModel.class)
                .where("_any matches ?", "tough")
                .selectAll();
        assertThat(fooAny, hasSize(1));

        List<SearchIndexModel> fooAnyA = Query
                .from(SearchIndexModel.class)
                .where("_any matches ?", "tou")
                .selectAll();
        assertThat(fooAnyA, hasSize(1));

        List<SearchIndexModel> fooAny2 = Query
                .from(SearchIndexModel.class)
                .where("_any matches ?", "BiLl")
                .selectAll();
        assertThat(fooAny2, hasSize(1));

        SearchIndexModel search1 = new SearchIndexModel();
        search1.eid = "939344";
        search1.name = "Joe";
        search1.message = "0000015b-3d08-dbe0-a5df-3fdc8d650000";
        search1.notAnnIndexed = "Will show in Anyonly";
        search1.save();

        List<SearchIndexModel> fooResult = Query
                .from(SearchIndexModel.class)
                .where("_any startswith ?", "0000015b-3d08-dbe0-a5df-3fdc8d650000")
                .selectAll();
        assertThat(fooResult, hasSize(1));

        List<SearchIndexModel> fooResult1 = Query
                .from(SearchIndexModel.class)
                .where("_any matchesany ?", "0000015b-3d08-dbe0-a5df-3fdc8d650000")
                .selectAll();
        assertThat(fooResult1, hasSize(1));

        List<SearchIndexModel> fooResult2 = Query
                .from(SearchIndexModel.class)
                .where("_any matchesall ?", "0000015b-3d08-dbe0-a5df-3fdc8d650000")
                .selectAll();
        assertThat(fooResult2, hasSize(1));

        List<SearchIndexModel> fooResult3 = Query
                .from(SearchIndexModel.class)
                .where("_any matchesall ?", search1.loginTokens.getState().getTypeId())
                .selectAll();
        assertThat(fooResult3, hasSize(2));

        // special feature _any field indexes all fields even if not indexed
        List<SearchIndexModel> fooResult4 = Query
                .from(SearchIndexModel.class)
                .where("_any matchesall ?", "Anyonly")
                .selectAll();
        assertThat(fooResult4, hasSize(1));

        // Case insensitive
        List<SearchIndexModel> fooResult5 = Query
                .from(SearchIndexModel.class)
                .where("_any matchesall ?", "anyonly")
                .selectAll();
        assertThat(fooResult5, hasSize(1));
    }

    /**
     * This one tests 2 different Record Classes
     */
    @Test
    public void testReferencePersonSubType() {
        PersonIndexModel person = new PersonIndexModel();
        person.setPersonName("Tony");
        person.save();

        SearchIndexModel search = new SearchIndexModel();
        search.eid = "939393";
        search.name = "Bill";
        search.message = "tough";
        search.personReference = person;
        search.save();

        List<SearchIndexModel> fooSubResult = Query
                .from(SearchIndexModel.class)
                .where("personReference = ?", Query.from(PersonIndexModel.class).where("personName = ?", "Tony"))
                .selectAll();
        assertThat(fooSubResult, hasSize(1));

        List<SearchIndexModel> fooResult = Query
                .from(SearchIndexModel.class)
                .where("personReference/personName = ?", "Tony")
                .selectAll();

        assertThat(fooResult, hasSize(1));
        assertThat(fooResult.get(0).getId(), is(fooSubResult.get(0).getId()));

        List<SearchIndexModel> missingResult = Query
                .from(SearchIndexModel.class)
                .where("personReference/personName = missing")
                .selectAll();

        assertThat(missingResult, hasSize(0));

        List<SearchIndexModel> notMissingResult = Query
                .from(SearchIndexModel.class)
                .where("personReference/personName != missing")
                .selectAll();

        assertThat(notMissingResult, hasSize(1));

        List<SearchIndexModel> bothMissingResult = Query
                .from(SearchIndexModel.class)
                .where("personReference/personName = missing or personReference/personName != missing")
                .selectAll();

        assertThat(bothMissingResult, hasSize(1));

        List<SearchIndexModel> bothAndMissingResult = Query
                .from(SearchIndexModel.class)
                .where("personReference/personName = missing and personReference/personName != missing")
                .selectAll();

        assertThat(bothAndMissingResult, hasSize(0));
    }

    @Test
    public void testReferencePersonAddressSubType() {
        AddressIndexModel address = new AddressIndexModel();
        address.setStreet("101 Main Street");
        address.save();

        PersonIndexModel person = new PersonIndexModel();
        person.setPersonName("Tony");
        person.setAddress(address);
        person.save();

        SearchIndexModel search = new SearchIndexModel();
        search.eid = "939393";
        search.name = "Bill";
        search.message = "tough";
        search.personReference = person;
        search.save();

        List<SearchIndexModel> fooResult = Query
                .from(SearchIndexModel.class)
                .where("personReference/address/street = ?", "101 Main Street")
                .selectAll();

        assertThat(fooResult, hasSize(1));

        List<SearchIndexModel> missingResult = Query
                .from(SearchIndexModel.class)
                .where("personReference/address/street = missing")
                .selectAll();

        assertThat(missingResult, hasSize(0));

        List<SearchIndexModel> notMissingResult = Query
                .from(SearchIndexModel.class)
                .where("personReference/address/street != missing")
                .selectAll();

        assertThat(notMissingResult, hasSize(1));

        List<SearchIndexModel> bothMissingResult = Query
                .from(SearchIndexModel.class)
                .where("personReference/address/street = missing or personReference/address/street != missing")
                .selectAll();

        assertThat(bothMissingResult, hasSize(1));

        List<SearchIndexModel> bothAndMissingResult = Query
                .from(SearchIndexModel.class)
                .where("personReference/address/street = missing and personReference/address/street != missing")
                .selectAll();

        assertThat(bothAndMissingResult, hasSize(0));
    }

    @Test
    public void testEmbeddedPersonSubType() {

        SearchIndexModel search = new SearchIndexModel();
        search.eid = "939393";
        search.name = "Bill";
        search.message = "tough";
        search.personEmbedded = new PersonIndexModel();
        search.personEmbedded.personName = "Tony";
        search.save();

        List<SearchIndexModel> fooResult = Query
                .from(SearchIndexModel.class)
                .where("personEmbedded/personName = ?", "Tony")
                .selectAll();

        assertThat(fooResult, hasSize(1));

        List<SearchIndexModel> fooResult1 = Query
                .from(SearchIndexModel.class)
                .where("personEmbedded/com.psddev.dari.test.PersonIndexModel/personName = ?", "Tony")
                .selectAll();

        assertThat(fooResult1, hasSize(1));

    }
}

