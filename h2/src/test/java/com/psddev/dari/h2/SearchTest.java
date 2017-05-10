package com.psddev.dari.h2;

import com.psddev.dari.db.Query;
import org.junit.After;
import org.junit.Test;

import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

public class SearchTest extends AbstractTest {

    private static final String FOO = "foo";

    @After
    public void deleteModels() {
        Query.from(SearchModel.class).deleteAll();
    }

    @Test
    public void oneMatches() {
        Stream.of(FOO, "bar", "qux").forEach(string -> {
            SearchModel model = new SearchModel();
            model.one = string;
            model.set.add(FOO);
            model.list.add(FOO);
            model.map.put(FOO, FOO);
            model.save();
        });

        List<SearchModel> fooResult = Query
                .from(SearchModel.class)
                .where("one matches ?", FOO)
                .selectAll();

        assertThat(fooResult, hasSize(1));
        assertThat(fooResult.get(0).one, equalTo(FOO));
    }

    @Test
    public void setMatches() {
        Stream.of(FOO, "bar", "qux").forEach(string -> {
            SearchModel model = new SearchModel();
            model.one = FOO;
            model.set.add(string);
            model.list.add(FOO);
            model.map.put(FOO, FOO);
            model.save();
        });

        List<SearchModel> fooResult = Query
                .from(SearchModel.class)
                .where("set matches ?", FOO)
                .selectAll();

        assertThat(fooResult, hasSize(1));
        assertThat(fooResult.get(0).set, hasSize(1));
        assertThat(fooResult.get(0).set.iterator().next(), equalTo(FOO));
    }

    @Test
    public void listMatches() {
        Stream.of(FOO, "bar", "qux").forEach(string -> {
            SearchModel model = new SearchModel();
            model.one = FOO;
            model.set.add(FOO);
            model.list.add(string);
            model.map.put(FOO, FOO);
            model.save();
        });

        List<SearchModel> fooResult = Query
                .from(SearchModel.class)
                .where("list matches ?", FOO)
                .selectAll();

        assertThat(fooResult, hasSize(1));
        assertThat(fooResult.get(0).list, hasSize(1));
        assertThat(fooResult.get(0).list.get(0), equalTo(FOO));
    }

    @Test
    public void mapMatches() {
        Stream.of(FOO, "bar", "qux").forEach(string -> {
            SearchModel model = new SearchModel();
            model.one = FOO;
            model.set.add(FOO);
            model.list.add(FOO);
            model.map.put(string, string);
            model.save();
        });

        List<SearchModel> fooResult = Query
                .from(SearchModel.class)
                .where("map matches ?", FOO)
                .selectAll();

        assertThat(fooResult, hasSize(1));
        assertThat(fooResult.get(0).map.size(), equalTo(1));
        assertThat(fooResult.get(0).map.values().iterator().next(), equalTo(FOO));
    }

    @Test
    public void anyMatches() {
        Stream.of(FOO, "bar", "qux").forEach(string -> {
            SearchModel model = new SearchModel();
            model.one = string;
            model.set.add(FOO);
            model.save();
        });

        List<SearchModel> fooResult = Query
                .from(SearchModel.class)
                .where("_any matches ?", FOO)
                .selectAll();

        assertThat(fooResult, hasSize(3));
    }

    @Test
    public void wildcard() {
        Stream.of("f", "fo", "foo").forEach(string -> {
            SearchModel model = new SearchModel();
            model.one = string;
            model.save();
        });

        assertThat(Query.from(SearchModel.class).where("one matches ?", "f*").count(), equalTo(3L));
        assertThat(Query.from(SearchModel.class).where("one matches ?", "fo*").count(), equalTo(2L));
        assertThat(Query.from(SearchModel.class).where("one matches ?", "foo*").count(), equalTo(1L));
    }

    @Test
    public void sortRelevant() {
        IntStream.range(0, 3).forEach(i -> {
            SearchModel model = new SearchModel();
            model.one = FOO;
            model.save();
        });

        List<SearchModel> fooResult = Query
                .from(SearchModel.class)
                .where("_any matches ?", FOO)
                .sortRelevant(1.0, "_any matches ?", FOO)
                .selectAll();

        assertThat(fooResult, hasSize(3));
        assertThat(fooResult.get(0).getId().toString(), greaterThan(fooResult.get(1).getId().toString()));
        assertThat(fooResult.get(1).getId().toString(), greaterThan(fooResult.get(2).getId().toString()));
    }
}
