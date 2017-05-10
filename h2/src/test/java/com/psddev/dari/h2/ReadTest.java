package com.psddev.dari.h2;

import com.psddev.dari.db.Database;
import com.psddev.dari.db.Grouping;
import com.psddev.dari.db.Query;
import com.psddev.dari.sql.AbstractSqlDatabase;
import com.psddev.dari.util.PaginatedResult;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

public class ReadTest extends AbstractTest {

    private static Set<ReadModel> MODELS;

    @BeforeClass
    public static void createModels() {
        MODELS = new HashSet<>();

        // b
        // cb, b
        // dcb, dc, d
        // ...
        for (int i = 0; i < 26; ++ i) {
            for (int j = 0; j < i; ++ j) {
                ReadModel model = new ReadModel();
                StringBuilder text = new StringBuilder();

                for (int k = i; k > j; -- k) {
                    text.append((char) ('a' + k));
                }

                if (text.length() > 0) {
                    model.text = text.toString();
                    model.save();
                    MODELS.add(model);
                }
            }
        }
    }

    @Test
    public void all() {
        assertThat(
                new HashSet<>(Query.from(ReadModel.class).selectAll()),
                is(MODELS));
    }

    @Test
    public void allGroupedOne() {
        List<Grouping<ReadModel>> groupings = Query.from(ReadModel.class).groupBy("firstLetter");

        assertThat(
                groupings,
                hasSize(25));

        groupings.forEach(g -> {
            String firstLetter = (String) g.getKeys().get(0);

            assertThat(
                    firstLetter,
                    g.getCount(),
                    is((long) (firstLetter.charAt(0) - 'a')));
        });
    }

    @Test
    public void allGroupedSet() {
        List<Grouping<ReadModel>> groupings = Query.from(ReadModel.class).groupBy("letters");

        assertThat(
                groupings,
                hasSize(25));

        groupings.forEach(g -> {
            String letter = (String) g.getKeys().get(0);
            long count = 0;

            for (int i = 0, l = letter.charAt(0) - 'a', j = 25; i < l; ++ i, j -= 2) {
                count += j;
            }

            assertThat(letter, g.getCount(), is(count));
        });
    }

    @Test
    public void count() {
        assertThat(
                Query.from(ReadModel.class).count(),
                is((long) MODELS.size()));
    }

    @Test
    public void first() {
        assertThat(
                Query.from(ReadModel.class).first(),
                isIn(MODELS));
    }

    private void iterable(boolean disableByIdIterator, int fetchSize) {
        Set<ReadModel> result = new HashSet<>();

        Query.from(ReadModel.class)
                .option(AbstractSqlDatabase.DISABLE_BY_ID_ITERATOR_OPTION, disableByIdIterator)
                .iterable(0)
                .forEach(result::add);

        assertThat(result, is(MODELS));
    }

    @Test
    public void iterableById0() {
        iterable(false, 0);
    }

    @Test
    public void iterableById1() {
        iterable(false, 1);
    }

    @Test
    public void iterableNotById0() {
        iterable(true, 0);
    }

    @Test
    public void iterableNotById1() {
        iterable(true, 1);
    }

    private void iterableNext(boolean disableByIdIterator) {
        Iterator<ReadModel> i = Query
                .from(ReadModel.class)
                .option(AbstractSqlDatabase.DISABLE_BY_ID_ITERATOR_OPTION, disableByIdIterator)
                .iterable(0)
                .iterator();

        while (i.hasNext()) {
            i.next();
        }

        i.next();
    }

    @Test(expected = NoSuchElementException.class)
    public void iterableNextById() {
        iterableNext(false);
    }

    @Test(expected = NoSuchElementException.class)
    public void iterableNextNotById() {
        iterableNext(true);
    }

    private void iterableRemove(boolean disableByIdIterator) {
        Query.from(ReadModel.class)
                .option(AbstractSqlDatabase.DISABLE_BY_ID_ITERATOR_OPTION, disableByIdIterator)
                .iterable(0)
                .iterator()
                .remove();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void iterableRemoveById() {
        iterableRemove(false);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void iterableRemoveNotById() {
        iterableRemove(true);
    }

    @Test
    public void lastUpdate() {
        assertThat(
                Query.from(ReadModel.class).lastUpdate().getTime(),
                lessThan(Database.Static.getDefault().now()));
    }

    @Test
    public void partial() {
        PaginatedResult<ReadModel> result = Query.from(ReadModel.class).select(0, 1);

        assertThat(result.getCount(), is((long) MODELS.size()));
        assertThat(result.getItems(), hasSize(1));
    }

    @Test
    public void partialGrouped() {
    }
}
