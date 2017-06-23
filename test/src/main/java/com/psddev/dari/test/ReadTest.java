package com.psddev.dari.test;

import com.psddev.dari.db.Database;
import com.psddev.dari.db.Grouping;
import com.psddev.dari.db.Query;
import com.psddev.dari.sql.AbstractSqlDatabase;
import com.psddev.dari.util.PaginatedResult;
import org.apache.commons.lang3.time.DateUtils;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isIn;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertThat;

public class ReadTest extends AbstractTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(ReadTest.class);

    private static Set<ReadModel> models;

    @BeforeClass
    public static void createModels() {
        models = new HashSet<>();

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
                    models.add(model);
                }
            }
        }
    }

    @Test
    public void all() {
        assertThat(
                new HashSet<>(Query.from(ReadModel.class).selectAll()),
                is(models));
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
                is((long) models.size()));
    }

    @Test
    public void first() {
        assertThat(
                Query.from(ReadModel.class).first(),
                isIn(models));
    }

    protected void iterable(boolean disableByIdIterator, int fetchSize, boolean isOption) {
        Set<ReadModel> result = new HashSet<>();

        if (isOption) {
            Query.from(ReadModel.class)
                    .option(AbstractSqlDatabase.DISABLE_BY_ID_ITERATOR_OPTION, disableByIdIterator)
                    .iterable(0)
                    .forEach(result::add);
        } else {
            Query.from(ReadModel.class)
                    .iterable(0)
                    .forEach(result::add);
        }

        assertThat(result, is(models));
    }

    @Test
    public void iterableById0() {
        iterable(false, 0, false);
    }

    @Test
    public void iterableById1() {
        iterable(false, 1, false);
    }

    @Test
    public void iterableNotById0() {
        iterable(true, 0, false);
    }

    @Test
    public void iterableNotById1() {
        iterable(true, 1, false);
    }

    protected void iterableNext(boolean disableByIdIterator, boolean isOption) {
        Iterator<ReadModel> i;
        if (isOption) {
            i = Query
                    .from(ReadModel.class)
                    .option(AbstractSqlDatabase.DISABLE_BY_ID_ITERATOR_OPTION, disableByIdIterator)
                    .iterable(0)
                    .iterator();
        } else {
            i = Query
                    .from(ReadModel.class)
                    .iterable(0)
                    .iterator();
        }

        while (i.hasNext()) {
            i.next();
        }

        i.next();
    }

    @Test(expected = NoSuchElementException.class)
    public void iterableNextById() {
        iterableNext(false, false);
    }

    @Test(expected = NoSuchElementException.class)
    public void iterableNextNotById() {
        iterableNext(true, false);
    }

    protected void iterableRemove(boolean disableByIdIterator, boolean isOption) {
        if (isOption) {
            Query.from(ReadModel.class)
                    .option(AbstractSqlDatabase.DISABLE_BY_ID_ITERATOR_OPTION, disableByIdIterator)
                    .iterable(0)
                    .iterator()
                    .remove();
        } else {
            Query.from(ReadModel.class)
                    .iterable(0)
                    .iterator()
                    .remove();
        }
    }

    @Test(expected = UnsupportedOperationException.class)
    public void iterableRemoveById() {
        iterableRemove(false, false);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void iterableRemoveNotById() {
        iterableRemove(true, false);
    }

    @Test
    public void lastUpdate() {
        Date n = Query.from(ReadModel.class).lastUpdate();
        LOGGER.info("stored max: {} and now: {}",  n.getTime(), Database.Static.getDefault().now());
        assertThat(
                n.getTime(),
                lessThan(Database.Static.getDefault().now()));

        assertThat(
                n.getTime(),
                greaterThan(DateUtils.addMinutes(new Date(), -5).getTime()));
    }

    @Test
    public void partial() {
        PaginatedResult<ReadModel> result = Query.from(ReadModel.class).select(0, 1);

        assertThat(result.getCount(), is((long) models.size()));
        assertThat(result.getItems(), hasSize(1));
    }

    @Test
    public void partialGrouped() {
    }
}
