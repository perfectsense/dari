package com.psddev.dari.h2;

import com.psddev.dari.db.AtomicOperation;
import com.psddev.dari.db.Database;
import com.psddev.dari.db.DatabaseException;
import com.psddev.dari.db.Query;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

public class WriteTest extends AbstractTest {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @After
    public void deleteModels() {
        createDeleteTestModels();
        Query.from(WriteModel.class).deleteAll();
    }

    @Test
    public void save() {
        WriteModel model = new WriteModel();
        model.save();
        assertThat(Query.from(WriteModel.class).first(), is(model));
    }

    @Test
    public void saveRetrySaved() {
        WriteModel model = new WriteModel();
        model.save();
        Query.from(WriteModel.class).deleteAll();
        model.save();
        assertThat(Query.from(WriteModel.class).first(), is(model));
    }

    @Test
    public void saveRetryNew() {
        WriteModel model1 = new WriteModel();
        model1.save();
        WriteModel model2 = new WriteModel();
        model2.getState().setId(model1.getId());
        model2.save();
        assertThat(Query.from(WriteModel.class).first(), is(model1));
    }

    @Test
    public void saveAtomicallyIncrement() {
        new WriteModel().save();
        WriteModel model1 = Query.from(WriteModel.class).first();
        WriteModel model2 = Query.from(WriteModel.class).first();

        model1.getState().incrementAtomically("number", 1);
        model1.save();
        assertThat(model1.number, is(1));

        assertThat(model2.number, is(0));
        model2.getState().incrementAtomically("number", 1);
        model2.save();
        assertThat(model2.number, is(2));

        assertThat(Query.from(WriteModel.class).first().number, is(2));
    }

    @Test
    public void saveAtomicallyDecrement() {
        WriteModel model = new WriteModel();
        model.number = 2;
        model.save();

        WriteModel model1 = Query.from(WriteModel.class).first();
        WriteModel model2 = Query.from(WriteModel.class).first();

        model1.getState().decrementAtomically("number", 1);
        model1.save();
        assertThat(model1.number, is(1));

        assertThat(model2.number, is(2));
        model2.getState().decrementAtomically("number", 1);
        model2.save();
        assertThat(model2.number, is(0));

        assertThat(Query.from(WriteModel.class).first().number, is(0));
    }

    @Test
    public void saveAtomicallyAdd() {
        new WriteModel().save();
        WriteModel model1 = Query.from(WriteModel.class).first();
        WriteModel model2 = Query.from(WriteModel.class).first();

        model1.getState().addAtomically("list", "foo");
        model1.save();
        assertThat(model1.list, hasSize(1));
        assertThat(model1.list, contains("foo"));

        assertThat(model2.list, empty());
        model2.getState().addAtomically("list", "bar");
        model2.save();
        assertThat(model2.list, hasSize(2));
        assertThat(model2.list, contains("foo", "bar"));

        List<String> list = Query.from(WriteModel.class).first().list;
        assertThat(list, hasSize(2));
        assertThat(list, contains("foo", "bar"));
    }

    @Test
    public void saveAtomicallyRemove() {
        WriteModel model = new WriteModel();
        model.list.add("foo");
        model.list.add("bar");
        model.save();

        WriteModel model1 = Query.from(WriteModel.class).first();
        WriteModel model2 = Query.from(WriteModel.class).first();

        model1.getState().removeAtomically("list", "foo");
        model1.save();
        assertThat(model1.list, hasSize(1));
        assertThat(model1.list, contains("bar"));

        assertThat(model2.list, hasSize(2));
        model2.getState().removeAtomically("list", "bar");
        model2.save();
        assertThat(model2.list, empty());

        assertThat(Query.from(WriteModel.class).first().list, empty());
    }

    @Test
    public void saveAtomicallyReplace() {
        WriteModel model = new WriteModel();
        model.string = "foo";
        model.save();

        WriteModel model1 = Query.from(WriteModel.class).first();
        WriteModel model2 = Query.from(WriteModel.class).first();

        model1.getState().replaceAtomically("string", "bar");
        model1.save();
        assertThat(model1.string, is("bar"));
        assertThat(Query.from(WriteModel.class).first().string, is("bar"));

        assertThat(model2.string, is("foo"));
        model2.getState().replaceAtomically("string", "bar");
        thrown.expect(DatabaseException.class);
        thrown.expectCause(instanceOf(AtomicOperation.ReplacementException.class));
        model2.save();
    }

    private List<WriteModel> createDeleteTestModels() {
        List<WriteModel> models = new ArrayList<>();

        for (int i = 0; i < 5; ++ i) {
            WriteModel model = new WriteModel();
            model.save();
            models.add(model);
        }

        return models;
    }

    @Test
    public void deleteFirst() {
        List<WriteModel> models = createDeleteTestModels();
        Query.from(WriteModel.class).first().delete();
        assertThat(Query.from(WriteModel.class).count(), is((long) models.size() - 1));
    }

    @Test
    public void deleteAll() {
        createDeleteTestModels();
        Query.from(WriteModel.class).deleteAll();
        assertThat(Query.from(WriteModel.class).count(), is(0L));
    }

    @Test
    public void rollback() {
        Database database = Database.Static.getDefault();

        database.beginWrites();

        try {
            new WriteModel().save();

        } finally {
            database.endWrites();
        }

        assertThat(Query.from(WriteModel.class).count(), is(0L));
    }
}
