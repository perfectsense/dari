package com.psddev.dari.elasticsearch;

import com.psddev.dari.db.Query;
import com.psddev.dari.test.AbstractTest;
import com.psddev.dari.test.WriteModel;
import org.junit.After;
import org.junit.AssumptionViolatedException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.Stopwatch;
import org.junit.runner.Description;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertThat;

public class ElasticWriteTest extends AbstractTest {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Rule
    public final Stopwatch stopwatch = new Stopwatch() {
        protected void succeeded(long nanos, Description description) {
            System.out.println(description.getMethodName() + " succeeded, time taken " + nanos);
        }

        /**
         * Invoked when a test fails
         */
        protected void failed(long nanos, Throwable e, Description description) {
            System.out.println(description.getMethodName() + " failed, time taken " + nanos);
        }

        /**
         * Invoked when a test is skipped due to a failed assumption.
         */
        protected void skipped(long nanos, AssumptionViolatedException e,
                               Description description) {
            System.out.println(description.getMethodName() + " skipped, time taken " + nanos);
        }

        /**
         * Invoked when a test method finishes (whether passing or failing)
         */
        protected void finished(long nanos, Description description) {
            System.out.println(description.getMethodName() + " finished, time taken " + nanos);
        }

    };

    @After
    public void deleteModels() {
        createDeleteTestModels();
        Query.from(WriteModel.class).deleteAll();
    }

    @Test
    public void perfTestEventually() {
        for (int i = 0; i < 2000; i++) {
            new WriteModel().saveEventually();
        }
        assertThat("After perfTestEventually",  stopwatch.runtime(TimeUnit.SECONDS), lessThan(10L));

    }

    @Test
    public void perfTest() {
        for (int i = 0; i < 2000; i++) {
            new WriteModel().save();
        }
        assertThat("After perfTest",  stopwatch.runtime(TimeUnit.SECONDS), greaterThan(4L));
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
}
