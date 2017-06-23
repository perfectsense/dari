package com.psddev.dari.test;

import com.psddev.dari.db.Query;
import org.junit.Test;

import java.util.List;

import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertThat;

public class SingletonTest extends AbstractTest {

    @Test
    public void exists() {
        SingletonModel model = Query.from(SingletonModel.class).first();
        assertThat(model, notNullValue());
        assertThat(model.getName(), notNullValue());

        List<Object> fooResult = Query
                .fromAll()
                .where("com.psddev.dari.db.Singleton$Data/dari.singleton.key equalsany ?", "com.psddev.dari.test.SingletonModel")
                .selectAll();

        assertThat(fooResult, notNullValue());
        assertThat(fooResult, hasSize(1));

        List<Object> fooResult2 = Query
                .fromAll()
                .where("_id notequalsall ? and com.psddev.dari.db.Singleton$Data/dari.singleton.key equalsany ?", model.getId(), "com.psddev.dari.test.SingletonModel")
                .selectAll();

        assertThat(fooResult2, notNullValue());
        assertThat(fooResult2, hasSize(0));
    }
}
