package com.psddev.dari.h2;

import com.psddev.dari.db.Query;
import org.junit.Test;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

public class SingletonTest extends AbstractTest {

    @Test
    public void exists() {
        SingletonModel model = Query.from(SingletonModel.class).first();
        assertThat(model, notNullValue());
        assertThat(model.getName(), notNullValue());
    }
}
