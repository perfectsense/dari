package com.psddev.dari.db;

import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

public class TriggerTest {

    private Trigger trigger;

    @Before
    public void before() {
        trigger = new Trigger() {
            @Override
            public void execute(Object object) {
            }

            @Override
            public boolean isMissing(Class<?> objectClass) {
                return false;
            }
        };
    }

    @Test
    public void isMissing() {
        assertThat(trigger.isMissing(null), equalTo(false));
    }
}
