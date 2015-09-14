package com.psddev.dari.db;

import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

public class TriggerTest {

    private Trigger trigger;

    @Before
    public void before() {
        trigger = object -> { };
    }

    @Test
    public void isMissing() {
        assertThat(trigger.isMissing(null), equalTo(false));
    }
}
