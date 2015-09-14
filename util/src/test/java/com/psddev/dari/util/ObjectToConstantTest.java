package com.psddev.dari.util;

import org.junit.Test;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

public class ObjectToConstantTest {

    @Test
    public void toConstant() {
        assertThat(ObjectToConstant.getInstance("foo").convert(null, null, 1), equalTo("foo"));
    }

    @Test
    public void toNull() {
        assertThat(ObjectToConstant.getInstance(null).convert(null, null, 1), equalTo(null));
    }
}
