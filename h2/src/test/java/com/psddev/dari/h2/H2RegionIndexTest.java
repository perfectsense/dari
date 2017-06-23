package com.psddev.dari.h2;

import com.psddev.dari.test.RegionIndexTest;
import org.junit.Test;

public class H2RegionIndexTest extends RegionIndexTest {

    @Override
    @Test
    public void gtNumber() {
        createCompareTestModels();
        assertCount(total, "one > 0");
    }

    @Override
    @Test
    public void geNumber() {
        createCompareTestModels();
        assertCount(total, "one >= 0");
    }

    @Override
    @Test
    public void ltNumber() {
        createCompareTestModels();
        assertCount(1, "one < 10");
    }

    @Override
    @Test
    public void leNumber() {
        createCompareTestModels();
        assertCount(1, "one <= 10");
    }

    @Override
    @Test
    public void startsWithNull() {
    }

    @Override
    @Test
    public void ne() {
    }

    @Override
    @Test
    public void lt() {
    }

    @Override
    @Test
    public void le() {
    }

    @Override
    @Test
    public void gt() {
    }

    @Override
    @Test
    public void ge() {
    }

    @Override
    @Test
    public void eq() {
    }
}
