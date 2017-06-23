package com.psddev.dari.h2;

import com.psddev.dari.test.RegionLocationTest;
import org.junit.Test;

public class H2RegionLocationTest extends RegionLocationTest {

    @Override
    @Test(expected = IllegalArgumentException.class)
    public void testGeoLocationQueryRegionWithLocation() throws Exception {
       super.testGeoLocationQueryRegionWithLocation();
    }
}

