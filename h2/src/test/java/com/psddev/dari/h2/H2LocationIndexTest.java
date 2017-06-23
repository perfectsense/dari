package com.psddev.dari.h2;

import com.psddev.dari.db.Region;
import com.psddev.dari.test.LocationIndexTest;
import org.junit.Test;

public class H2LocationIndexTest extends LocationIndexTest {

    // H2 bug 5.5 degrees is only 611km, from 0,0 to 5,5 is 750km
    @Override
    @Test
    public void eqRegion() {
        createCompareTestModels();
        assertCount(5, "one = ?", Region.sphericalCircle(0.0d, 0.0d, 5.5d));
    }

    // H2 bug 5.5 degrees is only 611km, from 0,0 to 5,5 is 750km
    @Override
    @Test
    public void eqRegionNotIn() {
        createCompareTestModels();
        assertCount(0, "one != ?", Region.sphericalCircle(0.0d, 0.0d, 5.5d));
    }

}
