package com.psddev.dari.test;

import com.psddev.dari.db.Location;
import com.psddev.dari.db.Query;
import com.psddev.dari.db.Region;
import com.psddev.dari.util.ObjectUtils;
import org.junit.After;
import org.junit.Test;

import java.util.List;

import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class RegionLocationTest extends AbstractTest {

    @After
    public void deleteModels() {
        Query.from(SearchIndexModel.class).deleteAllImmediately();
        Query.from(RegionIndexModel.class).deleteAllImmediately();
        Query.from(LocationIndexModel.class).deleteAllImmediately();
    }

    @Test
    public void testGeoLocationQueryRegionWithRegion() throws Exception {

        RegionIndexModel search = new RegionIndexModel();
        search.setOne(Region.sphericalCircle(0.0d, 0.0d, 5d));
        search.save();

        List<RegionIndexModel> fooResult = Query.from(RegionIndexModel.class).where("one = ?", Region.sphericalCircle(0.0d, 0.0d, 5d)).selectAll();

        assertThat(fooResult, hasSize(1));

        assertThat(fooResult.get(0).getOne().getX(), is(0.0d));
        assertThat(fooResult.get(0).getOne().getY(), is(0.0d));
        assertThat(fooResult.get(0).getOne().getRadius(), is(5d));
        assertThat(fooResult.get(0).getOne().toString(), is(Region.sphericalCircle(0.0d, 0.0d, 5d).toString()));
        assertThat(ObjectUtils.toJson(fooResult.get(0).getOne()), is(ObjectUtils.toJson(Region.sphericalCircle(0.0d, 0.0d, 5d))));
    }

    @Test
    public void testGeoLocationQueryRegionWithLocation() throws Exception {

        RegionIndexModel search = new RegionIndexModel();
        search.setOne(Region.sphericalCircle(0.0d, 0.0d, 5d));
        search.save();

        List<RegionIndexModel> fooResult = Query.from(RegionIndexModel.class).where("one = ?", new Location(0d, 0d)).selectAll();

        assertThat(fooResult, hasSize(1));

        assertThat(fooResult.get(0).getOne().getX(), is(0.0d));
        assertThat(fooResult.get(0).getOne().getY(), is(0.0d));
        assertThat(fooResult.get(0).getOne().getRadius(), is(5d));
        assertThat(fooResult.get(0).getOne().toString(), is(Region.sphericalCircle(0.0d, 0.0d, 5d).toString()));
        assertThat(ObjectUtils.toJson(fooResult.get(0).getOne()), is(ObjectUtils.toJson(Region.sphericalCircle(0.0d, 0.0d, 5d))));
    }

    @Test
    public void testGeoLocationQueryLocationWithRegion() throws Exception {

        LocationIndexModel search = new LocationIndexModel();
        search.setOne(new Location(0d, 0d));
        search.save();

        List<LocationIndexModel> fooResult = Query.from(LocationIndexModel.class).where("one = ?", Region.sphericalCircle(0.0d, 0.0d, 5d)).selectAll();

        assertThat(fooResult, hasSize(1));

        assertThat(fooResult.get(0).getOne().getX(), is(0.0d));
        assertThat(fooResult.get(0).getOne().getY(), is(0.0d));
        assertThat(fooResult.get(0).getOne().toString(), is(new Location(0d, 0d).toString()));
        assertThat(ObjectUtils.toJson(fooResult.get(0).getOne()), is(ObjectUtils.toJson(new Location(0d, 0d))));
    }

    @Test
    public void testGeoLocationQueryLocationWithLocation() throws Exception {

        LocationIndexModel search = new LocationIndexModel();
        search.setOne(new Location(0d, 0d));
        search.save();

        List<LocationIndexModel> fooResult = Query.from(LocationIndexModel.class).where("one = ?", new Location(0d, 0d)).selectAll();

        assertThat(fooResult, hasSize(1));

        assertThat(fooResult.get(0).getOne().getX(), is(0.0d));
        assertThat(fooResult.get(0).getOne().getY(), is(0.0d));
        assertThat(fooResult.get(0).getOne().toString(), is(new Location(0d, 0d).toString()));
        assertThat(ObjectUtils.toJson(fooResult.get(0).getOne()), is(ObjectUtils.toJson(new Location(0d, 0d))));
    }

    @Test
    public void testGeoLocationQueryString() throws Exception {
        SearchIndexModel search = new SearchIndexModel();
        search.setName("test");
        search.save();

        List<SearchIndexModel> fooResult = Query.from(SearchIndexModel.class).where("name = ?", "test").selectAll();

        assertThat(fooResult, hasSize(1));
        assertEquals("test", fooResult.get(0).getName());
    }
}

