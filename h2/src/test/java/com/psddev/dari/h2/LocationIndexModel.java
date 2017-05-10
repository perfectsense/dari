package com.psddev.dari.h2;

import com.psddev.dari.db.Location;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

public class LocationIndexModel extends AbstractIndexModel<LocationIndexModel, Location> {

    @Indexed
    private Location one;

    @Indexed
    private Set<Location> set;

    @Indexed
    private List<Location> list;

    @Indexed
    private LocationIndexModel referenceOne;

    @Indexed
    private Set<LocationIndexModel> referenceSet;

    @Indexed
    private List<LocationIndexModel> referenceList;

    @Embedded
    @Indexed
    private LocationIndexModel embeddedOne;

    @Embedded
    @Indexed
    private Set<LocationIndexModel> embeddedSet;

    @Embedded
    @Indexed
    private List<LocationIndexModel> embeddedList;

    @Override
    public Location getOne() {
        return one;
    }

    @Override
    public void setOne(Location one) {
        this.one = one;
    }

    @Override
    public Set<Location> getSet() {
        if (set == null) {
            set = new LinkedHashSet<>();
        }
        return set;
    }

    @Override
    public void setSet(Set<Location> set) {
        this.set = set;
    }

    @Override
    public List<Location> getList() {
        if (list == null) {
            list = new ArrayList<>();
        }
        return list;
    }

    @Override
    public void setList(List<Location> list) {
        this.list = list;
    }

    @Override
    public LocationIndexModel getReferenceOne() {
        return referenceOne;
    }

    @Override
    public void setReferenceOne(LocationIndexModel referenceOne) {
        this.referenceOne = referenceOne;
    }

    @Override
    public Set<LocationIndexModel> getReferenceSet() {
        if (referenceSet == null) {
            referenceSet = new LinkedHashSet<>();
        }
        return referenceSet;
    }

    @Override
    public void setReferenceSet(Set<LocationIndexModel> referenceSet) {
        this.referenceSet = referenceSet;
    }

    @Override
    public List<LocationIndexModel> getReferenceList() {
        if (referenceList == null) {
            referenceList = new ArrayList<>();
        }
        return referenceList;
    }

    @Override
    public void setReferenceList(List<LocationIndexModel> referenceList) {
        this.referenceList = referenceList;
    }

    @Override
    public LocationIndexModel getEmbeddedOne() {
        return embeddedOne;
    }

    @Override
    public void setEmbeddedOne(LocationIndexModel embeddedOne) {
        this.embeddedOne = embeddedOne;
    }

    @Override
    public Set<LocationIndexModel> getEmbeddedSet() {
        if (embeddedSet == null) {
            embeddedSet = new LinkedHashSet<>();
        }
        return embeddedSet;
    }

    @Override
    public void setEmbeddedSet(Set<LocationIndexModel> embeddedSet) {
        this.embeddedSet = embeddedSet;
    }

    @Override
    public List<LocationIndexModel> getEmbeddedList() {
        if (embeddedList == null) {
            embeddedList = new ArrayList<>();
        }
        return embeddedList;
    }

    @Override
    public void setEmbeddedList(List<LocationIndexModel> embeddedList) {
        this.embeddedList = embeddedList;
    }
}
