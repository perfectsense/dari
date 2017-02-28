package com.psddev.dari.elasticsearch;

import com.psddev.dari.db.Location;
import com.psddev.dari.db.Recordable;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

public class LocationElasticIndexModel extends AbstractElasticIndexModel<LocationElasticIndexModel, Location> {

    @Recordable.Indexed
    private Location one;

    @Recordable.Indexed
    private Set<Location> set;

    @Recordable.Indexed
    private List<Location> list;

    @Recordable.Indexed
    private LocationElasticIndexModel referenceOne;

    @Recordable.Indexed
    private Set<LocationElasticIndexModel> referenceSet;

    @Recordable.Indexed
    private List<LocationElasticIndexModel> referenceList;

    @Recordable.Embedded
    @Recordable.Indexed
    private LocationElasticIndexModel embeddedOne;

    @Recordable.Embedded
    @Recordable.Indexed
    private Set<LocationElasticIndexModel> embeddedSet;

    @Recordable.Embedded
    @Recordable.Indexed
    private List<LocationElasticIndexModel> embeddedList;

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
    public LocationElasticIndexModel getReferenceOne() {
        return referenceOne;
    }

    @Override
    public void setReferenceOne(LocationElasticIndexModel referenceOne) {
        this.referenceOne = referenceOne;
    }

    @Override
    public Set<LocationElasticIndexModel> getReferenceSet() {
        if (referenceSet == null) {
            referenceSet = new LinkedHashSet<>();
        }
        return referenceSet;
    }

    @Override
    public void setReferenceSet(Set<LocationElasticIndexModel> referenceSet) {
        this.referenceSet = referenceSet;
    }

    @Override
    public List<LocationElasticIndexModel> getReferenceList() {
        if (referenceList == null) {
            referenceList = new ArrayList<>();
        }
        return referenceList;
    }

    @Override
    public void setReferenceList(List<LocationElasticIndexModel> referenceList) {
        this.referenceList = referenceList;
    }

    @Override
    public LocationElasticIndexModel getEmbeddedOne() {
        return embeddedOne;
    }

    @Override
    public void setEmbeddedOne(LocationElasticIndexModel embeddedOne) {
        this.embeddedOne = embeddedOne;
    }

    @Override
    public Set<LocationElasticIndexModel> getEmbeddedSet() {
        if (embeddedSet == null) {
            embeddedSet = new LinkedHashSet<>();
        }
        return embeddedSet;
    }

    @Override
    public void setEmbeddedSet(Set<LocationElasticIndexModel> embeddedSet) {
        this.embeddedSet = embeddedSet;
    }

    @Override
    public List<LocationElasticIndexModel> getEmbeddedList() {
        if (embeddedList == null) {
            embeddedList = new ArrayList<>();
        }
        return embeddedList;
    }

    @Override
    public void setEmbeddedList(List<LocationElasticIndexModel> embeddedList) {
        this.embeddedList = embeddedList;
    }
}
