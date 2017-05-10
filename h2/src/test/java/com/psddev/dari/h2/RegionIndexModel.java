package com.psddev.dari.h2;

import com.psddev.dari.db.Region;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

public class RegionIndexModel extends AbstractIndexModel<RegionIndexModel, Region> {

    @Indexed
    private Region one;

    @Indexed
    private Set<Region> set;

    @Indexed
    private List<Region> list;

    @Indexed
    private RegionIndexModel referenceOne;

    @Indexed
    private Set<RegionIndexModel> referenceSet;

    @Indexed
    private List<RegionIndexModel> referenceList = new ArrayList<>();

    @Embedded
    @Indexed
    private RegionIndexModel embeddedOne;

    @Embedded
    @Indexed
    private Set<RegionIndexModel> embeddedSet;

    @Embedded
    @Indexed
    private List<RegionIndexModel> embeddedList = new ArrayList<>();

    @Override
    public Region getOne() {
        return one;
    }

    @Override
    public void setOne(Region one) {
        this.one = one;
    }

    @Override
    public Set<Region> getSet() {
        if (set == null) {
            set = new LinkedHashSet<>();
        }
        return set;
    }

    @Override
    public void setSet(Set<Region> set) {
        this.set = set;
    }

    @Override
    public List<Region> getList() {
        if (list == null) {
            list = new ArrayList<>();
        }
        return list;
    }

    @Override
    public void setList(List<Region> list) {
        this.list = list;
    }

    @Override
    public RegionIndexModel getReferenceOne() {
        return referenceOne;
    }

    @Override
    public void setReferenceOne(RegionIndexModel referenceOne) {
        this.referenceOne = referenceOne;
    }

    @Override
    public Set<RegionIndexModel> getReferenceSet() {
        if (referenceSet == null) {
            referenceSet = new LinkedHashSet<>();
        }
        return referenceSet;
    }

    @Override
    public void setReferenceSet(Set<RegionIndexModel> referenceSet) {
        this.referenceSet = referenceSet;
    }

    @Override
    public List<RegionIndexModel> getReferenceList() {
        if (referenceList == null) {
            referenceList = new ArrayList<>();
        }
        return referenceList;
    }

    @Override
    public void setReferenceList(List<RegionIndexModel> referenceList) {
        this.referenceList = referenceList;
    }

    @Override
    public RegionIndexModel getEmbeddedOne() {
        return embeddedOne;
    }

    @Override
    public void setEmbeddedOne(RegionIndexModel embeddedOne) {
        this.embeddedOne = embeddedOne;
    }

    @Override
    public Set<RegionIndexModel> getEmbeddedSet() {
        if (embeddedSet == null) {
            embeddedSet = new LinkedHashSet<>();
        }
        return embeddedSet;
    }

    @Override
    public void setEmbeddedSet(Set<RegionIndexModel> embeddedSet) {
        this.embeddedSet = embeddedSet;
    }

    @Override
    public List<RegionIndexModel> getEmbeddedList() {
        if (embeddedList == null) {
            embeddedList = new ArrayList<>();
        }
        return embeddedList;
    }

    @Override
    public void setEmbeddedList(List<RegionIndexModel> embeddedList) {
        this.embeddedList = embeddedList;
    }
}
