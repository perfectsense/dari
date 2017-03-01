package com.psddev.dari.elasticsearch;

import com.psddev.dari.db.Recordable;
import com.psddev.dari.db.Region;
import com.psddev.dari.elasticsearch.AbstractElasticIndexModel;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

public class RegionElasticIndexModel extends AbstractElasticIndexModel<RegionElasticIndexModel, Region> {

    @Recordable.Indexed
    private Region one;

    @Recordable.Indexed
    private Set<Region> set;

    @Recordable.Indexed
    private List<Region> list;

    @Recordable.Indexed
    private RegionElasticIndexModel referenceOne;

    @Recordable.Indexed
    private Set<RegionElasticIndexModel> referenceSet;

    @Recordable.Indexed
    private List<RegionElasticIndexModel> referenceList = new ArrayList<>();

    @Recordable.Embedded
    @Recordable.Indexed
    private RegionElasticIndexModel embeddedOne;

    @Recordable.Embedded
    @Recordable.Indexed
    private Set<RegionElasticIndexModel> embeddedSet;

    @Recordable.Embedded
    @Recordable.Indexed
    private List<RegionElasticIndexModel> embeddedList = new ArrayList<>();

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
    public RegionElasticIndexModel getReferenceOne() {
        return referenceOne;
    }

    @Override
    public void setReferenceOne(RegionElasticIndexModel referenceOne) {
        this.referenceOne = referenceOne;
    }

    @Override
    public Set<RegionElasticIndexModel> getReferenceSet() {
        if (referenceSet == null) {
            referenceSet = new LinkedHashSet<>();
        }
        return referenceSet;
    }

    @Override
    public void setReferenceSet(Set<RegionElasticIndexModel> referenceSet) {
        this.referenceSet = referenceSet;
    }

    @Override
    public List<RegionElasticIndexModel> getReferenceList() {
        if (referenceList == null) {
            referenceList = new ArrayList<>();
        }
        return referenceList;
    }

    @Override
    public void setReferenceList(List<RegionElasticIndexModel> referenceList) {
        this.referenceList = referenceList;
    }

    @Override
    public RegionElasticIndexModel getEmbeddedOne() {
        return embeddedOne;
    }

    @Override
    public void setEmbeddedOne(RegionElasticIndexModel embeddedOne) {
        this.embeddedOne = embeddedOne;
    }

    @Override
    public Set<RegionElasticIndexModel> getEmbeddedSet() {
        if (embeddedSet == null) {
            embeddedSet = new LinkedHashSet<>();
        }
        return embeddedSet;
    }

    @Override
    public void setEmbeddedSet(Set<RegionElasticIndexModel> embeddedSet) {
        this.embeddedSet = embeddedSet;
    }

    @Override
    public List<RegionElasticIndexModel> getEmbeddedList() {
        if (embeddedList == null) {
            embeddedList = new ArrayList<>();
        }
        return embeddedList;
    }

    @Override
    public void setEmbeddedList(List<RegionElasticIndexModel> embeddedList) {
        this.embeddedList = embeddedList;
    }
}
