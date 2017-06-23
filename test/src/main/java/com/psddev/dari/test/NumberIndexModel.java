package com.psddev.dari.test;

import com.psddev.dari.db.Recordable;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

public class NumberIndexModel extends AbstractIndexModel<NumberIndexModel, Double> {

    @Recordable.Indexed
    private Double one;

    @Recordable.Indexed
    private Set<Double> set;

    @Recordable.Indexed
    private List<Double> list;

    @Recordable.Indexed
    private NumberIndexModel referenceOne;

    @Recordable.Indexed
    private Set<NumberIndexModel> referenceSet;

    @Recordable.Indexed
    private List<NumberIndexModel> referenceList;

    @Recordable.Embedded
    @Recordable.Indexed
    private NumberIndexModel embeddedOne;

    @Recordable.Embedded
    @Recordable.Indexed
    private Set<NumberIndexModel> embeddedSet;

    @Recordable.Embedded
    @Recordable.Indexed
    private List<NumberIndexModel> embeddedList;

    @Override
    public Double getOne() {
        return one;
    }

    @Override
    public void setOne(Double one) {
        this.one = one;
    }

    @Override
    public Set<Double> getSet() {
        if (set == null) {
            set = new LinkedHashSet<>();
        }
        return set;
    }

    @Override
    public void setSet(Set<Double> set) {
        this.set = set;
    }

    @Override
    public List<Double> getList() {
        if (list == null) {
            list = new ArrayList<>();
        }
        return list;
    }

    @Override
    public void setList(List<Double> list) {
        this.list = list;
    }

    @Override
    public NumberIndexModel getReferenceOne() {
        return referenceOne;
    }

    @Override
    public void setReferenceOne(NumberIndexModel referenceOne) {
        this.referenceOne = referenceOne;
    }

    @Override
    public Set<NumberIndexModel> getReferenceSet() {
        if (referenceSet == null) {
            referenceSet = new LinkedHashSet<>();
        }
        return referenceSet;
    }

    @Override
    public void setReferenceSet(Set<NumberIndexModel> referenceSet) {
        this.referenceSet = referenceSet;
    }

    @Override
    public List<NumberIndexModel> getReferenceList() {
        if (referenceList == null) {
            referenceList = new ArrayList<>();
        }
        return referenceList;
    }

    @Override
    public void setReferenceList(List<NumberIndexModel> referenceList) {
        this.referenceList = referenceList;
    }

    @Override
    public NumberIndexModel getEmbeddedOne() {
        return embeddedOne;
    }

    @Override
    public void setEmbeddedOne(NumberIndexModel embeddedOne) {
        this.embeddedOne = embeddedOne;
    }

    @Override
    public Set<NumberIndexModel> getEmbeddedSet() {
        if (embeddedSet == null) {
            embeddedSet = new LinkedHashSet<>();
        }
        return embeddedSet;
    }

    @Override
    public void setEmbeddedSet(Set<NumberIndexModel> embeddedSet) {
        this.embeddedSet = embeddedSet;
    }

    @Override
    public List<NumberIndexModel> getEmbeddedList() {
        if (embeddedList == null) {
            embeddedList = new ArrayList<>();
        }
        return embeddedList;
    }

    @Override
    public void setEmbeddedList(List<NumberIndexModel> embeddedList) {
        this.embeddedList = embeddedList;
    }
}
