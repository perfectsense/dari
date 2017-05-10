package com.psddev.dari.h2;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

public class NumberIndexModel extends AbstractIndexModel<NumberIndexModel, Double> {

    @Indexed
    private Double one;

    @Indexed
    private Set<Double> set;

    @Indexed
    private List<Double> list;

    @Indexed
    private NumberIndexModel referenceOne;

    @Indexed
    private Set<NumberIndexModel> referenceSet;

    @Indexed
    private List<NumberIndexModel> referenceList;

    @Embedded
    @Indexed
    private NumberIndexModel embeddedOne;

    @Embedded
    @Indexed
    private Set<NumberIndexModel> embeddedSet;

    @Embedded
    @Indexed
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
