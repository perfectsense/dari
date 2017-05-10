package com.psddev.dari.h2;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

public class StringIndexModel extends AbstractIndexModel<StringIndexModel, String> {

    @Indexed
    private String one;

    @Indexed
    private Set<String> set;

    @Indexed
    private List<String> list;

    @Indexed
    private StringIndexModel referenceOne;

    @Indexed
    private Set<StringIndexModel> referenceSet;

    @Indexed
    private List<StringIndexModel> referenceList;

    @Embedded
    @Indexed
    private StringIndexModel embeddedOne;

    @Embedded
    @Indexed
    private Set<StringIndexModel> embeddedSet;

    @Embedded
    @Indexed
    private List<StringIndexModel> embeddedList;

    @Override
    public String getOne() {
        return one;
    }

    @Override
    public void setOne(String one) {
        this.one = one;
    }

    @Override
    public Set<String> getSet() {
        if (set == null) {
            set = new LinkedHashSet<>();
        }
        return set;
    }

    @Override
    public void setSet(Set<String> set) {
        this.set = set;
    }

    @Override
    public List<String> getList() {
        if (list == null) {
            list = new ArrayList<>();
        }
        return list;
    }

    @Override
    public void setList(List<String> list) {
        this.list = list;
    }

    @Override
    public StringIndexModel getReferenceOne() {
        return referenceOne;
    }

    @Override
    public void setReferenceOne(StringIndexModel referenceOne) {
        this.referenceOne = referenceOne;
    }

    @Override
    public Set<StringIndexModel> getReferenceSet() {
        if (referenceSet == null) {
            referenceSet = new LinkedHashSet<>();
        }
        return referenceSet;
    }

    @Override
    public void setReferenceSet(Set<StringIndexModel> referenceSet) {
        this.referenceSet = referenceSet;
    }

    @Override
    public List<StringIndexModel> getReferenceList() {
        if (referenceList == null) {
            referenceList = new ArrayList<>();
        }
        return referenceList;
    }

    @Override
    public void setReferenceList(List<StringIndexModel> referenceList) {
        this.referenceList = referenceList;
    }

    @Override
    public StringIndexModel getEmbeddedOne() {
        return embeddedOne;
    }

    @Override
    public void setEmbeddedOne(StringIndexModel embeddedOne) {
        this.embeddedOne = embeddedOne;
    }

    @Override
    public Set<StringIndexModel> getEmbeddedSet() {
        if (embeddedSet == null) {
            embeddedSet = new LinkedHashSet<>();
        }
        return embeddedSet;
    }

    @Override
    public void setEmbeddedSet(Set<StringIndexModel> embeddedSet) {
        this.embeddedSet = embeddedSet;
    }

    @Override
    public List<StringIndexModel> getEmbeddedList() {
        if (embeddedList == null) {
            embeddedList = new ArrayList<>();
        }
        return embeddedList;
    }

    @Override
    public void setEmbeddedList(List<StringIndexModel> embeddedList) {
        this.embeddedList = embeddedList;
    }
}
