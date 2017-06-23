package com.psddev.dari.test;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

public class SearchArticleIndexModel extends AbstractIndexModel<SearchArticleIndexModel, String> {

    @Indexed
    private String one;

    // to test not indexed
    private String notIndexed;

    @Indexed
    private Set<String> set;

    @Indexed
    private List<String> list;

    @Indexed
    private SearchArticleIndexModel referenceOne;

    @Indexed
    private Set<SearchArticleIndexModel> referenceSet;

    @Indexed
    private List<SearchArticleIndexModel> referenceList;

    @Embedded
    @Indexed
    private SearchArticleIndexModel embeddedOne;

    @Embedded
    @Indexed
    private Set<SearchArticleIndexModel> embeddedSet;

    @Embedded
    @Indexed
    private List<SearchArticleIndexModel> embeddedList;

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
    public SearchArticleIndexModel getReferenceOne() {
        return referenceOne;
    }

    @Override
    public void setReferenceOne(SearchArticleIndexModel referenceOne) {
        this.referenceOne = referenceOne;
    }

    @Override
    public Set<SearchArticleIndexModel> getReferenceSet() {
        if (referenceSet == null) {
            referenceSet = new LinkedHashSet<>();
        }
        return referenceSet;
    }

    @Override
    public void setReferenceSet(Set<SearchArticleIndexModel> referenceSet) {
        this.referenceSet = referenceSet;
    }

    @Override
    public List<SearchArticleIndexModel> getReferenceList() {
        if (referenceList == null) {
            referenceList = new ArrayList<>();
        }
        return referenceList;
    }

    @Override
    public void setReferenceList(List<SearchArticleIndexModel> referenceList) {
        this.referenceList = referenceList;
    }

    @Override
    public SearchArticleIndexModel getEmbeddedOne() {
        return embeddedOne;
    }

    @Override
    public void setEmbeddedOne(SearchArticleIndexModel embeddedOne) {
        this.embeddedOne = embeddedOne;
    }

    @Override
    public Set<SearchArticleIndexModel> getEmbeddedSet() {
        if (embeddedSet == null) {
            embeddedSet = new LinkedHashSet<>();
        }
        return embeddedSet;
    }

    @Override
    public void setEmbeddedSet(Set<SearchArticleIndexModel> embeddedSet) {
        this.embeddedSet = embeddedSet;
    }

    @Override
    public List<SearchArticleIndexModel> getEmbeddedList() {
        if (embeddedList == null) {
            embeddedList = new ArrayList<>();
        }
        return embeddedList;
    }

    @Override
    public void setEmbeddedList(List<SearchArticleIndexModel> embeddedList) {
        this.embeddedList = embeddedList;
    }
}
