package com.psddev.dari.h2;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

public class UuidIndexModel extends AbstractIndexModel<UuidIndexModel, UUID> {

    @Indexed
    private UUID one;

    @Indexed
    private Set<UUID> set;

    @Indexed
    private List<UUID> list;

    @Indexed
    private UuidIndexModel referenceOne;

    @Indexed
    private Set<UuidIndexModel> referenceSet;

    @Indexed
    private List<UuidIndexModel> referenceList;

    @Embedded
    @Indexed
    private UuidIndexModel embeddedOne;

    @Embedded
    @Indexed
    private Set<UuidIndexModel> embeddedSet;

    @Embedded
    @Indexed
    private List<UuidIndexModel> embeddedList;

    @Override
    public UUID getOne() {
        return one;
    }

    @Override
    public void setOne(UUID one) {
        this.one = one;
    }

    @Override
    public Set<UUID> getSet() {
        if (set == null) {
            set = new LinkedHashSet<>();
        }
        return set;
    }

    @Override
    public void setSet(Set<UUID> set) {
        this.set = set;
    }

    @Override
    public List<UUID> getList() {
        if (list == null) {
            list = new ArrayList<>();
        }
        return list;
    }

    @Override
    public void setList(List<UUID> list) {
        this.list = list;
    }

    @Override
    public UuidIndexModel getReferenceOne() {
        return referenceOne;
    }

    @Override
    public void setReferenceOne(UuidIndexModel referenceOne) {
        this.referenceOne = referenceOne;
    }

    @Override
    public Set<UuidIndexModel> getReferenceSet() {
        if (referenceSet == null) {
            referenceSet = new LinkedHashSet<>();
        }
        return referenceSet;
    }

    @Override
    public void setReferenceSet(Set<UuidIndexModel> referenceSet) {
        this.referenceSet = referenceSet;
    }

    @Override
    public List<UuidIndexModel> getReferenceList() {
        if (referenceList == null) {
            referenceList = new ArrayList<>();
        }
        return referenceList;
    }

    @Override
    public void setReferenceList(List<UuidIndexModel> referenceList) {
        this.referenceList = referenceList;
    }

    @Override
    public UuidIndexModel getEmbeddedOne() {
        return embeddedOne;
    }

    @Override
    public void setEmbeddedOne(UuidIndexModel embeddedOne) {
        this.embeddedOne = embeddedOne;
    }

    @Override
    public Set<UuidIndexModel> getEmbeddedSet() {
        if (embeddedSet == null) {
            embeddedSet = new LinkedHashSet<>();
        }
        return embeddedSet;
    }

    @Override
    public void setEmbeddedSet(Set<UuidIndexModel> embeddedSet) {
        this.embeddedSet = embeddedSet;
    }

    @Override
    public List<UuidIndexModel> getEmbeddedList() {
        if (embeddedList == null) {
            embeddedList = new ArrayList<>();
        }
        return embeddedList;
    }

    @Override
    public void setEmbeddedList(List<UuidIndexModel> embeddedList) {
        this.embeddedList = embeddedList;
    }
}
