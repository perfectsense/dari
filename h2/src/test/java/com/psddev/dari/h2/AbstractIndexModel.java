package com.psddev.dari.h2;

import com.psddev.dari.db.Record;

import java.util.List;
import java.util.Set;

public abstract class AbstractIndexModel<M extends AbstractIndexModel<M, T>, T> extends Record {

    public abstract T getOne();

    public abstract void setOne(T one);

    public abstract Set<T> getSet();

    public abstract void setSet(Set<T> set);

    public abstract List<T> getList();

    public abstract void setList(List<T> list);

    public abstract M getReferenceOne();

    public abstract void setReferenceOne(M referenceOne);

    public abstract Set<M> getReferenceSet();

    public abstract void setReferenceSet(Set<M> referenceSet);

    public abstract List<M> getReferenceList();

    public abstract void setReferenceList(List<M> referenceList);

    public abstract M getEmbeddedOne();

    public abstract void setEmbeddedOne(M embeddedOne);

    public abstract Set<M> getEmbeddedSet();

    public abstract void setEmbeddedSet(Set<M> embeddedSet);

    public abstract List<M> getEmbeddedList();

    public abstract void setEmbeddedList(List<M> embeddedList);
}
