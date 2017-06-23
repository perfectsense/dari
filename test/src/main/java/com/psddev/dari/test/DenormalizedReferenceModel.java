package com.psddev.dari.test;

import com.psddev.dari.db.Record;
import com.psddev.dari.db.Recordable;

@Recordable.FieldInternalNamePrefix("taggable.")
public class DenormalizedReferenceModel extends Record {

    private String name;

    @Indexed
    @Denormalized
    private IndexTag indexedTag;

    @Denormalized
    private IndexTag unindexedTag;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public IndexTag getIndexedTag() {
        return indexedTag;
    }

    public void setIndexedTag(IndexTag indexedTag) {
        this.indexedTag = indexedTag;
    }

    public IndexTag getUnindexedTag() {
        return unindexedTag;
    }

    public void setUnindexedTag(IndexTag unindexedTag) {
        this.unindexedTag = unindexedTag;
    }
}
