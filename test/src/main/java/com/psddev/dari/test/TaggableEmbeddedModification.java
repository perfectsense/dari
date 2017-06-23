package com.psddev.dari.test;

import com.psddev.dari.db.Modification;
import com.psddev.dari.db.Recordable;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

//@Modification.Classes({ModificationEmbeddedModel.class})
@Recordable.FieldInternalNamePrefix("tgd.")
public class TaggableEmbeddedModification extends Modification<TaggableEmbedded> {

    String name;

    @Indexed
    @Embedded
    private IndexTag primaryTag;

    @Indexed
    @Embedded
    private List<IndexTag> otherTags;

    @Indexed
    @Embedded
    private Set<IndexTag> otherTagsSet;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public IndexTag getPrimaryTag() {
        return primaryTag;
    }

    public void setPrimaryTag(IndexTag primaryTag) {
        this.primaryTag = primaryTag;
    }

    public List<IndexTag> getOtherTags() {
        if (otherTags == null) {
            otherTags = new ArrayList<>();
        }
        return otherTags;
    }

    public void setOtherTags(List<IndexTag> otherTags) {
        this.otherTags = otherTags;
    }

    public Set<IndexTag> getOtherTagsSet() {
        if (otherTagsSet == null) {
            otherTagsSet = new HashSet<>();
        }
        return otherTagsSet;
    }

    public void setOtherTagsSet(Set<IndexTag> otherTagsSet) {
        this.otherTagsSet = otherTagsSet;
    }
}
