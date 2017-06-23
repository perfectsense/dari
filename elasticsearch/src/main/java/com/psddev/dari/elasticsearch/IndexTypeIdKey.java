package com.psddev.dari.elasticsearch;

import com.google.common.base.MoreObjects;

import java.util.UUID;

class IndexTypeIdKey {
    private UUID indexTypeId;
    private String name;

    public UUID getIndexTypeId() {
        return indexTypeId;
    }

    public void setIndexTypeId(UUID indexTypeId) {
        this.indexTypeId = indexTypeId;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public String toString() {

        return MoreObjects.toStringHelper(this)
                .add("indexTypeId", indexTypeId)
                .add("name", name)
                .toString();

    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        IndexTypeIdKey indexKey = (IndexTypeIdKey) o;

        return indexTypeId.equals(indexKey.indexTypeId) && name.equals(indexKey.name);
    }

    @Override
    public int hashCode() {
        return indexTypeId.hashCode();
    }
}
