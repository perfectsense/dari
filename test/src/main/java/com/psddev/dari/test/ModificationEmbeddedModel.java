package com.psddev.dari.test;

import com.psddev.dari.db.Record;

public class ModificationEmbeddedModel extends Record implements
        TaggableEmbedded {

    private String name;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}
