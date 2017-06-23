package com.psddev.dari.test;

import com.psddev.dari.db.Record;

public class ModificationDenormalizedModel extends Record implements
        TaggableDenormalized {

    private String name;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}
