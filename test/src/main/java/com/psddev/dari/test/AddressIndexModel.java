package com.psddev.dari.test;

import com.psddev.dari.db.Record;

public class AddressIndexModel extends Record {
    @Indexed
    public String street;

    public String getStreet() {
        return street;
    }

    public void setStreet(String street) {
        this.street = street;
    }

}

