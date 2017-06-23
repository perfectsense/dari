package com.psddev.dari.test;

import com.psddev.dari.db.Record;

public class PersonIndexModel extends Record {
    @Indexed
    public String personName;

    @Indexed
    public AddressIndexModel address;

    public AddressIndexModel getAddress() {
        return address;
    }

    public void setAddress(AddressIndexModel address) {
        this.address = address;
    }

    public String getPersonName() {
        return personName;
    }

    public void setPersonName(String personName) {
        this.personName = personName;
    }

}

