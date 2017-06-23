package com.psddev.dari.test;

import com.psddev.dari.db.Record;
import com.psddev.dari.db.Recordable;
import com.psddev.dari.util.StringUtils;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Recordable.FieldInternalNamePrefix("taggable.")
public class MethodComplexModel extends Record {

    private String givenName;
    private String surname;

    public String getGivenName() {
        return givenName;
    }

    public void setGivenName(String givenName) {
        this.givenName = givenName;
    }

    public String getSurname() {
        return surname;
    }

    public void setSurname(String surname) {
        this.surname = surname;
    }

    @Indexed
    public String getFullName() {
        return givenName + " " + surname;
    }

    @Indexed
    public List<String> getNames() {
        List<String> names = new ArrayList<>(2);
        if (!StringUtils.isBlank(givenName)) {
            names.add(givenName);
        }
        if (!StringUtils.isBlank(surname)) {
            names.add(surname);
        }
        return names;
    }

    @Indexed
    public Set<String> getSetNames() {
        Set<String> names = new HashSet<>();
        if (!StringUtils.isBlank(givenName)) {
            names.add(givenName);
        }
        if (!StringUtils.isBlank(surname)) {
            names.add(surname);
        }
        return names;
    }
}
