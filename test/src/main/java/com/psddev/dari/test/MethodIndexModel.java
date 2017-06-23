package com.psddev.dari.test;

import com.psddev.dari.db.Record;

public class MethodIndexModel extends Record {

    @Required
    private String name;

    @Indexed
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Indexed
    private String title;

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    private String prefix;

    public String getPrefix() {
        return prefix;
    }

    public void setPrefix(String prefix) {
        this.prefix = prefix;
    }

    @Indexed
    public String getFoo() {
        return "Foo";
    }

    @Indexed
    @InternalName("taggable.getFoo2")
    public String getFoo2() {
        return "Foo2";
    }

    @Indexed
    public String getInfo() {
        return "This type of string is larger";
    }

    @Indexed
    public String getNameFirstLetter() {
        return getName().substring(0, 1);
    }

    @Indexed
    public String getPrefixName() {
        return getPrefix() + getName();
    }

    @Override
    protected void beforeSave() {
        if (prefix == null) {
            setPrefix("default");
        }
    }
}
