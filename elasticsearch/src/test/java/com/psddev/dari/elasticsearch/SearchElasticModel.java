package com.psddev.dari.elasticsearch;

import com.psddev.dari.db.Record;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class SearchElasticModel extends Record {


    @Indexed
    public String one;
    public String getOne() {
        return one;
    }
    public void setOne(String _type) {
        this.one = one;
    }

    @Indexed
    public String eid;
    public String getEid() {
        return eid;
    }
    public void setEid(String eid) {
        this.eid = eid;
    }

    @Indexed
    public Integer num;
    public Integer getNum() {
        return num;
    }
    public void setNum(Integer num) {
        this.num = num;
    }

    @Indexed
    public Float f;
    public Float getF() {
        return f;
    }
    public void setF(Float num) {
        this.f = f;
    }


    @Indexed
    public String post_date;
    public String getPostDate() {
        return post_date;
    }
    public void setPostDate(String post_date) {
        this.post_date = post_date;
    }


    @Indexed
    public String name;
    public String getName() {
        return name;
    }
    public void setName(String name) {
        this.name = name;
    }

    @Indexed
    public String guid;
    public String getGuid() {
        return guid;
    }
    public void setGuid(String guid) {
        this.guid = guid;
    }

    @Indexed
    public String message;
    public String getMessage() {
        return message;
    }
    public void setMessage(String message) {
        this.message = message;
    }

    @Indexed
    public final Set<String> set = new HashSet<>();

    @Indexed
    public final List<String> list = new ArrayList<>();

    @Indexed
    public final Map<String, String> map = new HashMap<>();

    @Indexed
    public SearchElasticModel reference;
    public SearchElasticModel getReference() {
        return reference;
    }
    public void setReference(SearchElasticModel reference) {
        this.reference = reference;
    }

}

