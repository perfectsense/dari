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
    public String _id;
    public String get_id() {
        return _id;
    }
    public void set_id(String _id) {
        this._id = _id;
    }

    @Indexed
    public String _type;
    public String get_type() {
        return _type;
    }
    public void set_type(String _type) {
        this._type = _type;
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
    public String post_date;
    public String getPostDate() {
        return eid;
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
        return eid;
    }
    public void setGuid(String guid) {
        this.guid = guid;
    }

    @Indexed
    public String message;
    public String getMessage() {
        return eid;
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
}

