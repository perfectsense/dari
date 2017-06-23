package com.psddev.dari.test;

import com.psddev.dari.db.Record;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Switch the types from SearchElasticModel
 * This looks weird on purpose - Elastic types cannot overlap
 */
public class SearchOverlapModel extends Record {

    @Indexed
    public Float one;
    public Float getOne() {
        return one;
    }
    public void setOne(Float one) {
        this.one = one;
    }

    @Indexed
    public Float eid;
    public Float getEid() {
        return eid;
    }
    public void setEid(Float eid) {
        this.eid = eid;
    }

    // these are Number checks
    @Indexed
    public Integer num;
    public Integer getNum() {
        return num;
    }
    public void setNum(Integer num) {
        this.num = num;
    }

    @Indexed
    public Byte b;
    public Byte getB() {
        return b;
    }
    public void setB(Byte b) {
        this.b = b;
    }

    @Indexed
    public Double d;
    public Double getD() {
        return d;
    }
    public void setD(Double d) {
        this.d = d;
    }

    // this is a string for testing
    @Indexed
    public String f;
    public String getF() {
        return f;
    }
    public void setF(String f) {
        this.f = f;
    }

    @Indexed
    public Long l;
    public Long getL() {
        return l;
    }
    public void setL(Long l) {
        this.l = l;
    }

    @Indexed
    public Short shortType;
    public Short getShortType() {
        return shortType;
    }
    public void setShortType(Short shortType) {
        this.shortType = shortType;
    }

    @Indexed
    public Date postDate;
    public Date getPostDate() {
        return postDate;
    }
    public void setPostDate(Date postDate) {
        this.postDate = postDate;
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
    public final Set<Float> set = new HashSet<>();

    @Indexed
    public final List<Float> list = new ArrayList<>();

    @Indexed
    public final Map<Float, Float> map = new HashMap<>();

    @Indexed
    public SearchOverlapModel reference;
    public SearchOverlapModel getReference() {
        return reference;
    }
    public void setReference(SearchOverlapModel reference) {
        this.reference = reference;
    }

}

