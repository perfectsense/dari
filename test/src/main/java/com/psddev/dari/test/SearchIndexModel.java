package com.psddev.dari.test;

import com.psddev.dari.db.Record;
import com.psddev.dari.db.Recordable;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

public class SearchIndexModel extends Record {

    @Indexed
    public String one;
    public String getOne() {
        return one;
    }
    public void setOne(String one) {
        this.one = one;
    }

    @Indexed
    public String neverIndexed;
    public String getNeverIndexed() {
        return neverIndexed;
    }
    public void setNeverIndexed(String neverIndexed) {
        this.neverIndexed = neverIndexed;
    }

    public String notAnnIndexed;
    public String getNotAnnIndexed() {
        return notAnnIndexed;
    }
    public void setNotAnnIndexed(String notAnnIndexed) {
        this.notAnnIndexed = notAnnIndexed;
    }

    @Indexed
    public String notAnyIndexed;
    public String getNotAnyIndexed() {
        return notAnyIndexed;
    }
    public void setNotAnyIndexed(String notAnyIndexed) {
        this.notAnyIndexed = notAnyIndexed;
    }

    @Indexed
    public String eid;
    public String getEid() {
        return eid;
    }
    public void setEid(String eid) {
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

    @Indexed
    public Float f;
    public Float getF() {
        return f;
    }
    public void setF(Float f) {
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
    public UUID idBox;
    public UUID getIdBox() {
        return idBox;
    }
    public void setIdBox(UUID idBox) {
        this.idBox = idBox;
    }

    @Indexed
    public Boolean isOn;
    public Boolean getIsOn() {
        return isOn;
    }
    public void setIsOn(Boolean isOn) {
        this.isOn = isOn;
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

    @Recordable.Embedded
    @Recordable.Indexed
    public final SearchObjectModel loginTokens = new SearchObjectModel();

    @Indexed
    public SearchIndexModel reference;
    public SearchIndexModel getReference() {
        return reference;
    }
    public void setReference(SearchIndexModel reference) {
        this.reference = reference;
    }

    @Indexed
    public PersonIndexModel personReference;
    public PersonIndexModel getPersonReference() {
        return personReference;
    }
    public void setPersonReference(PersonIndexModel personReference) {
        this.personReference = personReference;
    }

    @Embedded
    @Indexed
    public PersonIndexModel personEmbedded;

}

