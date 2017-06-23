package com.psddev.dari.db;

public class QueryPhrase {

    private String phrase = null;
    private Float slop = null;
    private Float boost = null;

    public QueryPhrase() {
    }

    public QueryPhrase(String phrase) {
        this.phrase = phrase;
    }

    public String getPhrase() {
        return phrase;
    }

    public QueryPhrase setPhrase(String phrase) {
        this.phrase = phrase;
        return this;
    }

    public Float getSlop() {
        return slop;
    }

    public QueryPhrase setSlop(Float distance) {
        this.slop = distance;
        return this;
    }

    public Float getBoost() {
        return boost;
    }

    public QueryPhrase setBoost(Float boost) {
        this.boost = boost;
        return this;
    }

}
