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

    public void setPhrase(String phrase) {
        this.phrase = phrase;
    }

    public Float getSlop() {
        return slop;
    }

    public void setSlop(Float distance) {
        this.slop = distance;
    }

    public Float getBoost() {
        return boost;
    }

    public void setBoost(Float boost) {
        this.boost = boost;
    }

}
