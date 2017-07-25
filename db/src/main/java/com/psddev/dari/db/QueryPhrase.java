package com.psddev.dari.db;

public final class QueryPhrase {

    private String phrase;
    private int proximity;
    private double weight;

    public static QueryPhraseBuilder builder() {
        return new QueryPhraseBuilder();
    }

    QueryPhrase(String phrase, int proximity, double weight) {
        this.phrase = phrase;
        this.proximity = proximity;
        this.weight = weight;
    }

    public String getPhrase() {
        return phrase;
    }

    public int getProximity() {
        return proximity;
    }

    public double getWeight() {
        return weight;
    }
}
