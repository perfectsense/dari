package com.psddev.dari.db;

@FunctionalInterface
public interface StateValueAdapter<S, T> {

    T adapt(S source);
}
