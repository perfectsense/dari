package com.psddev.dari.db;

public interface UpdateNotifier<T> {

    void onUpdate(T object) throws Exception;
}
