package com.psddev.dari.test;

import com.psddev.dari.db.Database;

public interface TestDatabase {

    public Database get();

    public void close();
}
