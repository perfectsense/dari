package com.psddev.dari.sql;

public final class SqlCompareOptions {

    private final String recordTableAlias;

    SqlCompareOptions(String recordTableAlias) {
        this.recordTableAlias = recordTableAlias;
    }

    /**
     * @return Nonnull.
     */
    public String getRecordTableAlias() {
        return recordTableAlias;
    }
}
