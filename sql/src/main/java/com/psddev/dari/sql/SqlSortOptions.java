package com.psddev.dari.sql;

public final class SqlSortOptions {

    private final String recordTableAlias;

    SqlSortOptions(String recordTableAlias) {
        this.recordTableAlias = recordTableAlias;
    }

    /**
     * @return Nonnull.
     */
    public String getRecordTableAlias() {
        return recordTableAlias;
    }
}
