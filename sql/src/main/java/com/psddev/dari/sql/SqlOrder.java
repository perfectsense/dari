package com.psddev.dari.sql;

import org.jooq.Field;
import org.jooq.SortField;
import org.jooq.SortOrder;

class SqlOrder {

    public final Field<?> field;
    public final SortField<?> sortField;

    public SqlOrder(Field<?> field, SortOrder order) {
        this.field = field;
        this.sortField = field.sort(order);
    }
}
