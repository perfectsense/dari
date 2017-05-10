package com.psddev.dari.sql;

import com.google.common.collect.ImmutableMap;
import com.psddev.dari.db.Location;
import com.psddev.dari.db.Sorter;
import org.jooq.Field;
import org.jooq.SortOrder;
import org.jooq.impl.DSL;

import java.util.List;
import java.util.Map;

@FunctionalInterface
interface SqlSorter {

    Map<String, SqlSorter> INSTANCES = ImmutableMap.<String, SqlSorter>builder()
            .put(Sorter.ASCENDING_OPERATOR, (database, join, options) -> new SqlOrder(area(database, join), SortOrder.ASC))
            .put(Sorter.DESCENDING_OPERATOR, (database, join, options) -> new SqlOrder(area(database, join), SortOrder.DESC))
            .put(Sorter.CLOSEST_OPERATOR, (database, join, options) -> new SqlOrder(distance(database, join, options), SortOrder.ASC))
            .put(Sorter.FARTHEST_OPERATOR, (database, join, options) -> new SqlOrder(distance(database, join, options), SortOrder.DESC))
            .build();

    static SqlSorter find(String operator) {
        SqlSorter sorter = INSTANCES.get(operator);

        if (sorter != null) {
            return sorter;

        } else {
            throw new UnsupportedOperationException(String.format(
                    "[%s] sorter isn't supported in SQL!",
                    operator));
        }
    }

    @SuppressWarnings("unchecked")
    static Field<Object> area(AbstractSqlDatabase database, SqlJoin join) {
        if (join.sqlIndex instanceof LocationSqlIndex) {
            throw new IllegalArgumentException();

        } else if (join.sqlIndex instanceof RegionSqlIndex) {
            return (Field) database.stArea(join.valueField);

        } else {
            return join.valueField;
        }
    }

    static Field<Double> distance(AbstractSqlDatabase database, SqlJoin join, List<Object> options) {
        if (!(join.sqlIndex instanceof LocationSqlIndex)) {
            throw new IllegalArgumentException("Can't sort by distance against non-location field!");
        }

        if (options.size() < 2) {
            throw new IllegalArgumentException();
        }

        Object option = options.get(1);

        if (!(option instanceof Location)) {
            throw new IllegalArgumentException();
        }

        return database.stLength(
                database.stMakeLine(
                        database.stGeomFromText(DSL.inline(((Location) option).toWkt())),
                        join.valueField));
    }

    SqlOrder createOrder(AbstractSqlDatabase database, SqlJoin join, List<Object> options);
}
