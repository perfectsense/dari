package com.psddev.dari.sql;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.psddev.dari.db.Location;
import com.psddev.dari.db.PredicateParser;
import com.psddev.dari.db.Region;
import com.psddev.dari.util.ObjectUtils;
import org.jooq.Condition;
import org.jooq.Field;
import org.jooq.Param;
import org.jooq.impl.DSL;

import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;

@FunctionalInterface
interface SqlComparison {

    Set<String> SUPPORTED_OPERATORS = ImmutableSet.of(
            PredicateParser.CONTAINS_OPERATOR,
            PredicateParser.STARTS_WITH_OPERATOR,
            PredicateParser.LESS_THAN_OPERATOR,
            PredicateParser.LESS_THAN_OR_EQUALS_OPERATOR,
            PredicateParser.GREATER_THAN_OPERATOR,
            PredicateParser.GREATER_THAN_OR_EQUALS_OPERATOR);

    Map<String, BiFunction<Field<Object>, Object, Condition>> COMPARE_FUNCTIONS = ImmutableMap.of(
            PredicateParser.LESS_THAN_OPERATOR, Field::lt,
            PredicateParser.LESS_THAN_OR_EQUALS_OPERATOR, Field::le,
            PredicateParser.GREATER_THAN_OPERATOR, Field::gt,
            PredicateParser.GREATER_THAN_OR_EQUALS_OPERATOR, Field::ge);

    @SuppressWarnings("unchecked")
    static SqlComparison find(AbstractSqlDatabase database, SqlJoin join, String operator) {
        if (!SUPPORTED_OPERATORS.contains(operator)) {
            throw new UnsupportedOperationException(String.format(
                    "[%s] comparison isn't supported in SQL!",
                    operator));
        }

        AbstractSqlIndex sqlIndex = join.sqlIndex;

        if (sqlIndex instanceof LocationSqlIndex) {
            throw new IllegalArgumentException();
        }

        if (PredicateParser.CONTAINS_OPERATOR.equals(operator)) {
            if (sqlIndex instanceof RegionSqlIndex) {
                return (value) -> {
                    String wkt;

                    if (value instanceof Location) {
                        wkt = ((Location) value).toWkt();

                    } else if (value instanceof Region) {
                        wkt = ((Region) value).toWkt();

                    } else {
                        throw new IllegalArgumentException();
                    }

                    return database.stContains(
                            join.valueField,
                            database.stGeomFromText(DSL.inline(wkt, String.class)));
                };

            } else if (sqlIndex instanceof StringSqlIndex) {
                return (value) -> join.valueField.like((Param) join.value("%" + value + "%"));

            } else {
                throw new IllegalArgumentException();
            }
        }

        if (PredicateParser.STARTS_WITH_OPERATOR.equals(operator)) {
            if (sqlIndex instanceof StringSqlIndex) {
                return (value) -> join.valueField.like((Param) join.value(value + "%"));

            } else {
                throw new IllegalArgumentException();
            }
        }

        BiFunction<Field<Object>, Object, Condition> compareFunction = COMPARE_FUNCTIONS.get(operator);

        if (sqlIndex instanceof RegionSqlIndex) {
            return (value) -> {
                if (value instanceof Region) {
                    return compareFunction.apply(
                            (Field) database.stArea(join.valueField),
                            database.stArea(
                                    database.stGeomFromText(
                                            DSL.inline(((Region) value).toWkt(), String.class))));

                } else {
                    Double valueDouble = ObjectUtils.to(Double.class, value);

                    if (valueDouble == null) {
                        throw new IllegalArgumentException();
                    }

                    return compareFunction.apply(
                            (Field) database.stArea(join.valueField),
                            DSL.inline(valueDouble, double.class));
                }
            };

        } else {
            return (value) -> compareFunction.apply(
                    join.valueField,
                    join.value(value));
        }
    }

    Condition createCondition(Object value);
}
