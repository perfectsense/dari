package com.psddev.dari.h2;

import com.psddev.dari.db.ComparisonPredicate;
import com.psddev.dari.db.PredicateParser;
import com.psddev.dari.db.Sorter;
import com.psddev.dari.sql.AbstractSqlDatabase;
import com.psddev.dari.sql.SqlCompareOptions;
import com.psddev.dari.sql.SqlDatabaseException;
import com.psddev.dari.sql.SqlSortOptions;
import com.psddev.dari.util.ObjectUtils;
import org.jooq.Condition;
import org.jooq.Converter;
import org.jooq.DSLContext;
import org.jooq.DataType;
import org.jooq.Field;
import org.jooq.Operator;
import org.jooq.Record;
import org.jooq.SQLDialect;
import org.jooq.SortField;
import org.jooq.Table;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Locale;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class H2Database extends AbstractSqlDatabase {

    private static final DataType<String> STRING_INDEX_TYPE = SQLDataType.VARCHAR.asConvertedDataType(new Converter<String, String>() {

        private static final int MAX_STRING_INDEX_TYPE_LENGTH = 500;

        @Override
        public String from(String string) {
            return string;
        }

        @Override
        public String to(String string) {
            return string != null && string.length() > MAX_STRING_INDEX_TYPE_LENGTH
                    ? string.substring(0, MAX_STRING_INDEX_TYPE_LENGTH)
                    : string;
        }

        @Override
        public Class<String> fromType() {
            return String.class;
        }

        @Override
        public Class<String> toType() {
            return String.class;
        }
    });

    private static final Table<Record> FT_WORDS_TABLE = DSL.table(DSL.name("FT", "WORDS"));
    private static final Field<String> FT_WORDS_NAME_FIELD = DSL.field(DSL.name("NAME"), String.class);

    @Override
    protected SQLDialect getDialect() {
        return SQLDialect.H2;
    }

    @Override
    protected DataType<String> stringIndexType() {
        return STRING_INDEX_TYPE;
    }

    @Override
    protected void setUp() {
        if (isIndexSpatial()) {
            Connection connection = openConnection();

            try {
                org.h2gis.ext.H2GISExtension.load(connection);

            } catch (SQLException error) {
                throw new SqlDatabaseException(this, "Can't load H2 GIS extension!", error);

            } finally {
                closeConnection(connection);
            }
        }

        super.setUp();
    }

    @Override
    protected String getSetUpResourcePath() {
        return "schema-12.sql";
    }

    @Override
    protected Condition compare(ComparisonPredicate comparison, SqlCompareOptions options) {
        String operator = comparison.getOperator();
        Operator compoundOperator;

        if (PredicateParser.MATCHES_ANY_OPERATOR.equals(operator)
                || PredicateParser.MATCHES_EXACT_ANY_OPERATOR.equals(operator)) {

            compoundOperator = Operator.OR;

        } else if (PredicateParser.MATCHES_ALL_OPERATOR.equals(operator)
                || PredicateParser.MATCHES_EXACT_ALL_OPERATOR.equals(operator)) {

            compoundOperator = Operator.AND;

        } else {
            return super.compare(comparison, options);
        }

        String key = comparison.getKey();
        int lastSlashAt = key.lastIndexOf('/');
        String fieldName = lastSlashAt > -1 ? key.substring(lastSlashAt + 1) : key;
        String recordTableAlias = options.getRecordTableAlias();
        Field<UUID> aliasedRecordIdField = DSL.field(DSL.name(recordTableAlias, recordIdField.getName()), uuidType());

        boolean exact = PredicateParser.MATCHES_EXACT_ANY_OPERATOR.equals(operator)
                || PredicateParser.MATCHES_EXACT_ALL_OPERATOR.equals(operator);

        return DSL.condition(
                compoundOperator,
                comparison.getValues().stream()
                        .filter(value -> !ObjectUtils.isBlank(value))
                        .map(Object::toString)

                        .flatMap(value -> {
                            if (!exact && value.length() > 1) {
                                value += "*";
                            }

                            if (value.contains("*") && value.length() > 1) {
                                Connection connection = openConnection();

                                try (DSLContext context = DSL.using(connection, getDialect())) {
                                    return context.select(FT_WORDS_NAME_FIELD)
                                            .from(FT_WORDS_TABLE)
                                            .where(FT_WORDS_NAME_FIELD.like(value
                                                    .replace("%", "\\%")
                                                    .replace("_", "\\_")
                                                    .replace("*", "%")
                                                    .toUpperCase(Locale.ENGLISH) + "%"))
                                            .fetch(FT_WORDS_NAME_FIELD)
                                            .stream();

                                } finally {
                                    closeConnection(connection);
                                }
                            }

                            return Stream.of(value);
                        })

                        .map(value -> "*".equals(value)
                                ? DSL.trueCondition()
                                : aliasedRecordIdField.in(
                                        DSL.select(DSL.field("KEYS[1]", uuidType()))
                                                .from(DSL.table("FT_SEARCH_DATA(?, 0, 0)", value))
                                                .where(DSL.field(DSL.name("TABLE"), String.class).eq(SearchUpdateTrigger.TABLE.getName()))
                                                .and(DSL.field("KEYS[0]", String.class).eq(fieldName))))

                        .collect(Collectors.toList()));
    }

    @Override
    protected SortField<?> sort(Sorter sorter, SqlSortOptions options) {
        if (Sorter.RELEVANT_OPERATOR.equals(sorter.getOperator())) {
            return DSL.field(DSL.name(options.getRecordTableAlias(), recordIdField.getName()), uuidType()).desc();

        } else {
            return super.sort(sorter, options);
        }
    }
}
