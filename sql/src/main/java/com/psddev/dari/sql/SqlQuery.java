package com.psddev.dari.sql;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.google.common.base.Preconditions;
import com.psddev.dari.db.ComparisonPredicate;
import com.psddev.dari.db.CompoundPredicate;
import com.psddev.dari.db.Location;
import com.psddev.dari.db.ObjectField;
import com.psddev.dari.db.ObjectIndex;
import com.psddev.dari.db.Predicate;
import com.psddev.dari.db.PredicateParser;
import com.psddev.dari.db.Query;
import com.psddev.dari.db.QueryPhrase;
import com.psddev.dari.db.Region;
import com.psddev.dari.db.Sorter;
import com.psddev.dari.db.SqlDatabase;
import com.psddev.dari.db.UnsupportedPredicateException;

import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.DataType;
import org.jooq.Field;
import org.jooq.JoinType;
import org.jooq.RenderContext;
import org.jooq.Select;
import org.jooq.SelectField;
import org.jooq.SortField;
import org.jooq.Table;
import org.jooq.conf.ParamType;
import org.jooq.impl.DSL;

class SqlQuery {

    public static final String COUNT_ALIAS = "_count";

    protected final AbstractSqlDatabase database;
    protected final Query<?> query;
    protected final String aliasPrefix;

    private final DSLContext dslContext;
    private final RenderContext tableRenderContext;
    protected final RenderContext renderContext;

    protected final String recordTableAlias;
    private final Table<?> recordTable;
    protected final Field<UUID> recordIdField;
    protected final Field<UUID> recordTypeIdField;
    protected final Map<String, Query.MappedKey> mappedKeys;
    protected final Map<String, ObjectIndex> selectedIndexes;

    private Condition whereCondition;
    private final List<SqlOrder> orders = new ArrayList<>();
    private final List<SortField<?>> orderByFields = new ArrayList<>();
    protected final List<SqlJoin> joins = new ArrayList<>();
    protected final List<SqlSubJoin> subJoins = new ArrayList<>();

    protected boolean needsDistinct;
    protected boolean forceLeftJoins;

    /**
     * Creates an instance that can translate the given {@code query}
     * with the given {@code database}.
     */
    public SqlQuery(AbstractSqlDatabase database, Query<?> query, String aliasPrefix) {
        this.database = database;
        this.query = query;
        this.aliasPrefix = aliasPrefix;

        dslContext = DSL.using(database.getDialect());
        tableRenderContext = dslContext.renderContext().paramType(ParamType.INLINED).declareTables(true);
        renderContext = dslContext.renderContext().paramType(ParamType.INLINED);

        recordTableAlias = aliasPrefix + "r";
        recordTable = DSL.table(DSL.name(database.recordTable.getName())).as(recordTableAlias);
        recordIdField = DSL.field(DSL.name(recordTableAlias, database.recordIdField.getName()), database.uuidType());
        recordTypeIdField = DSL.field(DSL.name(recordTableAlias, database.recordTypeIdField.getName()), database.uuidType());
        mappedKeys = query.mapEmbeddedKeys(database.getEnvironment());
        selectedIndexes = new HashMap<>();

        for (Map.Entry<String, Query.MappedKey> entry : mappedKeys.entrySet()) {
            selectIndex(entry.getKey(), entry.getValue());
        }
    }

    private void selectIndex(String queryKey, Query.MappedKey mappedKey) {
        ObjectIndex selectedIndex = null;
        int maxMatchCount = 0;

        for (ObjectIndex index : mappedKey.getIndexes()) {
            List<String> indexFields = index.getFields();
            int matchCount = 0;

            for (Query.MappedKey mk : mappedKeys.values()) {
                ObjectField mkf = mk.getField();
                if (mkf != null && indexFields.contains(mkf.getInternalName())) {
                    ++ matchCount;
                }
            }

            if (matchCount > maxMatchCount) {
                selectedIndex = index;
                maxMatchCount = matchCount;
            }
        }

        if (selectedIndex != null) {
            if (maxMatchCount == 1) {
                for (ObjectIndex index : mappedKey.getIndexes()) {
                    if (index.getFields().size() == 1) {
                        selectedIndex = index;
                        break;
                    }
                }
            }

            selectedIndexes.put(queryKey, selectedIndex);
        }
    }

    public SqlQuery(AbstractSqlDatabase database, Query<?> query) {
        this(database, query, "");
    }

    protected Field<Object> aliasedField(String alias, String field) {
        return field != null ? DSL.field(DSL.name(aliasPrefix + alias, field)) : null;
    }

    protected Table<?> initialize(Table<?> table) {

        // Build the WHERE clause.
        whereCondition = query.isFromAll()
                ? DSL.trueCondition()
                : recordTypeIdField.in(query.getConcreteTypeIds(database));

        Predicate predicate = query.getPredicate();

        if (predicate != null) {
            Condition condition = createWhereCondition(predicate, false);

            if (condition != null) {
                whereCondition = whereCondition.and(condition);
            }
        }

        // Creates jOOQ SortField from Dari Sorter.
        for (Sorter sorter : query.getSorters()) {
            SortField<?> sortField = database.sort(sorter, new SqlSortOptions(recordTableAlias));

            if (sortField != null) {
                orderByFields.add(sortField);
                continue;
            }

            SqlSorter sqlSorter = SqlSorter.find(sorter.getOperator());
            String queryKey = (String) sorter.getOptions().get(0);
            SqlJoin join = SqlJoin.findOrCreate(this, queryKey);

            join.useLeftOuter();

            Query<?> subQuery = mappedKeys.get(queryKey).getSubQueryWithSorter(sorter, 0);

            if (subQuery != null) {
                orders.addAll(
                        SqlSubJoin.create(this, subQuery, true, join)
                                .sqlQuery
                                .orders);

            } else {
                orders.add(
                        sqlSorter.createOrder(
                                database,
                                join,
                                sorter.getOptions()));
            }
        }

        orders.forEach(o -> orderByFields.add(o.sortField));

        // Join all index tables used so far.
        for (SqlJoin join : joins) {
            if (!join.symbolIds.isEmpty()) {
                table = table.join(join.table, forceLeftJoins || join.isLeftOuter() ? JoinType.LEFT_OUTER_JOIN : JoinType.JOIN)
                        .on(join.idField.eq(recordIdField))
                        .and(join.typeIdField.eq(recordTypeIdField))
                        .and(join.symbolIdField.in(join.symbolIds));
            }
        }

        // Join all index tables used in sub-queries.
        for (SqlSubJoin subJoin : subJoins) {
            table = table.join(subJoin.table).on(subJoin.on);
        }

        return table;
    }

    // Creates jOOQ Condition from Dari Predicate.
    private Condition createWhereCondition(Predicate predicate, boolean usesLeftJoin) {
        if (predicate instanceof CompoundPredicate) {
            CompoundPredicate compoundPredicate = (CompoundPredicate) predicate;
            String operator = compoundPredicate.getOperator();
            boolean isNot = PredicateParser.NOT_OPERATOR.equals(operator);

            // e.g. (child1) OR (child2) OR ... (child#)
            if (isNot || PredicateParser.OR_OPERATOR.equals(operator)) {
                List<Predicate> children = compoundPredicate.getChildren();
                boolean usesLeftJoinChildren;

                if (children.size() > 1) {
                    usesLeftJoinChildren = true;
                    needsDistinct = true;

                } else {
                    usesLeftJoinChildren = isNot;
                }

                Condition compoundCondition = null;

                for (Predicate child : children) {
                    Condition childCondition = createWhereCondition(child, usesLeftJoinChildren);

                    if (childCondition != null) {
                        compoundCondition = compoundCondition != null
                                ? compoundCondition.or(childCondition)
                                : childCondition;
                    }
                }

                return isNot && compoundCondition != null
                        ? compoundCondition.not()
                        : compoundCondition;

            // e.g. (child1) AND (child2) AND .... (child#)
            } else if (PredicateParser.AND_OPERATOR.equals(operator)) {
                Condition compoundCondition = null;

                for (Predicate child : compoundPredicate.getChildren()) {
                    Condition childCondition = createWhereCondition(child, usesLeftJoin);

                    if (childCondition != null) {
                        compoundCondition = compoundCondition != null
                                ? compoundCondition.and(childCondition)
                                : childCondition;
                    }
                }

                return compoundCondition;
            }

        } else if (predicate instanceof ComparisonPredicate) {
            ComparisonPredicate comparisonPredicate = (ComparisonPredicate) predicate;
            Condition condition = database.compare(comparisonPredicate, new SqlCompareOptions(recordTableAlias));

            if (condition != null) {
                return condition;
            }

            String queryKey = comparisonPredicate.getKey();
            Query.MappedKey mappedKey = mappedKeys.get(queryKey);
            boolean isFieldCollection = mappedKey.isInternalCollectionType();
            SqlJoin join = isFieldCollection
                    ? SqlJoin.create(this, queryKey)
                    : SqlJoin.findOrCreate(this, queryKey);

            if (usesLeftJoin) {
                join.useLeftOuter();
            }

            if (isFieldCollection && join.sqlIndex == null) {
                needsDistinct = true;
            }

            Query<?> valueQuery = mappedKey.getSubQueryWithComparison(comparisonPredicate);
            String operator = comparisonPredicate.getOperator();
            boolean isNotEqualsAll = PredicateParser.NOT_EQUALS_ALL_OPERATOR.equals(operator);

            // e.g. field IN (SELECT ...)
            if (valueQuery != null) {
                if (isNotEqualsAll || isFieldCollection) {
                    needsDistinct = true;
                }

                Select<?> subSelect = findSubSelect(mappedKey.getField(), query.getPredicate(), valueQuery);

                if (subSelect != null) {
                    return isNotEqualsAll
                            ? join.valueField.notIn(subSelect)
                            : join.valueField.in(subSelect);

                } else {
                    return SqlSubJoin.create(this, valueQuery, join.isLeftOuter(), join)
                            .sqlQuery
                            .whereCondition;
                }
            }

            List<Condition> comparisonConditions = new ArrayList<>();
            boolean hasMissing = false;

            if (isNotEqualsAll || PredicateParser.EQUALS_ANY_OPERATOR.equals(operator)) {
                List<Object> inValues = new ArrayList<>();

                for (Object value : comparisonPredicate.resolveValues(database)) {
                    if (value == null) {
                        comparisonConditions.add(DSL.falseCondition());

                    } else if (value instanceof QueryPhrase) {
                        throw new UnsupportedOperationException();

                    } else if (value == Query.MISSING_VALUE) {
                        hasMissing = true;

                        if (isNotEqualsAll) {
                            if (isFieldCollection) {
                                needsDistinct = true;
                            }

                            comparisonConditions.add(join.valueField.isNotNull());

                        } else {
                            join.useLeftOuter();
                            comparisonConditions.add(join.valueField.isNull());
                        }

                    } else if (join.sqlIndex instanceof LocationSqlIndex && !(value instanceof Location)) {
                        if (!(value instanceof Region)) {
                            throw new IllegalArgumentException();
                        }

                        Condition contains = database.stContains(
                                database.stGeomFromText(DSL.inline(((Region) value).toWkt())),
                                join.valueField);

                        comparisonConditions.add(isNotEqualsAll
                                ? contains.not()
                                : contains);

                    } else if (join.sqlIndex instanceof RegionSqlIndex && !(value instanceof Region)) {
                        throw new IllegalArgumentException();

                    } else if (isNotEqualsAll) {
                        needsDistinct = true;
                        hasMissing = true;

                        join.useLeftOuter();
                        comparisonConditions.add(
                                join.valueField.isNull().or(
                                        join.valueField.ne(join.value(value))));

                    } else {
                        inValues.add(join.value(value));
                    }
                }

                if (!inValues.isEmpty()) {
                    comparisonConditions.add(join.valueField.in(inValues));
                }

            } else {
                SqlComparison sqlComparison = SqlComparison.find(database, join, operator);

                // e.g. field OP value1 OR field OP value2 OR ... field OP value#
                for (Object value : comparisonPredicate.resolveValues(database)) {
                    if (value == null) {
                        comparisonConditions.add(DSL.falseCondition());

                    } else if (value == Query.MISSING_VALUE) {
                        throw new IllegalArgumentException();

                    } else {
                        comparisonConditions.add(sqlComparison.createCondition(value));
                    }
                }
            }

            if (comparisonConditions.isEmpty()) {
                return isNotEqualsAll ? DSL.trueCondition() : DSL.falseCondition();
            }

            Condition whereCondition = isNotEqualsAll
                    ? DSL.and(comparisonConditions)
                    : DSL.or(comparisonConditions);

            if (!hasMissing) {
                if (join.needsIndexTable) {
                    String indexKey = mappedKeys.get(queryKey).getIndexKey(selectedIndexes.get(queryKey));

                    if (indexKey != null) {
                        whereCondition = join.symbolIdField.eq(database.findSymbolId(indexKey, false)).and(whereCondition);
                    }
                }

                if (join.needsIsNotNull) {
                    whereCondition = join.valueField.isNotNull().and(whereCondition);
                }

                if (comparisonConditions.size() > 1) {
                    needsDistinct = true;
                }
            }

            return whereCondition;
        }

        throw new UnsupportedPredicateException(this, predicate);
    }

    private Select<?> findSubSelect(ObjectField field, Predicate predicate, Query<?> subQuery) {
        if (field != null) {
            if (predicate instanceof CompoundPredicate) {
                for (Predicate child : ((CompoundPredicate) predicate).getChildren()) {
                    Select<?> subSelect = findSubSelect(field, child, subQuery);

                    if (subSelect != null) {
                        return subSelect;
                    }
                }

            } else if (predicate instanceof ComparisonPredicate) {
                ComparisonPredicate comparison = (ComparisonPredicate) predicate;
                Query.MappedKey mappedKey = mappedKeys.get(comparison.getKey());

                if (field.equals(mappedKey.getField())
                        && mappedKey.getSubQueryWithComparison(comparison) == null) {

                    SqlQuery subSqlQuery = new SqlQuery(database, subQuery);
                    Table<?> subTable = subSqlQuery.initialize(recordTable);

                    return (subSqlQuery.needsDistinct
                            ? subSqlQuery.dslContext.selectDistinct(subSqlQuery.recordIdField)
                            : subSqlQuery.dslContext.select(subSqlQuery.recordIdField))
                            .from(subTable)
                            .where(subSqlQuery.whereCondition);
                }
            }
        }

        return null;
    }

    /**
     * Returns an SQL statement that can be used to get a count
     * of all rows matching the query.
     */
    public String countStatement() {
        Table<?> table = initialize(recordTable);

        return tableRenderContext.render(dslContext
                .select(needsDistinct ? recordIdField.countDistinct() : recordIdField.count())
                .from(table)
                .where(whereCondition));
    }

    /**
     * Returns an SQL statement that can be used to group rows by the values
     * of the given {@code groupKeys}.
     *
     * @param groupKeys Can't be {@code null} or empty.
     * @throws IllegalArgumentException If {@code groupKeys} is empty.
     * @throws NullPointerException If {@code groupKeys} is {@code null}.
     */
    public String groupStatement(String... groupKeys) {
        Preconditions.checkNotNull(groupKeys, "[groupKeys] can't be null!");
        Preconditions.checkArgument(groupKeys.length > 0, "[groupKeys] can't be empty!");

        List<Field<?>> groupByFields = new ArrayList<>();

        for (String groupKey : groupKeys) {
            Query.MappedKey mappedKey = query.mapEmbeddedKey(database.getEnvironment(), groupKey);

            mappedKeys.put(groupKey, mappedKey);
            selectIndex(groupKey, mappedKey);

            SqlJoin join = SqlJoin.findOrCreate(this, groupKey);
            Query<?> subQuery = mappedKey.getSubQueryWithGroupBy();

            if (subQuery == null) {
                groupByFields.add(join.valueField);

            } else {
                SqlSubJoin.create(this, subQuery, true, join)
                        .sqlQuery
                        .joins
                        .forEach(j -> groupByFields.add(j.valueField));
            }
        }

        Table<?> table = initialize(recordTable);
        List<Field<?>> selectFields = new ArrayList<>();

        selectFields.add((needsDistinct
                ? recordIdField.countDistinct()
                : recordIdField.count())
                .as(COUNT_ALIAS));

        selectFields.addAll(groupByFields);

        return tableRenderContext.render(dslContext
                .select(selectFields)
                .from(table)
                .where(whereCondition)
                .groupBy(groupByFields)
                .orderBy(orderByFields));
    }

    /**
     * Returns an SQL statement that can be used to get when the rows
     * matching the query were last updated.
     */
    public String lastUpdateStatement() {
        Table<?> table = initialize(DSL.table(DSL.name(database.recordUpdateTable.getName())).as(recordTableAlias));

        return tableRenderContext.render(dslContext
                .select(DSL.field(DSL.name(recordTableAlias, database.recordUpdateDateField.getName())).max())
                .from(table)
                .where(whereCondition));
    }

    /**
     * Returns an an SQL statement that can be used to list a subset of rows
     * matching the query.
     */
    public String select(int offset, int limit) {
        Table<?> table = initialize(recordTable);
        List<SelectField<?>> selectFields = new ArrayList<>();

        selectFields.add(recordIdField);
        selectFields.add(recordTypeIdField);

        boolean referenceOnly = query.isReferenceOnly();

        if (!referenceOnly) {
            selectFields.add(DSL.field(DSL.name(recordTableAlias, SqlDatabase.DATA_COLUMN)));
        }

        Select<?> select;

        if (needsDistinct) {
            List<SelectField<?>> distinctFields = new ArrayList<>();

            distinctFields.add(recordIdField);
            distinctFields.add(recordTypeIdField);

            for (int i = 0, size = orders.size(); i < size; ++ i) {
                distinctFields.add(orders.get(i).field.as("o" + i));
            }

            select = dslContext
                    .selectDistinct(distinctFields)
                    .from(table)
                    .where(whereCondition)
                    .orderBy(orderByFields)
                    .offset(offset)
                    .limit(limit);

            if (!referenceOnly) {
                String distinctAlias = aliasPrefix + "d";
                DataType<UUID> uuidType = database.uuidType();
                select = dslContext
                        .select(selectFields)
                        .from(recordTable)
                        .join(select.asTable().as(distinctAlias))
                        .on(recordTypeIdField.eq(DSL.field(DSL.name(distinctAlias, recordTypeIdField.getName()), uuidType)))
                        .and(recordIdField.eq(DSL.field(DSL.name(distinctAlias, recordIdField.getName()), uuidType)));
            }

        } else {
            select = dslContext
                    .select(selectFields)
                    .from(table)
                    .where(whereCondition)
                    .orderBy(orderByFields)
                    .offset(offset)
                    .limit(limit);
        }

        return tableRenderContext.render(select);
    }
}
