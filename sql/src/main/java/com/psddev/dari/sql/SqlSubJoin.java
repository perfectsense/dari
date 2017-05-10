package com.psddev.dari.sql;

import com.psddev.dari.db.Query;
import org.jooq.Condition;
import org.jooq.Field;
import org.jooq.Table;
import org.jooq.impl.DSL;

import java.util.List;

final class SqlSubJoin {

    public final SqlQuery sqlQuery;
    public final Table<?> table;
    public final Condition on;

    public static SqlSubJoin create(
            SqlQuery parent,
            Query<?> subQuery,
            boolean forceLeftJoins,
            SqlJoin join) {

        List<SqlSubJoin> subJoins = parent.subJoins;

        SqlQuery sub = new SqlQuery(
                parent.database,
                subQuery,
                parent.aliasPrefix + "s" + subJoins.size());

        sub.forceLeftJoins = forceLeftJoins;

        SqlSubJoin subJoin = new SqlSubJoin(parent, sub, join);

        subJoins.add(subJoin);

        return subJoin;
    }

    private SqlSubJoin(SqlQuery parent, SqlQuery sub, SqlJoin join) {
        this.sqlQuery = sub;

        AbstractSqlDatabase database = sub.database;
        String alias = sub.recordTableAlias;
        Field<?> id = DSL.field(DSL.name(alias, database.recordIdField.getName()), database.uuidType());

        this.table = sub.initialize(DSL.table(DSL.name(database.recordTable.getName())).as(alias));
        this.on = join.valueField.eq(id);

        if (sub.needsDistinct) {
            parent.needsDistinct = true;
        }
    }
}
