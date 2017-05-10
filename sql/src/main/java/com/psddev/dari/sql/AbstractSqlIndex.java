package com.psddev.dari.sql;

import com.psddev.dari.db.ObjectIndex;
import org.jooq.DataType;
import org.jooq.Field;
import org.jooq.Param;
import org.jooq.Record;
import org.jooq.Table;
import org.jooq.impl.DSL;

import java.util.Map;
import java.util.UUID;

abstract class AbstractSqlIndex {

    protected final AbstractSqlDatabase database;
    public final Table<Record> table;
    public final Field<UUID> idField;
    public final Param<UUID> idParam;
    public final Field<UUID> typeIdField;
    public final Param<UUID> typeIdParam;
    public final Field<Integer> symbolIdField;
    public final Param<Integer> symbolIdParam;
    public final Field<Object> valueField;

    protected AbstractSqlIndex(AbstractSqlDatabase database, String namePrefix, int version) {
        DataType<Integer> integerType = database.integerType();
        DataType<UUID> uuidType = database.uuidType();

        this.database = database;
        this.table = DSL.table(DSL.name(namePrefix + version));
        this.idField = DSL.field(DSL.name("id"), uuidType);
        this.idParam = DSL.param(idField.getName(), uuidType);
        this.typeIdField = DSL.field(DSL.name("typeId"), uuidType);
        this.typeIdParam = DSL.param(typeIdField.getName(), uuidType);
        this.symbolIdField = DSL.field(DSL.name("symbolId"), integerType);
        this.symbolIdParam = DSL.param(symbolIdField.getName(), integerType);
        this.valueField = DSL.field(DSL.name("value"));
    }

    public abstract Object valueParam();

    public abstract Map<String, Object> valueBindValues(ObjectIndex index, Object value);

    public abstract Object valueInline(ObjectIndex index, Object value);
}
